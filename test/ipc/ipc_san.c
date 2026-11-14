/*
 * Sanitizer harness for memcached/ipc.c: the rings, the slots, the eventfd
 * wakeups and the channel lifetime between two pthreads (the thread side
 * and TX) and a third that takes and drops references, outside Tarantool
 * (whose fiber stacks the sanitizers don't follow). It includes ipc.c and
 * runs its static functions; the Lua and box functions are never called,
 * coio_wait() is poll(). See CONTRIBUTING.md "The C data path":
 *
 *   gcc -O1 -g -fsanitize=thread -I/usr/include/tarantool -pthread \
 *       test/ipc/ipc_san.c -o /tmp/ipc_san \
 *       -Wl,--unresolved-symbols=ignore-all
 *   setarch x86_64 -R /tmp/ipc_san
 */
#define _GNU_SOURCE
#include <poll.h>
#include <stdio.h>
#include <assert.h>
#include "../../memcached/ipc.c"

int
coio_wait(int fd, int event, double timeout)
{
	struct pollfd p = {fd, POLLIN, 0};
	int rc = poll(&p, 1, (int)(timeout * 1000));
	return rc > 0 ? event : 0;
}

static struct ipc_chan *chan;
static uint64_t chan_id_v;
static const int ITERS = 200000;
static _Atomic int stop_finder;

/* TX: serve until closed and empty, then detach. */
static void *
server(void *arg)
{
	(void)arg;
	struct ipc_chan *c = chan_find(chan_id_v);
	assert(c != NULL);
	int expected = TX_NONE;
	assert(atomic_compare_exchange_strong(&c->tx_state, &expected,
					      TX_ATTACHED));
	bool closing = false;
	for (;;) {
		uint32_t idx = ring_pop(&c->req);
		if (idx == NO_SLOT) {
			if (closing)
				break;
			if (atomic_load(&c->closed)) {
				closing = true;
				continue;
			}
			ring_wait(&c->req, &c->closed, 1, 0.05);
			continue;
		}
		struct ipc_slot *s = &c->slots[idx];
		/* The reply: the request reversed, then a value. */
		size_t n = s->req_len;
		char *tmp = malloc(n + 1);
		memcpy(tmp, s->buf, n);
		assert(grow(s, n + 16) == 0);
		for (size_t i = 0; i < n; i++)
			s->buf[i] = tmp[n - 1 - i];
		free(tmp);
		lua_Number v = (lua_Number)s->max_commands;
		memcpy(s->buf + n, &v, sizeof(v));
		s->reply_len = n;
		s->vals_len = sizeof(v);
		s->nvals = 1;
		s->status = IPC_OK;
		ring_push(&c->done, idx);
	}
	atomic_store(&c->tx_state, TX_GONE);
	efd_signal(c->done.efd);
	chan_unref(c);
	return NULL;
}

/* Races with the lifetime: references taken and dropped. */
static void *
finder(void *arg)
{
	(void)arg;
	while (!atomic_load(&stop_finder)) {
		struct ipc_chan *c = chan_find(chan_id_v);
		if (c != NULL)
			chan_unref(c);
	}
	return NULL;
}

static void
check_reply(struct ipc_chan *c, uint32_t idx, uint64_t *sum)
{
	struct ipc_slot *s = &c->slots[idx];
	assert(s->status == IPC_OK);
	size_t n = s->req_len;
	assert(s->reply_len == n);
	for (size_t i = 0; i < n; i++) {
		char want = (char)((s->max_commands + (n - 1 - i)) & 0x7f);
		assert(s->buf[i] == want);
	}
	lua_Number v;
	memcpy(&v, s->buf + n, sizeof(v));
	assert(v == (lua_Number)s->max_commands);
	*sum += (uint64_t)v;
	slot_release(c, idx);
}

int
main(void)
{
	chan = chan_new(8);
	assert(chan != NULL);
	chan_id_v = chan->id;
	pthread_t tx, fd;
	pthread_create(&tx, NULL, server, NULL);
	pthread_create(&fd, NULL, finder, NULL);
	while (atomic_load(&chan->tx_state) != TX_ATTACHED)
		;
	struct ipc_chan *c = chan;
	uint64_t sent = 0, got = 0, want_sum = 0, sum = 0;
	unsigned seed = 1;
	while (got < (uint64_t)ITERS) {
		/* Send while there are free slots. */
		while (sent < (uint64_t)ITERS && c->nfree > 0) {
			uint32_t idx = c->free_head;
			struct ipc_slot *s = &c->slots[idx];
			size_t n = rand_r(&seed) % 64 == 0 ?
				   70000 + rand_r(&seed) % 70000 :
				   rand_r(&seed) % 300;
			assert(grow(s, n + 1) == 0);
			c->free_head = s->next_free;
			c->nfree--;
			s->max_commands = (lua_Integer)sent;
			for (size_t i = 0; i < n; i++)
				s->buf[i] = (char)((sent + i) & 0x7f);
			s->req_len = n;
			s->state = SLOT_SENT;
			want_sum += sent;
			sent++;
			ring_push(&c->req, idx);
		}
		uint32_t idx;
		bool any = false;
		while ((idx = ring_pop(&c->done)) != NO_SLOT) {
			check_reply(c, idx, &sum);
			got++;
			any = true;
		}
		if (!any)
			ring_wait(&c->done, &c->tx_state, TX_GONE, 0.05);
	}
	assert(sum == want_sum);
	atomic_store(&c->closed, 1);
	efd_signal(c->req.efd);
	pthread_join(tx, NULL);
	atomic_store(&stop_finder, 1);
	pthread_join(fd, NULL);
	assert(atomic_load(&c->tx_state) == TX_GONE);
	chan_unref(c);
	assert(atomic_load(&live_channels) == 0);
	printf("ok: %llu round trips\n", (unsigned long long)got);
	return 0;
}
