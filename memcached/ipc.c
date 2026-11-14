/*
 * memcached.ipc: a direct transport between the application threads and
 * TX for the raw batch of the front-ends in threads (backend/thread.lua
 * batch(), threads.lua "The IPC"), instead of threads.call.
 *
 * A channel connects one thread backend object (a front-end of one
 * instance in one application thread) with TX. It has:
 * - nslots slots, the requests in flight: a buffer that holds the request
 *   (the input bytes) and then the reply (the output bytes and the values
 *   engine.batch() returned), and its parameters. A slot belongs to the
 *   thread while it is free, to TX from the push of its request to the
 *   push of its reply, then to the thread again;
 * - two rings of slot indexes, single producer, single consumer: req
 *   (thread -> TX) and done (TX -> thread). Each holds at most nslots
 *   indexes, so a push never finds a ring full: back-pressure is a call
 *   that finds no free slot (IPC_UNAVAILABLE, the caller uses
 *   threads.call);
 * - an eventfd per ring for the consumer's wakeups. A consumer that finds
 *   its ring empty sets the ring's armed flag, checks the ring again and
 *   waits on the eventfd (coio_wait()); a producer writes the eventfd only
 *   if it clears the armed flag after its push (seq_cst fences on both
 *   sides: one of them sees the other's store). So a busy consumer costs
 *   its producer no system call, and one wakeup drains everything.
 *
 * The producers and the consumers are fibers: the fibers of a thread on
 * its side (the connections push, a receiver fiber pops), the fibers of a
 * channel in TX on TX's side (one pops at a time, see threads.lua). A push
 * or a pop doesn't yield, so each side is a single producer and a single
 * consumer as the rings see it.
 *
 * Lifetime: a channel is in a process-wide registry (the .so is loaded
 * once per process however many Lua states require it, and stays loaded
 * across mcc.reload(): luaopen resets nothing here) and has a reference
 * for each side. The thread side makes it (ipc.new()) and closes it
 * (close(): no more requests); TX attaches to it by id (ipc.attach()),
 * serves the requests until the thread closes it and then detaches. TX
 * detaches only once none of its fibers holds a slot: then every request
 * it took has its reply in the done ring, and a request it didn't take
 * never will be, so the thread may send it again by threads.call. The
 * memory is freed when both references are dropped.
 *
 * Only module.h, the Lua C API, libc, pthreads and eventfd.
 */
#include <errno.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <sys/eventfd.h>
#include <unistd.h>

#include <lua.h>
#include <lauxlib.h>
#include <module.h>

/* Bumped when the messages or the functions change (thread.lua checks). */
#define IPC_ABI 1

#define IPC_MAGIC 0x6d636369u /* "mcci" */

/* At most this many values in a reply. */
#define IPC_MAX_VALUES 64
/* A slot buffer bigger than this is freed after its reply is read. */
#define IPC_KEEP 65536
/* At most this many slots per channel. */
#define IPC_MAX_SLOTS 65536
/* At most this many bytes in a request or a reply. */
#define IPC_MAX_MSG ((size_t)1 << 30)

/* Call results (the second result of a call without a reply). */
enum {
	/* The reply: the values follow. */
	IPC_OK = 0,
	/* TX can't run the batch (engine.batch() returned nil, the instance
	 * is not running). */
	IPC_NOTRUN = 1,
	/* The batch raised in TX. */
	IPC_ERROR = 2,
	/* Not sent, or not taken by TX: use threads.call. */
	IPC_UNAVAILABLE = 3,
	/* The fiber was cancelled while it waited. */
	IPC_CANCELLED = 4,
};

/* TX's side. */
enum {
	TX_NONE = 0,
	TX_ATTACHED = 1,
	/* Detached: TX touches nothing of the channel any more. */
	TX_GONE = 2,
};

/* A slot on the thread's side. */
enum {
	SLOT_FREE = 0,
	/* Pushed to TX. */
	SLOT_SENT = 1,
	/* Its reply came back. */
	SLOT_DONE = 2,
	/* TX detached without taking it. */
	SLOT_LOST = 3,
};

/* Value tags in a reply. */
enum {
	V_NIL = 0,
	V_FALSE = 1,
	V_TRUE = 2,
	V_NUMBER = 3,
	V_STRING = 4,
	/* encode()d by TX, decode()d by the thread (tables, cdata). */
	V_PACKED = 5,
};

struct ipc_slot {
	/* The request, then the reply. Owned by the side that owns the
	 * slot, which may grow it. */
	char *buf;
	size_t cap;
	/* Set by the thread. */
	size_t req_len;
	lua_Integer max_commands;
	lua_Number max_item_size;
	/* Set by TX: the status, the output bytes, then the values. */
	int status;
	uint32_t nvals;
	size_t reply_len;
	size_t vals_len;
	/* The thread's side only. */
	struct fiber *waiter;
	uint32_t next_free;
	uint8_t state;
	bool abandoned;
};

struct ipc_ring {
	/* Written by the producer. */
	_Alignas(64) _Atomic uint64_t tail;
	/* Written by the consumer. */
	_Alignas(64) _Atomic uint64_t head;
	/* The consumer waits (or is about to) on efd. */
	_Alignas(64) _Atomic int armed;
	int efd;
	uint32_t mask;
	uint32_t *items;
};

struct ipc_chan {
	struct ipc_ring req;
	struct ipc_ring done;
	_Alignas(64) uint32_t magic;
	uint32_t abi;
	uint64_t id;
	struct ipc_chan *next;
	_Atomic int refs;
	/* The thread closed it: no more requests. */
	_Atomic int closed;
	_Atomic int tx_state;
	uint32_t nslots;
	struct ipc_slot *slots;
	/* The thread's side only. */
	_Alignas(64) uint32_t free_head;
	uint32_t nfree;
	/* Fibers waiting in call(). */
	uint32_t nwaiting;
	/* The receiver waits on done.efd: the channel must stay. */
	bool rx_waiting;
	/* TX's side only: the slots TX holds. */
	_Alignas(64) uint32_t tx_held;
};

#define NO_SLOT UINT32_MAX

/* The registry ------------------------------------------------------------ */

static pthread_mutex_t registry_lock = PTHREAD_MUTEX_INITIALIZER;
static struct ipc_chan *registry;
static uint64_t next_id = 1;
static _Atomic int64_t live_channels;

static void
ring_destroy(struct ipc_ring *r)
{
	if (r->efd >= 0)
		close(r->efd);
	free(r->items);
}

static int
ring_create(struct ipc_ring *r, uint32_t nslots)
{
	uint32_t cap = 1;
	while (cap < nslots)
		cap <<= 1;
	atomic_init(&r->tail, 0);
	atomic_init(&r->head, 0);
	atomic_init(&r->armed, 0);
	r->mask = cap - 1;
	r->items = calloc(cap, sizeof(*r->items));
	r->efd = eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
	if (r->items == NULL || r->efd < 0)
		return -1;
	return 0;
}

static void
chan_free(struct ipc_chan *c)
{
	if (c->slots != NULL) {
		for (uint32_t i = 0; i < c->nslots; i++)
			free(c->slots[i].buf);
	}
	free(c->slots);
	ring_destroy(&c->req);
	ring_destroy(&c->done);
	c->magic = 0;
	free(c);
	atomic_fetch_sub(&live_channels, 1);
}

static struct ipc_chan *
chan_new(uint32_t nslots)
{
	struct ipc_chan *c = NULL;
	if (posix_memalign((void **)&c, 64, sizeof(*c)) != 0)
		return NULL;
	memset(c, 0, sizeof(*c));
	c->req.efd = -1;
	c->done.efd = -1;
	atomic_fetch_add(&live_channels, 1);
	c->magic = IPC_MAGIC;
	c->abi = IPC_ABI;
	atomic_init(&c->refs, 1);
	atomic_init(&c->closed, 0);
	atomic_init(&c->tx_state, TX_NONE);
	c->nslots = nslots;
	c->slots = calloc(nslots, sizeof(*c->slots));
	if (c->slots == NULL || ring_create(&c->req, nslots) != 0 ||
	    ring_create(&c->done, nslots) != 0) {
		int err = errno;
		chan_free(c);
		errno = err;
		return NULL;
	}
	for (uint32_t i = 0; i < nslots; i++)
		c->slots[i].next_free = i + 1 < nslots ? i + 1 : NO_SLOT;
	c->free_head = 0;
	c->nfree = nslots;
	pthread_mutex_lock(&registry_lock);
	c->id = next_id++;
	c->next = registry;
	registry = c;
	pthread_mutex_unlock(&registry_lock);
	return c;
}

static void
chan_unref(struct ipc_chan *c)
{
	if (atomic_fetch_sub(&c->refs, 1) != 1)
		return;
	pthread_mutex_lock(&registry_lock);
	for (struct ipc_chan **p = &registry; *p != NULL; p = &(*p)->next) {
		if (*p == c) {
			*p = c->next;
			break;
		}
	}
	pthread_mutex_unlock(&registry_lock);
	chan_free(c);
}

/* A new reference to the channel id, or NULL. */
static struct ipc_chan *
chan_find(uint64_t id)
{
	struct ipc_chan *res = NULL;
	pthread_mutex_lock(&registry_lock);
	for (struct ipc_chan *c = registry; c != NULL; c = c->next) {
		if (c->id != id)
			continue;
		/* Not one whose last reference is being dropped. */
		int refs = atomic_load(&c->refs);
		while (refs > 0) {
			if (atomic_compare_exchange_weak(&c->refs, &refs,
							 refs + 1)) {
				res = c;
				break;
			}
		}
		break;
	}
	pthread_mutex_unlock(&registry_lock);
	return res;
}

/* The rings --------------------------------------------------------------- */

static void
efd_signal(int efd)
{
	uint64_t one = 1;
	ssize_t rc;
	do {
		rc = write(efd, &one, sizeof(one));
	} while (rc < 0 && errno == EINTR);
}

static void
efd_clear(int efd)
{
	uint64_t v;
	ssize_t rc;
	do {
		rc = read(efd, &v, sizeof(v));
	} while (rc < 0 && errno == EINTR);
}

static bool
ring_empty(struct ipc_ring *r)
{
	uint64_t head = atomic_load_explicit(&r->head, memory_order_relaxed);
	return atomic_load_explicit(&r->tail, memory_order_acquire) == head;
}

/* The producer: never full (at most nslots indexes are in a ring). */
static void
ring_push(struct ipc_ring *r, uint32_t idx)
{
	uint64_t tail = atomic_load_explicit(&r->tail, memory_order_relaxed);
	r->items[tail & r->mask] = idx;
	atomic_store_explicit(&r->tail, tail + 1, memory_order_release);
	atomic_thread_fence(memory_order_seq_cst);
	if (atomic_load_explicit(&r->armed, memory_order_relaxed) &&
	    atomic_exchange(&r->armed, 0))
		efd_signal(r->efd);
}

/* The consumer: the next index, or NO_SLOT. */
static uint32_t
ring_pop(struct ipc_ring *r)
{
	uint64_t head = atomic_load_explicit(&r->head, memory_order_relaxed);
	if (atomic_load_explicit(&r->tail, memory_order_acquire) == head)
		return NO_SLOT;
	uint32_t idx = r->items[head & r->mask];
	atomic_store_explicit(&r->head, head + 1, memory_order_release);
	return idx;
}

/*
 * The consumer: wait up to timeout seconds for a push (or a signal of
 * efd), unless the ring is not empty or *flag == value. Yields.
 */
static void
ring_wait(struct ipc_ring *r, _Atomic int *flag, int value, double timeout)
{
	atomic_store(&r->armed, 1);
	atomic_thread_fence(memory_order_seq_cst);
	if (!ring_empty(r) || atomic_load(flag) == value) {
		atomic_store(&r->armed, 0);
		return;
	}
	coio_wait(r->efd, COIO_READ, timeout);
	efd_clear(r->efd);
	atomic_store(&r->armed, 0);
}

/* Buffers ----------------------------------------------------------------- */

static int
grow(struct ipc_slot *s, size_t size)
{
	if (size <= s->cap)
		return 0;
	size_t cap = s->cap < 256 ? 256 : s->cap;
	while (cap < size)
		cap *= 2;
	char *buf = realloc(s->buf, cap);
	if (buf == NULL)
		return -1;
	s->buf = buf;
	s->cap = cap;
	return 0;
}

static void
ibuf_range(box_ibuf_t *ibuf, char **rpos, char **wpos)
{
	char **r, **w;
	box_ibuf_read_range(ibuf, &r, &w);
	*rpos = *r;
	*wpos = *w;
}

/* Empty an ibuf (its read position stays). */
static void
ibuf_clear(box_ibuf_t *ibuf)
{
	char **r, **w;
	box_ibuf_read_range(ibuf, &r, &w);
	*w = *r;
}

/* Append bytes to an ibuf; -1 when out of memory. */
static int
ibuf_append(box_ibuf_t *ibuf, const char *data, size_t len)
{
	if (len == 0)
		return 0;
	char *p = box_ibuf_reserve(ibuf, len);
	if (p == NULL)
		return -1;
	memcpy(p, data, len);
	char **w, **e;
	box_ibuf_write_range(ibuf, &w, &e);
	*w += len;
	return 0;
}

/* Lua handles ------------------------------------------------------------- */

#define THREAD_MT "memcached.ipc.thread"
#define TX_MT "memcached.ipc.tx"

struct handle {
	struct ipc_chan *chan;
};

static void
push_handle(lua_State *L, struct ipc_chan *c, const char *mt)
{
	struct handle *h = lua_newuserdata(L, sizeof(*h));
	h->chan = c;
	luaL_getmetatable(L, mt);
	lua_setmetatable(L, -2);
}

/* The thread's side -------------------------------------------------------- */

/*
 * Drop the thread's reference once the channel is closed, no fiber waits
 * in call() (the abandoned slots need nothing of the thread) and the
 * receiver doesn't wait on its eventfd, which the last reference closes.
 */
static void
thread_maybe_release(struct handle *h)
{
	struct ipc_chan *c = h->chan;
	if (c == NULL || !atomic_load(&c->closed) || c->nwaiting > 0 ||
	    c->rx_waiting)
		return;
	h->chan = NULL;
	chan_unref(c);
}

static void
slot_release(struct ipc_chan *c, uint32_t idx)
{
	struct ipc_slot *s = &c->slots[idx];
	if (s->cap > IPC_KEEP) {
		free(s->buf);
		s->buf = NULL;
		s->cap = 0;
	}
	s->state = SLOT_FREE;
	s->abandoned = false;
	s->waiter = NULL;
	s->next_free = c->free_head;
	c->free_head = idx;
	c->nfree++;
}

/* A slot is answered (DONE) or lost (LOST). */
static void
slot_finish(struct ipc_chan *c, uint32_t idx, uint8_t state)
{
	struct ipc_slot *s = &c->slots[idx];
	s->state = state;
	if (s->abandoned)
		slot_release(c, idx);
	else if (s->waiter != NULL)
		fiber_wakeup(s->waiter);
}

/*
 * Take the replies of the done ring; when TX is gone, the slots it didn't
 * take are lost. Doesn't yield.
 */
static void
thread_drain(struct ipc_chan *c)
{
	uint32_t idx;
	while ((idx = ring_pop(&c->done)) != NO_SLOT)
		slot_finish(c, idx, SLOT_DONE);
	if (atomic_load(&c->tx_state) != TX_GONE)
		return;
	/* After the TX_GONE load every reply TX pushed is visible. */
	while ((idx = ring_pop(&c->done)) != NO_SLOT)
		slot_finish(c, idx, SLOT_DONE);
	for (uint32_t i = 0; i < c->nslots; i++) {
		if (c->slots[i].state == SLOT_SENT)
			slot_finish(c, i, SLOT_LOST);
	}
}

/* ipc.new([nslots]) -> channel (the thread's side), or nil, error */
static int
ipc_new(lua_State *L)
{
	lua_Integer n = luaL_optinteger(L, 1, 1024);
	if (n < 1 || n > IPC_MAX_SLOTS)
		return luaL_argerror(L, 1, "bad number of slots");
	errno = 0;
	struct ipc_chan *c = chan_new((uint32_t)n);
	if (c == NULL) {
		lua_pushnil(L);
		lua_pushstring(L, strerror(errno != 0 ? errno : ENOMEM));
		return 2;
	}
	push_handle(L, c, THREAD_MT);
	return 1;
}

/* channel:id() */
static int
chan_id(lua_State *L)
{
	struct handle *h = lua_touserdata(L, 1);
	if (h == NULL || h->chan == NULL)
		return 0;
	lua_pushnumber(L, (lua_Number)h->chan->id);
	return 1;
}

/* Push the values of the reply in slot s. */
static void
push_values(lua_State *L, struct ipc_slot *s, int decode)
{
	const char *p = s->buf + s->reply_len;
	const char *end = p + s->vals_len;
	for (uint32_t i = 0; i < s->nvals && p < end; i++) {
		uint8_t tag = (uint8_t)*p++;
		switch (tag) {
		case V_FALSE:
		case V_TRUE:
			lua_pushboolean(L, tag == V_TRUE);
			break;
		case V_NUMBER: {
			lua_Number v;
			memcpy(&v, p, sizeof(v));
			p += sizeof(v);
			lua_pushnumber(L, v);
			break;
		}
		case V_STRING:
		case V_PACKED: {
			uint32_t len;
			memcpy(&len, p, sizeof(len));
			p += sizeof(len);
			if (tag == V_STRING) {
				lua_pushlstring(L, p, len);
			} else {
				lua_pushvalue(L, decode);
				lua_pushlstring(L, p, len);
				if (lua_pcall(L, 1, 1, 0) != 0) {
					lua_pop(L, 1);
					lua_pushnil(L);
				}
			}
			p += len;
			break;
		}
		default:
			lua_pushnil(L);
			break;
		}
	}
}

/*
 * channel:call(input, n, more, max_commands, max_item_size, output,
 *              decode)
 *   -> the values of engine.batch() in TX, or nil, status
 *
 * Send the first n bytes of input (an ibuf; with a "\0" after them when
 * more is true) to TX and wait for the reply: its output bytes are
 * appended to output (an ibuf), its values returned (decode(string) for
 * the packed ones). Without a reply: nil and IPC_NOTRUN, IPC_ERROR,
 * IPC_UNAVAILABLE (not run by TX: the channel is closed, TX is not
 * attached or gone, no slot is free, the request is too big) or
 * IPC_CANCELLED (the fiber was cancelled while it waited: the request
 * still runs in TX). Yields.
 */
static int
chan_call(lua_State *L)
{
	struct handle *h = luaL_checkudata(L, 1, THREAD_MT);
	box_ibuf_t *in = luaT_toibuf(L, 2);
	if (in == NULL)
		return luaL_argerror(L, 2, "ibuf expected");
	lua_Integer n = luaL_checkinteger(L, 3);
	bool more = lua_toboolean(L, 4);
	lua_Integer max_commands = luaL_checkinteger(L, 5);
	lua_Number max_item_size = luaL_checknumber(L, 6);
	box_ibuf_t *out = luaT_toibuf(L, 7);
	if (out == NULL)
		return luaL_argerror(L, 7, "ibuf expected");
	luaL_checktype(L, 8, LUA_TFUNCTION);
	char *rpos, *wpos;
	ibuf_range(in, &rpos, &wpos);
	if (n < 0 || n > wpos - rpos)
		return luaL_argerror(L, 3, "bad size");
	struct ipc_chan *c = h->chan;
	if (c == NULL || atomic_load(&c->closed) ||
	    atomic_load(&c->tx_state) != TX_ATTACHED || c->nfree == 0 ||
	    (size_t)n + 1 > IPC_MAX_MSG)
		goto unavailable;
	uint32_t idx = c->free_head;
	struct ipc_slot *s = &c->slots[idx];
	if (grow(s, (size_t)n + 1) != 0)
		goto unavailable;
	c->free_head = s->next_free;
	c->nfree--;
	memcpy(s->buf, rpos, n);
	s->req_len = n;
	if (more)
		s->buf[s->req_len++] = '\0';
	s->max_commands = max_commands;
	s->max_item_size = max_item_size;
	s->state = SLOT_SENT;
	s->abandoned = false;
	s->waiter = fiber_self();
	c->nwaiting++;
	ring_push(&c->req, idx);

	while (s->state == SLOT_SENT) {
		/* Woken by the receiver; the timeout covers a receiver that
		 * is gone. */
		fiber_sleep(1.0);
		if (s->state != SLOT_SENT)
			break;
		if (fiber_is_cancelled()) {
			s->waiter = NULL;
			s->abandoned = true;
			c->nwaiting--;
			thread_maybe_release(h);
			lua_pushnil(L);
			lua_pushinteger(L, IPC_CANCELLED);
			return 2;
		}
		thread_drain(c);
	}
	s->waiter = NULL;
	c->nwaiting--;
	int nres = 2;
	if (s->state == SLOT_LOST) {
		lua_pushnil(L);
		lua_pushinteger(L, IPC_UNAVAILABLE);
	} else if (s->status != IPC_OK) {
		lua_pushnil(L);
		lua_pushinteger(L, s->status);
	} else if (!lua_checkstack(L, (int)s->nvals + 4) ||
		   ibuf_append(out, s->buf, s->reply_len) != 0) {
		lua_pushnil(L);
		lua_pushinteger(L, IPC_ERROR);
	} else {
		int top = lua_gettop(L);
		push_values(L, s, 8);
		nres = lua_gettop(L) - top;
	}
	slot_release(c, idx);
	thread_maybe_release(h);
	return nres;

unavailable:
	lua_pushnil(L);
	lua_pushinteger(L, IPC_UNAVAILABLE);
	return 2;
}

/*
 * channel:receive([timeout]) -> whether to call it again
 *
 * The receiver fiber of the thread: wake the fibers whose replies came,
 * then wait up to timeout seconds for more. false once the channel is
 * closed and no fiber waits. Yields.
 */
static int
chan_receive(lua_State *L)
{
	struct handle *h = luaL_checkudata(L, 1, THREAD_MT);
	double timeout = luaL_optnumber(L, 2, 1.0);
	struct ipc_chan *c = h->chan;
	if (c != NULL) {
		thread_drain(c);
		thread_maybe_release(h);
	}
	if (h->chan == NULL) {
		lua_pushboolean(L, 0);
		return 1;
	}
	c->rx_waiting = true;
	if (atomic_load(&c->tx_state) != TX_GONE)
		ring_wait(&c->done, &c->tx_state, TX_GONE, timeout);
	else
		fiber_sleep(timeout);
	c->rx_waiting = false;
	thread_drain(c);
	thread_maybe_release(h);
	lua_pushboolean(L, h->chan != NULL);
	return 1;
}

/* channel:close(): no more requests. The fibers waiting get their replies. */
static int
chan_close(lua_State *L)
{
	struct handle *h = luaL_checkudata(L, 1, THREAD_MT);
	struct ipc_chan *c = h->chan;
	if (c == NULL)
		return 0;
	if (!atomic_exchange(&c->closed, 1)) {
		efd_signal(c->req.efd);
		efd_signal(c->done.efd);
	}
	thread_maybe_release(h);
	return 0;
}

/* channel:info() -> {id, tx, closed, slots, free, waiting} (for tests) */
static int
chan_info(lua_State *L)
{
	struct handle *h = lua_touserdata(L, 1);
	struct ipc_chan *c = h != NULL ? h->chan : NULL;
	lua_createtable(L, 0, 6);
	if (c == NULL)
		return 1;
	lua_pushnumber(L, (lua_Number)c->id);
	lua_setfield(L, -2, "id");
	lua_pushinteger(L, atomic_load(&c->tx_state));
	lua_setfield(L, -2, "tx");
	lua_pushboolean(L, atomic_load(&c->closed));
	lua_setfield(L, -2, "closed");
	lua_pushinteger(L, c->nslots);
	lua_setfield(L, -2, "slots");
	lua_pushinteger(L, c->nfree);
	lua_setfield(L, -2, "free");
	lua_pushinteger(L, c->nwaiting);
	lua_setfield(L, -2, "waiting");
	return 1;
}

static int
thread_gc(lua_State *L)
{
	struct handle *h = lua_touserdata(L, 1);
	struct ipc_chan *c = h->chan;
	if (c == NULL)
		return 0;
	/* No fiber waits: a waiting fiber holds the handle. */
	if (!atomic_exchange(&c->closed, 1))
		efd_signal(c->req.efd);
	h->chan = NULL;
	chan_unref(c);
	return 0;
}

/* TX's side ---------------------------------------------------------------- */

/* ipc.attach(id) -> channel (TX's side), or nil, error */
static int
ipc_attach(lua_State *L)
{
	uint64_t id = (uint64_t)luaL_checknumber(L, 1);
	struct ipc_chan *c = chan_find(id);
	if (c == NULL) {
		lua_pushnil(L);
		lua_pushstring(L, "no such channel");
		return 2;
	}
	int expected = TX_NONE;
	if (c->magic != IPC_MAGIC || c->abi != IPC_ABI ||
	    atomic_load(&c->closed) ||
	    !atomic_compare_exchange_strong(&c->tx_state, &expected,
					    TX_ATTACHED)) {
		chan_unref(c);
		lua_pushnil(L);
		lua_pushstring(L, "the channel is closed or attached");
		return 2;
	}
	push_handle(L, c, TX_MT);
	return 1;
}

/*
 * channel:next(ibuf) -> slot, max_commands, max_item_size, or nothing
 *
 * Take the next request: its bytes replace the contents of ibuf. The slot
 * must get a reply().
 */
static int
chan_next(lua_State *L)
{
	struct handle *h = luaL_checkudata(L, 1, TX_MT);
	box_ibuf_t *ibuf = luaT_toibuf(L, 2);
	if (ibuf == NULL)
		return luaL_argerror(L, 2, "ibuf expected");
	struct ipc_chan *c = h->chan;
	if (c == NULL)
		return 0;
	uint32_t idx;
	while ((idx = ring_pop(&c->req)) != NO_SLOT) {
		struct ipc_slot *s = &c->slots[idx];
		ibuf_clear(ibuf);
		if (ibuf_append(ibuf, s->buf, s->req_len) == 0) {
			c->tx_held++;
			lua_pushinteger(L, idx);
			lua_pushinteger(L, s->max_commands);
			lua_pushnumber(L, s->max_item_size);
			return 3;
		}
		s->status = IPC_ERROR;
		s->reply_len = 0;
		s->vals_len = 0;
		s->nvals = 0;
		ring_push(&c->done, idx);
	}
	return 0;
}

static size_t
value_size(lua_State *L, int i)
{
	switch (lua_type(L, i)) {
	case LUA_TNUMBER:
		return 1 + sizeof(lua_Number);
	case LUA_TSTRING:
		return 1 + sizeof(uint32_t) + lua_objlen(L, i);
	default:
		return 1;
	}
}

static char *
put_value(lua_State *L, int i, bool packed, char *p)
{
	switch (lua_type(L, i)) {
	case LUA_TBOOLEAN:
		*p++ = lua_toboolean(L, i) ? V_TRUE : V_FALSE;
		return p;
	case LUA_TNUMBER: {
		lua_Number v = lua_tonumber(L, i);
		*p++ = V_NUMBER;
		memcpy(p, &v, sizeof(v));
		return p + sizeof(v);
	}
	case LUA_TSTRING: {
		size_t len;
		const char *s = lua_tolstring(L, i, &len);
		uint32_t len32 = (uint32_t)len;
		*p++ = packed ? V_PACKED : V_STRING;
		memcpy(p, &len32, sizeof(len32));
		p += sizeof(len32);
		memcpy(p, s, len);
		return p + len;
	}
	default:
		*p++ = V_NIL;
		return p;
	}
}

/*
 * channel:reply(slot, status, obuf, encode, ...)
 *
 * The reply to a slot from next(): with IPC_OK, the bytes of obuf and the
 * values ... (nil, booleans, numbers and strings as they are, the others
 * as encode(value), a string); otherwise the status only. obuf (or nil)
 * is emptied.
 */
static int
chan_reply(lua_State *L)
{
	struct handle *h = luaL_checkudata(L, 1, TX_MT);
	lua_Integer idx = luaL_checkinteger(L, 2);
	int status = (int)luaL_checkinteger(L, 3);
	box_ibuf_t *obuf = lua_isnil(L, 4) ? NULL : luaT_toibuf(L, 4);
	struct ipc_chan *c = h->chan;
	if (c == NULL || idx < 0 || idx >= c->nslots)
		return luaL_error(L, "ipc: bad reply slot");
	struct ipc_slot *s = &c->slots[idx];
	const int first = 6;
	int top = lua_gettop(L);
	int nvals = top >= first ? top - first + 1 : 0;
	if (nvals > IPC_MAX_VALUES)
		nvals = IPC_MAX_VALUES;
	uint64_t packed = 0;
	char *rpos = NULL, *wpos = NULL;
	size_t reply_len = 0, vals_len = 0;
	if (status == IPC_OK) {
		for (int i = 0; i < nvals; i++) {
			int t = lua_type(L, first + i);
			if (t == LUA_TNIL || t == LUA_TBOOLEAN ||
			    t == LUA_TNUMBER || t == LUA_TSTRING)
				continue;
			/* encode(value); on errors the value is nil. */
			lua_pushvalue(L, 5);
			lua_pushvalue(L, first + i);
			if (lua_pcall(L, 1, 1, 0) != 0 ||
			    lua_type(L, -1) != LUA_TSTRING) {
				lua_pop(L, 1);
				lua_pushnil(L);
			} else {
				packed |= (uint64_t)1 << i;
			}
			lua_replace(L, first + i);
		}
		for (int i = 0; i < nvals; i++)
			vals_len += value_size(L, first + i);
		if (obuf != NULL) {
			ibuf_range(obuf, &rpos, &wpos);
			reply_len = wpos - rpos;
		}
		if (reply_len + vals_len > IPC_MAX_MSG ||
		    grow(s, reply_len + vals_len) != 0)
			status = IPC_ERROR;
	}
	if (status == IPC_OK) {
		if (reply_len > 0)
			memcpy(s->buf, rpos, reply_len);
		char *p = s->buf + reply_len;
		for (int i = 0; i < nvals; i++)
			p = put_value(L, first + i, (packed >> i) & 1, p);
	} else {
		nvals = 0;
		reply_len = vals_len = 0;
	}
	if (obuf != NULL)
		ibuf_clear(obuf);
	s->status = status;
	s->reply_len = reply_len;
	s->vals_len = vals_len;
	s->nvals = (uint32_t)nvals;
	if (c->tx_held > 0)
		c->tx_held--;
	ring_push(&c->done, (uint32_t)idx);
	return 0;
}

/*
 * channel:wait([timeout]): wait up to timeout seconds for a request,
 * unless there is one or the channel is closed. Yields.
 */
static int
chan_wait(lua_State *L)
{
	struct handle *h = luaL_checkudata(L, 1, TX_MT);
	double timeout = luaL_optnumber(L, 2, 1.0);
	struct ipc_chan *c = h->chan;
	if (c == NULL)
		return 0;
	ring_wait(&c->req, &c->closed, 1, timeout);
	return 0;
}

/* channel:closed() -> whether the thread closed it (or TX detached) */
static int
chan_closed(lua_State *L)
{
	struct handle *h = luaL_checkudata(L, 1, TX_MT);
	struct ipc_chan *c = h->chan;
	lua_pushboolean(L, c == NULL || atomic_load(&c->closed));
	return 1;
}

/*
 * TX is done with the channel. Called when no fiber of TX holds a slot:
 * the replies are pushed, and the requests TX didn't take are lost for the
 * thread, which sends them by threads.call.
 */
static void
tx_detach(struct handle *h)
{
	struct ipc_chan *c = h->chan;
	if (c == NULL)
		return;
	h->chan = NULL;
	atomic_store(&c->tx_state, TX_GONE);
	efd_signal(c->done.efd);
	chan_unref(c);
}

/* channel:detach() */
static int
chan_detach(lua_State *L)
{
	tx_detach(luaL_checkudata(L, 1, TX_MT));
	return 0;
}

static int
tx_gc(lua_State *L)
{
	tx_detach(lua_touserdata(L, 1));
	return 0;
}

/* ipc.count() -> the channels alive in the process (for tests) */
static int
ipc_count(lua_State *L)
{
	lua_pushnumber(L, (lua_Number)atomic_load(&live_channels));
	return 1;
}

static void
register_mt(lua_State *L, const char *name, const luaL_Reg *methods,
	    lua_CFunction gc)
{
	luaL_newmetatable(L, name);
	lua_newtable(L);
	luaL_register(L, NULL, methods);
	lua_setfield(L, -2, "__index");
	lua_pushcfunction(L, gc);
	lua_setfield(L, -2, "__gc");
	lua_pop(L, 1);
}

LUA_API int
luaopen_memcached_ipc(lua_State *L)
{
	static const luaL_Reg thread_methods[] = {
		{"id", chan_id},
		{"call", chan_call},
		{"receive", chan_receive},
		{"close", chan_close},
		{"info", chan_info},
		{NULL, NULL},
	};
	static const luaL_Reg tx_methods[] = {
		{"id", chan_id},
		{"next", chan_next},
		{"reply", chan_reply},
		{"wait", chan_wait},
		{"closed", chan_closed},
		{"detach", chan_detach},
		{"info", chan_info},
		{NULL, NULL},
	};
	static const luaL_Reg funcs[] = {
		{"new", ipc_new},
		{"attach", ipc_attach},
		{"count", ipc_count},
		{NULL, NULL},
	};
	register_mt(L, THREAD_MT, thread_methods, thread_gc);
	register_mt(L, TX_MT, tx_methods, tx_gc);
	lua_newtable(L);
	luaL_register(L, NULL, funcs);
	lua_pushinteger(L, IPC_ABI);
	lua_setfield(L, -2, "ABI");
	lua_pushinteger(L, IPC_OK);
	lua_setfield(L, -2, "OK");
	lua_pushinteger(L, IPC_NOTRUN);
	lua_setfield(L, -2, "NOTRUN");
	lua_pushinteger(L, IPC_ERROR);
	lua_setfield(L, -2, "ERROR");
	lua_pushinteger(L, IPC_UNAVAILABLE);
	lua_setfield(L, -2, "UNAVAILABLE");
	lua_pushinteger(L, IPC_CANCELLED);
	lua_setfield(L, -2, "CANCELLED");
	return 1;
}
