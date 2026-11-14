/*
 * memcached.datapath loop(): the connection loop of the TX thread in C.
 * Built into the datapath module (app.manifest.toml), on the datapath.h
 * API. See DESIGN.md, "The C connection loop".
 *
 * A connection fiber of a server with the local backend calls it instead
 * of a step of the Lua loop (server.lua conn_step()). It reads the socket
 * into the connection's input buffer, runs the C batch
 * (mcc_dp_batch_run()) on it, writes the replies, and repeats, for as
 * long as the C batch serves the whole input. It returns to Lua whenever
 * Lua must act:
 * - LOOP_STOP: the batch stopped before the end of the input (a command
 *   it doesn't serve, an incomplete one): the replies of the commands it
 *   served are in the output buffer, not written, and Lua goes on with the
 *   same batch from there, as conn_step() after a turn of the C batch;
 *   the second and third results are the commands served and the items
 *   their reads hold (nil or a set {key = true});
 * - LOOP_CLOSE: end of input, a socket error, or the fiber cancelled while
 *   it waits for a write: the connection is to be closed;
 * - LOOP_WAKE: a wait for input ended without input: the idle timeout, a
 *   fiber:wakeup() (srv:cfg(), a close) or a cancel. Lua looks at why;
 * - LOOP_INPUTS: the inputs of the batch taken at the call may be out of
 *   date (state.dp_gen moved, or the schema version): Lua calls again with
 *   new ones. Checked before each batch, after the read;
 * - LOOP_KEEP: a batch had more than `keep` bytes of input or output, or a
 *   buffer grew past that: Lua shrinks the buffers (as its loop does), after
 *   the replies are written.
 * Nothing else is returned: the commands served, the counters (into the
 * counters userdata of the server, which Lua adds to its stats on demand:
 * datapath.take()), conn.state and conn.last_cmd are kept up to date in
 * place.
 *
 * The waits and reads are those of server.lua read_more() and
 * wait_input(): after the replies of a batch that took all the input, wait
 * for readability before the next read (which yields to the other fibers
 * once per batch, as the Lua loop does); a read that would block waits
 * too. The writes are write_ptr()'s: a write that would block waits for
 * the socket to be writable, in conn_mwrite.
 */
#include <errno.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>

#include <lua.h>
#include <lauxlib.h>
#include <module.h>

#include "datapath.h"

/* Tarantool's TIMEOUT_INFINITY, which module.h doesn't have. */
#define FOREVER (365 * 86400 * 100.0)

/* The reasons to return to Lua (server.lua knows them by number). */
enum loop_reason {
	LOOP_STOP = 1,
	LOOP_CLOSE = 2,
	LOOP_WAKE = 3,
	LOOP_INPUTS = 4,
	LOOP_KEEP = 5,
};

/* The counters userdata ------------------------------------------------- */

/*
 * The counters of the commands served by the C loops of a server and the
 * bytes they read and wrote, which datapath.take() hands to Lua and
 * clears; and running totals of the commands served, for the tests and
 * the diagnostics (datapath.served()), which it keeps.
 */
enum {
	CNT_BYTES_READ = MCC_DP_STAT_COUNT,
	CNT_BYTES_WRITTEN,
	/* The values take() returns and clears. */
	CNT_TAKE,
	/* Totals: the commands, the keys of the retrievals, the stores. */
	CNT_SERVED = CNT_TAKE,
	CNT_SERVED_GETS,
	CNT_SERVED_SETS,
	CNT_COUNT,
};

#define COUNTERS_MT "memcached.datapath.counters"

struct counters {
	uint64_t v[CNT_COUNT];
};

static struct counters *
check_counters(lua_State *L, int idx)
{
	return luaL_checkudata(L, idx, COUNTERS_MT);
}

/* counters() -> a new counters userdata, all 0. */
static int
dp_counters(lua_State *L)
{
	struct counters *c = lua_newuserdata(L, sizeof(*c));
	memset(c, 0, sizeof(*c));
	luaL_getmetatable(L, COUNTERS_MT);
	lua_setmetatable(L, -2);
	return 1;
}

/*
 * take(counters) -> the counters in the order of mcc_dp_stat, then
 * bytes_read and bytes_written; clears them.
 */
static int
dp_take(lua_State *L)
{
	struct counters *c = check_counters(L, 1);
	for (int i = 0; i < CNT_TAKE; i++) {
		lua_pushnumber(L, (double)c->v[i]);
		c->v[i] = 0;
	}
	return CNT_TAKE;
}

/* served(counters) -> commands, gets, sets: the totals, never cleared. */
static int
dp_served(lua_State *L)
{
	struct counters *c = check_counters(L, 1);
	for (int i = CNT_SERVED; i < CNT_COUNT; i++)
		lua_pushnumber(L, (double)c->v[i]);
	return CNT_COUNT - CNT_SERVED;
}

/* The loop -------------------------------------------------------------- */

/* The arguments of loop(), by stack index. */
enum {
	A_CONN = 1,
	A_FD,
	A_IBUF,
	A_OBUF,
	A_COUNTERS,
	A_SPACE,
	A_MAX_ITEM_SIZE,
	A_FLUSH,
	A_TRACK,
	A_LA_INTERVAL,
	A_MVCC,
	A_BUCKET,
	A_STATE,
	A_TOKENS,
	A_IDLE,
	A_REPLIED,
	A_NEW_CMD,
	A_LAST_CMD,
	A_MAX_COMMANDS,
	A_READ_SIZE,
	A_KEEP,
	/* The clock function of the batch (see batch_clock()). */
	A_CLOCK,
	A_TOP = A_CLOCK,
};

/* The upvalues of loop() (luaopen: mcc_dp_loop_open()). */
enum {
	U_FIBER = 1,
	U_K_STATE,
	U_K_LAST_CMD,
	U_K_GEN,
	U_K_TIME,
	U_CONN_READ,
	U_CONN_PARSE_CMD,
	U_CONN_MWRITE,
	U_COUNT = U_CONN_MWRITE,
};

/* conn.state, as server.lua names them (memcached's names). */
enum conn_state {
	ST_UNKNOWN,
	ST_NEW_CMD,
	ST_READ,
	ST_PARSE_CMD,
	ST_MWRITE,
};

struct loop {
	lua_State *L;
	int fd;
	box_ibuf_t *in;
	box_ibuf_t *out;
	struct counters *cnt;
	/* idle_timeout for this connection (0: none) and conn.last_cmd. */
	double idle;
	double last_cmd;
	size_t read_size;
	size_t keep;
	uint32_t max_commands;
	/* What conn.state is, as this loop last saw or set it. */
	enum conn_state state;
	/* server.lua conn.replied: wait for input before the next read. */
	bool replied;
	/* The generation of the inputs and the schema version at the call. */
	double gen;
	uint64_t schema;
};

/* conn.state = the name of st (no change: nothing). */
static void
set_state(struct loop *lp, enum conn_state st)
{
	if (lp->state == st)
		return;
	int name;
	switch (st) {
	case ST_READ:
		name = U_CONN_READ;
		break;
	case ST_MWRITE:
		name = U_CONN_MWRITE;
		break;
	case ST_PARSE_CMD:
		name = U_CONN_PARSE_CMD;
		break;
	default:
		/* Never set from here: conn_new_cmd only stays. */
		return;
	}
	lp->state = st;
	lua_State *L = lp->L;
	lua_pushvalue(L, lua_upvalueindex(U_K_STATE));
	lua_pushvalue(L, lua_upvalueindex(name));
	lua_rawset(L, A_CONN);
}

static void
set_last_cmd(struct loop *lp, double clock)
{
	lua_State *L = lp->L;
	lp->last_cmd = clock;
	lua_pushvalue(L, lua_upvalueindex(U_K_LAST_CMD));
	lua_pushnumber(L, clock);
	lua_rawset(L, A_CONN);
}

static bool
would_block(int e)
{
	return e == EAGAIN || e == EWOULDBLOCK || e == EINTR;
}

/*
 * server.lua wait_input() for a connection waiting for a command (the
 * input buffer is empty here): conn_read, or conn_new_cmd before the first
 * input; with idle_timeout, up to the end of the idle time. Returns 0 when
 * the socket is readable, LOOP_WAKE otherwise.
 */
static int
wait_input(struct loop *lp)
{
	if (lp->state != ST_NEW_CMD)
		set_state(lp, ST_READ);
	double timeout = FOREVER;
	if (lp->idle > 0) {
		timeout = lp->last_cmd + lp->idle - fiber_clock();
		if (timeout <= 0)
			return LOOP_WAKE;
	}
	if (coio_wait(lp->fd, COIO_READ, timeout) == 0)
		return LOOP_WAKE;
	return 0;
}

/* server.lua read_more() into an empty input buffer. */
static int
read_more(struct loop *lp)
{
	int rc;
	if (lp->replied) {
		/* The input is not here yet: wait first, one syscall fewer. */
		lp->replied = false;
		if ((rc = wait_input(lp)) != 0)
			return rc;
	}
	while (true) {
		char **wpos, **epos;
		box_ibuf_write_range(lp->in, &wpos, &epos);
		if ((size_t)(*epos - *wpos) < lp->read_size) {
			if (box_ibuf_reserve(lp->in, lp->read_size) == NULL)
				return LOOP_CLOSE;
			box_ibuf_write_range(lp->in, &wpos, &epos);
		}
		ssize_t n = read(lp->fd, *wpos, *epos - *wpos);
		if (n > 0) {
			*wpos += n;
			lp->cnt->v[CNT_BYTES_READ] += n;
			return 0;
		}
		if (n == 0 || !would_block(errno))
			return LOOP_CLOSE;
		if ((rc = wait_input(lp)) != 0)
			return rc;
	}
}

/*
 * server.lua write_obuf(): write the output buffer and empty it. 0, or
 * LOOP_CLOSE on an error or when the fiber is cancelled while it waits.
 */
static int
write_out(struct loop *lp)
{
	char **rpos, **wpos;
	box_ibuf_read_range(lp->out, &rpos, &wpos);
	size_t len = *wpos - *rpos;
	while (*rpos < *wpos) {
		ssize_t n = send(lp->fd, *rpos, *wpos - *rpos, MSG_NOSIGNAL);
		if (n >= 0) {
			*rpos += n;
			continue;
		}
		if (!would_block(errno))
			return LOOP_CLOSE;
		enum conn_state prev = lp->state;
		set_state(lp, ST_MWRITE);
		coio_wait(lp->fd, COIO_WRITE, FOREVER);
		set_state(lp, prev);
		if (fiber_is_cancelled())
			return LOOP_CLOSE;
	}
	lp->cnt->v[CNT_BYTES_WRITTEN] += len;
	return 0;
}

/*
 * The clock of a batch, as engine.batch() takes it: fiber.time(), looked
 * up in the fiber module and called per batch, as the Lua engine calls it
 * per request (the tests and the compat shim replace it, before or after
 * the connection enters the loop). The function becomes the batch's clock
 * (A_CLOCK), which it calls again after a write that yielded.
 */
static double
batch_clock(lua_State *L)
{
	lua_pushvalue(L, lua_upvalueindex(U_K_TIME));
	lua_rawget(L, lua_upvalueindex(U_FIBER));
	lua_pushvalue(L, -1);
	lua_replace(L, A_CLOCK);
	lua_call(L, 0, 1);
	double now = lua_tonumber(L, -1);
	lua_pop(L, 1);
	return now;
}

static double
inputs_gen(lua_State *L)
{
	lua_pushvalue(L, lua_upvalueindex(U_K_GEN));
	lua_rawget(L, A_STATE);
	double gen = lua_tonumber(L, -1);
	lua_pop(L, 1);
	return gen;
}

/*
 * loop(conn, fd, ibuf, obuf, counters, space_id, max_item_size, flush,
 *      track, la_interval, mvcc, bucket_id, state, tokens, idle, replied,
 *      new_cmd, last_cmd, max_commands, read_size, keep)
 *   -> reason[, commands, held]
 *
 * conn is the connection's table (conn.state and conn.last_cmd are set in
 * it), fd its socket, ibuf and obuf its input and output buffers (the
 * input read and not consumed, the output empty), counters the server's
 * counters userdata. The batch's arguments are those of engine.batch()
 * (datapath.c batch()), taken once: space_id, max_item_size, the flush
 * time (nil: none), track_access, the last access interval, whether memtx
 * MVCC is on (no writes then), the bucket id of new tuples, the engine's
 * state table and the win tokens kept in memory (nil: none); the
 * read-only flag is box_is_ro() per batch, and the clock is fiber.time()
 * per batch (batch_clock()). Then the connection's: its idle timeout (0:
 * none), whether its last batch took all the input and was replied to
 * (conn.replied), whether it is in conn_new_cmd, conn.last_cmd; and the
 * server's MAX_BATCH, READ_SIZE and BUFFER_KEEP.
 */
static int
dp_loop(lua_State *L)
{
	lua_settop(L, A_TOP);
	struct loop lp;
	lp.L = L;
	lp.fd = (int)luaL_checkinteger(L, A_FD);
	lp.in = luaT_toibuf(L, A_IBUF);
	lp.out = luaT_toibuf(L, A_OBUF);
	if (lp.in == NULL || lp.out == NULL)
		return luaL_error(L, "loop: ibuf expected");
	lp.cnt = check_counters(L, A_COUNTERS);
	lp.idle = lua_tonumber(L, A_IDLE);
	lp.replied = lua_toboolean(L, A_REPLIED);
	lp.state = lua_toboolean(L, A_NEW_CMD) ? ST_NEW_CMD : ST_UNKNOWN;
	lp.last_cmd = luaL_checknumber(L, A_LAST_CMD);
	lua_Integer max_commands = luaL_checkinteger(L, A_MAX_COMMANDS);
	if (max_commands < 1)
		max_commands = 1;
	if (max_commands > UINT32_MAX)
		max_commands = UINT32_MAX;
	lp.max_commands = (uint32_t)max_commands;
	lp.read_size = (size_t)luaL_checkinteger(L, A_READ_SIZE);
	lp.keep = (size_t)luaL_checkinteger(L, A_KEEP);
	luaL_checktype(L, A_CONN, LUA_TTABLE);
	luaL_checktype(L, A_STATE, LUA_TTABLE);

	/* The batch's arguments that stay. */
	struct mcc_dp_batch tpl;
	memset(&tpl, 0, sizeof(tpl));
	tpl.L = L;
	tpl.state = A_STATE;
	tpl.tokens = lua_istable(L, A_TOKENS) ? A_TOKENS : 0;
	tpl.clock = A_CLOCK;
	tpl.held_in = 0;
	tpl.space_id = (uint32_t)luaL_checkinteger(L, A_SPACE);
	tpl.out = lp.out;
	tpl.max_item_size = luaL_checknumber(L, A_MAX_ITEM_SIZE);
	tpl.has_flush = !lua_isnil(L, A_FLUSH);
	tpl.flush = tpl.has_flush ? luaL_checknumber(L, A_FLUSH) : 0;
	tpl.track = lua_toboolean(L, A_TRACK);
	tpl.la_interval = luaL_checknumber(L, A_LA_INTERVAL);
	bool mvcc = lua_toboolean(L, A_MVCC);
	tpl.bucket_id = luaL_checknumber(L, A_BUCKET);

	lp.gen = inputs_gen(L);
	lp.schema = box_schema_version();

	while (true) {
		char **rpos, **wpos;
		box_ibuf_read_range(lp.in, &rpos, &wpos);
		if (*rpos == *wpos) {
			int rc = read_more(&lp);
			if (rc != 0) {
				lua_pushinteger(L, rc);
				return 1;
			}
		}
		if (inputs_gen(L) != lp.gen ||
		    box_schema_version() != lp.schema) {
			lua_pushinteger(L, LOOP_INPUTS);
			return 1;
		}
		/* Executing a batch (it may yield on a write). */
		set_state(&lp, ST_PARSE_CMD);
		struct mcc_dp_batch b = tpl;
		b.now = batch_clock(L);
		b.ro = box_is_ro();
		b.store = !b.ro && !mvcc;

		size_t region = box_region_used();
		const char *in = *rpos;
		size_t len = *wpos - *rpos;
		uint32_t commands;
		size_t consumed = mcc_dp_batch_run(&b, in, len,
						   lp.max_commands, &commands);
		*rpos += consumed;
		for (int i = 0; i < MCC_DP_STAT_COUNT; i++)
			lp.cnt->v[i] += b.stats[i];
		lp.cnt->v[CNT_SERVED] += commands;
		lp.cnt->v[CNT_SERVED_GETS] += b.stats[MCC_DP_CMD_GET];
		lp.cnt->v[CNT_SERVED_SETS] += b.stats[MCC_DP_STORED] +
					      b.stats[MCC_DP_UNSTORED];
		if (consumed < len && commands < lp.max_commands) {
			/* Lua goes on with this batch. */
			lua_pushinteger(L, LOOP_STOP);
			lua_pushinteger(L, commands);
			mcc_dp_batch_push_held(&b);
			box_region_truncate(region);
			return 3;
		}
		box_region_truncate(region);

		/* The batch took all the input, or MAX_BATCH commands. */
		char **orpos, **owpos;
		box_ibuf_read_range(lp.out, &orpos, &owpos);
		size_t out_len = *owpos - *orpos;
		if (out_len > 0) {
			int rc = write_out(&lp);
			if (rc != 0) {
				lua_pushinteger(L, rc);
				return 1;
			}
		}
		set_last_cmd(&lp, fiber_clock());
		box_ibuf_read_range(lp.in, &rpos, &wpos);
		lp.replied = *rpos == *wpos;
		char **iwpos, **iepos, **owpos2, **oepos;
		box_ibuf_write_range(lp.in, &iwpos, &iepos);
		box_ibuf_write_range(lp.out, &owpos2, &oepos);
		if (len > lp.keep || out_len > lp.keep ||
		    (size_t)(*iepos - *rpos) > lp.keep ||
		    (size_t)(*oepos - *owpos2) > lp.keep) {
			lua_pushinteger(L, LOOP_KEEP);
			return 1;
		}
	}
}

void
mcc_dp_loop_open(lua_State *L)
{
	luaL_newmetatable(L, COUNTERS_MT);
	lua_pop(L, 1);
	lua_pushcfunction(L, dp_counters);
	lua_setfield(L, -2, "counters");
	lua_pushcfunction(L, dp_take);
	lua_setfield(L, -2, "take");
	lua_pushcfunction(L, dp_served);
	lua_setfield(L, -2, "served");

	/* The fiber module: see batch_clock(). */
	lua_getfield(L, LUA_REGISTRYINDEX, "_LOADED");
	lua_getfield(L, -1, "fiber");
	lua_remove(L, -2);
	if (!lua_istable(L, -1)) {
		/* No loop without it: server.lua keeps its own. */
		lua_pop(L, 1);
		return;
	}
	lua_pushliteral(L, "state");
	lua_pushliteral(L, "last_cmd");
	lua_pushliteral(L, "dp_gen");
	lua_pushliteral(L, "time");
	lua_pushliteral(L, "conn_read");
	lua_pushliteral(L, "conn_parse_cmd");
	lua_pushliteral(L, "conn_mwrite");
	/* The upvalues take the fiber module off the stack. */
	lua_pushcclosure(L, dp_loop, U_COUNT);
	lua_setfield(L, -2, "loop");
}
