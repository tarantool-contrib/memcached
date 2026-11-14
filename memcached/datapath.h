/*
 * memcached.datapath: the C batch as a C API, for C code built into the
 * same module (a source of the `datapath` module in app.manifest.toml).
 * TX thread only. See datapath.c and DESIGN.md, "The C data path".
 *
 * The batch serves the commands at the head of an input that it can serve
 * (parsed as proto/text.lua and proto/meta.lua parse them, run as
 * engine.lua runs them), appends their replies to an output buffer, and
 * stops at the first command it doesn't serve. What needs the Lua state is
 * read from and written to Lua values on the stack of L: the engine's
 * state table (cas_counter, stamp_last), the win tokens kept in memory,
 * the clock function and the keys held by the reads before the batch.
 */
#ifndef MCC_DATAPATH_H
#define MCC_DATAPATH_H

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#include <lua.h>
#include <module.h>

/* The counters of the served commands, as server.account() counts them. */
enum mcc_dp_stat {
	/* cmd_get: the keys of the retrievals (get, gets, gat, gats, mg). */
	MCC_DP_CMD_GET,
	MCC_DP_GET_HITS,
	/* The stores that stored: cmd_set and total_items. */
	MCC_DP_STORED,
	/* The stores that didn't (NOT_STORED, EXISTS, NOT_FOUND): cmd_set. */
	MCC_DP_UNSTORED,
	MCC_DP_CMD_TOUCH,
	MCC_DP_TOUCH_HITS,
	MCC_DP_DELETE_HITS,
	MCC_DP_DELETE_MISSES,
	MCC_DP_INCR_HITS,
	MCC_DP_INCR_MISSES,
	MCC_DP_DECR_HITS,
	MCC_DP_DECR_MISSES,
	MCC_DP_CAS_HITS,
	MCC_DP_CAS_MISSES,
	MCC_DP_CAS_BADVAL,
	MCC_DP_CMD_META,
	MCC_DP_STAT_COUNT,
};

/* A change of the items the reads of the batch hold (engine.lua held). */
struct mcc_dp_held_event {
	const char *key;
	uint32_t key_len;
	/* true: a hit holds the key; false: a write released it. */
	bool hit;
};

/*
 * One batch call. The caller sets the fields marked "in" (the others
 * zeroed), calls mcc_dp_batch_run() once or more, then
 * mcc_dp_batch_push_held(), within box_region_used() /
 * box_region_truncate(): the batch allocates on the box region.
 */
struct mcc_dp_batch {
	/* in: the Lua state and the stack indexes of the Lua values. */
	lua_State *L;
	/* in: the engine's state table (engine.lua state), required. */
	int state;
	/* in: state.tokens[<space>], 0 for none. */
	int tokens;
	/* in: the clock function (fiber.time), required. */
	int clock;
	/* in: a table {key = true}: the items held before, 0 for none. */
	int held_in;
	/* in: the data space and the output buffer. */
	uint32_t space_id;
	box_ibuf_t *out;
	/* in: the front-end's max_item_size. */
	double max_item_size;
	/* in: fiber.time(); read again after a write that yielded. */
	double now;
	/* in: the flush time, if any. */
	bool has_flush;
	double flush;
	/* in: whether the instance is read-only (no optional writes). */
	bool ro;
	/* in: the instance option track_access (the engine's env.track). */
	bool track;
	/* in: engine.LA_UPDATE_INTERVAL. */
	double la_interval;
	/* in: whether writes may be served: not read-only, no memtx MVCC. */
	bool store;
	/* in: the bucket id of the new tuples. */
	double bucket_id;
	/* out: the counters, added up over the calls. */
	uint32_t stats[MCC_DP_STAT_COUNT];
	/* Private. */
	uint64_t csw;
	bool held_cleared;
	struct mcc_dp_held_event *events;
	uint32_t nevents;
	uint32_t events_cap;
	/* The events from here on count: an mn dropped those before. */
	uint32_t events_base;
	/*
	 * The group: the writes of consecutive commands in one transaction
	 * (datapath.c, "The group"). Whether this run may open one, whether
	 * one is open, and its writes and the value bytes they wrote.
	 */
	bool group_ok;
	bool in_group;
	/* The command is the last of the run: its writes need no group. */
	bool cmd_last;
	uint32_t group_writes;
	size_t group_bytes;
	/* fiber_csw() at the open: whether the commit yielded. */
	uint64_t group_csw;
	/* The state before the group's first command, to go back to. */
	struct mcc_dp_mark {
		const char *pos;
		uint32_t commands;
		size_t out_used;
		uint32_t nevents;
		uint32_t events_base;
		bool held_cleared;
	} cmd_mark, group_mark;
	uint64_t group_counter;
	double group_stamp;
	uint32_t group_stats[MCC_DP_STAT_COUNT];
};

/*
 * Serve the commands at the head of [in, in + len), at most max_commands.
 * Returns the number of bytes consumed; *commands is the number of
 * commands served. The writes of the commands served are committed when
 * it returns (it may yield for that, as each write did when it was its
 * own statement), so their replies may go out.
 */
size_t
mcc_dp_batch_run(struct mcc_dp_batch *b, const char *in, size_t len,
		 uint32_t max_commands, uint32_t *commands);

/*
 * Push the items the reads hold after the batch onto L: a table
 * {key = true} (held_in itself when it is given, updated), or nil.
 */
void
mcc_dp_batch_push_held(struct mcc_dp_batch *b);

/*
 * Internal: add the C connection loop (cloop.c) to the module table at the
 * top of L's stack: loop(), counters(), take(), served().
 */
void
mcc_dp_loop_open(lua_State *L);

#endif /* MCC_DATAPATH_H */
