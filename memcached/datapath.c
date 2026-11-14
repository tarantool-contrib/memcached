/*
 * memcached.datapath: the C data path of the most common memcached commands
 * on a memtx data space. TX thread only.
 *
 * The entry points (DESIGN.md, "The C data path"):
 * - get() and set() serve one engine request that the text parser marked
 *   with `fast`; engine.lua calls them per request;
 * - batch() serves the commands at the head of a connection's input
 *   itself: it parses them, runs them and appends their replies to an
 *   output buffer, and stops at the first command it can't serve, which
 *   the Lua front-end then parses as usual. The classic get, gets, gat,
 *   gats, set, add, replace, append, prepend, cas, delete, incr, decr and
 *   touch, and the meta mg, ms, md, ma and mn, in the forms described at
 *   each; mcc_dp_batch_run() (datapath.h) is its C API;
 * - loop() (cloop.c, built into this module) is the connection loop of TX
 *   around that batch: it reads, runs the batch and writes in C.
 * Everything else, and every request the C code can't finish, goes
 * through the Lua engine. The semantics are the Lua ones: engine.lua ops,
 * is_dead(), next_cas(), stamp(), track_held(), proto/text.lua's and
 * proto/meta.lua's parsers and encoders. What needs the Lua state (the
 * clock, the CAS counter, the created stamp, the win tokens kept in
 * memory, the held items, the flush time and the read-only flag) is passed
 * in by engine.lua. The C code takes no win token: a command where one is
 * involved is left to Lua. The batch writes the commands it serves in one
 * transaction (see "The group").
 *
 * The box C API (module.h) only: no FFI, so a hot reload can require the
 * module again. A change of this file needs a restart.
 */
#include <math.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <string.h>

#include <lua.h>
#include <lauxlib.h>
#include <module.h>

#include "datapath.h"

/* Checked by engine.lua: the Lua side and the .so must agree on the API. */
#define DATAPATH_ABI 4

/* Field numbers of the data space (schema.lua), 1-based as in Lua. */
enum {
	F_KEY = 1,
	F_VALUE,
	F_FLAGS,
	F_EXPTIME,
	F_CAS,
	F_CREATED,
	F_LAST_ACCESS,
	F_FETCHED,
	F_STALE,
	F_BUCKET_ID,
	/* Format v3; nullable: a tuple of format v2 ends before it. */
	F_TOKEN,
	/* The fields of the tuples written here. */
	F_COUNT = F_TOKEN,
	/* The fields of every tuple (format v2). */
	F_MIN_COUNT = F_BUCKET_ID,
};

/* The primary index: the first one schema.create() makes. */
#define PRIMARY 0

/* Largest MsgPack header of a str, an array or a number. */
#define MP_HEADER_MAX 9

/* MsgPack --------------------------------------------------------------- */

static inline uint16_t
load_u16(const char *p)
{
	uint16_t v;
	memcpy(&v, p, sizeof(v));
	return __builtin_bswap16(v);
}

static inline uint32_t
load_u32(const char *p)
{
	uint32_t v;
	memcpy(&v, p, sizeof(v));
	return __builtin_bswap32(v);
}

static inline uint64_t
load_u64(const char *p)
{
	uint64_t v;
	memcpy(&v, p, sizeof(v));
	return __builtin_bswap64(v);
}

static inline char *
store_u16(char *p, uint16_t v)
{
	v = __builtin_bswap16(v);
	memcpy(p, &v, sizeof(v));
	return p + sizeof(v);
}

static inline char *
store_u32(char *p, uint32_t v)
{
	v = __builtin_bswap32(v);
	memcpy(p, &v, sizeof(v));
	return p + sizeof(v);
}

static inline char *
store_u64(char *p, uint64_t v)
{
	v = __builtin_bswap64(v);
	memcpy(p, &v, sizeof(v));
	return p + sizeof(v);
}

/*
 * Decoders. The tuple data is valid MsgPack (box checked it), so only the
 * types are checked: an unexpected one makes the caller give up and leave
 * the request to the Lua engine.
 */

static bool
dec_array(const char **p, uint32_t *n)
{
	uint8_t c = (uint8_t)**p;
	if ((c & 0xf0) == 0x90) {
		*n = c & 0x0f;
		*p += 1;
	} else if (c == 0xdc) {
		*n = load_u16(*p + 1);
		*p += 3;
	} else if (c == 0xdd) {
		*n = load_u32(*p + 1);
		*p += 5;
	} else {
		return false;
	}
	return true;
}

static bool
dec_str(const char **p, const char **s, uint32_t *len)
{
	uint8_t c = (uint8_t)**p;
	uint32_t l;
	int h;
	if ((c & 0xe0) == 0xa0) {
		l = c & 0x1f;
		h = 1;
	} else if (c == 0xd9) {
		l = (uint8_t)(*p)[1];
		h = 2;
	} else if (c == 0xda) {
		l = load_u16(*p + 1);
		h = 3;
	} else if (c == 0xdb) {
		l = load_u32(*p + 1);
		h = 5;
	} else {
		return false;
	}
	*s = *p + h;
	*len = l;
	*p += h + l;
	return true;
}

static bool
dec_uint(const char **p, uint64_t *v)
{
	uint8_t c = (uint8_t)**p;
	if (c <= 0x7f) {
		*v = c;
		*p += 1;
		return true;
	}
	switch (c) {
	case 0xcc:
		*v = (uint8_t)(*p)[1];
		*p += 2;
		return true;
	case 0xcd:
		*v = load_u16(*p + 1);
		*p += 3;
		return true;
	case 0xce:
		*v = load_u32(*p + 1);
		*p += 5;
		return true;
	case 0xcf:
		*v = load_u64(*p + 1);
		*p += 9;
		return true;
	}
	return false;
}

/* Any MsgPack number as a Lua number (a double), as Lua would read it. */
static bool
dec_num(const char **p, double *v)
{
	uint64_t u;
	if (dec_uint(p, &u)) {
		*v = (double)u;
		return true;
	}
	uint8_t c = (uint8_t)**p;
	if (c >= 0xe0) {
		*v = (int8_t)c;
		*p += 1;
		return true;
	}
	switch (c) {
	case 0xd0:
		*v = (int8_t)(*p)[1];
		*p += 2;
		return true;
	case 0xd1:
		*v = (int16_t)load_u16(*p + 1);
		*p += 3;
		return true;
	case 0xd2:
		*v = (int32_t)load_u32(*p + 1);
		*p += 5;
		return true;
	case 0xd3:
		*v = (double)(int64_t)load_u64(*p + 1);
		*p += 9;
		return true;
	case 0xca: {
		uint32_t bits = load_u32(*p + 1);
		float f;
		memcpy(&f, &bits, sizeof(f));
		*v = f;
		*p += 5;
		return true;
	}
	case 0xcb: {
		uint64_t bits = load_u64(*p + 1);
		memcpy(v, &bits, sizeof(*v));
		*p += 9;
		return true;
	}
	}
	return false;
}

static bool
dec_bool(const char **p, bool *v)
{
	uint8_t c = (uint8_t)**p;
	if (c != 0xc2 && c != 0xc3)
		return false;
	*v = c == 0xc3;
	*p += 1;
	return true;
}

/* Encoders: the MsgPack that Tarantool's Lua serializer writes (msgpuck). */

static char *
enc_array(char *p, uint32_t n)
{
	if (n <= 15) {
		*p = (char)(0x90 | n);
		return p + 1;
	} else if (n <= UINT16_MAX) {
		*p = (char)0xdc;
		return store_u16(p + 1, (uint16_t)n);
	}
	*p = (char)0xdd;
	return store_u32(p + 1, n);
}

/* The header of a str of len bytes. */
static char *
enc_str_header(char *p, uint32_t len)
{
	if (len <= 31) {
		*p++ = (char)(0xa0 | len);
	} else if (len <= UINT8_MAX) {
		*p++ = (char)0xd9;
		*p++ = (char)len;
	} else if (len <= UINT16_MAX) {
		*p++ = (char)0xda;
		p = store_u16(p, (uint16_t)len);
	} else {
		*p++ = (char)0xdb;
		p = store_u32(p, len);
	}
	return p;
}

static char *
enc_str(char *p, const char *s, uint32_t len)
{
	p = enc_str_header(p, len);
	memcpy(p, s, len);
	return p + len;
}

static char *
enc_uint(char *p, uint64_t v)
{
	if (v <= 0x7f) {
		*p = (char)v;
		return p + 1;
	} else if (v <= UINT8_MAX) {
		*p = (char)0xcc;
		p[1] = (char)v;
		return p + 2;
	} else if (v <= UINT16_MAX) {
		*p = (char)0xcd;
		return store_u16(p + 1, (uint16_t)v);
	} else if (v <= UINT32_MAX) {
		*p = (char)0xce;
		return store_u32(p + 1, (uint32_t)v);
	}
	*p = (char)0xcf;
	return store_u64(p + 1, v);
}

/* A negative integer, as mp_encode_int(). */
static char *
enc_neg(char *p, int64_t v)
{
	if (v >= -0x20) {
		*p = (char)(uint8_t)v;
		return p + 1;
	} else if (v >= INT8_MIN) {
		*p = (char)0xd0;
		p[1] = (char)(uint8_t)v;
		return p + 2;
	} else if (v >= INT16_MIN) {
		*p = (char)0xd1;
		return store_u16(p + 1, (uint16_t)v);
	} else if (v >= INT32_MIN) {
		*p = (char)0xd2;
		return store_u32(p + 1, (uint32_t)v);
	}
	*p = (char)0xd3;
	return store_u64(p + 1, (uint64_t)v);
}

static char *
enc_double(char *p, double v)
{
	uint64_t bits;
	memcpy(&bits, &v, sizeof(bits));
	*p = (char)0xcb;
	return store_u64(p + 1, bits);
}

static char *
enc_bool(char *p, bool v)
{
	*p = v ? (char)0xc3 : (char)0xc2;
	return p + 1;
}

/*
 * A Lua number the way the Lua serializer encodes it (luaL_tofield() in
 * src/lua/serializer.c): a non-integral finite number as a double, an
 * integral one as an unsigned or a negative integer when it fits, anything
 * else as a double. So a tuple stored here is byte for byte the one
 * box.space:replace() would store for the same Lua values.
 */
static char *
enc_lua_number(char *p, double num)
{
	double intpart;
	if (isfinite(num) && modf(num, &intpart) != 0.0)
		return enc_double(p, num);
	if (num >= 0 && num < 18446744073709551616.0)
		return enc_uint(p, (uint64_t)num);
	if (num >= -9223372036854775808.0 && num < 9223372036854775808.0)
		return enc_neg(p, (int64_t)num);
	return enc_double(p, num);
}

/* Decimal of an unsigned integer, without a terminating NUL. */
static char *
fmt_u64(char *p, uint64_t v)
{
	char tmp[20];
	int n = 0;
	do {
		tmp[n++] = (char)('0' + v % 10);
		v /= 10;
	} while (v != 0);
	while (n > 0)
		*p++ = tmp[--n];
	return p;
}


/* Decimal of a signed integer. */
static char *
fmt_i64(char *p, int64_t v)
{
	if (v < 0) {
		*p++ = '-';
		return fmt_u64(p, (uint64_t)0 - (uint64_t)v);
	}
	return fmt_u64(p, (uint64_t)v);
}

/* Items ------------------------------------------------------------------- */

/* The fields of a stored item the data path reads. */
struct item {
	const char *key;
	uint32_t key_len;
	const char *value;
	uint32_t value_len;
	uint64_t flags;
	double exptime;
	uint64_t cas;
	double created;
	double last_access;
	bool fetched;
	bool stale;
	/* The token field: the win token is out (false when absent). */
	bool token;
};

/* Skip a MsgPack nil, boolean, number or string. */
static bool
dec_skip(const char **p)
{
	uint8_t c = (uint8_t)**p;
	if (c <= 0x7f || c >= 0xe0 || c == 0xc0 || c == 0xc2 || c == 0xc3) {
		*p += 1;
		return true;
	}
	switch (c) {
	case 0xcc:
	case 0xd0:
		*p += 2;
		return true;
	case 0xcd:
	case 0xd1:
		*p += 3;
		return true;
	case 0xce:
	case 0xd2:
	case 0xca:
		*p += 5;
		return true;
	case 0xcf:
	case 0xd3:
	case 0xcb:
		*p += 9;
		return true;
	}
	const char *s;
	uint32_t len;
	return dec_str(p, &s, &len);
}

/* A boolean field that may be nil, as Lua tests it: nil is false. */
static bool
dec_opt_bool(const char **p, bool *v)
{
	if ((uint8_t)**p == 0xc0) {
		*v = false;
		*p += 1;
		return true;
	}
	return dec_bool(p, v);
}

static bool
item_decode(box_tuple_t *tuple, struct item *it)
{
	const char *p = box_tuple_data(tuple, NULL);
	uint32_t n;
	if (!dec_array(&p, &n) || n < F_MIN_COUNT)
		return false;
	if (!dec_str(&p, &it->key, &it->key_len) ||
	    !dec_str(&p, &it->value, &it->value_len) ||
	    !dec_uint(&p, &it->flags) ||
	    !dec_num(&p, &it->exptime) ||
	    !dec_uint(&p, &it->cas) ||
	    !dec_num(&p, &it->created) ||
	    !dec_num(&p, &it->last_access) ||
	    !dec_bool(&p, &it->fetched) ||
	    !dec_opt_bool(&p, &it->stale) ||
	    !dec_skip(&p))
		return false;
	it->token = false;
	return n < F_TOKEN || dec_opt_bool(&p, &it->token);
}

/* engine.is_dead(): expired, or created before a flush that has come. */
static bool
item_is_dead(const struct item *it, double now, bool has_flush, double flush)
{
	if (it->exptime != 0 && it->exptime <= now)
		return true;
	return has_flush && flush <= now && it->created <= flush;
}

/* engine.lua expired(). */
static bool
expired(double exptime, double now)
{
	return exptime != 0 && exptime <= now;
}

/* The key [key] of the primary index. */
static char *
enc_key(char *p, const char *key, uint32_t len)
{
	return enc_str(enc_array(p, 1), key, len);
}

/* The largest key: proto/text.lua MAX_KEY_LENGTH. */
#define MAX_KEY_LENGTH 250

/* The size of a key of the primary index of at most MAX_KEY_LENGTH bytes. */
#define KEY_BUF_SIZE (1 + MP_HEADER_MAX + MAX_KEY_LENGTH)

/* The largest VALUE line, without the key and the data. */
#define VALUE_LINE_MAX (sizeof("VALUE ") - 1 + 1 + 20 + 1 + 10 + 1 + 20 + 2)

/* The VALUE block of a hit: "VALUE <key> <flags> <bytes>[ <cas>]\r\n<data>\r\n". */
static size_t
value_size(const struct item *it)
{
	return VALUE_LINE_MAX + it->key_len + (size_t)it->value_len + 2;
}

static char *
put_value(char *q, const struct item *it, bool with_cas)
{
	memcpy(q, "VALUE ", 6);
	q += 6;
	memcpy(q, it->key, it->key_len);
	q += it->key_len;
	*q++ = ' ';
	q = fmt_u64(q, it->flags);
	*q++ = ' ';
	q = fmt_u64(q, it->value_len);
	if (with_cas) {
		*q++ = ' ';
		q = fmt_u64(q, it->cas);
	}
	*q++ = '\r';
	*q++ = '\n';
	memcpy(q, it->value, it->value_len);
	q += it->value_len;
	*q++ = '\r';
	*q++ = '\n';
	return q;
}

/* Whether a hit records the access (ops.get with bumps(env, req)). */
static bool
item_needs_bump(const struct item *it, double now, double la_interval)
{
	return !it->fetched || now - it->last_access >= la_interval;
}

/* The header of an update operation {'=', field, <value>}. */
static char *
enc_op(char *p, uint32_t field)
{
	p = enc_array(p, 3);
	p = enc_str(p, "=", 1);
	return enc_uint(p, field);
}

/* [['=', last_access, now], ['=', fetched, true]] */
static int
item_bump(uint32_t space_id, const char *kbuf, const char *kend, double now)
{
	char ops[64];
	char *o = enc_array(ops, 2);
	o = enc_lua_number(enc_op(o, F_LAST_ACCESS), now);
	o = enc_bool(enc_op(o, F_FETCHED), true);
	return box_update(space_id, PRIMARY, kbuf, kend, ops, o, 1, NULL);
}

/* The fields of a new item (engine.lua new_tuple()). */
struct new_item {
	const char *key;
	uint32_t key_len;
	/* The value: value, then value2 (the halves of an append). */
	const char *value;
	uint32_t value_len;
	const char *value2;
	uint32_t value2_len;
	double flags;
	double exptime;
	uint64_t cas;
	double created;
	double now;
	double bucket_id;
	bool stale;
	bool token;
};

/*
 * Replace the item with {key, value, flags, exptime, cas, created, now,
 * false, stale, bucket_id, token}: not fetched. One autocommit statement.
 * Returns box_replace()'s result.
 */
static int
item_store(uint32_t space_id, const struct new_item *n)
{
	size_t used = box_region_used();
	size_t size = 1 + 2 * MP_HEADER_MAX + n->key_len + n->value_len +
		      n->value2_len + 6 * MP_HEADER_MAX + 4;
	char *tuple = box_region_alloc(size);
	if (tuple == NULL)
		return -1;
	char *p = enc_array(tuple, F_COUNT);
	p = enc_str(p, n->key, n->key_len);
	p = enc_str_header(p, n->value_len + n->value2_len);
	memcpy(p, n->value, n->value_len);
	p += n->value_len;
	if (n->value2_len > 0) {
		memcpy(p, n->value2, n->value2_len);
		p += n->value2_len;
	}
	p = enc_lua_number(p, n->flags);
	p = enc_lua_number(p, n->exptime);
	p = enc_uint(p, n->cas);
	p = enc_lua_number(p, n->created);
	p = enc_lua_number(p, n->now);
	p = enc_bool(p, false);
	p = enc_bool(p, n->stale);
	p = enc_lua_number(p, n->bucket_id);
	p = enc_bool(p, n->token);
	int rc = box_replace(space_id, tuple, p, NULL);
	box_region_truncate(used);
	return rc;
}

/* Lua functions ----------------------------------------------------------- */

/* The flush time argument: nil (no flush) or a number. */
static bool
opt_flush(lua_State *L, int idx, double *flush)
{
	if (lua_isnoneornil(L, idx)) {
		*flush = 0;
		return false;
	}
	*flush = luaL_checknumber(L, idx);
	return true;
}

static uint32_t
check_len(lua_State *L, int idx, size_t len)
{
	if (len > UINT32_MAX)
		luaL_argerror(L, idx, "too long");
	return (uint32_t)len;
}

/*
 * get(space_id, key, now, flush, ro, bump, with_cas, la_interval)
 *   -> string   a hit: "VALUE <key> <flags> <bytes>[ <cas>]\r\n<data>\r\n"
 *   -> 0        a miss
 *   -> 1        a miss: a dead item was deleted
 *   -> nil      not done: run the request through the Lua engine
 *
 * As ops.get for a plain get: a dead item is deleted unless ro, and a hit
 * records the access (bump: the engine's bumps()) when the item hasn't been
 * fetched yet or the last access is la_interval or more ago. Every write is
 * one autocommit statement. When a write fails, nothing has changed and
 * nil is returned: the Lua engine then runs the request and handles the
 * failure (a read-only instance, a get without its optional write).
 */
static int
datapath_get(lua_State *L)
{
	uint32_t space_id = (uint32_t)luaL_checkinteger(L, 1);
	size_t len;
	const char *key = luaL_checklstring(L, 2, &len);
	uint32_t key_len = check_len(L, 2, len);
	double now = luaL_checknumber(L, 3);
	double flush;
	bool has_flush = opt_flush(L, 4, &flush);
	bool ro = lua_toboolean(L, 5);
	bool bump = lua_toboolean(L, 6);
	bool with_cas = lua_toboolean(L, 7);
	double la_interval = luaL_checknumber(L, 8);

	size_t used = box_region_used();
	char *kbuf = box_region_alloc(1 + MP_HEADER_MAX + key_len);
	if (kbuf == NULL)
		goto fallback;
	char *kend = enc_key(kbuf, key, key_len);

	box_tuple_t *tuple;
	if (box_index_get(space_id, PRIMARY, kbuf, kend, &tuple) != 0)
		goto fallback;
	if (tuple == NULL) {
		box_region_truncate(used);
		lua_pushinteger(L, 0);
		return 1;
	}
	struct item it;
	if (!item_decode(tuple, &it))
		goto fallback;
	if (item_is_dead(&it, now, has_flush, flush)) {
		if (ro) {
			box_region_truncate(used);
			lua_pushinteger(L, 0);
			return 1;
		}
		if (box_delete(space_id, PRIMARY, kbuf, kend, NULL) != 0)
			goto fallback;
		box_region_truncate(used);
		lua_pushinteger(L, 1);
		return 1;
	}

	/*
	 * The reply, copied out of the tuple before any write: the tuple is
	 * only guaranteed to live until the next box call.
	 */
	char *reply = box_region_alloc(value_size(&it));
	if (reply == NULL)
		goto fallback;
	char *q = put_value(reply, &it, with_cas);
	if (bump && !ro && item_needs_bump(&it, now, la_interval) &&
	    item_bump(space_id, kbuf, kend, now) != 0)
		goto fallback;
	lua_pushlstring(L, reply, q - reply);
	box_region_truncate(used);
	return 1;
fallback:
	box_region_truncate(used);
	lua_pushnil(L);
	return 1;
}

/*
 * set(space_id, key, value, flags, exptime, cas, created, now, flush,
 *     bucket_id) -> cas | nil
 *
 * As ops.store for a plain set (no CAS, not expired at once): cas is a new
 * value of the CAS counter, and the stored CAS is the larger of it and the
 * old live item's CAS + 1 (engine.next_cas()). The item
 * {key, value, flags, exptime, cas, created, now, false, false, bucket_id,
 * false} (not fetched, not stale, no win token) replaces the old one, live
 * or dead, in one autocommit statement. Returns the stored CAS, or nil when
 * nothing was stored: the Lua engine then runs the request.
 */
static int
datapath_set(lua_State *L)
{
	uint32_t space_id = (uint32_t)luaL_checkinteger(L, 1);
	struct new_item n;
	memset(&n, 0, sizeof(n));
	size_t len;
	n.key = luaL_checklstring(L, 2, &len);
	n.key_len = check_len(L, 2, len);
	n.value = luaL_checklstring(L, 3, &len);
	n.value_len = check_len(L, 3, len);
	n.flags = luaL_checknumber(L, 4);
	n.exptime = luaL_checknumber(L, 5);
	n.cas = luaL_checkuint64(L, 6);
	n.created = luaL_checknumber(L, 7);
	n.now = luaL_checknumber(L, 8);
	double flush;
	bool has_flush = opt_flush(L, 9, &flush);
	n.bucket_id = luaL_checknumber(L, 10);

	size_t used = box_region_used();
	char *kbuf = box_region_alloc(1 + MP_HEADER_MAX + n.key_len);
	if (kbuf == NULL)
		goto fallback;
	char *kend = enc_key(kbuf, n.key, n.key_len);

	box_tuple_t *old;
	if (box_index_get(space_id, PRIMARY, kbuf, kend, &old) != 0)
		goto fallback;
	if (old != NULL) {
		struct item it;
		if (!item_decode(old, &it))
			goto fallback;
		if (!item_is_dead(&it, n.now, has_flush, flush) &&
		    it.cas + 1 > n.cas)
			n.cas = it.cas + 1;
	}
	if (item_store(space_id, &n) != 0)
		goto fallback;
	box_region_truncate(used);
	luaL_pushuint64(L, n.cas);
	return 1;
fallback:
	box_region_truncate(used);
	lua_pushnil(L);
	return 1;
}

/* The batch --------------------------------------------------------------- */

/*
 * The text and meta protocols as proto/text.lua and proto/meta.lua parse
 * them, for the commands served here: only the forms whose every reply is
 * known are taken, anything else stops the batch and is left to the Lua
 * parser, which answers it (an error, a swallowed value, a silent close).
 * The parsers below refuse every form the Lua ones answer with an error,
 * and read the others to the same values.
 */

/* Only the first this many tokens of a line count (proto/text.lua). */
#define MAX_TOKENS 23
/* proto/meta.lua MAX_TOKENS: the command, the key and the flags. */
#define META_MAX_TOKENS 20
/* proto/meta.lua MAX_OPAQUE_LENGTH, the 'O' included. */
#define MAX_OPAQUE_LENGTH 32
/* engine.MAX_RELATIVE_TIME. */
#define MAX_RELATIVE_TIME 2592000
/* proto/text.lua item_size(): 48 + key + 1 + flags + value + 2 + 8. */
#define ITEM_OVERHEAD (48 + 1 + 2 + 8)
/* mcmc_tokto32 reads at most this many characters (TOKTO32_MAX). */
#define TOKTO32_MAX 11
/* mcmc_toktou64: TOKTO64_MAX. */
#define TOKTO64_MAX 22

/* The output -------------------------------------------------------------- */

static size_t
out_used(struct mcc_dp_batch *b)
{
	char **rpos, **wpos;
	box_ibuf_read_range(b->out, &rpos, &wpos);
	return *wpos - *rpos;
}

static void
out_truncate(struct mcc_dp_batch *b, size_t used)
{
	char **rpos, **wpos;
	box_ibuf_read_range(b->out, &rpos, &wpos);
	*wpos = *rpos + used;
}

/* Room for size bytes at the end of the output; NULL when out of memory. */
static char *
out_reserve(struct mcc_dp_batch *b, size_t size)
{
	return box_ibuf_reserve(b->out, size);
}

/* The output ends at end, in the room out_reserve() gave. */
static void
out_commit(struct mcc_dp_batch *b, char *end)
{
	char **wpos, **epos;
	box_ibuf_write_range(b->out, &wpos, &epos);
	*wpos = end;
}

static bool
out_put(struct mcc_dp_batch *b, const char *s, size_t len)
{
	char *q = out_reserve(b, len);
	if (q == NULL)
		return false;
	memcpy(q, s, len);
	out_commit(b, q + len);
	return true;
}

static char *
put(char *q, const char *s, size_t len)
{
	memcpy(q, s, len);
	return q + len;
}

#define PUT(q, lit) put((q), (lit), sizeof(lit) - 1)

/* State ------------------------------------------------------------------- */

/*
 * After a write or a commit: an autocommit statement or a commit yields
 * while it waits for the WAL, and then other fibers have run and the clock
 * has moved on. The Lua engine reads the clock per request (env.now =
 * fiber.time()): read it again through the same function, which only a
 * yield changes.
 */
static void
after_write(struct mcc_dp_batch *b)
{
	uint64_t csw = fiber_csw(NULL);
	if (csw == b->csw)
		return;
	b->csw = csw;
	lua_pushvalue(b->L, b->clock);
	lua_call(b->L, 0, 1);
	b->now = lua_tonumber(b->L, -1);
	lua_pop(b->L, 1);
}

/* The CTypeID of uint64_t, for the CAS counter. */
static uint32_t ctid_uint64;

static void
set_counter(struct mcc_dp_batch *b, uint64_t c)
{
	*(uint64_t *)luaL_pushcdata(b->L, ctid_uint64) = c;
	lua_setfield(b->L, b->state, "cas_counter");
}

/*
 * engine.lua counter(): the value of state.cas_counter, which moves on by
 * one. In the state table itself, read and written per write, so that the
 * fibers that run while a write yields take the next values. It stays a
 * uint64_t cdata (luaL_pushuint64() would push a small value as a Lua
 * number). false when the counter isn't set (engine.lua sets it before the
 * call).
 */
static bool
next_counter(struct mcc_dp_batch *b, uint64_t *cas)
{
	lua_State *L = b->L;
	lua_getfield(L, b->state, "cas_counter");
	uint64_t c = luaL_touint64(L, -1);
	lua_pop(L, 1);
	if (c == 0)
		return false;
	set_counter(b, c + 1);
	*cas = c;
	return true;
}

/* engine.lua stamp(now): state.stamp_last, strictly increasing. */
static double
next_stamp(struct mcc_dp_batch *b, double *last_out)
{
	lua_State *L = b->L;
	lua_getfield(L, b->state, "stamp_last");
	double last = lua_tonumber(L, -1);
	lua_pop(L, 1);
	double now = b->now;
	if (now <= last)
		now = last + 1e-6;
	lua_pushnumber(L, now);
	lua_setfield(L, b->state, "stamp_last");
	*last_out = last;
	return now;
}

/*
 * The state a write takes (a CAS value, a created stamp), given back when
 * the write fails before it yields: then no other fiber has seen it, and
 * the Lua engine that runs the command takes the same values.
 */
struct undo {
	uint64_t csw;
	uint64_t counter;
	double stamp_last;
	bool has_counter;
	bool has_stamp;
};

static void
undo_start(struct undo *u)
{
	u->csw = fiber_csw(NULL);
	u->has_counter = false;
	u->has_stamp = false;
}

static bool
undo_counter(struct mcc_dp_batch *b, struct undo *u, uint64_t *cas)
{
	if (!next_counter(b, cas))
		return false;
	u->counter = *cas;
	u->has_counter = true;
	return true;
}

static double
undo_stamp(struct mcc_dp_batch *b, struct undo *u)
{
	double now = next_stamp(b, &u->stamp_last);
	u->has_stamp = true;
	return now;
}

static void
undo_restore(struct mcc_dp_batch *b, const struct undo *u)
{
	if (fiber_csw(NULL) != u->csw)
		return;
	if (u->has_counter)
		set_counter(b, u->counter);
	if (u->has_stamp) {
		lua_pushnumber(b->L, u->stamp_last);
		lua_setfield(b->L, b->state, "stamp_last");
	}
}

/* The group --------------------------------------------------------------- */

/*
 * The writes of consecutive commands of a run go into one box transaction
 * (the group) instead of one autocommit statement each: one journal entry,
 * one WAL round trip and one fiber wakeup per group instead of per write
 * (DESIGN.md, "The C data path", "The group").
 *
 * - A group opens at the first write of a run (group_write()), when the
 *   run may serve writes (store), no win token is kept in memory (tokens:
 *   forget_win() changes Lua state that a rollback wouldn't give back) and
 *   no transaction is active (the caller's then has the writes, as
 *   before). It is committed when the run returns, the commands served
 *   until then included, or before, after the command that takes it to
 *   GROUP_MAX_WRITES writes or GROUP_MAX_BYTES of values; the next write
 *   opens the next one. The writes of the last command of the input are
 *   autocommit statements (cmd_last), as before: there is mostly one,
 *   and a transaction would only add its cost. An mn
 *   doesn't end a group: it releases the holds (events_base).
 * - memtx without MVCC: the reads of the group see its writes, and nothing
 *   in a run yields but a commit (a yield would abort the transaction: its
 *   next statement or its commit fails, as below).
 * - A failed statement is rolled back by box as a statement (the
 *   transaction stays), the command gives back what it took (undo) and is
 *   left to Lua, as when its autocommit statement failed: the run stops
 *   there and commits the commands before it. So an out of memory goes to
 *   the Lua engine, which evicts and runs the command again.
 * - A failed commit (a WAL error, a cascading rollback, a transaction
 *   aborted by a yield) rolls the whole group back: the run goes back to
 *   the state before the group's first command (input, replies, counters,
 *   holds) and stops there, so those commands and the rest are the Lua
 *   front-end's, which answers each as if C had never been tried: what each
 *   would have answered after its own failed autocommit write. The CAS
 *   counter and the created stamp are given back too, unless the commit
 *   yielded: then other fibers may have taken the values after them.
 */

/* A group is committed after the command that takes it to this many. */
#define GROUP_MAX_WRITES 1024
#define GROUP_MAX_BYTES (1024 * 1024)

/* The state before a command, which a group goes back to. */
static void
mark_command(struct mcc_dp_batch *b, const char *pos, uint32_t commands)
{
	struct mcc_dp_mark *m = &b->cmd_mark;
	m->pos = pos;
	m->commands = commands;
	m->out_used = out_used(b);
	m->nevents = b->nevents;
	m->events_base = b->events_base;
	m->held_cleared = b->held_cleared;
}

/*
 * Before a write of the command marked last (mark_command()): open a
 * group, unless one is open or the run has none. Without a group (a failed
 * box_txn_begin() too) the write is an autocommit statement.
 */
static void
group_write(struct mcc_dp_batch *b)
{
	if (b->in_group) {
		b->group_writes++;
		return;
	}
	/*
	 * Nothing after this command: its writes go out at once anyway, as
	 * autocommit statements (cheaper for a single write).
	 */
	if (!b->group_ok || b->cmd_last)
		return;
	if (box_txn_begin() != 0) {
		b->group_ok = false;
		return;
	}
	lua_State *L = b->L;
	b->in_group = true;
	b->group_writes = 1;
	b->group_bytes = 0;
	b->group_csw = fiber_csw(NULL);
	b->group_mark = b->cmd_mark;
	memcpy(b->group_stats, b->stats, sizeof(b->stats));
	lua_getfield(L, b->state, "cas_counter");
	b->group_counter = luaL_touint64(L, -1);
	lua_getfield(L, b->state, "stamp_last");
	b->group_stamp = lua_tonumber(L, -1);
	lua_pop(L, 2);
}

/*
 * Commit the group. Returns false when the commit failed: then nothing of
 * the group is left (see above), and the run stops at its first command
 * (group_mark).
 */
static bool
group_commit(struct mcc_dp_batch *b)
{
	b->in_group = false;
	int rc = box_txn_commit();
	if (rc != 0 && box_txn())
		box_txn_rollback();
	bool yielded = fiber_csw(NULL) != b->group_csw;
	after_write(b);
	if (rc == 0)
		return true;
	if (!yielded) {
		if (b->group_counter != 0)
			set_counter(b, b->group_counter);
		lua_pushnumber(b->L, b->group_stamp);
		lua_setfield(b->L, b->state, "stamp_last");
	}
	const struct mcc_dp_mark *m = &b->group_mark;
	out_truncate(b, m->out_used);
	b->nevents = m->nevents;
	b->events_base = m->events_base;
	b->held_cleared = m->held_cleared;
	memcpy(b->stats, b->group_stats, sizeof(b->stats));
	b->group_ok = false;
	return false;
}

/*
 * No live item: drop a win token kept in memory for the key, if any
 * (engine.lua forget_win()).
 */
static void
forget_win(struct mcc_dp_batch *b, const char *key, uint32_t key_len)
{
	if (b->tokens == 0)
		return;
	lua_pushlstring(b->L, key, key_len);
	lua_pushnil(b->L);
	lua_rawset(b->L, b->tokens);
}

/*
 * Whether a win token is kept in memory for the key: then token_sent()
 * decides, which the Lua engine runs.
 */
static bool
token_kept(struct mcc_dp_batch *b, const char *key, uint32_t key_len)
{
	if (b->tokens == 0)
		return false;
	lua_pushlstring(b->L, key, key_len);
	lua_rawget(b->L, b->tokens);
	bool kept = !lua_isnil(b->L, -1);
	lua_pop(b->L, 1);
	return kept;
}

/* Held items ------------------------------------------------------------ */

/*
 * The items the reads of the batch hold (engine.lua track_held()): those
 * of held_in, unless an mn released them (held_cleared), changed by the
 * events of the batch, in their order: a hit holds its key, a write that
 * replaces or deletes the item releases it.
 */

/* Room for one more event, so that held_add() can't fail after a write. */
static bool
held_reserve(struct mcc_dp_batch *b)
{
	if (b->nevents < b->events_cap)
		return true;
	uint32_t cap = b->events_cap == 0 ? 64 : 2 * b->events_cap;
	struct mcc_dp_held_event *events = box_region_aligned_alloc(
		cap * sizeof(*events), _Alignof(struct mcc_dp_held_event));
	if (events == NULL)
		return false;
	if (b->nevents > 0)
		memcpy(events, b->events, b->nevents * sizeof(*events));
	b->events = events;
	b->events_cap = cap;
	return true;
}

static bool
held_base(const struct mcc_dp_batch *b)
{
	return b->held_in != 0 && !b->held_cleared;
}

/* After held_reserve(). The key must live until the end of the call. */
static void
held_add(struct mcc_dp_batch *b, const char *key, uint32_t key_len, bool hit)
{
	/* A release matters only after a hold. */
	if (!hit && b->nevents == b->events_base && !held_base(b))
		return;
	struct mcc_dp_held_event *e = &b->events[b->nevents++];
	e->key = key;
	e->key_len = key_len;
	e->hit = hit;
}

static bool
held_has(struct mcc_dp_batch *b, const char *key, uint32_t key_len)
{
	for (uint32_t i = b->nevents; i-- > b->events_base;) {
		const struct mcc_dp_held_event *e = &b->events[i];
		if (e->key_len == key_len && memcmp(e->key, key, key_len) == 0)
			return e->hit;
	}
	if (!held_base(b))
		return false;
	lua_pushlstring(b->L, key, key_len);
	lua_rawget(b->L, b->held_in);
	bool held = lua_toboolean(b->L, -1);
	lua_pop(b->L, 1);
	return held;
}

/*
 * mn: memcached writes the replies out, which release their items. The
 * events before stay in the array (a failed group commit goes back to
 * them), past events_base.
 */
static void
held_release_all(struct mcc_dp_batch *b)
{
	b->events_base = b->nevents;
	b->held_cleared = true;
}

void
mcc_dp_batch_push_held(struct mcc_dp_batch *b)
{
	lua_State *L = b->L;
	bool base = held_base(b);
	if (b->nevents == b->events_base) {
		if (base)
			lua_pushvalue(L, b->held_in);
		else
			lua_pushnil(L);
		return;
	}
	if (base)
		lua_pushvalue(L, b->held_in);
	else
		lua_createtable(L, 0, b->nevents - b->events_base);
	for (uint32_t i = b->events_base; i < b->nevents; i++) {
		struct mcc_dp_held_event *e = &b->events[i];
		lua_pushlstring(L, e->key, e->key_len);
		if (e->hit)
			lua_pushboolean(L, 1);
		else
			lua_pushnil(L);
		lua_rawset(L, -3);
	}
}

/* Numbers ----------------------------------------------------------------- */

static bool
is_digit(char c)
{
	return (unsigned)((unsigned char)c - '0') <= 9;
}

/* Lua's %s (lj_char_isspace): the space and \t \n \v \f \r. */
static bool
is_space(char c)
{
	return c == ' ' || (unsigned)((unsigned char)c - '\t') <= '\r' - '\t';
}

/*
 * The digits of [s, s + len) as an unsigned number, false when there is
 * anything else, more than max_len of them or the value is above max. The
 * only forms of the storage commands' numbers taken here: the parser's
 * readings (mcmc_tokto32 for the length, strtoul and strtol for the flags
 * and the exptime) agree on them.
 */
static bool
parse_digits(const char *s, uint32_t len, uint32_t max_len, uint64_t max,
	     uint64_t *v)
{
	uint64_t x = 0;
	if (len == 0 || len > max_len)
		return false;
	for (uint32_t i = 0; i < len; i++) {
		unsigned d = (unsigned char)s[i] - '0';
		if (d > 9)
			return false;
		x = x * 10 + d;
		if (x > max)
			return false;
	}
	*v = x;
	return true;
}

/*
 * proto/text.lua and proto/meta.lua parse_i32() (mcmc_tokto32): at most 11
 * characters, an optional '-', then digits (none: 0), a signed 32-bit
 * value.
 */
static bool
parse_i32(const char *s, uint32_t len, int64_t *v)
{
	if (len > TOKTO32_MAX)
		return false;
	bool neg = len > 0 && s[0] == '-';
	int64_t x = 0;
	for (uint32_t i = neg; i < len; i++) {
		if (!is_digit(s[i]))
			return false;
		x = x * 10 + (s[i] - '0');
	}
	if (neg)
		x = -x;
	if (x < INT32_MIN || x > INT32_MAX)
		return false;
	*v = x;
	return true;
}

/* proto/meta.lua parse_u32(): at most 11 digits, at most 2^32-1. */
static bool
parse_u32(const char *s, uint32_t len, uint64_t *v)
{
	if (len > TOKTO32_MAX)
		return false;
	uint64_t x = 0;
	for (uint32_t i = 0; i < len; i++) {
		if (!is_digit(s[i]))
			return false;
		x = x * 10 + (uint64_t)(s[i] - '0');
	}
	if (x > UINT32_MAX)
		return false;
	*v = x;
	return true;
}

/* The digits [s, s + len) as a uint64; false above 2^64-1. */
static bool
digits_u64(const char *s, uint32_t len, uint64_t *v)
{
	uint64_t x = 0;
	for (uint32_t i = 0; i < len; i++) {
		uint64_t d = (uint64_t)(s[i] - '0');
		if (x > (UINT64_MAX - d) / 10)
			return false;
		x = x * 10 + d;
	}
	*v = x;
	return true;
}

/*
 * proto/text.lua and proto/meta.lua parse_u64() (mcmc_toktou64): 1 to 22
 * digits, at most 2^64-1.
 */
static bool
parse_u64(const char *s, uint32_t len, uint64_t *v)
{
	if (len == 0 || len > TOKTO64_MAX)
		return false;
	for (uint32_t i = 0; i < len; i++) {
		if (!is_digit(s[i]))
			return false;
	}
	return digits_u64(s, len, v);
}

/*
 * engine.lua value_u64(): the value of an item as memcached's
 * safe_strtoull() reads it. Leading whitespace, an optional sign, digits,
 * then the end, a NUL or whitespace (and anything after it). A '-' wraps,
 * and a result negative as an int64 is refused.
 */
static bool
value_u64(const char *s, uint32_t len, uint64_t *v)
{
	uint32_t i = 0;
	while (i < len && is_space(s[i]))
		i++;
	bool neg = false;
	if (i < len && (s[i] == '+' || s[i] == '-')) {
		neg = s[i] == '-';
		i++;
	}
	uint32_t start = i;
	while (i < len && is_digit(s[i]))
		i++;
	if (i == start)
		return false;
	if (i < len && s[i] != '\0' && !is_space(s[i]))
		return false;
	while (start < i && s[start] == '0')
		start++;
	uint64_t x;
	if (i - start > 20 || !digits_u64(s + start, i - start, &x))
		return false;
	if (neg) {
		x = (uint64_t)0 - x;
		if (x >= (uint64_t)1 << 63)
			return false;
	}
	*v = x;
	return true;
}

/* engine.abs_time(): 0 never, -1 already expired, relative up to 30 days. */
static double
abs_time(const struct mcc_dp_batch *b, int64_t raw)
{
	if (raw == 0)
		return 0;
	if (raw < 0)
		return -1;
	if (raw <= MAX_RELATIVE_TIME)
		return b->now + (double)raw;
	return (double)raw;
}

/* proto/text.lua item_size(). */
static double
item_size(uint32_t key_len, uint64_t flags, double value_len)
{
	return (double)ITEM_OVERHEAD + key_len + (flags != 0 ? 4 : 0) +
	       value_len;
}

/* Tokens ------------------------------------------------------------------ */

/* A token of a line: [s, s + len). */
struct token {
	const char *s;
	uint32_t len;
};

/*
 * Split [p, end) on spaces, as the parsers do: up to max tokens, the
 * rest is not looked at. Returns the number of tokens.
 */
static uint32_t
split(const char *p, const char *end, struct token *tokens, uint32_t max)
{
	uint32_t n = 0;
	while (n < max) {
		while (p < end && *p == ' ')
			p++;
		if (p == end)
			break;
		const char *s = p;
		while (p < end && *p != ' ')
			p++;
		tokens[n].s = s;
		tokens[n].len = (uint32_t)(p - s);
		n++;
	}
	return n;
}

static bool
is_noreply(const struct token *t)
{
	return t->len == 7 && memcmp(t->s, "noreply", 7) == 0;
}

/* base64, as proto/meta.lua (memcached's base64.c) ---------------------- */

static const char B64[] =
	"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

/* The value of a base64 character, 0 for '=', -1 outside the alphabet. */
static int
b64_value(unsigned char c)
{
	if (c >= 'A' && c <= 'Z')
		return c - 'A';
	if (c >= 'a' && c <= 'z')
		return c - 'a' + 26;
	if (c >= '0' && c <= '9')
		return c - '0' + 52;
	if (c == '+')
		return 62;
	if (c == '/')
		return 63;
	if (c == '=')
		return 0;
	return -1;
}

static size_t
b64_encoded_len(uint32_t len)
{
	return 4 * (((size_t)len + 2) / 3);
}

static char *
b64_encode(char *q, const char *s, uint32_t len)
{
	const unsigned char *p = (const unsigned char *)s;
	uint32_t i = 0;
	for (; i + 2 < len; i += 3) {
		*q++ = B64[p[i] >> 2];
		*q++ = B64[((p[i] & 3) << 4) | (p[i + 1] >> 4)];
		*q++ = B64[((p[i + 1] & 15) << 2) | (p[i + 2] >> 6)];
		*q++ = B64[p[i + 2] & 63];
	}
	if (len - i == 1) {
		*q++ = B64[p[i] >> 2];
		*q++ = B64[(p[i] & 3) << 4];
		*q++ = '=';
		*q++ = '=';
	} else if (len - i == 2) {
		*q++ = B64[p[i] >> 2];
		*q++ = B64[((p[i] & 3) << 4) | (p[i + 1] >> 4)];
		*q++ = B64[(p[i + 1] & 15) << 2];
		*q++ = '=';
	}
	return q;
}

/*
 * meta.lua b64decode(): characters outside the alphabet are skipped, a
 * multiple of 4 alphabet characters is needed, the first padded block
 * ends it. The result into out (at least len bytes); -1 when there is
 * none.
 */
static int64_t
b64_decode(const char *s, uint32_t len, char *out)
{
	uint32_t count = 0;
	for (uint32_t i = 0; i < len; i++) {
		if (b64_value((unsigned char)s[i]) >= 0)
			count++;
	}
	if (count == 0 || count % 4 != 0)
		return -1;
	int64_t n = 0;
	int k = 0, pad = 0;
	int v[3] = {0, 0, 0};
	for (uint32_t i = 0; i < len; i++) {
		unsigned char c = (unsigned char)s[i];
		int x = b64_value(c);
		if (x < 0)
			continue;
		if (c == '=')
			pad++;
		if (k < 3) {
			v[k++] = x;
			continue;
		}
		k = 0;
		char chunk[3] = {
			(char)(((v[0] << 2) | (v[1] >> 4)) & 255),
			(char)(((v[1] << 4) | (v[2] >> 2)) & 255),
			(char)(((v[2] << 6) | x) & 255),
		};
		if (pad > 0) {
			if (pad > 2)
				return -1;
			memcpy(out + n, chunk, 3 - pad);
			n += 3 - pad;
			break;
		}
		memcpy(out + n, chunk, 3);
		n += 3;
	}
	return n == 0 ? -1 : n;
}

/* Meta flags ------------------------------------------------------------- */

#define FLAG(c) ((uint64_t)1 << ((c) - 'A'))

/* The flags reflected in the replies (proto/meta.lua RET_*). */
#define RET_MG (FLAG('s') | FLAG('t') | FLAG('c') | FLAG('f') | FLAG('l') | \
		FLAG('h') | FLAG('O') | FLAG('k'))
#define RET_MS (FLAG('O') | FLAG('k') | FLAG('c') | FLAG('s'))
#define RET_MD (FLAG('O') | FLAG('k'))
#define RET_MA (FLAG('c') | FLAG('t') | FLAG('O') | FLAG('k'))

/* The flags of a meta command (proto/meta.lua preparse()). */
struct meta {
	/* The key: the token or its base64 decoding (b, on the region). */
	const char *key;
	uint32_t key_len;
	/* The flags given, FLAG(<letter>). */
	uint64_t seen;
	int64_t T;
	uint64_t F;
	uint64_t C;
	uint64_t D;
	/* The character after M, 0 for none. */
	char M;
	/* The tokens of the flags reflected in the reply, in their order. */
	struct token ret[META_MAX_TOKENS];
	uint32_t nret;
};

static bool
has(const struct meta *m, char c)
{
	return (m->seen & FLAG(c)) != 0;
}

/*
 * preparse() of the tokens t[0..n) (the command, the key, the flags from
 * t[first]), and its checks before it (bad_flag_chars()), the token count
 * and the opaque length: false for every form proto/meta.lua answers with
 * an error. The argument of N, R, E and J is checked, not kept: the
 * commands served here refuse those flags or ignore them.
 */
static bool
meta_parse(const struct token *t, uint32_t n, uint32_t first,
	   uint64_t ret_set, struct meta *m)
{
	if (n < 2 || n > META_MAX_TOKENS || t[1].len > MAX_KEY_LENGTH)
		return false;
	m->key = t[1].s;
	m->key_len = t[1].len;
	m->seen = 0;
	m->T = 0;
	m->F = 0;
	m->C = 0;
	m->D = 0;
	m->M = 0;
	m->nret = 0;
	for (uint32_t i = first; i < n; i++) {
		unsigned char c = (unsigned char)t[i].s[0];
		/* An unknown flag, or not a letter (bad_flag_chars()). */
		if (c < 'A' || c > 'z')
			return false;
		uint64_t bit = FLAG(c);
		if (m->seen & bit)
			return false;
		m->seen |= bit;
		if (ret_set & bit)
			m->ret[m->nret++] = t[i];
		const char *a = t[i].s + 1;
		uint32_t alen = t[i].len - 1;
		uint64_t u;
		int64_t s;
		switch (c) {
		case 'l': case 'h': case 'P': case 'L': case 'k': case 's':
		case 't': case 'c': case 'f': case 'v': case 'u': case 'q':
		case 'x': case 'I': case 'b':
			break;
		case 'O':
			if (t[i].len > MAX_OPAQUE_LENGTH)
				return false;
			break;
		case 'N': case 'T': case 'R':
			if (alen == 0 || !parse_i32(a, alen, &s))
				return false;
			if (c == 'T')
				m->T = s;
			break;
		case 'F':
			if (alen == 0 || !parse_u32(a, alen, &u))
				return false;
			m->F = u;
			break;
		case 'C': case 'E': case 'J': case 'D':
			if (alen == 0 || !parse_u64(a, alen, &u))
				return false;
			if (c == 'C')
				m->C = u;
			else if (c == 'D')
				m->D = u;
			break;
		case 'M':
			m->M = alen > 0 ? a[0] : 0;
			break;
		default:
			return false;
		}
	}
	if (has(m, 'b')) {
		char *key = box_region_alloc(m->key_len);
		if (key == NULL)
			return false;
		int64_t len = b64_decode(m->key, m->key_len, key);
		if (len < 0)
			return false;
		m->key = key;
		m->key_len = (uint32_t)len;
	}
	return true;
}

/* The size of the key flag of a reply: " k<key>" or " k<base64> b". */
static size_t
key_flag_size(const struct meta *m)
{
	return 2 + b64_encoded_len(m->key_len) + 2;
}

static char *
put_key_flag(char *q, const struct meta *m)
{
	q = PUT(q, " k");
	if (!has(m, 'b'))
		return put(q, m->key, m->key_len);
	q = b64_encode(q, m->key, m->key_len);
	return PUT(q, " b");
}

/* The bound of the flags of a reply: the key, the tokens, the numbers. */
static size_t
ret_size(const struct meta *m)
{
	size_t size = key_flag_size(m);
	for (uint32_t i = 0; i < m->nret; i++)
		size += 1 + m->ret[i].len + 22;
	return size;
}

/* The O and k flags, the only ones in a reply without an item. */
static char *
put_opaque_and_key(char *q, const struct meta *m)
{
	for (uint32_t i = 0; i < m->nret; i++) {
		const struct token *t = &m->ret[i];
		if (t->s[0] == 'O') {
			*q++ = ' ';
			q = put(q, t->s, t->len);
		} else if (t->s[0] == 'k') {
			q = put_key_flag(q, m);
		}
	}
	return q;
}

/*
 * meta.lua ttl_str(): the TTL left, -1 for none; seconds_since(). false
 * when the number is out of the range of an integer (the C code leaves
 * such an item to Lua).
 */
static bool
put_ttl(char **q, double exptime, double now)
{
	if (exptime == 0) {
		*q = PUT(*q, "-1");
		return true;
	}
	double ttl = ceil(exptime - now);
	if (ttl < 0)
		ttl = 0;
	if (!(ttl < 9e18))
		return false;
	*q = fmt_i64(*q, (int64_t)ttl);
	return true;
}

static bool
put_since(char **q, double t, double now)
{
	double d = floor(now - t);
	if (d < 0)
		d = 0;
	if (!(d < 9e18))
		return false;
	*q = fmt_i64(*q, (int64_t)d);
	return true;
}

/* Reads ------------------------------------------------------------------- */

/* The reply of a hit of get_key(). */
enum emit {
	/* None: the caller replies. */
	EMIT_NONE,
	/* A VALUE block (get, gat). */
	EMIT_VALUE,
	/* A VALUE block with the CAS (gets, gats). */
	EMIT_VALUE_CAS,
	/* An mg reply. */
	EMIT_MG,
};

/* One key of a read: an engine get request. */
struct get_req {
	const char *key;
	uint32_t key_len;
	/* touch_ttl: the new absolute exptime. */
	bool touch;
	double exptime;
	/* bumps(env, req). */
	bool bump;
	/* mg: a get with win_token. */
	bool win_token;
	enum emit emit;
	const struct meta *meta;
};

enum get_result {
	GET_MISS,
	GET_HIT,
	/* Not done: the command is left to Lua. */
	GET_STOP,
};

/* meta.lua encode_mg() of a hit: the item as before the access. */
static bool
emit_mg(struct mcc_dp_batch *b, const struct item *it, const struct meta *m)
{
	bool with_value = has(m, 'v') && !(has(m, 'C') && it->cas == m->C);
	size_t size = 3 + 10 + ret_size(m) + 2 + it->value_len + 2;
	char *q = out_reserve(b, size);
	if (q == NULL)
		return false;
	if (with_value) {
		q = PUT(q, "VA ");
		q = fmt_u64(q, it->value_len);
	} else {
		q = PUT(q, "HD");
	}
	for (uint32_t i = 0; i < m->nret; i++) {
		const struct token *t = &m->ret[i];
		switch (t->s[0]) {
		case 's':
			q = PUT(q, " s");
			q = fmt_u64(q, it->value_len);
			break;
		case 't':
			q = PUT(q, " t");
			if (!put_ttl(&q, it->exptime, b->now))
				return false;
			break;
		case 'c':
			q = PUT(q, " c");
			q = fmt_u64(q, it->cas);
			break;
		case 'f':
			q = PUT(q, " f");
			q = fmt_u64(q, it->flags);
			break;
		case 'l':
			q = PUT(q, " l");
			if (!put_since(&q, it->last_access, b->now))
				return false;
			break;
		case 'h':
			q = it->fetched ? PUT(q, " h1") : PUT(q, " h0");
			break;
		case 'O':
			*q++ = ' ';
			q = put(q, t->s, t->len);
			break;
		default:
			q = put_key_flag(q, m);
			break;
		}
	}
	q = PUT(q, "\r\n");
	if (with_value) {
		q = put(q, it->value, it->value_len);
		q = PUT(q, "\r\n");
	}
	out_commit(b, q);
	return true;
}

static bool
emit_hit(struct mcc_dp_batch *b, const struct get_req *g,
	 const struct item *it)
{
	if (g->emit == EMIT_MG)
		return emit_mg(b, it, g->meta);
	char *q = out_reserve(b, value_size(it));
	if (q == NULL)
		return false;
	out_commit(b, put_value(q, it, g->emit == EMIT_VALUE_CAS));
	return true;
}

/*
 * One key of a read, as ops.get for a get without vivify, recache and
 * peek: the lazy deletion of a dead item (unless ro), the touch, the last
 * access. A hit's reply (g->emit) goes out before the writes, which may
 * free the tuple; when a write fails, it is taken back and nothing has
 * changed. An mg is left to Lua when a win token is involved: the item is
 * stale or its token is out (Z, X, W).
 */
static enum get_result
get_key(struct mcc_dp_batch *b, const struct get_req *g)
{
	char kbuf[KEY_BUF_SIZE];
	char *kend = enc_key(kbuf, g->key, g->key_len);
	box_tuple_t *tuple;
	if (box_index_get(b->space_id, PRIMARY, kbuf, kend, &tuple) != 0)
		return GET_STOP;
	if (tuple == NULL) {
		forget_win(b, g->key, g->key_len);
		return GET_MISS;
	}
	struct item it;
	if (!item_decode(tuple, &it))
		return GET_STOP;
	if (item_is_dead(&it, b->now, b->has_flush, b->flush)) {
		if (!b->ro) {
			group_write(b);
			if (box_delete(b->space_id, PRIMARY, kbuf, kend,
				       NULL) != 0)
				return GET_STOP;
			after_write(b);
		}
		forget_win(b, g->key, g->key_len);
		return GET_MISS;
	}
	if (g->win_token && (it.stale || it.token ||
			     token_kept(b, g->key, g->key_len)))
		return GET_STOP;
	if (g->touch)
		it.exptime = g->exptime;
	size_t used = out_used(b);
	if (g->emit != EMIT_NONE && !emit_hit(b, g, &it)) {
		out_truncate(b, used);
		return GET_STOP;
	}
	if (g->touch && expired(g->exptime, b->now)) {
		/* A touch into the past deletes the item: still a hit. */
		group_write(b);
		if (box_delete(b->space_id, PRIMARY, kbuf, kend, NULL) != 0) {
			out_truncate(b, used);
			return GET_STOP;
		}
		after_write(b);
		return GET_HIT;
	}
	bool bump = g->bump && !b->ro &&
		    item_needs_bump(&it, b->now, b->la_interval);
	if (!g->touch && !bump)
		return GET_HIT;
	group_write(b);
	int rc;
	if (g->touch) {
		char ops[96];
		char *o = enc_array(ops, bump ? 3 : 1);
		o = enc_lua_number(enc_op(o, F_EXPTIME), g->exptime);
		if (bump) {
			o = enc_lua_number(enc_op(o, F_LAST_ACCESS), b->now);
			o = enc_bool(enc_op(o, F_FETCHED), true);
		}
		rc = box_update(b->space_id, PRIMARY, kbuf, kend, ops, o, 1,
				NULL);
	} else {
		rc = item_bump(b->space_id, kbuf, kend, b->now);
	}
	if (rc != 0) {
		out_truncate(b, used);
		return GET_STOP;
	}
	after_write(b);
	return GET_HIT;
}

/*
 * get|gets <key>* and gat|gats <exptime> <key>*: the arguments are
 * [p, end). Every key is checked before any is looked up, as the parser
 * checks them. Returns false when the command is left to Lua: then its
 * output and its held keys are dropped and it isn't counted (the side
 * writes done for its first keys are those that Lua makes again, so it
 * answers the same). A gat that expires its items at once is left to Lua,
 * whose answer to the keys after a failure would depend on them.
 */
static bool
do_retrieve(struct mcc_dp_batch *b, const char *p, const char *end,
	    bool with_cas, bool touch)
{
	int64_t ttl = 0;
	if (touch) {
		while (p < end && *p == ' ')
			p++;
		const char *s = p;
		while (p < end && *p != ' ')
			p++;
		if (!b->store || !parse_i32(s, (uint32_t)(p - s), &ttl) ||
		    expired(abs_time(b, ttl), b->now))
			return false;
	}
	uint32_t nkeys = 0;
	for (const char *s = p; s < end;) {
		if (*s == ' ') {
			s++;
			continue;
		}
		const char *k = s;
		while (s < end && *s != ' ')
			s++;
		if (s - k > MAX_KEY_LENGTH)
			return false;
		nkeys++;
	}
	if (nkeys == 0)
		return false;
	size_t used = out_used(b);
	uint32_t nevents = b->nevents;
	uint32_t hits = 0;
	struct get_req g = {
		.touch = touch,
		.bump = b->track,
		.emit = with_cas ? EMIT_VALUE_CAS : EMIT_VALUE,
	};
	for (const char *s = p; s < end;) {
		if (*s == ' ') {
			s++;
			continue;
		}
		const char *k = s;
		while (s < end && *s != ' ')
			s++;
		g.key = k;
		g.key_len = (uint32_t)(s - k);
		/* Per key, as the engine reads the clock per request. */
		g.exptime = abs_time(b, ttl);
		if (!held_reserve(b))
			goto stop;
		enum get_result r = get_key(b, &g);
		if (r == GET_STOP)
			goto stop;
		if (r == GET_HIT) {
			hits++;
			held_add(b, g.key, g.key_len, true);
		}
	}
	if (!out_put(b, "END\r\n", 5))
		goto stop;
	b->stats[MCC_DP_CMD_GET] += nkeys;
	b->stats[MCC_DP_GET_HITS] += hits;
	if (touch) {
		b->stats[MCC_DP_CMD_TOUCH] += nkeys;
		b->stats[MCC_DP_TOUCH_HITS] += hits;
	}
	return true;
stop:
	out_truncate(b, used);
	b->nevents = nevents;
	return false;
}

/* touch <key> <exptime> [noreply]: the line is [p, end). */
static bool
do_touch(struct mcc_dp_batch *b, const char *p, const char *end)
{
	struct token t[MAX_TOKENS];
	uint32_t n = split(p, end, t, MAX_TOKENS);
	int64_t ttl;
	if (n < 3 || t[1].len > MAX_KEY_LENGTH ||
	    !parse_i32(t[2].s, t[2].len, &ttl) || !b->store)
		return false;
	bool noreply = is_noreply(&t[n - 1]);
	char *reply = NULL;
	if (!noreply && (reply = out_reserve(b, 11)) == NULL)
		return false;
	struct get_req g = {
		.key = t[1].s,
		.key_len = t[1].len,
		.touch = true,
		.exptime = abs_time(b, ttl),
		.bump = b->track,
		.emit = EMIT_NONE,
	};
	enum get_result r = get_key(b, &g);
	if (r == GET_STOP)
		return false;
	b->stats[MCC_DP_CMD_TOUCH]++;
	if (r == GET_HIT)
		b->stats[MCC_DP_TOUCH_HITS]++;
	if (reply != NULL)
		out_commit(b, r == GET_HIT ? PUT(reply, "TOUCHED\r\n") :
				 PUT(reply, "NOT_FOUND\r\n"));
	return true;
}

/*
 * mg <key> <flags>*: as proto/meta.lua parse_mg() and ops.get, without N
 * (vivify) and R (recache), and without a win token involved (see
 * get_key()). E without N doesn't matter.
 */
static bool
do_mg(struct mcc_dp_batch *b, const char *p, const char *end)
{
	struct token t[META_MAX_TOKENS + 1];
	uint32_t n = split(p, end, t, META_MAX_TOKENS + 1);
	struct meta m;
	if (!meta_parse(t, n, 2, RET_MG, &m) || has(&m, 'N') || has(&m, 'R'))
		return false;
	struct get_req g = {
		.key = m.key,
		.key_len = m.key_len,
		.touch = has(&m, 'T'),
		.bump = b->track && !has(&m, 'u'),
		.win_token = true,
		.emit = EMIT_MG,
		.meta = &m,
	};
	if (g.touch) {
		if (!b->store)
			return false;
		g.exptime = abs_time(b, m.T);
	}
	if (!held_reserve(b))
		return false;
	enum get_result r = get_key(b, &g);
	if (r == GET_STOP)
		return false;
	if (r == GET_MISS && !has(&m, 'q')) {
		char *q = out_reserve(b, 2 + ret_size(&m) + 2);
		if (q == NULL)
			return false;
		q = PUT(q, "EN");
		q = put_opaque_and_key(q, &m);
		out_commit(b, PUT(q, "\r\n"));
	}
	b->stats[MCC_DP_CMD_GET]++;
	if (g.touch)
		b->stats[MCC_DP_CMD_TOUCH]++;
	if (r == GET_HIT) {
		b->stats[MCC_DP_GET_HITS]++;
		if (g.touch)
			b->stats[MCC_DP_TOUCH_HITS]++;
		held_add(b, m.key, m.key_len, true);
	}
	b->stats[MCC_DP_CMD_META]++;
	return true;
}

/* Stores ------------------------------------------------------------------ */

enum store_mode {
	MODE_SET,
	MODE_ADD,
	MODE_REPLACE,
	MODE_APPEND,
	MODE_PREPEND,
};

enum store_status {
	STATUS_STORED,
	STATUS_NOT_STORED,
	STATUS_EXISTS,
	STATUS_NOT_FOUND,
	/* Not done: the command is left to Lua. */
	STATUS_STOP,
};

/* An engine store request without vivify and E. */
struct store_req {
	const char *key;
	uint32_t key_len;
	const char *value;
	uint32_t value_len;
	uint64_t flags;
	/* The raw exptime. */
	int64_t exptime;
	enum store_mode mode;
	bool has_cas;
	uint64_t cas;
	bool invalidate;
};

/*
 * ops.store. A dead old item is deleted only when nothing replaces it (a
 * replace over it gives the state of ops.store's lazy deletion and
 * replace). *cas and *size are those of a stored item.
 */
static enum store_status
store_op(struct mcc_dp_batch *b, const struct store_req *r, uint64_t *cas,
	 uint32_t *size)
{
	char kbuf[KEY_BUF_SIZE];
	char *kend = enc_key(kbuf, r->key, r->key_len);
	box_tuple_t *old;
	if (box_index_get(b->space_id, PRIMARY, kbuf, kend, &old) != 0)
		return STATUS_STOP;
	struct item it = {.key = NULL};
	bool live = false, dead = false;
	if (old != NULL) {
		if (!item_decode(old, &it))
			return STATUS_STOP;
		if (item_is_dead(&it, b->now, b->has_flush, b->flush))
			dead = true;
		else
			live = true;
	}
	struct new_item n = {
		.key = r->key,
		.key_len = r->key_len,
		.value = r->value,
		.value_len = r->value_len,
		.flags = (double)r->flags,
		.bucket_id = b->bucket_id,
	};
	enum store_status status = STATUS_STORED;
	bool keep_token = false;
	enum store_mode mode = r->mode;
	if (!live) {
		if (r->has_cas && (mode == MODE_SET || mode == MODE_REPLACE))
			status = STATUS_NOT_FOUND;
		else if (mode != MODE_SET && mode != MODE_ADD)
			status = STATUS_NOT_STORED;
		else
			n.exptime = abs_time(b, r->exptime);
	} else if (mode == MODE_ADD) {
		status = STATUS_NOT_STORED;
	} else if (mode == MODE_APPEND || mode == MODE_PREPEND) {
		/* As memcached's do_store_item, a CAS of 0 is no CAS. */
		if (r->has_cas && r->cas != 0 && r->cas != it.cas) {
			status = STATUS_EXISTS;
		} else if (item_size(r->key_len, it.flags, (double)it.value_len +
				     r->value_len) > b->max_item_size) {
			status = STATUS_NOT_STORED;
		} else {
			n.flags = (double)it.flags;
			n.exptime = it.exptime;
			if (mode == MODE_APPEND) {
				n.value = it.value;
				n.value_len = it.value_len;
				n.value2 = r->value;
				n.value2_len = r->value_len;
			} else {
				n.value2 = it.value;
				n.value2_len = it.value_len;
			}
		}
	} else if (r->has_cas && (r->cas == 0 || r->cas != it.cas)) {
		if (r->cas == 0 || !(r->invalidate && r->cas < it.cas)) {
			status = STATUS_EXISTS;
		} else {
			/* Stored stale, with the old TTL and win token. */
			n.stale = true;
			keep_token = true;
			n.exptime = it.exptime;
		}
	} else {
		n.exptime = abs_time(b, r->exptime);
	}
	if (status != STATUS_STORED) {
		if (dead) {
			group_write(b);
			if (box_delete(b->space_id, PRIMARY, kbuf, kend,
				       NULL) != 0)
				return STATUS_STOP;
			after_write(b);
		}
		if (!live)
			forget_win(b, r->key, r->key_len);
		return status;
	}
	if (keep_token) {
		/* token_sent(): a token kept in memory is Lua's to judge. */
		if (!it.token && token_kept(b, r->key, r->key_len))
			return STATUS_STOP;
		n.token = it.token;
	}
	/* Before the CAS value: a failed group commit gives it back. */
	group_write(b);
	struct undo u;
	undo_start(&u);
	if (!undo_counter(b, &u, &n.cas))
		return STATUS_STOP;
	if (live && it.cas + 1 > n.cas)
		n.cas = it.cas + 1;
	*cas = n.cas;
	*size = n.value_len + n.value2_len;
	if (expired(n.exptime, b->now)) {
		/* Stored and expired at once: no item. */
		if (old != NULL) {
			if (box_delete(b->space_id, PRIMARY, kbuf, kend,
				       NULL) != 0) {
				undo_restore(b, &u);
				return STATUS_STOP;
			}
			after_write(b);
		}
		if (!live)
			forget_win(b, r->key, r->key_len);
		return STATUS_STORED;
	}
	n.created = undo_stamp(b, &u);
	n.now = b->now;
	if (item_store(b->space_id, &n) != 0) {
		undo_restore(b, &u);
		return STATUS_STOP;
	}
	b->group_bytes += n.value_len + n.value2_len;
	after_write(b);
	if (!live)
		forget_win(b, r->key, r->key_len);
	return STATUS_STORED;
}

/* The counters of a store, as server.account() counts them. */
static void
count_store(struct mcc_dp_batch *b, const struct store_req *r,
	    enum store_status status)
{
	b->stats[status == STATUS_STORED ? MCC_DP_STORED :
		 MCC_DP_UNSTORED]++;
	if (r->has_cas && (r->mode == MODE_SET || r->mode == MODE_REPLACE)) {
		if (status == STATUS_STORED)
			b->stats[MCC_DP_CAS_HITS]++;
		else if (status == STATUS_EXISTS)
			b->stats[MCC_DP_CAS_BADVAL]++;
		else if (status == STATUS_NOT_FOUND)
			b->stats[MCC_DP_CAS_MISSES]++;
	}
}

static const char *const STORE_REPLY[] = {
	[STATUS_STORED] = "STORED\r\n",
	[STATUS_NOT_STORED] = "NOT_STORED\r\n",
	[STATUS_EXISTS] = "EXISTS\r\n",
	[STATUS_NOT_FOUND] = "NOT_FOUND\r\n",
};

/*
 * set|add|replace|append|prepend <key> <flags> <exptime> <bytes>
 * [noreply] and cas <key> <flags> <exptime> <bytes> <cas> [noreply]: the
 * line is [p, end), its data block starts at data; in_end is the end of
 * the input. As the parser takes them: a valid key, digits only for the
 * numbers (flags up to 2^32-1, exptime up to 2^31-1), the data block
 * complete and followed by "\r\n", within max_item_size; then ops.store.
 * *next is the end of the command.
 */
static bool
do_storage(struct mcc_dp_batch *b, enum store_mode mode, bool with_cas,
	   const char *p, const char *end, const char *data,
	   const char *in_end, const char **next)
{
	struct token t[MAX_TOKENS];
	uint32_t n = split(p, end, t, MAX_TOKENS);
	uint64_t vlen, flags, exptime;
	struct store_req r = {.mode = mode, .has_cas = with_cas};
	if (n < 5 || t[1].len > MAX_KEY_LENGTH ||
	    !parse_digits(t[4].s, t[4].len, TOKTO32_MAX, INT32_MAX - 2, &vlen) ||
	    !parse_digits(t[2].s, t[2].len, UINT32_MAX, UINT32_MAX, &flags) ||
	    !parse_digits(t[3].s, t[3].len, UINT32_MAX, INT32_MAX, &exptime))
		return false;
	if (with_cas && (n < 6 || !parse_u64(t[5].s, t[5].len, &r.cas)))
		return false;
	bool noreply = is_noreply(&t[n - 1]);
	if (item_size(t[1].len, flags, (double)vlen) > b->max_item_size)
		return false;
	if ((size_t)(in_end - data) < vlen + 2 || data[vlen] != '\r' ||
	    data[vlen + 1] != '\n')
		return false;
	if (!b->store)
		return false;
	if (data + vlen + 2 == in_end)
		b->cmd_last = true;
	r.key = t[1].s;
	r.key_len = t[1].len;
	r.value = data;
	r.value_len = (uint32_t)vlen;
	r.flags = flags;
	r.exptime = (int64_t)exptime;
	/* The reply and the held key first: after the write nothing fails. */
	char *reply = NULL;
	if (!noreply && (reply = out_reserve(b, 12)) == NULL)
		return false;
	if (!held_reserve(b))
		return false;
	uint64_t cas;
	uint32_t size;
	enum store_status status = store_op(b, &r, &cas, &size);
	if (status == STATUS_STOP)
		return false;
	count_store(b, &r, status);
	if (status == STATUS_STORED)
		held_add(b, r.key, r.key_len, false);
	if (reply != NULL) {
		const char *s = STORE_REPLY[status];
		out_commit(b, put(reply, s, strlen(s)));
	}
	*next = data + vlen + 2;
	return true;
}

static const char *const MS_CODE[] = {
	[STATUS_STORED] = "HD",
	[STATUS_NOT_STORED] = "NS",
	[STATUS_EXISTS] = "EX",
	[STATUS_NOT_FOUND] = "NF",
};

/*
 * ms <key> <datalen> <flags>*: as proto/meta.lua parse_ms() and
 * ops.store, without E (an explicit CAS) and N (vivify on append): the
 * line is [p, end), its data block starts at data.
 */
static bool
do_ms(struct mcc_dp_batch *b, const char *p, const char *end,
      const char *data, const char *in_end, const char **next)
{
	struct token t[META_MAX_TOKENS + 1];
	uint32_t n = split(p, end, t, META_MAX_TOKENS + 1);
	int64_t vlen;
	struct meta m;
	if (n < 3 || !parse_i32(t[2].s, t[2].len, &vlen) || vlen < 0 ||
	    vlen > INT32_MAX - 2 || !meta_parse(t, n, 3, RET_MS, &m))
		return false;
	struct store_req r = {.mode = MODE_SET};
	if (has(&m, 'M')) {
		switch (m.M) {
		case 'S': r.mode = MODE_SET; break;
		case 'E': r.mode = MODE_ADD; break;
		case 'A': r.mode = MODE_APPEND; break;
		case 'P': r.mode = MODE_PREPEND; break;
		case 'R': r.mode = MODE_REPLACE; break;
		default: return false;
		}
	}
	r.flags = has(&m, 'F') ? m.F : 0;
	if (item_size(m.key_len, r.flags, (double)vlen) > b->max_item_size)
		return false;
	if (in_end - data < vlen + 2 || data[vlen] != '\r' ||
	    data[vlen + 1] != '\n')
		return false;
	if (data + vlen + 2 == in_end)
		b->cmd_last = true;
	bool append = r.mode == MODE_APPEND || r.mode == MODE_PREPEND;
	if (has(&m, 'E') || (append && has(&m, 'N')) || !b->store)
		return false;
	/*
	 * A CAS turns set and replace into the cas command, add ignores it,
	 * append and prepend check it; invalidate only works in the cas
	 * command.
	 */
	if (has(&m, 'C')) {
		if (r.mode == MODE_REPLACE)
			r.mode = MODE_SET;
		if (r.mode != MODE_ADD) {
			r.has_cas = true;
			r.cas = m.C;
		}
	}
	r.invalidate = has(&m, 'I') && r.has_cas && r.mode == MODE_SET;
	r.key = m.key;
	r.key_len = m.key_len;
	r.value = data;
	r.value_len = (uint32_t)vlen;
	r.exptime = has(&m, 'T') ? m.T : 0;
	char *reply = out_reserve(b, 2 + ret_size(&m) + 2);
	if (reply == NULL || !held_reserve(b))
		return false;
	uint64_t cas;
	uint32_t size;
	enum store_status status = store_op(b, &r, &cas, &size);
	if (status == STATUS_STOP)
		return false;
	count_store(b, &r, status);
	b->stats[MCC_DP_CMD_META]++;
	bool ok = status == STATUS_STORED;
	if (ok)
		held_add(b, r.key, r.key_len, false);
	if (!(ok && has(&m, 'q'))) {
		char *q = put(reply, MS_CODE[status], 2);
		for (uint32_t i = 0; i < m.nret; i++) {
			const struct token *tok = &m.ret[i];
			switch (tok->s[0]) {
			case 'O':
				*q++ = ' ';
				q = put(q, tok->s, tok->len);
				break;
			case 'k':
				q = put_key_flag(q, &m);
				break;
			case 'c':
				q = PUT(q, " c");
				q = fmt_u64(q, ok ? cas : 0);
				break;
			default:
				q = PUT(q, " s");
				q = fmt_u64(q, ok ? size : r.value_len);
				break;
			}
		}
		out_commit(b, PUT(q, "\r\n"));
	}
	*next = data + vlen + 2;
	return true;
}

/* Deletes ----------------------------------------------------------------- */

/* ops.delete without invalidate and clear_value. */
static enum store_status
delete_op(struct mcc_dp_batch *b, const char *key, uint32_t key_len,
	  bool has_cas, uint64_t cas)
{
	char kbuf[KEY_BUF_SIZE];
	char *kend = enc_key(kbuf, key, key_len);
	box_tuple_t *tuple;
	if (box_index_get(b->space_id, PRIMARY, kbuf, kend, &tuple) != 0)
		return STATUS_STOP;
	if (tuple != NULL) {
		struct item it;
		if (!item_decode(tuple, &it))
			return STATUS_STOP;
		if (!item_is_dead(&it, b->now, b->has_flush, b->flush)) {
			if (has_cas && cas != it.cas)
				return STATUS_EXISTS;
			group_write(b);
			if (box_delete(b->space_id, PRIMARY, kbuf, kend,
				       NULL) != 0)
				return STATUS_STOP;
			after_write(b);
			return STATUS_STORED;
		}
		group_write(b);
		if (box_delete(b->space_id, PRIMARY, kbuf, kend, NULL) != 0)
			return STATUS_STOP;
		after_write(b);
	}
	forget_win(b, key, key_len);
	return STATUS_NOT_FOUND;
}

static void
count_delete(struct mcc_dp_batch *b, enum store_status status)
{
	if (status == STATUS_STORED)
		b->stats[MCC_DP_DELETE_HITS]++;
	else if (status == STATUS_NOT_FOUND)
		b->stats[MCC_DP_DELETE_MISSES]++;
}

/* delete <key> [noreply]: the line is [p, end); more tokens are ignored. */
static bool
do_delete(struct mcc_dp_batch *b, const char *p, const char *end)
{
	struct token t[MAX_TOKENS];
	uint32_t n = split(p, end, t, MAX_TOKENS);
	if (n < 2 || t[1].len > MAX_KEY_LENGTH || !b->store)
		return false;
	bool noreply = is_noreply(&t[n - 1]);
	char *reply = NULL;
	if (!noreply && (reply = out_reserve(b, 11)) == NULL)
		return false;
	if (!held_reserve(b))
		return false;
	enum store_status status = delete_op(b, t[1].s, t[1].len, false, 0);
	if (status == STATUS_STOP)
		return false;
	count_delete(b, status);
	if (status == STATUS_STORED)
		held_add(b, t[1].s, t[1].len, false);
	if (reply != NULL)
		out_commit(b, status == STATUS_STORED ?
			   PUT(reply, "DELETED\r\n") :
			   PUT(reply, "NOT_FOUND\r\n"));
	return true;
}

static const char *const MD_CODE[] = {
	[STATUS_STORED] = "HD",
	[STATUS_EXISTS] = "EX",
	[STATUS_NOT_FOUND] = "NF",
};

/* md <key> <flags>*: as proto/meta.lua parse_md(), without I and x. */
static bool
do_md(struct mcc_dp_batch *b, const char *p, const char *end)
{
	struct token t[META_MAX_TOKENS + 1];
	uint32_t n = split(p, end, t, META_MAX_TOKENS + 1);
	struct meta m;
	if (!meta_parse(t, n, 2, RET_MD, &m) || has(&m, 'I') ||
	    has(&m, 'x') || !b->store)
		return false;
	char *reply = out_reserve(b, 2 + ret_size(&m) + 2);
	if (reply == NULL || !held_reserve(b))
		return false;
	enum store_status status = delete_op(b, m.key, m.key_len,
					     has(&m, 'C'), m.C);
	if (status == STATUS_STOP)
		return false;
	count_delete(b, status);
	b->stats[MCC_DP_CMD_META]++;
	if (status == STATUS_STORED)
		held_add(b, m.key, m.key_len, false);
	if (!(status == STATUS_STORED && has(&m, 'q'))) {
		char *q = put(reply, MD_CODE[status], 2);
		q = put_opaque_and_key(q, &m);
		out_commit(b, PUT(q, "\r\n"));
	}
	return true;
}

/* Arithmetic -------------------------------------------------------------- */

/* An engine arith request without vivify and E. */
struct arith_req {
	const char *key;
	uint32_t key_len;
	bool incr;
	uint64_t delta;
	bool has_cas;
	uint64_t cas;
	bool has_ttl;
	int64_t ttl;
};

struct arith_res {
	uint64_t value;
	uint64_t cas;
	double exptime;
};

/*
 * ops.arith. A value that is empty or not a number (CLIENT_ERROR) is left
 * to Lua, and so is an arithmetic in place on an item whose win token is
 * kept in memory (token_sent()).
 */
static enum store_status
arith_op(struct mcc_dp_batch *b, const struct arith_req *r,
	 struct arith_res *res)
{
	char kbuf[KEY_BUF_SIZE];
	char *kend = enc_key(kbuf, r->key, r->key_len);
	box_tuple_t *tuple;
	if (box_index_get(b->space_id, PRIMARY, kbuf, kend, &tuple) != 0)
		return STATUS_STOP;
	struct item it = {.key = NULL};
	if (tuple != NULL) {
		if (!item_decode(tuple, &it))
			return STATUS_STOP;
		if (item_is_dead(&it, b->now, b->has_flush, b->flush)) {
			group_write(b);
			if (box_delete(b->space_id, PRIMARY, kbuf, kend,
				       NULL) != 0)
				return STATUS_STOP;
			after_write(b);
			tuple = NULL;
		}
	}
	if (tuple == NULL) {
		forget_win(b, r->key, r->key_len);
		return STATUS_NOT_FOUND;
	}
	if (it.value_len == 0)
		return STATUS_STOP;
	if (r->has_cas && r->cas != 0 && r->cas != it.cas)
		return STATUS_EXISTS;
	uint64_t value;
	if (!value_u64(it.value, it.value_len, &value))
		return STATUS_STOP;
	if (r->incr)
		value += r->delta;
	else
		value = r->delta > value ? 0 : value - r->delta;
	char num[20];
	uint32_t num_len = (uint32_t)(fmt_u64(num, value) - num);
	/*
	 * As memcached's do_add_delta: in place, padded with spaces to the
	 * old length, when the result fits and no read of the batch holds
	 * the item; otherwise a new item of its own length.
	 */
	bool in_place = num_len <= it.value_len &&
			!held_has(b, r->key, r->key_len);
	bool token = false;
	if (in_place) {
		if (!it.token && token_kept(b, r->key, r->key_len))
			return STATUS_STOP;
		token = it.token;
	}
	uint32_t text_len = in_place ? it.value_len : num_len;
	size_t used = box_region_used();
	char *ops = box_region_alloc(128 + MP_HEADER_MAX + text_len);
	if (ops == NULL)
		return STATUS_STOP;
	/* Before the CAS value: a failed group commit gives it back. */
	group_write(b);
	struct undo u;
	undo_start(&u);
	uint64_t cas;
	if (!undo_counter(b, &u, &cas)) {
		box_region_truncate(used);
		return STATUS_STOP;
	}
	if (it.cas + 1 > cas)
		cas = it.cas + 1;
	double created = undo_stamp(b, &u);
	double exptime = it.exptime;
	bool bump_la = b->track && b->now - it.last_access >= b->la_interval;
	uint32_t nops = 3 + (in_place ? 1 + bump_la : 4);
	if (r->has_ttl) {
		exptime = abs_time(b, r->ttl);
		nops++;
	}
	char *o = enc_array(ops, nops);
	o = enc_str_header(enc_op(o, F_VALUE), text_len);
	memcpy(o, num, num_len);
	memset(o + num_len, ' ', text_len - num_len);
	o += text_len;
	o = enc_uint(enc_op(o, F_CAS), cas);
	o = enc_lua_number(enc_op(o, F_CREATED), created);
	if (in_place) {
		o = enc_bool(enc_op(o, F_TOKEN), token);
		if (bump_la)
			o = enc_lua_number(enc_op(o, F_LAST_ACCESS), b->now);
	} else {
		o = enc_bool(enc_op(o, F_TOKEN), false);
		o = enc_lua_number(enc_op(o, F_LAST_ACCESS), b->now);
		o = enc_bool(enc_op(o, F_FETCHED), false);
		o = enc_bool(enc_op(o, F_STALE), false);
	}
	int rc;
	if (r->has_ttl && expired(exptime, b->now)) {
		rc = box_delete(b->space_id, PRIMARY, kbuf, kend, NULL);
	} else {
		if (r->has_ttl)
			o = enc_lua_number(enc_op(o, F_EXPTIME), exptime);
		rc = box_update(b->space_id, PRIMARY, kbuf, kend, ops, o, 1,
				NULL);
	}
	box_region_truncate(used);
	if (rc != 0) {
		undo_restore(b, &u);
		return STATUS_STOP;
	}
	after_write(b);
	res->value = value;
	res->cas = cas;
	res->exptime = exptime;
	return STATUS_STORED;
}

static void
count_arith(struct mcc_dp_batch *b, bool incr, enum store_status status)
{
	if (status == STATUS_STORED)
		b->stats[incr ? MCC_DP_INCR_HITS : MCC_DP_DECR_HITS]++;
	else if (status == STATUS_NOT_FOUND)
		b->stats[incr ? MCC_DP_INCR_MISSES : MCC_DP_DECR_MISSES]++;
}

/* incr|decr <key> <delta> [noreply]: the line is [p, end). */
static bool
do_incr(struct mcc_dp_batch *b, const char *p, const char *end, bool incr)
{
	struct token t[MAX_TOKENS];
	uint32_t n = split(p, end, t, MAX_TOKENS);
	struct arith_req r = {.incr = incr};
	if (n < 3 || t[1].len > MAX_KEY_LENGTH ||
	    !parse_u64(t[2].s, t[2].len, &r.delta) || !b->store)
		return false;
	r.key = t[1].s;
	r.key_len = t[1].len;
	bool noreply = is_noreply(&t[n - 1]);
	char *reply = NULL;
	if (!noreply && (reply = out_reserve(b, 22)) == NULL)
		return false;
	if (!held_reserve(b))
		return false;
	struct arith_res res;
	enum store_status status = arith_op(b, &r, &res);
	if (status == STATUS_STOP)
		return false;
	count_arith(b, incr, status);
	if (status == STATUS_STORED)
		held_add(b, r.key, r.key_len, false);
	if (reply != NULL) {
		char *q;
		if (status == STATUS_STORED)
			q = PUT(fmt_u64(reply, res.value), "\r\n");
		else
			q = PUT(reply, "NOT_FOUND\r\n");
		out_commit(b, q);
	}
	return true;
}

/*
 * ma <key> <flags>*: as proto/meta.lua parse_ma() and ops.arith, without N
 * (vivify) and E. J without N doesn't matter.
 */
static bool
do_ma(struct mcc_dp_batch *b, const char *p, const char *end)
{
	struct token t[META_MAX_TOKENS + 1];
	uint32_t n = split(p, end, t, META_MAX_TOKENS + 1);
	struct meta m;
	if (!meta_parse(t, n, 2, RET_MA, &m) || has(&m, 'N') ||
	    has(&m, 'E') || !b->store)
		return false;
	struct arith_req r = {
		.key = m.key,
		.key_len = m.key_len,
		.incr = true,
		.delta = has(&m, 'D') ? m.D : 1,
		.has_cas = has(&m, 'C'),
		.cas = m.C,
		.has_ttl = has(&m, 'T'),
		.ttl = m.T,
	};
	if (has(&m, 'M')) {
		if (m.M == 'D' || m.M == '-')
			r.incr = false;
		else if (m.M != 'I' && m.M != '+')
			return false;
	}
	char *reply = out_reserve(b, 3 + 10 + ret_size(&m) + 2 + 20 + 2);
	if (reply == NULL || !held_reserve(b))
		return false;
	struct arith_res res;
	enum store_status status = arith_op(b, &r, &res);
	if (status == STATUS_STOP)
		return false;
	count_arith(b, r.incr, status);
	b->stats[MCC_DP_CMD_META]++;
	char *q = reply;
	if (status != STATUS_STORED) {
		q = put(q, status == STATUS_EXISTS ? "EX" : "NF", 2);
		q = put_opaque_and_key(q, &m);
		out_commit(b, PUT(q, "\r\n"));
		return true;
	}
	held_add(b, r.key, r.key_len, false);
	if (has(&m, 'q'))
		return true;
	char num[20];
	uint32_t num_len = (uint32_t)(fmt_u64(num, res.value) - num);
	if (has(&m, 'v')) {
		q = PUT(q, "VA ");
		q = fmt_u64(q, num_len);
	} else {
		q = PUT(q, "HD");
	}
	for (uint32_t i = 0; i < m.nret; i++) {
		const struct token *tok = &m.ret[i];
		switch (tok->s[0]) {
		case 'c':
			q = PUT(q, " c");
			q = fmt_u64(q, res.cas);
			break;
		case 't':
			q = PUT(q, " t");
			/* The item is written: a bad TTL can't stop it now. */
			if (!put_ttl(&q, res.exptime, b->now))
				q = PUT(q, "0");
			break;
		case 'O':
			*q++ = ' ';
			q = put(q, tok->s, tok->len);
			break;
		default:
			q = put_key_flag(q, &m);
			break;
		}
	}
	q = PUT(q, "\r\n");
	if (has(&m, 'v')) {
		q = put(q, num, num_len);
		q = PUT(q, "\r\n");
	}
	out_commit(b, q);
	return true;
}

/* mn: the meta no-op. */
static bool
do_mn(struct mcc_dp_batch *b)
{
	if (!out_put(b, "MN\r\n", 4))
		return false;
	held_release_all(b);
	b->stats[MCC_DP_CMD_META]++;
	return true;
}

/* The batch --------------------------------------------------------------- */

/* Whether [p, end) starts with the literal lit (a command and a space). */
#define STARTS(p, len, lit) \
	((len) > sizeof(lit) - 1 && memcmp((p), (lit), sizeof(lit) - 1) == 0)

/*
 * One command at pos: its line is [pos, line_end), next is the byte after
 * its "\n", end the end of the input. Returns whether it was served; then
 * *next is the end of the command.
 */
static bool
batch_command(struct mcc_dp_batch *b, const char *pos, const char *line_end,
	      const char *end, const char **next)
{
	size_t len = line_end - pos;
	const char *data = *next;
	switch (pos[0]) {
	case 'g':
		if (STARTS(pos, len, "get "))
			return do_retrieve(b, pos + 4, line_end, false, false);
		if (STARTS(pos, len, "gets "))
			return do_retrieve(b, pos + 5, line_end, true, false);
		if (STARTS(pos, len, "gat "))
			return do_retrieve(b, pos + 4, line_end, false, true);
		if (STARTS(pos, len, "gats "))
			return do_retrieve(b, pos + 5, line_end, true, true);
		return false;
	case 's':
		if (STARTS(pos, len, "set "))
			return do_storage(b, MODE_SET, false, pos, line_end,
					  data, end, next);
		return false;
	case 'a':
		if (STARTS(pos, len, "add "))
			return do_storage(b, MODE_ADD, false, pos, line_end,
					  data, end, next);
		if (STARTS(pos, len, "append "))
			return do_storage(b, MODE_APPEND, false, pos, line_end,
					  data, end, next);
		return false;
	case 'r':
		if (STARTS(pos, len, "replace "))
			return do_storage(b, MODE_REPLACE, false, pos,
					  line_end, data, end, next);
		return false;
	case 'p':
		if (STARTS(pos, len, "prepend "))
			return do_storage(b, MODE_PREPEND, false, pos,
					  line_end, data, end, next);
		return false;
	case 'c':
		if (STARTS(pos, len, "cas "))
			return do_storage(b, MODE_SET, true, pos, line_end,
					  data, end, next);
		return false;
	case 'd':
		if (STARTS(pos, len, "delete "))
			return do_delete(b, pos, line_end);
		if (STARTS(pos, len, "decr "))
			return do_incr(b, pos, line_end, false);
		return false;
	case 'i':
		if (STARTS(pos, len, "incr "))
			return do_incr(b, pos, line_end, true);
		return false;
	case 't':
		if (STARTS(pos, len, "touch "))
			return do_touch(b, pos, line_end);
		return false;
	case 'm':
		/*
		 * A meta command: "m" and its letter, the first token, then a
		 * space or the end of the line, which takes 4 bytes at least
		 * (proto/meta.lua parse(), server.lua parse_batch()).
		 */
		if (data - pos < 4 || (pos[2] != ' ' && line_end != pos + 2))
			return false;
		switch (pos[1]) {
		case 'g':
			return do_mg(b, pos, line_end);
		case 's':
			return do_ms(b, pos, line_end, data, end, next);
		case 'd':
			return do_md(b, pos, line_end);
		case 'a':
			return do_ma(b, pos, line_end);
		case 'n':
			return do_mn(b);
		}
		return false;
	}
	return false;
}

size_t
mcc_dp_batch_run(struct mcc_dp_batch *b, const char *in, size_t len,
		 uint32_t max_commands, uint32_t *commands)
{
	b->csw = fiber_csw(NULL);
	/* See "The group". */
	b->group_ok = b->store && b->tokens == 0 && !box_txn();
	b->in_group = false;
	const char *pos = in;
	const char *end = in + len;
	uint32_t n = 0;
	while (pos < end && n < max_commands) {
		const char *nl = memchr(pos, '\n', end - pos);
		if (nl == NULL)
			break;
		/* The line is [pos, line_end): without "\n" and one "\r". */
		const char *line_end = nl;
		if (line_end > pos && line_end[-1] == '\r')
			line_end--;
		const char *next = nl + 1;
		if (b->group_ok && !b->in_group) {
			mark_command(b, pos, n);
			/* After a store's data block: do_storage(), do_ms(). */
			b->cmd_last = next == end || n + 1 >= max_commands;
		}
		if (!batch_command(b, pos, line_end, end, &next))
			break;
		pos = next;
		n++;
		if (b->in_group && (b->group_writes >= GROUP_MAX_WRITES ||
				    b->group_bytes >= GROUP_MAX_BYTES) &&
		    !group_commit(b))
			goto failed;
	}
	if (b->in_group && !group_commit(b))
		goto failed;
	*commands = n;
	return pos - in;
failed:
	/* Nothing of the group is left: its commands are Lua's. */
	*commands = b->group_mark.commands;
	return b->group_mark.pos - in;
}

/*
 * batch(space_id, input, output, max_commands, max_item_size, now, flush,
 *       ro, track, la_interval, store, bucket_id, state, tokens, clock,
 *       held)
 *   -> consumed, commands, gets, hits, stored, held,
 *      unstored, touches, touch_hits, delete_hits, delete_misses,
 *      incr_hits, incr_misses, decr_hits, decr_misses,
 *      cas_hits, cas_misses, cas_badval, meta
 *
 * Serve the commands at the head of input (an ibuf, whose [rpos, wpos) is
 * read and not consumed, or a string), up to max_commands of them: get,
 * gets, gat, gats, set, add, replace, append, prepend, cas, delete, incr,
 * decr, touch, and the meta mg, ms, md, ma and mn, in the forms described
 * at each. Their replies are appended to output (an ibuf, not the input's,
 * which stays where it is: output may move). Stops at the first command
 * it doesn't serve: any other command, a form or a value the parser or the
 * engine would answer otherwise (an error, a value too large, a flag left
 * to Lua, a write when store is false), an incomplete command, a failed
 * box call. That command and the rest are left to the Lua front-end:
 * nothing of it has changed (but the side writes of the first keys of a
 * get, which Lua makes again). The writes of the commands served are in
 * one transaction (see "The group"), committed before it returns; when
 * that commit fails, the call stops before the group's first command
 * instead, as if it had stopped there.
 *
 * The arguments are those of the engine for the whole call: the clock
 * (now; clock() is called for it again after a write that yielded), the
 * flush time, the read-only flag, track_access (track: the gets record
 * the access), the last access interval, whether writes may be served
 * (store), the bucket id. state is the engine's state table, whose
 * cas_counter and stamp_last each write reads and moves on; tokens is
 * state.tokens[<space>] or nil; held is nil or the items that the reads
 * before this call hold, a set {key = true} (engine.lua track_held()),
 * which the call updates.
 *
 * Returns the bytes consumed, the commands served, the counters (see
 * mcc_dp_stat: gets is cmd_get, hits get_hits, stored the stores that
 * stored, which count in cmd_set and total_items, unstored those that
 * didn't, which count in cmd_set, then the others in the order of
 * mcc_dp_stat; nothing from unstored on when they are all 0), and held:
 * nil, or, when the batch stopped at a command with more input after, the
 * items the reads hold for what runs that command and the rest: a set
 * {key = true}.
 */
static int
datapath_batch(lua_State *L)
{
	struct mcc_dp_batch b;
	memset(&b, 0, sizeof(b));
	b.L = L;
	b.space_id = (uint32_t)luaL_checkinteger(L, 1);
	const char *in;
	size_t in_len;
	box_ibuf_t *ibuf = luaT_toibuf(L, 2);
	if (ibuf != NULL) {
		char **rpos, **wpos;
		box_ibuf_read_range(ibuf, &rpos, &wpos);
		in = *rpos;
		in_len = *wpos - *rpos;
	} else {
		in = luaL_checklstring(L, 2, &in_len);
	}
	b.out = luaT_toibuf(L, 3);
	if (b.out == NULL)
		luaL_argerror(L, 3, "ibuf expected");
	lua_Integer max_commands = luaL_checkinteger(L, 4);
	b.max_item_size = luaL_checknumber(L, 5);
	b.now = luaL_checknumber(L, 6);
	b.has_flush = opt_flush(L, 7, &b.flush);
	b.ro = lua_toboolean(L, 8);
	b.track = lua_toboolean(L, 9);
	b.la_interval = luaL_checknumber(L, 10);
	b.store = lua_toboolean(L, 11);
	b.bucket_id = luaL_checknumber(L, 12);
	luaL_checktype(L, 13, LUA_TTABLE);
	b.state = 13;
	b.tokens = lua_istable(L, 14) ? 14 : 0;
	luaL_checktype(L, 15, LUA_TFUNCTION);
	b.clock = 15;
	b.held_in = lua_istable(L, 16) ? 16 : 0;
	if (max_commands < 0)
		max_commands = 0;
	if (max_commands > UINT32_MAX)
		max_commands = UINT32_MAX;

	size_t region = box_region_used();
	uint32_t commands;
	size_t consumed = mcc_dp_batch_run(&b, in, in_len,
					   (uint32_t)max_commands, &commands);
	lua_pushinteger(L, consumed);
	lua_pushinteger(L, commands);
	lua_pushinteger(L, b.stats[MCC_DP_CMD_GET]);
	lua_pushinteger(L, b.stats[MCC_DP_GET_HITS]);
	lua_pushinteger(L, b.stats[MCC_DP_STORED]);
	if (consumed < in_len && commands < max_commands)
		mcc_dp_batch_push_held(&b);
	else
		lua_pushnil(L);
	/* The other counters only when one of them moved: fewer results. */
	int nres = 6;
	for (int i = MCC_DP_UNSTORED; i < MCC_DP_STAT_COUNT; i++) {
		if (b.stats[i] != 0) {
			nres = 6 + MCC_DP_STAT_COUNT - MCC_DP_UNSTORED;
			break;
		}
	}
	for (int i = MCC_DP_UNSTORED; i < MCC_DP_STAT_COUNT && nres > 6; i++)
		lua_pushinteger(L, b.stats[i]);
	box_region_truncate(region);
	return nres;
}

LUA_API int
luaopen_memcached_datapath(lua_State *L)
{
	static const struct luaL_Reg funcs[] = {
		{"get", datapath_get},
		{"set", datapath_set},
		{"batch", datapath_batch},
		{NULL, NULL},
	};
	ctid_uint64 = luaL_ctypeid(L, "uint64_t");
	lua_newtable(L);
	luaL_register(L, NULL, funcs);
	mcc_dp_loop_open(L);
	lua_pushinteger(L, DATAPATH_ABI);
	lua_setfield(L, -2, "ABI");
	return 1;
}
