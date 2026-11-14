# tarantool-contrib/memcached

A memcached server for Tarantool 3, in Lua with a small C data path. It
speaks the memcached text protocol (the classic commands and the meta
commands of memcached 1.6) and keeps the data in a Tarantool space, so the
cache is persistent, replicated and, with vshard, sharded.

- a Tarantool 3 role, `roles.memcached`, configured in the cluster
  config, and a Lua API, `require('memcached')`;
- a local space, or a sharded one on vshard storages with the memcached
  front-ends on the routers;
- front-ends in Tarantool application threads;
- stale reads from read views, and `lru_crawler metadump`/`mgdump`;
- hot reload of the code: listeners and connections survive it;
- metrics for the built-in `metrics` module, and per-instance health checks.

It succeeds [tarantool/memcached](https://github.com/tarantool/memcached),
whose data path is in C, but it is a new module with a new space format: it
does not read the data of the old one. `DESIGN.md` is the detailed
contract; this file is the user documentation.

**Status:** pre-release: no version is tagged and no rock is published
yet; the first release will be the tag `v0.1.0`. See
[Installation](#installation).

## Contents

- [Requirements](#requirements)
- [Installation](#installation)
- [Quick start](#quick-start)
- [Options](#options)
- [Lua API](#lua-api)
- [Protocol support](#protocol-support)
- [Read modes](#read-modes)
- [Expiration](#expiration)
- [Eviction](#eviction)
- [Hot reload](#hot-reload)
- [Metrics](#metrics)
- [Health check](#health-check)
- [Performance](#performance)
- [Limitations](#limitations)
- [Development](#development)

## Requirements

- Tarantool 3.9 or newer: application threads, the `threads` module and
  read views are used, and the role's per-instance health checks need 3.9.
- [vshard](https://github.com/tarantool/vshard), only for the vshard
  backend: vshard 1.x from 1.0.0 (`>= 1.0.0, < 2.0.0`), tested with 1.0.0.
  The backend relies on some vshard internals, so with a version outside
  that range (or one that doesn't tell its version) it logs a warning once
  when it loads the router module, and goes on.
- [tt v3](https://github.com/tarantool/tt), the next major version of tt
  (not released yet; see [`CONTRIBUTING.md`](CONTRIBUTING.md#the-package)
  for the commit this repository builds and how), to install the rock or
  build the package.
- To build from source (a checkout, or the source rock): a C compiler and
  Tarantool's headers (`module.h`; the `tarantool-dev` package), for the C
  data path. The binary rock has it built. Without it the module still
  works, slower (see [The C data path](#the-c-data-path)).
- For 1 MiB values, `memtx.max_tuple_size` (`box.cfg.memtx_max_tuple_size`)
  of about 2 MiB: Tarantool's default is 1 MiB, and an item also stores its
  key and metadata. `create()` logs a warning when the limit is too small.

## Installation

The package is described by [`app.manifest.toml`](app.manifest.toml) for
tt v3, which builds it and packs its rocks; the rockspec is generated into
the rocks, there is none in the repository. An application installs it with
tt v3 as a dependency in its own `app.manifest.toml`, and `tt package build`
installs it into the application's `.rocks/`.

### From a rock

No rock is published yet. A checkout packs the rocks into a directory that
serves as a rock server (the `pack` workflow keeps them as the artifact
`rocks`):

```console
$ tt package pack --format rock,rock-src --out /path/to/dist
/path/to/dist/memcached-<version>-1.linux-x86_64.rock
/path/to/dist/memcached-<version>-1.src.rock
/path/to/dist/memcached-<version>-1.rockspec
```

- the binary rock: the Lua modules, the role and the compiled C data path,
  for the platform it was packed on;
- the source rock: the source tree, which tt builds where no binary rock of
  the platform is on the server; that needs a C compiler and Tarantool's
  headers.

The application depends on it from that server:

```toml
[dependencies]
memcached = { version = "*", registry = "/path/to/dist" }
vshard = ">=1.0.0,<2.0.0"        # only for the vshard backend
```

Once the rock is published on a rock server the application uses, the
`registry` is not needed.

### From a checkout

An application can depend on a checkout, which tt builds from its manifest
(a C compiler and Tarantool's headers needed):

```toml
[dependencies]
memcached = { source = "path", path = "../memcached" }   # relative to the manifest
```

In the checkout itself, `tt package build` builds the package into its own
`.rocks/`: the C data path as `.rocks/lib/tarantool/memcached/datapath.so`,
the thread transport as `ipc.so` next to it, and `memcached/version.lua`. Tarantool started in the checkout runs the
sources and finds those in `.rocks/`, as the tests do. A checkout tt didn't
build runs on the Lua engine only, and reports the version `scm`.

### The version

The version is that of the `v*` git tags: `0.1.0` on the tag `v0.1.0`,
`0.1.1-dev.3+gabc1234` three commits after it (`dirty` is added for
uncommitted changes). tt writes it into `memcached/version.lua` when it
builds the package; `require('memcached').VERSION` and the `version` reply
(`VERSION 1.6.45-mcc-<version>`) give it.

The rock installs the modules `memcached`, `memcached.*` and
`roles.memcached`. These are the names tarantool/memcached uses too, so the
two rocks can't be installed into one application.

## Quick start

### Lua API

```lua
box.cfg{
    listen = 3301,
    memtx_max_tuple_size = 2 * 1024 * 1024,
    app_threads = 4,              -- one application thread group, 'app'
}

local mcc = require('memcached')

-- A cache on port 11211, served by the TX thread.
local cache = mcc.create('cache', {listen = '127.0.0.1:11211'})

-- A second instance: front-ends in the 'app' threads, stale reads from a
-- read view refreshed every 0.5 s, metrics exported.
mcc.create('sessions', {
    listen = '127.0.0.1:11212',
    threads = 'app',
    read_mode = 'snapshot',
    snapshot_refresh = 0.5,
    metrics = true,
})

cache:cfg({max_connections = 1000})   -- runtime options
```

```console
$ printf 'set k 0 0 5\r\nhello\r\nget k\r\nmg k v c t\r\n' | nc -q1 127.0.0.1 11211
STORED
VALUE k 0 5
hello
END
VA 5 c1794431205055805 t-1
hello
```

The data space is `__mcc_<name>` (`__mcc_cache` here) unless `space_name`
is set. `create()` creates it on a writable instance.

### The role

`roles_cfg['roles.memcached']` maps memcached instance names to
their [options](#options). The role creates, reconfigures and destroys the
instances on every config reload. The examples below run on one host; they
were checked with Tarantool 3.9.0 and vshard 1.0.

A single instance:

```yaml
memtx:
  max_tuple_size: 2097152     # room for 1 MiB values plus the key and fields

roles: [roles.memcached]
roles_cfg:
  roles.memcached:
    cache:                    # the memcached instance name
      listen: 127.0.0.1:11211

groups:
  group-001:
    replicasets:
      replicaset-001:
        instances:
          instance-001:
            iproto:
              listen:
                - uri: 127.0.0.1:3301
```

```sh
tarantool --name instance-001 --config config.yaml
```

The front-ends in application threads: add a thread group and name it in
`threads`. The kernel spreads the connections over the threads
(SO_REUSEPORT); the data operations still run in TX. The threads do the
socket work and parse the other commands; the gets and sets of the C data
path cross to TX as raw bytes, through shared-memory channels
(`memcached.ipc`, built with the rock).

```yaml
threads:
  groups:
    - name: memcached
      size: 4

roles: [roles.memcached]
roles_cfg:
  roles.memcached:
    cache:
      listen: 127.0.0.1:11211
      threads: memcached      # the front-ends run in the 4 threads
```

A replicaset: every instance runs the memcached instance. The leader serves
reads and writes; a replica serves reads, and its writes answer
`SERVER_ERROR Can't modify data on a read-only instance`. The data, the
flush time of `flush_all` and the CAS values are replicated; on a failover
the new leader has them.

```yaml
credentials:
  users:
    replicator:
      password: secret
      roles: [replication]

iproto:
  advertise:
    peer:
      login: replicator

replication:
  failover: manual

memtx:
  max_tuple_size: 2097152

roles: [roles.memcached]
roles_cfg:
  roles.memcached:
    cache:
      listen: 127.0.0.1:11211

groups:
  group-001:
    replicasets:
      replicaset-001:
        leader: instance-001
        instances:
          instance-001:
            iproto:
              listen:
                - uri: 127.0.0.1:3301
          instance-002:
            iproto:
              listen:
                - uri: 127.0.0.1:3302
            # On one host the replica needs another port; roles_cfg is
            # merged key by key, so only listen is overridden.
            roles_cfg:
              roles.memcached:
                cache:
                  listen: 127.0.0.1:11212
```

A replica whose space has not arrived yet waits for it (status `waiting`)
and starts when it does.

vshard: the routers and the storages share one `roles_cfg`. On a storage,
the role serves the space of every instance with `backend: vshard` to the
routers and starts no front-end. On a router, it runs the memcached
front-end, which routes each key to its bucket. Here the routers also run
their front-ends in 2 application threads, each with its own vshard router:

```yaml
credentials:
  users:
    replicator:
      password: secret
      roles: [replication]
    storage:
      password: secret
      roles: [sharding]

iproto:
  advertise:
    peer:
      login: replicator
    sharding:
      login: storage

sharding:
  bucket_count: 3000

memtx:
  max_tuple_size: 2097152

# One roles_cfg for the routers and the storages: the storages serve the
# space __mcc_cache, the routers run the memcached front-end.
roles: [roles.memcached]
roles_cfg:
  roles.memcached:
    cache:
      listen: 127.0.0.1:11211
      backend: vshard
      vshard:
        timeout: 1

groups:
  storages:
    sharding:
      roles: [storage]
    replication:
      failover: manual
    replicasets:
      storage-a:
        leader: storage-a-001
        instances:
          storage-a-001:
            iproto: {listen: [{uri: 127.0.0.1:3311}]}
          storage-a-002:
            iproto: {listen: [{uri: 127.0.0.1:3312}]}
      storage-b:
        leader: storage-b-001
        instances:
          storage-b-001:
            iproto: {listen: [{uri: 127.0.0.1:3321}]}
          storage-b-002:
            iproto: {listen: [{uri: 127.0.0.1:3322}]}
  routers:
    sharding:
      roles: [router]
    # The front-ends of the routers run in 2 application threads, each with
    # its own vshard router.
    threads:
      groups:
        - name: memcached
          size: 2
    roles_cfg:
      roles.memcached:
        cache:
          threads: memcached
    replicasets:
      router-a:
        instances:
          router-a-001:
            iproto: {listen: [{uri: 127.0.0.1:3301}]}
```

Start every instance, then bootstrap the buckets once, from a router:

```lua
-- tt connect <app>:router-a-001
require('vshard').router.bootstrap({if_not_bootstrapped = true})
```

or with `tt replicaset vshard bootstrap`. Without `threads` the routers run
the front-end in TX. A front-end can also use a named router
(`vshard: {router: <name>}`) instead of the static one.

## Options

The same options go to `mcc.create(name, opts)`, `inst:cfg(opts)` and the
role config.

| option | default | |
|---|---|---|
| `listen` | required | `host:port`, a port number, or `unix/:<path>` |
| `space_name` | `__mcc_<name>` | the data space; can't be changed |
| `storage` | `memory` | `memory` (memtx) or `disk` (vinyl); can't be changed |
| `backend` | `local` | `local`, or `vshard` (needs vshard) |
| `threads` | none | the name of an application thread group to run the front-ends in. TCP with a fixed port only. `false` (or `box.NULL`): in TX |
| `read_mode` | `rw` | see [Read modes](#read-modes): `rw`; `ro`, `bro` (vshard); `snapshot` (local, memory) |
| `snapshot_refresh` | `1` | seconds between read views in `snapshot` mode, at least 0.1 |
| `max_item_size` | `1048576` | the largest item, in bytes (1 .. 1 GiB), counted as memcached counts it: the value plus its key and 61 to 65 bytes of header, so a 1 MiB value is too large |
| `max_connections` | `4096` | per front-end, that is per thread with `threads` |
| `idle_timeout` | `0` | seconds (a number >= 0): close a TCP connection that waits for a command longer than that, as memcached's `-o idle_timeout`; counted in `stats` `idle_kicks`. `0`: never |
| `expire.enabled` | `true` | run the expiration sweeper |
| `expire.batch` | `100` | items deleted (with `expire.mode = 'scan'`: items checked) per sweeper transaction |
| `expire.mode` | `scan` (memory), `index` (disk) | how the sweeper finds the expired and flushed items, see [Expiration](#expiration): `index` keeps an index on the expiration time in the data space, `scan` has none, so writes are cheaper, and scans the space instead (memory storage only). Switching it on an existing space creates or drops the index |
| `evict.enabled` | `true` | evict items when memtx memory is near full, see [Eviction](#eviction); `false` is memcached's `-M`: writes that don't fit answer `SERVER_ERROR out of memory storing object` |
| `evict.high`, `evict.low` | `0.95`, `0.9` | fractions of `memtx_memory`: eviction starts at `high` and goes down to `low` |
| `evict.samples` | `8` | random items looked at per eviction (1..64) |
| `evict.batch` | `100` | evictions per transaction |
| `vshard.router` | the static router | the name of a vshard router (`vshard.router.new()`); the Lua API also takes a router object |
| `vshard.timeout` | `1` | seconds per batch of requests through the router |
| `metrics` | `false` | export the instance's [metrics](#metrics) |
| `track_access` | `true` | record the last access and the fetched flag (`mg` `l`/`h`, `me`, dumps). `false`: gets never write the item for it, so reads cost no WAL writes (a win token of `mg` still does); `h` is always `h0` and `l` counts from the last store. With `backend = 'vshard'` it is set on the storages: the role passes it there, the Lua API takes it in `storage.cfg()` |
| `commit_wait` | `complete` | when a write is answered: `complete` after its WAL write; `submit` or `none` before it, so that pipelined writes are written together. See [Commit wait](#commit-wait). With `backend = 'vshard'` it is set on the storages, as `track_access` |

- `inst:cfg()` changes options at runtime. A new `listen` is bound before
  the old one is closed and the client connections are kept. `space_name`
  and `storage` can't be changed. Changing `threads` restarts the front-ends
  and closes the connections; `threads = false` moves them back to TX.
- In the role, an option removed from the config goes back to its default
  (`threads`: back to TX, which closes the connections). An instance is
  recreated (its connections are closed, its data stays) when `space_name`
  or `storage` changes. Listen URIs may move between instances in one
  reload (a swap, a cycle); the connections stay with their instances.
- The role checks the config before applying it: the option schema, two
  instances with overlapping listen URIs, instances that share a space with
  different `storage`, backends, `evict` or `expire.mode` (or, for vshard,
  different `expire`, `track_access` or `commit_wait`), `expire.mode: scan`
  with `storage: disk`, `evict.low` not below
  `evict.high`, `backend: vshard` on an instance that is neither a router
  nor a storage, and whatever `mcc.create()` would
  reject (`mcc.check()`).

### Commit wait

By default (`commit_wait: complete`) a write is answered after Tarantool
has written it to the WAL, and every write waits for its own WAL write, so
the writes pipelined on one connection are written one by one. The other
values are those of Tarantool's `box.commit({wait = ...})`:

- `submit`: a write is answered once it is handed to the WAL queue. The
  writes of a pipelined batch then go to the WAL together. A write waits
  only while the queue is full (`box.cfg.wal_queue_max_size`, 16 MiB by
  default).
- `none`: the same, but when the queue is full the write fails at once with
  `SERVER_ERROR The WAL queue is full` (a get is then answered without its
  last-access write).

What it means:

- The client gets `STORED`, `DELETED`, ... before the write is durable. If
  the WAL write then fails (an I/O error, a full disk), Tarantool rolls the
  write back, with every write submitted after it: the client was told it
  succeeded, and readers see the old value again (or no item). Tarantool
  logs the WAL error. A crash loses the writes not yet written. That is
  still more than memcached keeps, which is nothing: it has no durability.
- A connection reads its own writes on the same instance, in the same batch
  and after it, with or without memtx MVCC and on `disk` storage (the
  writes run with `txn_isolation = 'read-committed'` there for this).
  Other connections see a write at once without MVCC, as with `complete`;
  with MVCC, other applications reading the space directly see it after
  the WAL write.
- Replication is the same: replicas get the writes from the WAL after they
  are written, so they never see a write that was rolled back.
- It applies to every write of the instance: stores, deletes, arithmetic,
  touches and the writes of gets (the last access, the deletion of an
  expired item), on the C data path too. `flush_all` and the expiration
  sweeper keep waiting for the WAL.
- With `backend: vshard` the writes run on the storages: set it in the
  role config (it goes to the storages) or with
  `storage.cfg({spaces = {[<space>] = {commit_wait = 'submit'}}})`. The
  Lua API refuses it on a vshard front-end.
- `inst:cfg({commit_wait = ...})` changes it at runtime.

## Lua API

```lua
local mcc = require('memcached')
local inst = mcc.create(name, opts)   -- create the space if needed and start
inst:cfg(opts)                        -- change options at runtime
inst:stop() / inst:start()            -- stop closes the listener and the connections
inst:destroy()                        -- stop and free the name; the space is kept
inst:info()                           -- status, counters, connections, port, ...
inst.status                           -- 'running' | 'stopped' | 'waiting' | 'failed' | 'destroyed'
mcc.get(name), mcc.destroy(name)
mcc.check(name, opts)                 -- raise if create() would reject the options
mcc.reload()                          -- hot reload of the code
mcc.tune_gc({pause = 400})            -- Lua GC of the calling thread; returns the previous values
mcc.datapath()                        -- {available = <bool>, enabled = <bool>}; see below
mcc.datapath(false)                   -- serve everything with the Lua engine (true: back to C)
mcc.datapath_loop()                   -- {available = <bool>, enabled = <bool>}: the C connection loop
mcc.datapath_loop(false)              -- the Lua connection loop, with the C data path (true: back)
```

`mcc.tune_gc({pause = <100..1000>, stepmul = <100..10000>})` sets
`collectgarbage('setpause'/'setstepmul')` of the calling thread (TX, or the
application thread it runs in) and returns the previous values, so they can
be restored. The module never tunes the collector by itself: it is
shared by everything in the process. A larger pause trades memory for fewer
GC cycles, which helps a process that is mostly a memcached server.

### The C data path

The most common commands on memory storage run in C
(`memcached/datapath.c`, built with the rock), parsed straight from the
connection's input: the classic `get`, `gets`, `gat`, `gats`, `set`, `add`,
`replace`, `append`, `prepend`, `cas`, `delete`, `touch`, `incr` and
`decr`, and the meta `mg`, `ms`, `md`, `ma` and `mn` with their common
flags. Everything else (disk storage, `me`, `flush_all`, `stats`, the win
token, vivify, recache and explicit CAS cases of the meta commands, errors)
and anything the C code can't finish (a failed write, for example) runs in
the Lua engine, with the same semantics and the same replies; the next
command the C code serves goes back to it. This needs the `local` backend,
no `read_mode = 'snapshot'` and no `commit_wait` mode. With `threads`, the
threads send the input bytes of a batch that starts with `get`, `gets` or
`set` to TX as they are, and get the replies back the same way.

- In TX, a connection whose commands the C code serves stays in a
  connection loop in C (`memcached/cloop.c`): its reads, those commands
  and its writes run without the Lua loop, which takes over for the other
  commands and comes back to C after them. The stats, `stats conns`, the
  idle timeout, `inst:cfg()`, flushes and a hot reload see and reach these
  connections as the others. `mcc.datapath_loop(false)` (or
  `MEMCACHED_DATAPATH=batch` from the start) keeps the C data path and
  runs the Lua loop; the replies are the same.
- `mcc.datapath()` tells whether it is built and loaded (`available`) and
  used (`enabled`).
- `mcc.datapath(false)` switches it off for the process, `true` back on; it
  survives a hot reload. The environment variable `MEMCACHED_DATAPATH=lua`
  switches it off from the start.
- When it is not built (a source tree tt didn't build, see
  [Installation](#installation)), a warning is logged and the Lua engine
  serves every request.
- A hot reload reloads the Lua code only: a new version of the C code needs
  a restart. After an upgrade that changes its interface, the reloaded Lua
  code doesn't use the old `.so` (an error in the log says so) until the
  restart.

`inst:info()` has the memcached counters (`cmd_get`, `get_hits`, ...), the
error counters (`protocol_errors`, `client_errors`, `server_errors`), the
sweeper's `expired`, `flushed` and `checked`, the eviction counters of the space
(`evictions`, `reclaimed`, `evicted_unfetched`, `evicted_active`,
`direct_reclaims`), and with threads the sums over the
threads plus a per-thread list in `threads`. With threads it yields.

## Protocol support

This module implements the ASCII protocol of memcached 1.6.45, the
classic and the meta commands, and its `version` reply is `VERSION
1.6.45-mcc-0.1.0`. The parsing follows 1.6.45 too: a line shorter than 4
bytes or a command without its arguments is `CLIENT_ERROR bad command line
format`, extra arguments are ignored, and a storage command rejected after
its length is read swallows its data block. The differential test suite
(`test/compat/`) runs the same requests against memcached 1.6.45 and
compares the replies.

| feature | memcached 1.6 | this module |
|---|---|---|
| `get` `gets` `gat` `gats` | yes | yes |
| `set` `add` `replace` `append` `prepend` `cas` | yes | yes |
| `delete` `incr` `decr` `touch` | yes | yes |
| `noreply` | yes | yes |
| `flush_all [delay]` | yes | yes; the flush time is persisted and replicated |
| `version` `quit` | yes | yes |
| `verbosity` | yes | accepted, no effect |
| `stats` | yes | yes, the general counters, the eviction counters and `store_no_memory`; `auth_*` are 0 |
| `stats settings` `stats reset` | yes | yes (a subset of the settings) |
| `stats conns` | yes | yes, as memcached: per connection `<fd>:addr`, `listen_addr`, `state`, `secs_since_last_cmd`; with `threads`, the connections of every thread |
| `stats sizes` | yes | `sizes_status disabled`, as memcached by default |
| `stats items` `slabs` | yes | an empty list |
| `stats detail on\|off\|dump` | yes | the replies of memcached with detail off: `on`/`off` answer `OK`, but no per-prefix stats are recorded, so `dump` is empty |
| other `stats` subcommands | yes | `ERROR` |
| meta `mg` `ms` `md` `ma` `mn` `me` | yes | yes, see below |
| `lru_crawler metadump` / `mgdump` | yes | yes, from a read view (memory storage) or bucket by bucket (vshard) |
| `lru_crawler crawl` `tocrawl` `sleep` `enable` `disable` | yes | `ERROR` |
| `slabs`, `lru`, `watch`, `shutdown`, `cache_memlimit`, `debugtime`, `extstore` | yes | `ERROR` |
| binary protocol | deprecated | no |
| SASL authentication | yes | no |
| UDP, TLS | yes | no |
| eviction (LRU) | yes | sampled approximated LRU, see [Eviction](#eviction) |

Meta flags:

| command | flags |
|---|---|
| `mg` | `b` `c` `f` `h` `k` `l` `O` `q` `s` `t` `u` `v` `C` `E` `N` `R` `T`; `P` and `L` are accepted and ignored |
| `ms` | `b` `c` `k` `O` `q` `s` `C` `E` `F` `I` `M` (`E` `A` `P` `R` `S`) `N` `T` |
| `md` | `b` `k` `O` `q` `C` `E` `F` `I` `T` `x` |
| `ma` | `b` `c` `k` `O` `q` `t` `v` `C` `D` `E` `J` `M` (`I` `+` `D` `-`) `N` `T` |
| `me` | `me <key> [b]` |
| `mn` | |

The win tokens of `mg` (`W`, `Z`, stale items marked by `md I`) are a
field of the item, so they are persisted and replicated, and survive a
restart, a failover and a vshard bucket move. A win costs one WAL write. A
read-only instance (a replica serving `ro`/`bro` reads, a master that went
read-only) can't write the item: it keeps the tokens it hands out in memory,
per replica, and sees the ones the master has handed out.

### Intentional divergences

The full list, with the reasons and the memcached source references, is
[`test/compat/allowlist.lua`](test/compat/allowlist.lua). In short:

- `ma T`/`E` always apply to the stored item;
- meta `t` and `R` use the TTL as of the whole request, whatever the flag
  order; a TTL in the past shows as `t0` instead of the unsigned wrap;
- `mg`/`me` `k` returns a base64 key only when the request has `b`;
- an opaque longer than 32 bytes is rejected before the command runs;
- `me` shows `cls=1` and `size=<key + value length>`, and so do the dumps;
  `mgdump` base64-encodes a key by its bytes, not by how it was set;
- a failing `get` keeps the replies of the pipelined commands before it;
- two bugs of memcached 1.6.45 are not copied: the extra `\r\n` after the
  error of a `get` with a too long key, and `me <key> b` ignoring `b`;
- `stats detail on` records no per-prefix stats: `stats detail dump` is
  always empty.

## Read modes

- **`rw`** (default): every request goes to the data. Reads see their own
  writes. On a read-only replica, reads work and writes answer
  `SERVER_ERROR`.
- **`ro`, `bro`** (vshard only): plain gets (no touch, vivify or recache)
  go through `vshard.router.callro`/`callbro`, so they may be served by a
  replica and lag behind by the replication delay. A batch to one bucket
  that contains anything else goes to the master as a whole. Tarantool 3
  config gives the router no zones, so `callro` picks the master; use `bro`
  to read from replicas.
- **`snapshot`** (local backend, memory storage): plain gets (`get`,
  `gets`, `mg` without `T`/`N`/`R`, `me`) are served from a read view that
  TX refreshes every `snapshot_refresh` seconds; with `threads`, each thread
  reads it without a cross-thread call. Everything else goes to TX.
  - **Staleness.** The reads lag by up to `snapshot_refresh` seconds (at
    least 0.1: Tarantool opens at most one read view per 100 ms per process,
    shared by all the instances and dumps). There is no read-your-writes,
    even on the same connection: a `get` right after a `set` may miss.
    `gets` may return an old CAS, so a `cas` based on it answers `EXISTS`.
  - The reads have no side effects: they don't update the last access or
    the fetched flag (`mg l`/`h`). Hits on stale items whose win token is
    not out go to TX, which hands it out.
  - `flush_all` is seen at once by the connection that sent it, and by the
    other threads at the next refresh.

## Expiration

Expired and flushed items are never returned: a read deletes them, as in
memcached. Besides that, a sweeper fiber per instance (per space on a
vshard storage, on the writable instance only) deletes the ones nobody
reads, to give their memory back early. `expire.mode` chooses how it finds
them:

- `scan`, the default for `storage = 'memory'`: the data space has no
  index on the expiration time, so a set or the first get after it updates
  one index less (in-container benchmarks: a `replace` takes about 2.6
  instead of 3.6 µs, a 100 B set through the C path 4.7 instead of 5.6 µs).
  The sweeper scans the space with a cursor, as Redis' active expiration
  does: about ten times a second it checks `expire.batch` items per
  transaction, and goes on while more than 10% of them are dead, for up to
  25 ms. When it finds nothing it slows down to once a second, but still
  goes over the whole space within 10 minutes. So after a mass expiration or
  a `flush_all` the memory comes back within seconds; a few dead items
  among many live ones may take up to that pass time (or until they are
  read or the evictor meets them).
- `index`, the default (and the only mode) for `storage = 'disk'`: an index
  on the expiration time, which the sweeper walks, so every expired item is
  deleted within about a second.

A change of `expire.mode` on an existing space creates or drops the index,
on a writable instance (at the instance's start, `inst:cfg()` or the vshard
storage's apply); replicas get it by replication and read-only instances
never change it. A hot reload changes no index.

## Eviction

When memtx memory is near full, items are evicted, as memcached evicts its
LRU items. Only the items of the memcached spaces with `evict.enabled` are
deleted, never other spaces, and only when the memtx memory shared by all
the spaces of the Tarantool instance (`memtx.memory`) is near full. Disk
storage is never evicted from (its limit is the disk). The evictions are
writes: they go to the WAL and to the replicas.

- **Approximated LRU.** memtx keeps no LRU order, so each eviction looks at
  `evict.samples` random items, as Redis does, and deletes the expired or
  flushed ones (`reclaimed`) and then the one used least recently: the oldest
  last access with `track_access`, else the oldest store. Recently used items
  survive more often, not always; the last access is recorded at most once a
  minute.
- **In the background.** One fiber per process checks the memory every
  0.1 s. When the memory the memtx allocations use reaches `evict.high` of
  `memtx.memory`, it evicts until it is at `evict.low`, in transactions of
  `evict.batch` evictions, from the spaces in proportion to their size.
  Memory that read views hold (a checkpoint, `snapshot` reads, dumps) is not
  counted as evictable: it comes back when they close.
- **On demand.** A write that fails because memtx can't allocate evicts a
  few items of about its size itself and runs once more. If it still fails
  it answers `SERVER_ERROR out of memory storing object` (stores; a `set`
  then drops the old value, as memcached does) or `SERVER_ERROR out of
  memory`, and `store_no_memory` counts it.
- **Watching it.** `stats`: `evictions`, `reclaimed`, `evicted_unfetched`,
  `evicted_active`, `direct_reclaims` (writes that had to evict themselves),
  `store_no_memory`; `stats settings`: `evictions on|off`;
  `tnt_memcached_evictions_total`; `inst:info()`;
  `require('memcached.evict').info()` for the memory fill.
- **vshard:** the data is on the storages, so `evict` is set there: the role
  passes it from the instance config, the Lua API takes it in
  `storage.cfg({spaces = {[<space>] = {evict = {...}}}})`. Each writable
  storage evicts by its own memory.
- **Limits.** The slab allocator keeps a pool per size class, and freed
  memory stays in its class (slab calcification): a write of a size no
  stored item has may still fail while the memory is in other classes. If
  `direct_reclaims` or `store_no_memory` grow, lower `evict.high` for more
  headroom. Tuples the Lua code still references are freed by the Lua GC,
  so the evictor may evict a little more than needed. Give the replicas the
  same `memtx.memory` as the master.

## Hot reload

```lua
require('memcached').reload()
```

reloads every `memcached.*` module from disk (as `require` finds
it) in TX and in the application threads that run front-ends. Listeners,
client connections, counters, read views, win tokens and thread routers
survive; each connection runs the new code from its next batch. If a module
fails to load, the old code is kept and the call raises. The role module
uses the reloaded code too. The C data path is not reloaded: its changes
need a restart. See `DESIGN.md`, "Hot reload", for what else is not
reloaded.

## Metrics

With `metrics: true` the instance is exported through Tarantool's built-in
`metrics` module, so every exporter (`roles.metrics-export`, the Prometheus
plugin, `metrics.http_middleware`) shows it. The prefix is
`tnt_memcached_`; every metric but `storage_*` has the label `name` (the
memcached instance), and the front-end metrics of an instance with
`threads` have the label `thread` (one series per application thread,
polled every second).

| metric | kind | labels | |
|---|---|---|---|
| `tnt_memcached_connections` | gauge | thread | current client connections |
| `tnt_memcached_connections_total` | counter | thread | accepted connections |
| `tnt_memcached_rejected_connections_total` | counter | thread | rejected over `max_connections` |
| `tnt_memcached_idle_kicks_total` | counter | thread | closed by `idle_timeout` |
| `tnt_memcached_commands_total` | counter | thread, op | op: get, set, touch, flush, delete, incr, decr |
| `tnt_memcached_meta_commands_total` | counter | thread | meta commands |
| `tnt_memcached_hits_total`, `tnt_memcached_misses_total` | counter | thread, op | op: get, touch, delete, incr, decr, cas |
| `tnt_memcached_cas_badval_total` | counter | thread | `cas` with a stale CAS |
| `tnt_memcached_stored_items_total` | counter | thread | items stored |
| `tnt_memcached_read_bytes_total`, `tnt_memcached_written_bytes_total` | counter | thread | bytes from and to the clients |
| `tnt_memcached_errors_total` | counter | thread, class | class: protocol (`ERROR`), client (`CLIENT_ERROR`), server (`SERVER_ERROR`) |
| `tnt_memcached_running` | gauge | | 1 if the instance is running |
| `tnt_memcached_items`, `tnt_memcached_bytes` | gauge | | items and bytes of the local space |
| `tnt_memcached_expired_items_total`, `tnt_memcached_flushed_items_total` | counter | | deleted by the sweeper |
| `tnt_memcached_evictions_total` | counter | | items evicted from the local space |
| `tnt_memcached_storage_items`, `tnt_memcached_storage_bytes` | gauge | space | a space of this vshard storage |
| `tnt_memcached_storage_expired_items_total`, `tnt_memcached_storage_flushed_items_total` | counter | space | its sweeper |
| `tnt_memcached_storage_evictions_total` | counter | space | its evictions |
| `tnt_memcached_storage_requests_total`, `tnt_memcached_storage_batches_total` | counter | space | requests and batches from the routers |

The commands are counted as memcached's `stats` counts them: per key, a
`cas` is also a set, and `gat` is both a get and a touch. A vshard router
exports no items; the storages export `storage_*` when the instance has
`metrics: true` in the role config. `stats reset` resets the counters.

For example with `roles.metrics-export` (the `metrics-export-role` rock,
which needs the `http` rock):

```yaml
roles: [roles.memcached, roles.metrics-export]
roles_cfg:
  roles.metrics-export:
    http:
      - listen: 8081
        endpoints:
          - path: /metrics
            format: prometheus
  roles.memcached:
    cache:
      listen: 11211
      metrics: true
```

## Health check

On Tarantool 3.9+ the role reports one readiness subcheck per memcached
instance, `role.roles.memcached.<name>`:

```
tarantool> box.info.health.readiness.checks['role.roles.memcached.cache']
---
- status: ready
...
```

- `ready`: the instance is running (on a vshard storage: its space exists
  with the `bucket_id` index);
- `not_ready` with a reason: `stopped`, `failed to start`, `destroyed`,
  `waiting for the space '<space>' to be replicated`, or the error of the
  last config apply;
- `degraded`: the instance is running but its last config failed to apply.

A check that is not ready also shows up as an alert in `config:info()`.
The check doesn't yield and reads only local state.

## Performance

memtier_benchmark, 4 threads x 25 connections, 10000 keys, the server
pinned to one core, `wal_mode = 'none'`; ops/s, median of 3 runs of 20 s,
run-to-run spread 2-13% (up to 21% for memcached `-t 4`). This module as of
2026-11-12, with the C data path and without it (`MEMCACHED_DATAPATH=lua`):

| workload | this module | Lua only | tarantool/memcached (C) | memcached `-t 1` | memcached `-t 4` (2 cores) |
|---|---:|---:|---:|---:|---:|
| w1: 1 set : 10 gets, 100 B, pipeline 1 | 30,923 | 24,676 | 58,152 | 62,834 | 135,130 |
| w2: 1:10, 100 B, pipeline 16 | 120,540 | 64,618 | 443,908 | 94,004 * | 192,020 * |
| w3: 1:10, 4 KB, pipeline 1 | 23,141 | 18,755 | 48,759 | 55,123 | 120,696 |
| w4: get misses, pipeline 1 | 34,716 | 34,870 | 63,580 | 69,634 | 143,859 |
| w5: 1 set : 1 10-key get | 16,973 | 10,467 | 45,633 | 49,202 | 98,630 |
| w6: sets only, 100 B, pipeline 16 | 85,637 | | 353,457 | 557,217 | 619,812 |

\* memcached is CPU-bound at pipeline 16 with gets in this setup, for a
reason that was not found; don't read it as slower than the C module.

- Server CPU per request: 32 us at w1 and 8.3 us at w2, against 17 us and
  2.3 us for the C module and 16 us and 11 us for memcached `-t 1`. At
  pipeline 1 most of the C module's 17 us is the syscall and loopback
  floor that every server pays; at pipeline 16 the difference is the Lua
  front-end (buffers, parsing, batching) and the GC: 1.3-2.2 KB of Lua heap
  per request with the C data path.
- Against the first benchmark of the pure Lua code (2026-11-11: 17,141 at
  w1, 53,478 at w2, 13,889 at w3, 27,965 at w4, 8,576 at w5), the Lua-level
  optimizations and the C data path give 1.8x at w1, 2.3x at w2, 1.7x at
  w3, 1.2x at w4 (misses take no C path) and 2x at w5.
- The C data path (classic `get`/`gets`/`set` on memtx, see DESIGN.md)
  gives +25% at w1 and 1.9x at w2 over the Lua engine alone.
- `track_access = false` removes the write of the first get after every
  store, and `mcc.tune_gc({pause = 400})` gave up to +10% at pipeline 16
  (nothing at pipeline 1) on the Lua engine.
- `wal_mode = 'write'` costs little on top. Every write waits for its own WAL
  commit, so pipelined writes from one connection don't group, unless
  `commit_wait: submit`. On one connection with sets only at pipeline 16
  (w6, `wal_mode = 'write'` on tmpfs, 2 runs of 20 s) `submit` gave 57,026
  ops/s against 28,867 with `complete` (p50 0.24 ms against 0.48 ms). With
  100 connections the server core is the limit and the WAL already groups
  the writes of the connections: 71,813 against 75,759 ops/s on w6 (-5%,
  about the run-to-run spread of 4%) and 112,015 against 113,033 on w2.
- Application threads with the local backend send the classic gets and
  sets to TX as raw bytes through shared-memory channels (about 25 µs per
  batch on a single connection, 2-3 µs each with 50 concurrent ones; a
  `threads.call` took 100 and 20 µs). In the in-suite benchmark on one
  5-CPU box shared with the client, that is 1.4-2.1x what the threads gave
  with `threads.call`, and with many connections at pipeline 1 1.4-2.1x
  TX alone; pipelined gets are still faster in TX alone there. Measure on
  your hardware (DESIGN.md "Application threads"). vshard routers in threads give
  1.3-2.5x over a router in TX (TX is not in the data path then), measured
  before the C data path.

To run the benchmarks, see [`bench/README.md`](bench/README.md). In short,
with the dev image and the memtier image built:

```sh
export MCC_SRC=/path/to/snapshot CMOD_SRC=/path/to/tarantool-memcached
git clone --local . "$MCC_SRC"           # benchmark a copy of the tree
"$MCC_SRC"/docker/run.sh 'tt package build'
SERVERS="mcc cmod" WORKLOADS=w1 REPS=1 DURATION=10 bench/run.sh   # quick
bench/run.sh                             # the whole matrix, ~50 min
```

## Limitations

- **Eviction** is a sampled approximation of LRU and can't move memory
  between slab size classes; see [Eviction](#eviction).
- **No binary protocol, SASL, UDP or TLS.**
- **Win tokens** (`mg` `W`/`Z`) cost a WAL write per win. On read-only
  replicas (`ro`/`bro` mode) the tokens a replica hands out are per replica,
  in memory, and lost on its restart.
- **Writes** each wait for their own WAL commit, unless `commit_wait` is
  `submit` or `none`, which answer before the write is durable (see
  [Commit wait](#commit-wait)). The first get after a store also writes the
  last access time, unless `track_access = false`.
- **Read-only instances** log Tarantool's "Can't modify data on a read-only
  instance" error for every write a client sends there.
- **Snapshot reads** only with the local backend and memory storage; they
  are stale (see [Read modes](#read-modes)).
- **Dumps** need memory storage (or vshard); on vshard they are not a
  snapshot and take at least one call per bucket.
- **vshard:** `flush_all` uses each storage's clock, so an item written right
  after a flush whose bucket then moves can be flushed on the new storage
  (the window is the clock skew). `stats` and `flush_all` go through
  vshard's `map_callrw`, which waits for the buckets in transfer: during a
  rebalance they can take up to `vshard.timeout` and then fail (`stats`
  shows no items). A bucket in transfer is never counted twice.
  The router in an application thread relies on vshard internals
  (`router.current_cfg`, `vshard.router.internal.routers`) and on a shim for
  `vshard.util`'s load-time check; a thread router is never dropped.
- **Threads:** unix sockets and port 0 are not supported with `threads`; one
  cross-thread call per batch with the local backend, two when other
  commands follow the classic gets and sets at its head.
- **Role:** recreating an instance keeps its old space and data.
- **`stats`** has no per-slab or per-item details (`stats items`, `stats
  slabs`).
- **Not compatible with tarantool/memcached data:** the space format is new.

## Development

See [`CONTRIBUTING.md`](CONTRIBUTING.md) for the dev image, the test suites
and the rules, and [`DESIGN.md`](DESIGN.md) for the design. Changes are in
[`CHANGELOG.md`](CHANGELOG.md).

## License

BSD 2-Clause, see [`LICENSE`](LICENSE).
