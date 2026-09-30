# Store backends

By default, DiffSync supports a local memory storage. All the loaded models from the adapters will be stored in memory, and become available for the diff calculation and sync process. This default behavior works well when executing all the steps in the same process, having access to the same memory space. However, if you want to scale out the execution of the tasks, running it in different processes or in totally different workers, a more distributed memory support is necessary.

The `store` is a class attribute in the `Adapter` class, but all the store operations in that class are abstracted in the following methods: `get_all_model_names`, `get`, `get_by_uids`, `add`, `update`, `remove`, `get_or_instantiate`, `update_or_instantiate` and `count`.

## Unified store contract

Since 2.3, all backends additionally implement a small key/value contract in `diffsync.store.contract.StoreContract`:

- `get(key, namespace=None)` / `set(key, value, namespace=None)` / `delete(key, namespace=None)` and `keys(namespace=None)`;
- namespace isolation: a store constructed with a `namespace` is bound to it and cross-namespace access raises `NamespaceBoundaryError`;
- a `ready()` probe returning one of three states: `READY`, `DEGRADED` or `RECOVERING`.

Backends are registered by name and can be selected when constructing an `Adapter` using a name, a class or an instance:

```python
Adapter(internal_storage_engine="local")
Adapter(internal_storage_engine="redis")
```

Key/value serialization is JSON by default. Pickle is only used when the store is explicitly constructed with `allow_pickle=True`; in that case an encode/decode roundtrip is verified at construction. Data written without a codec prefix by older DiffSync releases is treated as legacy pickle and remains readable when pickle is enabled.

### Degradation and ordered recovery

`RedisStore` applies a `socket_timeout`/`socket_connect_timeout` to every command and reconnects automatically via a background health monitor. When Redis is unreachable the store transitions explicitly to `DEGRADED` (never silently swallowing the error: it is logged and recorded as a switch event), serves reads and writes from an in-memory fallback, and queues writes in a FIFO replay log. Once Redis is reachable again it transitions to `RECOVERING`, drains the queued writes in order under a single store lock, and only then returns to `READY`. Writes that arrive while recovering are serialized after the in-flight replay, so writes within a namespace always converge in order.

### Observing backends with `store_status`

The `store_status` command continuously prints every live backend, its `READY`/`DEGRADED`/`RECOVERING` state, bound namespace and pending writes, followed by the most recent switch events. It is convenient for screen recording; a self-contained outage/recovery scenario (no external Redis required) is available via `--demo`:

```bash
store_status --interval 1
store_status --demo
python -m diffsync.store.status --once
```

## Use the `LocalStore` Backend

When you initialize the `Adapter` class, there is an optional keyed-argument, `internal_storage_engine`, defaulting to the `LocalStore` class.

```python
>> > from diffsync import Adapter
>> > adapter = Adapter()
>> > type(adapter.store)
<

class 'diffsync.store.local.LocalStore'>
```

## Use the `RedisStore` Backend

To get it, you have to install diffsync package with the "redis" extra option: `pip install diffsync[redis]`

The `RedisStore` backend, as the name suggests, connects to an external Redis service, to store data loaded by the `Adapter` tasks. The biggest change is that it requires to initialize the Redis store class, before using it in the `Adapter` adapter class.

```python
>> > from diffsync import Adapter
>> > from diffsync.store.redis import RedisStore
>> > store = RedisStore(host="redis host")
>> > adapter = Adapter(internal_storage_engine=store)
>> > type(adapter.store)
<

class 'diffsync.store.local.RedisStore'>
```

Notice that the `RedisStore` will validate, when initialized, that there is a reachability to the Redis host, and if not, will raise an exception:

```python
>>> from diffsync.store.redis import RedisStore
>>> store = RedisStore(host="redis host")
redis.exceptions.ConnectionError

The above exception was the direct cause of the following exception:

Traceback (most recent call last):
  File "<stdin>", line 1, in <module>
  File "/Users/chadell/github.ntc/diffsync/diffsync/store/redis.py", line 34, in __init__
    raise ObjectStoreException("Redis store is unavailable.") from RedisConnectionError
diffsync.exceptions.ObjectStoreException: Redis store is unavailable.
```

Using `RedisStore`, every adapter uses a specific Redis label, generated automatically, if not provided via the `store_id` keyed-argument. This `store_id` can be used to point an adapter to the specific memory state needed for diffsync operations.
