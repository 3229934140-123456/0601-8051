import uuid
import threading
import time
from typing import Optional, Tuple, Any

try:
    import redis as _redis
    RedisClientType = "_redis.Redis"
    RedisError = _redis.RedisError
except ImportError:
    RedisClientType = "Any"
    _DUMMY = type("RedisError", (Exception,), {})
    RedisError = _DUMMY


class RedisDistributedLock:
    """
    基于 Redis 的可靠分布式锁实现。

    核心机制:
    1. 原子加锁: SET key value NX EX ttl_seconds
       - NX: 仅当 key 不存在时才设置 (互斥性)
       - EX: 自动过期 (避免死锁)
       - value: 唯一随机标识符 (只有持有者能释放自己的锁)

    2. 原子解锁: Lua 脚本校验 value 后删除
       - 防止释放其他客户端刚获取到的"同名新锁"

    3. 看门狗续期 (Watchdog):
       - 后台守护线程在锁持有期间定期延长 TTL
       - 防止正常执行的业务因耗时过长而锁被误过期

    4. Fencing Token:
       - 每次成功获取锁时返回一个单调递增的 token
       - 被保护资源通过比较 token 大小拒绝"迟到"的过期锁操作
    """

    UNLOCK_SCRIPT = """
    if redis.call('GET', KEYS[1]) == ARGV[1] then
        return redis.call('DEL', KEYS[1])
    else
        return 0
    end
    """

    def __init__(
        self,
        redis_client: Any,
        lock_key: str,
        ttl_seconds: int = 10,
        watchdog_enabled: bool = True,
        watchdog_interval_ratio: float = 0.3,
    ):
        self.redis = redis_client
        self.lock_key = lock_key
        self.ttl_seconds = ttl_seconds
        self.watchdog_enabled = watchdog_enabled
        self.watchdog_interval = ttl_seconds * watchdog_interval_ratio

        self._owner_value: Optional[str] = None
        self._watchdog_thread: Optional[threading.Thread] = None
        self._watchdog_stop = threading.Event()
        self._lock = threading.Lock()

    def acquire(self, timeout_seconds: float = 30.0) -> Optional[int]:
        """
        尝试获取锁,返回 fencing token (单调递增整数)。
        获取失败返回 None。

        每次获取锁时,通过 INCR 指令让 Redis 生成全局唯一且严格递增的 token。
        即使同一把锁被释放后被重新获取,新 token 也一定大于旧 token。
        """
        deadline = time.monotonic() + timeout_seconds
        owner_value = uuid.uuid4().hex

        while time.monotonic() < deadline:
            acquired = self.redis.set(
                name=self.lock_key,
                value=owner_value,
                nx=True,
                ex=self.ttl_seconds,
            )
            if acquired:
                with self._lock:
                    self._owner_value = owner_value
                    fencing_token = self.redis.incr(f"{self.lock_key}:fencing:counter")

                    if self.watchdog_enabled:
                        self._start_watchdog()

                    return fencing_token

            time.sleep(0.05)

        return None

    def release(self) -> bool:
        """
        原子释放锁:只有 value 匹配才删除,防止误删别人的锁。

        陷阱场景:
            客户端 A 获取锁 (value=A), 发生 STW GC 停顿 15s,
            锁 TTL=10s 已过期。期间客户端 B 获取了同一把锁 (value=B)。
            如果 A 醒来后直接 DEL key, 就会把 B 的锁删掉。
            Lua 脚本校验 value 就是为了防御这种情况。
        """
        with self._lock:
            if self._owner_value is None:
                return False

            self._stop_watchdog()

            unlock_fn = self.redis.register_script(self.UNLOCK_SCRIPT)
            result = unlock_fn(keys=[self.lock_key], args=[self._owner_value])

            self._owner_value = None
            return bool(result)

    def _start_watchdog(self) -> None:
        self._watchdog_stop.clear()
        self._watchdog_thread = threading.Thread(
            target=self._watchdog_loop,
            daemon=True,
            name=f"watchdog-{self.lock_key}",
        )
        self._watchdog_thread.start()

    def _stop_watchdog(self) -> None:
        self._watchdog_stop.set()
        if self._watchdog_thread and self._watchdog_thread.is_alive():
            self._watchdog_thread.join(timeout=1.0)
        self._watchdog_thread = None

    def _watchdog_loop(self) -> None:
        """
        看门狗:只要锁未被主动释放,就每隔一段时间续期 TTL。

        注意:看门狗不是万能的!
        如果整个进程发生长时间 STW GC (看门狗线程也被挂起),
        或者客户端与 Redis 之间发生长时间网络分区,
        看门狗同样无法续期,锁还是会过期。这正是需要 fencing token 的根本原因。
        """
        while not self._watchdog_stop.is_set():
            time.sleep(self.watchdog_interval)
            if self._watchdog_stop.is_set():
                break

            with self._lock:
                if self._owner_value is None:
                    break

                try:
                    current_value = self.redis.get(self.lock_key)
                    if current_value == self._owner_value:
                        self.redis.expire(self.lock_key, self.ttl_seconds)
                    else:
                        break
                except RedisError:
                    pass

    def __enter__(self) -> Optional[int]:
        token = self.acquire()
        if token is None:
            raise RuntimeError("Failed to acquire distributed lock")
        return token

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.release()
