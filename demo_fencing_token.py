"""
分布式锁陷阱演示: GC 停顿 / 网络分区 导致的双写问题, 以及 Fencing Token 如何解决。

运行方式:
    pip install -r requirements.txt
    # 确保本地 Redis 运行在 localhost:6379 (或脚本会自动使用内存 mock)
    python demo_fencing_token.py
"""

import time
import threading
import sys
import io
import os
from typing import Optional

os.environ.setdefault('PYTHONUTF8', '1')

try:
    import redis
    HAS_REDIS = True
except ImportError:
    HAS_REDIS = False

from distributed_lock import RedisDistributedLock
from fenced_resource import FencedResource


# ======================================================================
# 内存版 Mock Redis: 如果本地没有 Redis, 脚本也能跑
# ======================================================================
class MockRedis:
    """最小化的内存模拟,仅实现此演示需要的 Redis 命令。"""

    def __init__(self):
        self._data: dict = {}
        self._expire_at: dict = {}
        self._mu = threading.RLock()

    def _check_expired(self, key: str) -> None:
        if key in self._expire_at and time.monotonic() > self._expire_at[key]:
            self._data.pop(key, None)
            self._expire_at.pop(key, None)

    def set(self, name, value, nx=False, ex=None) -> bool:
        with self._mu:
            self._check_expired(name)
            if nx and name in self._data:
                return False
            self._data[name] = value
            if ex is not None:
                self._expire_at[name] = time.monotonic() + ex
            return True

    def get(self, key):
        with self._mu:
            self._check_expired(key)
            return self._data.get(key)

    def delete(self, *keys) -> int:
        with self._mu:
            count = 0
            for k in keys:
                if k in self._data:
                    self._data.pop(k, None)
                    self._expire_at.pop(k, None)
                    count += 1
            return count

    def expire(self, key, seconds) -> bool:
        with self._mu:
            self._check_expired(key)
            if key not in self._data:
                return False
            self._expire_at[key] = time.monotonic() + seconds
            return True

    def incr(self, key) -> int:
        with self._mu:
            self._data[key] = int(self._data.get(key, 0)) + 1
            return self._data[key]

    def register_script(self, script_src):
        def _run(keys=None, args=None):
            with self._mu:
                key = keys[0]
                expected = args[0]
                self._check_expired(key)
                if self._data.get(key) == expected:
                    self._data.pop(key, None)
                    self._expire_at.pop(key, None)
                    return 1
                return 0
        return _run

    def flushall(self) -> None:
        with self._mu:
            self._data.clear()
            self._expire_at.clear()


def get_redis_client():
    """优先连接真实 Redis, 失败则退回 Mock。"""
    if not HAS_REDIS:
        print("[INFO] redis package not installed, using in-memory MockRedis")
        return MockRedis()
    try:
        r = redis.Redis(host="localhost", port=6379, db=0, socket_timeout=1.0)
        r.ping()
        print("[INFO] Connected to real Redis server at localhost:6379")
        r.flushall()
        return r
    except Exception as e:
        print(f"[INFO] Real Redis not available ({e}), using in-memory MockRedis")
        return MockRedis()


# ======================================================================
# 场景一: 没有 Fencing Token 保护 → 两个客户端都写入, 数据错乱
# ======================================================================
def scenario_unsafe_no_fencing(redis_client, resource: FencedResource, lock_key: str,
                               ttl: int = 3, gc_pause: float = 5.0):
    """
    时序 (时间轴 t=0 开始):
      t=0    Client A 获取锁 (TTL=3s)
      t=0.1  Client A 进入业务逻辑,准备写入前突然发生 STW GC 停顿 5s
      t=3.0  Redis 中 A 的锁自动过期
      t=3.1  Client B 成功获取锁,执行写入 (balance += 100)
      t=5.1  Client A 从 GC 中苏醒,自认为仍持有锁,也执行写入 (balance += 100)

    预期结果: 两个客户端都写成功 → balance = 200 (错误!应该只有一个成功)
    """
    print("\n" + "#" * 70)
    print("# SCENARIO 1: UNSAFE — 没有 Fencing Token 保护")
    print("#")
    print("# 模拟: Client A 持有锁期间发生 STW GC 停顿 (sleep {:.1f}s)".format(gc_pause))
    print("#       锁 TTL={}s < GC 停顿时间 → 锁过期 → Client B 获取锁".format(ttl))
    print("#       A 醒来后误写资源 → 并发写导致数据错乱".format(ttl))
    print("#" * 70)

    gc_happened_barrier = threading.Barrier(2, timeout=15)

    # --- Client A ---
    def client_a_unsafe():
        lock_a = RedisDistributedLock(redis_client, lock_key, ttl_seconds=ttl,
                                      watchdog_enabled=False)
        token = lock_a.acquire(timeout_seconds=5)
        if token is None:
            print("  [Client-A] ❌ Failed to acquire lock")
            return
        print(f"  [Client-A] ✅ Acquired lock, got (unused) fencing token={token}")

        # 模拟: 即将写入资源前, 发生了超长 STW GC 停顿
        print(f"  [Client-A] 💤 Entering simulated STW GC pause ({gc_pause:.1f}s)...")
        gc_happened_barrier.wait()
        time.sleep(gc_pause)
        print(f"  [Client-A] ⏰ Woke up from GC pause. Still thinks it holds the lock.")

        # 没有 fencing token! 直接写, 只要自己认为拿着锁就写
        ok, msg = resource.write_unfenced("Client-A", +100)
        print(f"  [Client-A] {msg}")

        # 尝试释放锁 (会被 Lua 脚本拒绝, 因为 value 不匹配了)
        released = lock_a.release()
        print(f"  [Client-A] release() returned {released} "
              f"(Lua script prevented releasing B's lock)")

    # --- Client B ---
    def client_b_unsafe():
        time.sleep(0.5)
        gc_happened_barrier.wait()
        time.sleep(ttl + 0.5)  # 等 A 的锁过期

        lock_b = RedisDistributedLock(redis_client, lock_key, ttl_seconds=ttl,
                                      watchdog_enabled=False)
        token = lock_b.acquire(timeout_seconds=5)
        if token is None:
            print("  [Client-B] ❌ Failed to acquire lock (unexpected)")
            return
        print(f"  [Client-B] ✅ Acquired lock after A's lock expired, token={token}")

        ok, msg = resource.write_unfenced("Client-B", +100)
        print(f"  [Client-B] {msg}")

        lock_b.release()
        print(f"  [Client-B] 🔓 Released lock normally")

    ta = threading.Thread(target=client_a_unsafe, name="Client-A-Unsafe")
    tb = threading.Thread(target=client_b_unsafe, name="Client-B-Unsafe")
    ta.start()
    tb.start()
    ta.join()
    tb.join()

    print(f"\n  ❗ EXPECTED balance = 100 (only ONE client should have written)")
    print(f"  ❗ ACTUAL   balance = {resource.balance} (TWO writes! Data corruption)")
    resource.print_log()


# ======================================================================
# 场景二: 有 Fencing Token 保护 → 过期锁持有者的迟到写操作被拒绝
# ======================================================================
def scenario_safe_with_fencing(redis_client, resource: FencedResource, lock_key: str,
                                ttl: int = 3, gc_pause: float = 5.0):
    """
    同样的时序, 但写操作必须携带 fencing token:
      t=0    Client A 获取锁, fencing_token = 1
      t=0.1  Client A 发生 STW GC 停顿 5s
      t=3.0  Redis 中 A 的锁自动过期
      t=3.1  Client B 获取锁, fencing_token = 2, 写入成功 (processed_token=2)
      t=5.1  Client A 苏醒, 带着 token=1 尝试写入
             → 资源端发现 1 <= 2, 直接 REJECT

    预期结果: 只有 B 写成功, A 被拒绝 → balance = 100 (正确)
    """
    print("\n" + "#" * 70)
    print("# SCENARIO 2: SAFE — 带 Fencing Token 保护")
    print("#")
    print("# 相同时序, 但写入必须携带单调递增的 token。")
    print("# Client A 苏醒后带着旧 token=1 写资源 → 被资源端 REJECT。")
    print("#" * 70)

    gc_happened_barrier = threading.Barrier(2, timeout=15)

    def client_a_safe():
        lock_a = RedisDistributedLock(redis_client, lock_key, ttl_seconds=ttl,
                                      watchdog_enabled=False)
        token = lock_a.acquire(timeout_seconds=5)
        if token is None:
            print("  [Client-A] ❌ Failed to acquire lock")
            return
        print(f"  [Client-A] ✅ Acquired lock, fencing_token={token}")

        print(f"  [Client-A] 💤 Entering simulated STW GC pause ({gc_pause:.1f}s)...")
        gc_happened_barrier.wait()
        time.sleep(gc_pause)
        print(f"  [Client-A] ⏰ Woke up from GC pause. Still thinks it holds the lock.")

        # 带着 token 写 → 会被资源端拒绝
        ok, msg = resource.write_fenced("Client-A", +100, token)
        print(f"  [Client-A] {msg}")

        released = lock_a.release()
        print(f"  [Client-A] release() returned {released}")

    def client_b_safe():
        time.sleep(0.5)
        gc_happened_barrier.wait()
        time.sleep(ttl + 0.5)

        lock_b = RedisDistributedLock(redis_client, lock_key, ttl_seconds=ttl,
                                      watchdog_enabled=False)
        token = lock_b.acquire(timeout_seconds=5)
        if token is None:
            print("  [Client-B] ❌ Failed to acquire lock (unexpected)")
            return
        print(f"  [Client-B] ✅ Acquired lock after A's lock expired, fencing_token={token}")

        ok, msg = resource.write_fenced("Client-B", +100, token)
        print(f"  [Client-B] {msg}")

        lock_b.release()
        print(f"  [Client-B] 🔓 Released lock normally")

    ta = threading.Thread(target=client_a_safe, name="Client-A-Safe")
    tb = threading.Thread(target=client_b_safe, name="Client-B-Safe")
    ta.start()
    tb.start()
    ta.join()
    tb.join()

    print(f"\n  ✅ EXPECTED balance = 100 (only Client B wrote)")
    print(f"  ✅ ACTUAL   balance = {resource.balance}")
    resource.print_log()


# ======================================================================
# 场景三: 看门狗续期 + 同 token 多次写入 + 旧 token 被拒绝
# ======================================================================
def scenario_watchdog_and_multi_write(redis_client, resource: FencedResource,
                                       lock_key: str,
                                       ttl: int = 2, business_time: float = 5.0):
    """
    验证三个关键行为:

    1. 看门狗续期: Client-A 持锁执行业务 {business_time}s, 超过 TTL={ttl}s,
       但看门狗在后台续期 → Client-B 在原 TTL 到点时抢不到锁。

    2. 同 token 多次写入: Client-A 在一次持锁期间用同一个 fencing token
       连续写 3 次资源, 全部应该成功。

    3. 旧 token 被拒绝: Client-A 释放锁后, Client-B 获取新锁 (更大 token)。
       此后 Client-A 用旧 token 再写 → 应该被拒绝。

    时序:
      t=0     Client-A 获取锁, token=T_A, 看门狗启动 (TTL={ttl}s)
      t=0.5   Client-B 尝试抢锁 → 失败 (A 的锁还在, 看门狗续期中)
      t=1     Client-A 写入 #1 (token=T_A)  → balance += 10
      t=2     原始 TTL 到了 → 但看门狗已续期, 锁仍在
      t=2.5   Client-B 再次尝试抢锁 → 仍然失败
      t=3     Client-A 写入 #2 (token=T_A)  → balance += 20
      t=4     Client-A 写入 #3 (token=T_A)  → balance += 30
      t=5     Client-A 主动释放锁, 看门狗停止
      t=5.1   Client-B 获取锁, token=T_B (> T_A)
      t=5.5   Client-B 写入 (token=T_B)     → balance += 50
      t=6     Client-A 用旧 token=T_A 再写  → REJECTED!
    """
    print("\n" + "#" * 70)
    print("# SCENARIO 3: 看门狗续期 + 同 token 多次写入 + 旧 token 被拒绝")
    print("#")
    print(f"# TTL={ttl}s, 业务执行时间={business_time}s (> TTL)")
    print("# 看门狗开启 → 在业务执行期间持续续期,锁不会过期")
    print("#" * 70)

    a_lock_ref: list = [None]
    a_token_ref: list = [None]
    phase = threading.Barrier(2, timeout=15)

    results_a: list = []
    results_b: list = []
    results_late_a: list = []

    def client_a():
        lock_a = RedisDistributedLock(redis_client, lock_key, ttl_seconds=ttl,
                                      watchdog_enabled=True)
        token = lock_a.acquire(timeout_seconds=5)
        if token is None:
            print("  [Client-A] ❌ Failed to acquire lock")
            phase.wait()
            return
        a_lock_ref[0] = lock_a
        a_token_ref[0] = token
        print(f"  [Client-A] ✅ Acquired lock, fencing_token={token}, "
              f"watchdog started (interval={lock_a.watchdog_interval:.1f}s)")

        # 通知 B 可以开始尝试抢锁
        phase.wait()

        # ---- 写入 #1 ----
        time.sleep(1.0)
        ok, msg = resource.write_fenced("Client-A", +10, token)
        results_a.append(("write-1", ok, msg))
        print(f"  [Client-A] write #1: {msg}")

        # ---- 模拟业务执行 (超过原始 TTL) ----
        time.sleep(business_time / 2)

        # ---- 写入 #2 (同一 token) ----
        ok, msg = resource.write_fenced("Client-A", +20, token)
        results_a.append(("write-2", ok, msg))
        print(f"  [Client-A] write #2: {msg}")

        # ---- 继续业务 ----
        time.sleep(business_time / 2 - 1.0)

        # ---- 写入 #3 (同一 token) ----
        ok, msg = resource.write_fenced("Client-A", +30, token)
        results_a.append(("write-3", ok, msg))
        print(f"  [Client-A] write #3: {msg}")

        # ---- 主动释放锁 ----
        released = lock_a.release()
        print(f"  [Client-A] 🔓 Released lock (release={released}), watchdog stopped")

    def client_b():
        # 等待 A 获取锁
        phase.wait()

        # ---- 原始 TTL 到点时尝试抢锁 → 应该失败 (看门狗在续期) ----
        time.sleep(ttl + 0.5)
        t_now_1 = ttl + 0.5
        lock_b = RedisDistributedLock(redis_client, lock_key, ttl_seconds=ttl,
                                      watchdog_enabled=False)
        token_b = lock_b.acquire(timeout_seconds=0.5)
        if token_b is None:
            print(f"  [Client-B] 🚫 Failed to grab lock at t≈{t_now_1:.1f}s "
                  f"(watchdog is renewing — GOOD)")
        else:
            print(f"  [Client-B] ⚠️ Unexpectedly grabbed lock at t≈{t_now_1:.1f}s!")
            lock_b.release()
            return

        # ---- 再等 1.5s, 仍然在 A 持锁期间 (A 约 5s 后才释放) ----
        time.sleep(1.5)
        t_now_2 = t_now_1 + 1.5
        lock_b2 = RedisDistributedLock(redis_client, lock_key, ttl_seconds=ttl,
                                       watchdog_enabled=False)
        token_b2 = lock_b2.acquire(timeout_seconds=0.5)
        if token_b2 is None:
            print(f"  [Client-B] 🚫 Failed to grab lock at t≈{t_now_2:.1f}s "
                  f"(watchdog still renewing — GOOD)")
        else:
            print(f"  [Client-B] ⚠️ Unexpectedly grabbed lock at t≈{t_now_2:.1f}s!")
            lock_b2.release()
            return

        # ---- 等 A 完成业务并释放锁 ----
        time.sleep(business_time - t_now_2 + 1.5)
        lock_b3 = RedisDistributedLock(redis_client, lock_key, ttl_seconds=ttl,
                                       watchdog_enabled=False)
        token_b3 = lock_b3.acquire(timeout_seconds=5)
        if token_b3 is None:
            print("  [Client-B] ❌ Failed to acquire lock after A released (unexpected)")
            return
        print(f"  [Client-B] ✅ Acquired lock after A released, fencing_token={token_b3}")

        # ---- B 写入资源 ----
        ok, msg = resource.write_fenced("Client-B", +50, token_b3)
        results_b.append(("write-B", ok, msg))
        print(f"  [Client-B] write: {msg}")

        # ---- A 用旧 token 迟到写入 ----
        old_token = a_token_ref[0]
        if old_token is not None:
            time.sleep(0.5)
            ok, msg = resource.write_fenced("Client-A(late)", +999, old_token)
            results_late_a.append(("late-write", ok, msg))
            print(f"  [Client-A(late)] stale write: {msg}")

        lock_b3.release()
        print(f"  [Client-B] 🔓 Released lock normally")

    ta = threading.Thread(target=client_a, name="Client-A-Watchdog")
    tb = threading.Thread(target=client_b, name="Client-B-Watchdog")
    ta.start()
    tb.start()
    ta.join(timeout=30)
    tb.join(timeout=30)

    # ---- 汇总验证 ----
    print("\n  ──── Verification ────")

    all_a_writes_ok = all(ok for _, ok, _ in results_a)
    b_write_ok = all(ok for _, ok, _ in results_b)
    late_a_rejected = all(not ok for _, ok, _ in results_late_a)

    expected_balance = 10 + 20 + 30 + 50  # 110

    print(f"  Client-A 3 writes with same token:  {'✅ ALL ACCEPTED' if all_a_writes_ok else '❌ SOME REJECTED'}")
    print(f"  Client-B write with new token:      {'✅ ACCEPTED' if b_write_ok else '❌ REJECTED'}")
    print(f"  Client-A late write with old token: {'✅ REJECTED' if late_a_rejected else '❌ ACCEPTED (BUG!)'}")
    print(f"  Expected balance: {expected_balance}")
    print(f"  Actual balance:   {resource.balance}")
    print(f"  Balance match:    {'✅' if resource.balance == expected_balance else '❌ MISMATCH'}")
    resource.print_log()



def explain_why_ttl_extension_is_not_enough():
    print("\n" + "#" * 70)
    print("# WHY LONGER TTL / WATCHDOG ≠ 根治方案")
    print("#" * 70)
    explanation = """
  ┌─────────────────────────────────────────────────────────────────┐
  │ 误区 1: "把 TTL 设得超长 (比如 1 小时), GC 停顿就不会过期了吧?"  │
  ├─────────────────────────────────────────────────────────────────┤
  │ 反驳:                                                           │
  │  ① 你不能 100% 保证 GC / 网络分区 一定小于 1 小时。               │
  │     JVM CMS/G1 最坏情况停顿可达数十秒, 网络分区完全可能持续小时级。 │
  │  ② 即使 99.99% 的情况都没问题, 剩下的 0.01% 一旦发生就是数据错。 │
  │     分布式系统设计必须考虑 "发生了怎么办", 而不是赌 "不会发生"。  │
  │  ③ 大 TTL 的副作用: 某客户端崩溃后, 其他客户端要等 1 小时才能    │
  │     继续工作, 可用性大大降低。                                    │
  │     => 超长 TTL = 在 "一致性" 和 "可用性" 之间做了糟糕的权衡。    │
  └─────────────────────────────────────────────────────────────────┘

  ┌─────────────────────────────────────────────────────────────────┐
  │ 误区 2: "有看门狗 (Watchdog) 自动续期, 锁就不会过期了吧?"        │
  ├─────────────────────────────────────────────────────────────────┤
  │ 反驳:                                                           │
  │  看门狗也是本进程里的一个线程, 它同样会被 STW GC 挂起!            │
  │                                                                 │
  │  时序:                                                          │
  │   t=0    A 获取锁, 看门狗启动 (计划每 3s 续期一次)              │
  │   t=0.5  A 发生 FULL GC → 整个 JVM 所有线程暂停 (包括看门狗)    │
  │   t=10   GC 仍在继续... 锁 TTL=10s 已过期 → 看门狗也没机会续期   │
  │   t=10.1 B 获取锁                                              │
  │   t=12   GC 结束, A 和看门狗同时苏醒 → 但为时已晚                │
  │                                                                 │
  │  网络分区同理: 客户端与 Redis 之间断网, 看门狗也连不上 Redis,    │
  │  续期请求全部失败, 锁还是会过期。                                 │
  └─────────────────────────────────────────────────────────────────┘

  ┌─────────────────────────────────────────────────────────────────┐
  │ 根本原因: 第三方无法区分"客户端真的活着"还是"假活着"             │
  │                                 ────────────────                │
  │  分布式锁服务 (Redis) 只能通过心跳/续期 推断客户端是否存活,       │
  │  但这是 "异步推断", 永远存在误判的可能:                          │
  │    • 推断 "死了" → 可能是假死 (GC 停顿 / 网络抖动) → 锁过期     │
  │    • 推断 "活着" → 可能真死了 (进程没续期就挂了) → 死锁          │
  │                                                                 │
  │ Fencing Token 的做法是: **承认误判不可避免, 让资源端兜底。**     │
  │  就算锁被误判过期了 (两个客户端都自认为持有锁),                   │
  │  被保护资源通过 token 比较, 也只认"锁颁发顺序在后"的那个客户端,   │
  │  另一个一定被拒。                                                │
  │  => 这是 "fail-safe" 而非 "fail-prevent"。                      │
  └─────────────────────────────────────────────────────────────────┘
"""
    print(explanation)


# ======================================================================
# main
# ======================================================================
def main():
    redis_client = get_redis_client()

    # 清理可能的残留 (真实 Redis 才需要, mock 是新实例)
    if hasattr(redis_client, 'flushall'):
        try:
            redis_client.flushall()
        except Exception:
            pass

    # 场景 1: 无 fencing token → 数据错乱
    resource_unsafe = FencedResource("unsafe-account", initial_balance=0)
    scenario_unsafe_no_fencing(redis_client, resource_unsafe,
                               lock_key="demo:lock:unsafe",
                               ttl=3, gc_pause=5.0)

    # 场景 2: 有 fencing token → 正确
    if hasattr(redis_client, 'flushall'):
        try:
            redis_client.flushall()
        except Exception:
            pass
    resource_safe = FencedResource("safe-account", initial_balance=0)
    scenario_safe_with_fencing(redis_client, resource_safe,
                               lock_key="demo:lock:safe",
                               ttl=3, gc_pause=5.0)

    # 场景 3: 看门狗续期 + 同 token 多次写入 + 旧 token 被拒绝
    if hasattr(redis_client, 'flushall'):
        try:
            redis_client.flushall()
        except Exception:
            pass
    resource_watchdog = FencedResource("watchdog-account", initial_balance=0)
    scenario_watchdog_and_multi_write(redis_client, resource_watchdog,
                                      lock_key="demo:lock:watchdog",
                                      ttl=2, business_time=5.0)

    # 为什么 TTL / 看门狗 不够
    explain_why_ttl_extension_is_not_enough()

    # 总结
    print("\n" + "#" * 70)
    print("# 总结: 完整的分布式锁安全组合拳")
    print("#" * 70)
    print("""
  一层: SET key value NX EX ttl          ← 基础互斥 + 防死锁
  二层: Lua 脚本校验 value 后 DEL        ← 防止误删别人的锁
  三层: Watchdog 后台续期                ← 减少正常业务锁过期概率
  四层: Fencing Token + 资源端校验        ← 兜底, 最终一致性保障
         ↑↑↑ 这层才是根治双写问题的关键 ↑↑↑

  前三层都在 "客户端 ↔ 锁服务 (Redis)" 之间做文章,
  第四层引入了 "被保护资源" 作为独立裁决者, 突破了
  "锁服务无法 100% 准确推断客户端死活" 这一根本困境。
""")


if __name__ == "__main__":
    main()
