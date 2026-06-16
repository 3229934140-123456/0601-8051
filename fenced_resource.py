import threading
from typing import Any, Dict, Optional, Tuple


class FencedResource:
    """
    受 Fencing Token 保护的共享资源模拟实现。

    核心思想 (Martin Kleppmann 2016):
        分布式锁本身无法保证"持有锁的客户端在操作资源时锁一定仍然有效"，
        因为 GC 停顿 / 网络分区可以发生在 "锁持有者准备写资源" 的瞬间。
        因此 **被保护资源自身必须参与一致性校验**，这就是 fencing token 的角色。

    工作流程:
    1. 每次成功获取锁时,锁服务返回一个单调递增的 fencing token。
    2. 客户端对资源发起写操作时,必须携带这个 token。
    3. 资源端持久化记录自己"已经接受过的最大 token" (processed_token)。
    4. 任何写操作的 token <= processed_token 时,资源端直接拒绝。

    为什么这样就安全了?
        假设 token 是严格单调递增的:
        - 如果客户端 B 成功获取了锁,它拿到的 token_N 一定 > 所有历史 token。
        - 不管过期的客户端 A 多晚到达,它携带的 token_M 一定满足 M < N。
        - 资源只要见过 token_N,就永远不会再接受 token_M 或更小的值。
        => A 的"迟到操作"必然被拒绝,不会污染数据。
    """

    def __init__(self, resource_name: str, initial_balance: int = 0):
        self.resource_name = resource_name
        self._balance = initial_balance
        self._processed_token: int = 0
        self._operation_log: list[Dict[str, Any]] = []
        self._mu = threading.RLock()

    # ------------------------------------------------------------------
    # 不安全版本: 没有 fencing token 校验
    # 模拟"纯靠分布式锁保护"的场景——一旦锁意外过期,就会发生并发写
    # ------------------------------------------------------------------
    def write_unfenced(self, client_id: str, delta: int) -> Tuple[bool, str]:
        """
        不校验 fencing token 的写操作。
        只要客户端"自认为"持有锁就能写,非常危险。
        """
        with self._mu:
            old = self._balance
            self._balance += delta
            self._operation_log.append({
                "mode": "unfenced",
                "client": client_id,
                "delta": delta,
                "before": old,
                "after": self._balance,
                "accepted": True,
            })
            return True, f"[UNSAFE] {client_id} applied delta={delta}, balance {old} -> {self._balance}"

    # ------------------------------------------------------------------
    # 安全版本: 带 fencing token 校验
    # 资源端主动拒绝过期锁持有者的迟到操作
    # ------------------------------------------------------------------
    def write_fenced(
        self, client_id: str, delta: int, fencing_token: int
    ) -> Tuple[bool, str]:
        """
        校验 fencing token 的写操作。

        规则: fencing_token 必须严格大于当前已处理的最大 token 才会被接受。
        这保证了操作顺序与锁的颁发顺序严格一致。
        """
        with self._mu:
            if fencing_token <= self._processed_token:
                self._operation_log.append({
                    "mode": "fenced",
                    "client": client_id,
                    "delta": delta,
                    "token": fencing_token,
                    "processed_token": self._processed_token,
                    "accepted": False,
                    "reason": "stale token (expired lock holder)",
                })
                return False, (
                    f"[REJECTED] {client_id} token={fencing_token} <= "
                    f"processed_token={self._processed_token}. "
                    f"This client's lock has EXPIRED — another client acquired the lock first."
                )

            old = self._balance
            self._balance += delta
            self._processed_token = fencing_token

            self._operation_log.append({
                "mode": "fenced",
                "client": client_id,
                "delta": delta,
                "token": fencing_token,
                "before": old,
                "after": self._balance,
                "accepted": True,
            })
            return True, (
                f"[SAFE]   {client_id} token={fencing_token} applied delta={delta}, "
                f"balance {old} -> {self._balance}"
            )

    # ------------------------------------------------------------------
    # 辅助方法
    # ------------------------------------------------------------------
    @property
    def balance(self) -> int:
        with self._mu:
            return self._balance

    @property
    def processed_token(self) -> int:
        with self._mu:
            return self._processed_token

    def get_operation_log(self) -> list[Dict[str, Any]]:
        with self._mu:
            return list(self._operation_log)

    def print_log(self) -> None:
        print(f"\n=== Operation Log for '{self.resource_name}' ===")
        for entry in self._operation_log:
            accepted = "✔ ACCEPTED" if entry["accepted"] else "✘ REJECTED"
            mode = entry["mode"].upper()
            client = entry["client"]
            if entry["mode"] == "fenced":
                tok_info = f" token={entry.get('token', '?')}"
            else:
                tok_info = ""
            if not entry["accepted"]:
                reason = f" — {entry.get('reason', '')}"
            else:
                reason = f" — balance {entry.get('before', '?')} -> {entry.get('after', '?')}"
            print(f"  [{mode}] {accepted} {client}{tok_info}{reason}")
        print(f"  Final balance: {self._balance}")
        print(f"  Processed token (high-water mark): {self._processed_token}")
        print("=" * 60)
