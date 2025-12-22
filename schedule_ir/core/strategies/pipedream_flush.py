# -*- coding: utf-8 -*-
from typing import List, Tuple, Dict
from ..events import Event
from ..params import ScheduleSpec

class OneFOneBStrategy:
    """
    时间优先（time-first）的 1F1B（PipeDream-Flush 风格）：
      - 任务：F[s,m] / B[s,m]
      - 依赖：
          F[s,m] 依赖 F[s-1,m]（若 s>0）
          B[s,m] 依赖 F[s,m] 以及 B[s+1,m]（若 s<S-1；否则仅依赖 F[S-1,m]）
      - 资源：每个 stage 在任意时刻只能跑一个任务（通过 stage_free[s] 体现）
      - 选择：优先选择"最早可开始(start)的任务"；若同一 start，则 B 优先，再以更深的 stage（s 大）优先
      - 多 mini 顺序执行；stage_free 在 mini 间沿用（不重置）
    """

    def build_fb_events(self, spec: ScheduleSpec) -> List[Event]:
        S = spec.num_stages
        M = spec.num_micro
        N = spec.num_mini
        Fdur = spec.forward
        Bdur = spec.backward
        lane_compute = spec.lanes.get("compute", 0)

        events: List[Event] = []
        stage_free = [0.0] * S              # 跨 mini 延续

        # 为消除浮点比较误差，允许一个很小的容差
        EPS = 1e-9

        for n in range(N):
            # 当前 mini 的完成时间表
            f_finish: Dict[Tuple[int, int], float] = {}  # (s,m) -> finish_time
            b_finish: Dict[Tuple[int, int], float] = {}  # (s,m) -> finish_time

            # 初始可考虑的任务：F[0,m]，其余任务由依赖完成后解锁
            ready: List[Tuple[str, int, int]] = [("F", 0, m) for m in range(M)]
            enqueued_F = {(0, m) for m in range(M)}
            enqueued_B = set()

            scheduled = 0
            total_needed = 2 * S * M  # 本 mini 必须完成的任务总数

            while scheduled < total_needed:
                candidates = []  # (start, tie_b_is_0, -s, typ, s, m)

                for typ, s, m in ready:
                    if typ == "F":
                        # 依赖：若 s>0 则需 F[s-1,m] 先完成
                        if s > 0 and (s-1, m) not in f_finish:
                            continue
                        dep = 0.0 if s == 0 else f_finish[(s-1, m)]
                        start = max(stage_free[s], dep)
                        # 时间优先；同一时间点内 B 优先（tie_b_is_0=1 for F, 0 for B）
                        candidates.append((start, 1, -s, typ, s, m))

                    else:  # "B"
                        # 依赖：F[s,m] 完成；且若 s<S-1 则需 B[s+1,m] 完成
                        if (s, m) not in f_finish:
                            continue
                        if s < S - 1 and (s+1, m) not in b_finish:
                            continue
                        dep_f = f_finish[(s, m)]
                        dep_b = b_finish[(s+1, m)] if s < S - 1 else dep_f
                        start = max(stage_free[s], dep_f, dep_b)
                        candidates.append((start, 0, -s, typ, s, m))

                if not candidates:
                    # 若暂时无可排任务，尝试用拓扑方式解锁更多任务（通常很少触发）
                    self._unlock_more_tasks(S, M, f_finish, b_finish, enqueued_F, enqueued_B, ready)
                    # 若依旧为空，则说明图或依赖有问题
                    if not candidates:
                        raise RuntimeError("No schedulable candidates; dependency unlocking failed.")
                    continue

                # 选择最早可开始的任务；若 start 相同，则 B 优先，再选更深的 stage
                candidates.sort(key=lambda x: (x[0], x[1], x[2]))
                start, _, _, typ, s, m = candidates[0]

                if typ == "F":
                    dur = Fdur[s]
                    ev = Event(stage=s, lane=lane_compute, start=start, dur=dur, op="F", mini=n, micro=m)
                    events.append(ev)
                    finish = start + dur
                    stage_free[s] = finish
                    f_finish[(s, m)] = finish
                    scheduled += 1

                    # 解锁：F[s+1,m] 或（若 s 是尾层）B[S-1,m]
                    if s + 1 < S:
                        if (s+1, m) not in enqueued_F:
                            ready.append(("F", s+1, m))
                            enqueued_F.add((s+1, m))
                    else:
                        if (S-1, m) not in enqueued_B:
                            ready.append(("B", S-1, m))
                            enqueued_B.add((S-1, m))

                else:  # "B"
                    dur = Bdur[s]
                    ev = Event(stage=s, lane=lane_compute, start=start, dur=dur, op="B", mini=n, micro=m)
                    events.append(ev)
                    finish = start + dur
                    stage_free[s] = finish
                    b_finish[(s, m)] = finish
                    scheduled += 1

                    # 解锁：B[s-1,m]
                    if s - 1 >= 0 and (s-1, m) not in enqueued_B:
                        ready.append(("B", s-1, m))
                        enqueued_B.add((s-1, m))

                # 移除已执行的任务
                try:
                    ready.remove((typ, s, m))
                except ValueError:
                    # 可能被重复解锁到 ready（防御处置）
                    pass

                # 继续拓扑解锁，防止 ready 枯竭
                self._unlock_more_tasks(S, M, f_finish, b_finish, enqueued_F, enqueued_B, ready)

            # 完整性检查：每个 (s,m) 必须有一条 F 和一条 B
            for s in range(S):
                for m in range(M):
                    if (s, m) not in f_finish:
                        raise AssertionError(f"[mini={n}] Missing F at stage={s}, micro={m}")
                    if (s, m) not in b_finish:
                        raise AssertionError(f"[mini={n}] Missing B at stage={s}, micro={m}")

        return events

    @staticmethod
    def _unlock_more_tasks(
        S: int, M: int,
        f_finish: Dict[Tuple[int, int], float],
        b_finish: Dict[Tuple[int, int], float],
        enqF: set, enqB: set, ready: List[Tuple[str, int, int]]
    ):
        """
        拓扑式解锁更多任务，避免 ready 枯竭：
          - 若 F[s,m] 完成且 s+1 存在：解锁 F[s+1,m]
          - 若 F[S-1,m] 完成：解锁 B[S-1,m]
          - 若 B[s,m] 完成且 s-1 存在：解锁 B[s-1,m]
        """
        for (s, m) in list(f_finish.keys()):
            if s + 1 < S and (s+1, m) not in enqF:
                ready.append(("F", s+1, m)); enqF.add((s+1, m))
            if s == S - 1 and (s, m) not in enqB:
                ready.append(("B", s, m)); enqB.add((s, m))

        for (s, m) in list(b_finish.keys()):
            if s - 1 >= 0 and (s-1, m) not in enqB:
                ready.append(("B", s-1, m)); enqB.add((s-1, m))
