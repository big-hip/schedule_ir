# # -*- coding: utf-8 -*-
# """
# PipeDream-2BW (Two-Bubble Warmup) pipeline schedule strategy.

# Compared to 1F1B, 2BW allows two forward "bubbles" during warmup and flush,
# reducing overall pipeline idle time. The key idea is to start the backward pass
# two microbatches earlier than the final forward ends, resulting in higher utilization.

# Author: ChatGPT (2025)
# """

# from typing import List
# from ..events import Event
# from ..params import ScheduleSpec


# class PipeDream2BWStrategy:
#     """
#     PipeDream-2BW scheduling (Two-Bubble Warmup).

#     Stage behavior:
#       - Warmup: fill S + 1 microbatches
#       - Steady: each step executes 1 forward and 1 backward in overlap
#       - Flush: drain the pipeline with S + 1 backward steps

#     Differences from 1F1B:
#       - Forward runs one additional round before starting the first backward.
#       - Backward starts earlier (two bubbles lead and tail).
#       - Maintains same throughput in steady phase, with reduced idle regions.

#     """

#     def build_fb_events(self, spec: ScheduleSpec) -> List[Event]:
#         S = spec.num_stages
#         M = spec.num_micro
#         N = spec.num_mini
#         Fdur = spec.forward
#         Bdur = spec.backward
#         lane_compute = spec.lanes.get("compute", 0)

#         events: List[Event] = []
#         stage_free = [0.0] * S  # cumulative stage availability

#         for n in range(N):
#             # -----------------
#             # 1. Warmup: fill S+1 microbatches
#             # -----------------
#             for warm in range(S + 1):
#                 for s in range(min(warm + 1, S)):
#                     m = warm - s
#                     if m >= M:
#                         continue
#                     start = max(stage_free[s], stage_free[s - 1] if s > 0 else 0.0)
#                     dur = Fdur[s]
#                     events.append(Event(stage=s, lane=lane_compute,
#                                         start=start, dur=dur,
#                                         op="F", mini=n, micro=m))
#                     stage_free[s] = start + dur

#             # -----------------
#             # 2. Steady phase: alternate F/B with two bubbles lead
#             # -----------------
#             for step in range(M - (S + 1)):
#                 for s in range(S):
#                     # forward micro index
#                     f_m = step + s + 1
#                     if 0 <= f_m < M:
#                         start_f = max(stage_free[s], stage_free[s - 1] if s > 0 else 0.0)
#                         dur_f = Fdur[s]
#                         events.append(Event(stage=s, lane=lane_compute,
#                                             start=start_f, dur=dur_f,
#                                             op="F", mini=n, micro=f_m))
#                         stage_free[s] = start_f + dur_f

#                     # backward micro index (2-bubble offset)
#                     b_m = step + s - (S - 1)
#                     if 0 <= b_m < M:
#                         start_b = stage_free[s]
#                         dur_b = Bdur[s]
#                         events.append(Event(stage=s, lane=lane_compute,
#                                             start=start_b, dur=dur_b,
#                                             op="B", mini=n, micro=b_m))
#                         stage_free[s] = start_b + dur_b

#             # -----------------
#             # 3. Flush phase: drain remaining backward
#             # -----------------
#             for flush in range(S + 1):
#                 for s in reversed(range(S)):
#                     m = M - S - 1 + flush + (S - 1 - s)
#                     if m < 0 or m >= M:
#                         continue
#                     start = stage_free[s]
#                     dur = Bdur[s]
#                     events.append(Event(stage=s, lane=lane_compute,
#                                         start=start, dur=dur,
#                                         op="B", mini=n, micro=m,))
#                     stage_free[s] = start + dur

#         return events

# -*- coding: utf-8 -*-
# -*- coding: utf-8 -*-
"""
PipeDream-2BW 调度策略（简化 demo 版）

说明：
- 本实现只关心时间表（F / B 在各 stage / micro / mini 上的排布），
  不涉及真实的权重版本管理。
- 思路：把所有 mini × micro 展开成一条全局序列，在这一条上做
  "async 1F1B" 调度：
    * 对每个 (global_k, stage s) 有一次前向 F 和一次反向 B。
    * 依赖：
        F(s,k) 依赖 F(s-1,k)          （s=0 无此依赖）
        B(s,k) 依赖 F(s,k)、B(s+1,k)  （s=S-1 无后者依赖）
    * 调度：
        - 计算所有 ready 事件的最早可开始时间 start
        - 按 (start, 是否B优先, stage越靠后越优先, global_k) 选一个执行

  对于你 demo 里的单 step / 单 mini 场景，这个时间表就是一个
  高利用率的 1F1B/2BW 样式流水线，不会出现奇怪的空洞。
"""

from typing import List, Tuple, Dict
from ..events import Event
from ..params import ScheduleSpec


class PipeDream2BWStrategy:
    def build_fb_events(self, spec: ScheduleSpec) -> List[Event]:
        S = spec.num_stages
        M = spec.num_micro
        N = spec.num_mini
        lane_compute = spec.lanes["compute"]

        if S <= 0 or M <= 0 or N <= 0:
            return []

        # 每个 stage 的 F / B 时长（ScheduleSpec 已保证长度与 num_stages 一致）
        f_dur = list(spec.forward)
        b_dur = list(spec.backward)

        # 把所有 mini × micro 展开成一条全局 micro 序列 k ∈ [0, N*M)
        total_micro = N * M
        if total_micro == 0:
            return []

        def global_to_mini_micro(k: int) -> Tuple[int, int]:
            mini = k // M
            micro = k % M
            return mini, micro

        # 每个 stage 当前空闲时间
        stage_free = [0.0 for _ in range(S)]

        # 记录 F / B 完成时间：key = (global_k, stage)
        f_finish: Dict[Tuple[int, int], float] = {}
        b_finish: Dict[Tuple[int, int], float] = {}

        # ready 队列中的元素：(op, stage, global_k)，op ∈ {"F","B"}
        ready: List[Tuple[str, int, int]] = []

        # 初始：所有 global micro 的 stage0 前向可以排队
        for k in range(total_micro):
            ready.append(("F", 0, k))
        enqueued_F = {(0, k) for k in range(total_micro)}
        enqueued_B = set()

        events: List[Event] = []
        # 每个 (s,k) 含一次 F 和一次 B
        total_tasks = 2 * S * total_micro
        scheduled = 0

        while scheduled < total_tasks:
            candidates = []

            for op, s, k in ready:
                if op == "F":
                    # F 依赖同 micro 下游 stage 的 F
                    if s > 0 and (k, s - 1) not in f_finish:
                        continue
                    dep = 0.0 if s == 0 else f_finish[(k, s - 1)]
                    start = max(stage_free[s], dep)
                    # rank=1：让 F 在同一时刻略逊于 B
                    candidates.append((start, 1, -s, op, s, k))
                else:  # "B"
                    # B 依赖本 stage 的 F 以及下游 stage 的 B
                    if (k, s) not in f_finish:
                        continue
                    if s < S - 1 and (k, s + 1) not in b_finish:
                        continue
                    dep_f = f_finish[(k, s)]
                    dep_b = b_finish[(k, s + 1)] if s < S - 1 else dep_f
                    start = max(stage_free[s], dep_f, dep_b)
                    # rank=0：B 优先；同 start 下 stage 越靠后越优先
                    candidates.append((start, 0, -s, op, s, k))

            if not candidates:
                # 理论上不该出现；出现说明依赖图有问题
                raise RuntimeError("PipeDream2BWStrategy deadlock: no schedulable candidates")

            # 选 (start 最小 -> B 优先 -> stage 靠后 -> global_k 小)
            candidates.sort(key=lambda x: (x[0], x[1], x[2], x[5]))
            start, _, _, op, s, k = candidates[0]

            mini, micro = global_to_mini_micro(k)
            dur = f_dur[s] if op == "F" else b_dur[s]

            ev = Event(
                stage=s,
                lane=lane_compute,
                start=start,
                dur=float(dur),
                op=op,
                mini=mini,
                micro=micro,
            )
            events.append(ev)

            end_t = start + float(dur)
            stage_free[s] = end_t
            scheduled += 1

            # 从 ready 移除当前事件
            try:
                ready.remove((op, s, k))
            except ValueError:
                # 理论上不会发生；容错处理
                pass

            if op == "F":
                f_finish[(k, s)] = end_t
                # 解锁同 micro 下一个 stage 的 F，或者尾 stage 的 B 链起点
                if s + 1 < S:
                    if (s + 1, k) not in enqueued_F:
                        enqueued_F.add((s + 1, k))
                        ready.append(("F", s + 1, k))
                else:
                    if (s, k) not in enqueued_B:
                        enqueued_B.add((s, k))
                        ready.append(("B", s, k))
            else:  # "B"
                b_finish[(k, s)] = end_t
                # 解锁上游 stage 的 B
                if s - 1 >= 0 and (s - 1, k) not in enqueued_B:
                    enqueued_B.add((s - 1, k))
                    ready.append(("B", s - 1, k))

        return events
