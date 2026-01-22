# # ppflow/strategies/xpipe.py
# # -*- coding: utf-8 -*-
# from typing import List, Tuple, Optional
# from ..events import Event
# from ..params import ScheduleSpec

# class XPipeStrategy:
#     """
#     XPipe 调度（跨 mini 异步交错，无全局 flush）。
#     约束：
#       - 输入/输出接口保持不变：build_fb_events(spec)->List[Event]；Event 字段不变
#       - 仅改中间“生成逻辑”，修复潜在死循环
#     思路：
#       - 显式构建待调度集合 pending = {F(s,k), B(s,k)}；每轮选择一个最早可开的任务执行
#       - 依赖满足即可候选；若无候选则属于逻辑不可能（因 F(0,k) 总能逐步就绪）
#       - 优先级：F@stage0 注水最高，其次 B，其次普通 F；同起点先更深 s，再更小 k
#     记号：
#       - 全局 micro 索引 k ∈ [0, N*M)，mini = k//M，micro = k%M
#     """
#     def build_fb_events(self, spec: ScheduleSpec) -> List[Event]:
#         S: int = int(spec.num_stages)
#         M: int = int(spec.num_micro)
#         N: int = int(getattr(spec, "num_mini", 1))

#         # 时长（逐 stage 列表）
#         Fdur = spec.forward if isinstance(spec.forward, list) else [spec.forward] * S
#         Bdur = spec.backward if isinstance(spec.backward, list) else [spec.backward] * S
#         lane_compute = spec.lanes.get("compute", 0)

#         total = N * M  # 全局 micro 数
#         events: List[Event] = []

#         # 资源与完成时间
#         stage_free = [0.0] * S               # 每个 stage 可用时间
#         f_finish = {}  # (k, s) -> t
#         b_finish = {}  # (k, s) -> t

#         # 待调度集合（显式列出所有 F/B 任务，保证每轮至少调度一个 → 无死循环）
#         pending = []  # 列表项：(kind, s, k)；kind ∈ {"F","B"}
#         for k in range(total):
#             for s in range(S):
#                 pending.append(("F", s, k))
#                 pending.append(("B", s, k))

#         # 依赖就绪时间计算；就绪则返回最早可开时间，否则返回 None
#         def ready_time(kind: str, s: int, k: int):
#             deps = [stage_free[s]]
#             if kind == "F":
#                 if s == 0:
#                     # 注水：只需本 stage 空闲
#                     return max(deps)
#                 # 依赖前一 stage 的 F 完成
#                 if (k, s-1) not in f_finish:
#                     return None
#                 deps.append(f_finish[(k, s-1)])
#                 return max(deps)

#             # kind == "B"
#             # 依赖本层 F 完成
#             if (k, s) not in f_finish:
#                 return None
#             deps.append(f_finish[(k, s)])
#             # 依赖下一层 B 完成（尾层例外）
#             if s < S - 1:
#                 if (k, s+1) not in b_finish:
#                     return None
#                 deps.append(b_finish[(k, s+1)])
#             return max(deps)

#         # 选择器优先级
#         def pri(kind: str, s: int, k: int, start: float):
#             inject = (kind == "F" and s == 0)
#             return (-1 if inject else (0 if kind == "B" else 1), start, -s, k)

#         # 主循环：每轮调度 1 个任务（pending 严格减少 → 必终止）
#         while pending:
#             candidates: List[tuple] = []  # (start, kind, s, k)

#             # 枚举可开任务
#             for (kind, s, k) in pending:
#                 t0 = ready_time(kind, s, k)
#                 if t0 is not None:
#                     candidates.append((t0, kind, s, k))

#             if not candidates:
#                 # 防御：选一个最早的 F@stage0 作为注水候选
#                 best = None
#                 for (kind, s, k) in pending:
#                     if kind == "F" and s == 0:
#                         t0 = stage_free[0]
#                         cand = (t0, kind, s, k)
#                         if best is None or pri(*cand[1:], cand[0]) < pri(*best[1:], best[0]):
#                             best = cand
#                 if best is None:
#                     # 极端情况下仍无（理论上不可能），直接中断，避免死循环
#                     break
#                 candidates.append(best)

#             # 选出优先级最高的候选
#             start, kind, s, k = min(candidates, key=lambda c: pri(c[1], c[2], c[3], c[0]))
#             mini = k // M
#             micro = k % M

#             # 执行并记录
#             if kind == "F":
#                 dur = float(Fdur[s])
#                 ev = Event(stage=s, lane=lane_compute, start=start, dur=dur,
#                            op="F", mini=mini, micro=micro)
#                 events.append(ev)
#                 finish = start + dur
#                 stage_free[s] = finish
#                 f_finish[(k, s)] = finish
#             else:
#                 dur = float(Bdur[s])
#                 ev = Event(stage=s, lane=lane_compute, start=start, dur=dur,
#                            op="B", mini=mini, micro=micro)
#                 events.append(ev)
#                 finish = start + dur
#                 stage_free[s] = finish
#                 b_finish[(k, s)] = finish

#             # 移除该任务
#             pending.remove((kind, s, k))

#         return events
# -*- coding: utf-8 -*-
# -*- coding: utf-8 -*-
"""
XPipeStrategy: 跨 mini 的 async 1F1B 调度骨架（无全局 flush）。

语义：
- 把 num_mini * num_micro 展开成一条全局 micro 链（global_k ∈ [0, N*M)）
- 对每个 (stage s, global_k) 有一个 F(s,k) 和一个 B(s,k)
- 依赖：
  * F(s,k) 依赖 F(s-1,k)
  * B(s,k) 依赖 F(s,k) 和 B(s+1,k)
- 调度策略：
  * 始终在“最早可开跑”的事件里选一个
  * 同一时间点：B 优先，其次 stage 越靠尾（s 越大）优先
  * 这样自然形成 async 1F1B 流水，跨 mini 串起来，不在 mini 间 flush

注意：这里只负责 F/B 的时序，权重预测 / 更新由 insertions 里单独的算子完成。
"""

from typing import List, Tuple, Dict
from ..events import Event
from ..params import ScheduleSpec


class XPipeStrategy:
    def build_fb_events(self, spec: ScheduleSpec) -> List[Event]:
        S = spec.num_stages
        M = spec.num_micro
        N = spec.num_mini
        lane_compute = spec.lanes["compute"]

        f_dur = spec.forward
        b_dur = spec.backward

        total_micro = N * M
        if total_micro == 0:
            return []

        # 每个 stage 当前空闲时间
        stage_free = [0.0 for _ in range(S)]

        # 记录 F / B 完成时间：key = (global_k, stage)
        f_finish: Dict[Tuple[int, int], float] = {}
        b_finish: Dict[Tuple[int, int], float] = {}

        # ready 队列中的元素：("F"|"B", stage, global_k)
        ready: List[Tuple[str, int, int]] = []

        # 初始：所有 global micro 的 stage0 前向可以排队
        for k in range(total_micro):
            ready.append(("F", 0, k))
        enqueued_F = {(0, k) for k in range(total_micro)}
        enqueued_B = set()

        events: List[Event] = []
        total_tasks = 2 * S * total_micro  # 每个 (s,k) 一次 F + 一次 B
        scheduled = 0

        def global_to_mini_micro(k: int) -> Tuple[int, int]:
            mini = k // M
            micro = k % M
            return mini, micro

        while scheduled < total_tasks:
            candidates = []

            for op, s, k in ready:
                if op == "F":
                    # 依赖：同 micro 下游 stage 的 F 完成
                    if s > 0 and (k, s - 1) not in f_finish:
                        continue
                    dep = 0.0 if s == 0 else f_finish[(k, s - 1)]
                    start = max(stage_free[s], dep)
                    # rank=1 让 F 的优先级略低于 B
                    candidates.append((start, 1, -s, op, s, k))
                else:  # "B"
                    # 依赖：本 stage 的 F，和下游 stage 的 B
                    if (k, s) not in f_finish:
                        continue
                    if s < S - 1 and (k, s + 1) not in b_finish:
                        continue
                    dep_f = f_finish[(k, s)]
                    dep_b = b_finish[(k, s + 1)] if s < S - 1 else dep_f
                    start = max(stage_free[s], dep_f, dep_b)
                    # rank=0：B 优先；同时间点 stage 越靠后(-s越大)越优
                    candidates.append((start, 0, -s, op, s, k))

            if not candidates:
                raise RuntimeError("XPipeStrategy deadlock: no schedulable candidates")

            # 选出最优候选：
            #   1) start 最小
            #   2) B 优先 (rank)
            #   3) stage 靠后优先 (-s)
            #   4) global_k 更小优先
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
            ready.remove((op, s, k))
            scheduled += 1

            if op == "F":
                f_finish[(k, s)] = end_t
                # 解锁下一个 stage 的 F 或者尾阶段的 B
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

