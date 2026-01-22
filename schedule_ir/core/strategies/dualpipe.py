# """
# DualPipeStrategy: F + Bx + Bw 的 1F1B-ish 双管线调度。

# 设计目标：
# - 用 async 1F1B 做骨架，但把反向拆成 Bx / Bw：
#   * Bx：激活梯度回传，按 stage 从尾到头串链
#   * Bw：本 stage 的权重梯度相关计算，依赖本地 Bx
# - 优先级：Bx > F > Bw，Bw 主要用于吃泡
# - 一样是“跨 mini 的长流水线”，不在 mini 间 flush

# 这里只负责 F/Bx/Bw 三类 compute 事件的时序；
# 后续的 manual tuner 可以在此基础上对 Bx/Bw 做保守微调。
# """

# from typing import List, Tuple, Dict
# from ..events import Event
# from ..params import ScheduleSpec


# class DualPipeStrategy:
#     def build_fb_events(self, spec: ScheduleSpec) -> List[Event]:
#         S = spec.num_stages
#         M = spec.num_micro
#         N = spec.num_mini
#         lane_compute = spec.lanes["compute"]

#         f_dur = spec.forward
#         bx_dur = spec.bx
#         bw_dur = spec.bw

#         total_micro = N * M
#         if total_micro == 0:
#             return []

#         stage_free = [0.0 for _ in range(S)]

#         # 完成时间
#         f_finish: Dict[Tuple[int, int], float] = {}
#         bx_finish: Dict[Tuple[int, int], float] = {}
#         bw_finish: Dict[Tuple[int, int], float] = {}

#         # ready 集合：("F"|"Bx"|"Bw", stage, global_k)
#         ready: List[Tuple[str, int, int]] = []

#         for k in range(total_micro):
#             ready.append(("F", 0, k))
#         enqueued_F = {(0, k) for k in range(total_micro)}
#         enqueued_Bx = set()
#         enqueued_Bw = set()

#         events: List[Event] = []
#         total_tasks = 3 * S * total_micro  # F + Bx + Bw
#         scheduled = 0

#         # op 优先级：数越小优先级越高
#         op_rank = {"Bx": 0, "F": 1, "Bw": 2}

#         def global_to_mini_micro(k: int) -> Tuple[int, int]:
#             mini = k // M
#             micro = k % M
#             return mini, micro

#         while scheduled < total_tasks:
#             candidates = []

#             for op, s, k in ready:
#                 if op == "F":
#                     if s > 0 and (k, s - 1) not in f_finish:
#                         continue
#                     dep = 0.0 if s == 0 else f_finish[(k, s - 1)]
#                     start = max(stage_free[s], dep)
#                     candidates.append((start, op_rank[op], -s, op, s, k))

#                 elif op == "Bx":
#                     if (k, s) not in f_finish:
#                         continue
#                     if s < S - 1 and (k, s + 1) not in bx_finish:
#                         continue
#                     dep_f = f_finish[(k, s)]
#                     dep_bx = bx_finish[(k, s + 1)] if s < S - 1 else dep_f
#                     start = max(stage_free[s], dep_f, dep_bx)
#                     candidates.append((start, op_rank[op], -s, op, s, k))

#                 else:  # "Bw"
#                     if (k, s) not in bx_finish:
#                         continue
#                     dep = bx_finish[(k, s)]
#                     start = max(stage_free[s], dep)
#                     candidates.append((start, op_rank[op], -s, op, s, k))

#             if not candidates:
#                 raise RuntimeError("DualPipeStrategy deadlock: no schedulable candidates")

#             # 选：start 最早 -> op_rank 最小(Bx>F>Bw) -> stage 靠后 -> global_k 小
#             candidates.sort(key=lambda x: (x[0], x[1], x[2], x[5]))
#             start, _, _, op, s, k = candidates[0]

#             mini, micro = global_to_mini_micro(k)
#             if op == "F":
#                 dur = f_dur[s]
#             elif op == "Bx":
#                 dur = bx_dur[s]
#             else:
#                 dur = bw_dur[s]

#             ev = Event(
#                 stage=s,
#                 lane=lane_compute,
#                 start=start,
#                 dur=float(dur),
#                 op=op,
#                 mini=mini,
#                 micro=micro,
#             )
#             events.append(ev)

#             end_t = start + float(dur)
#             stage_free[s] = end_t
#             ready.remove((op, s, k))
#             scheduled += 1

#             if op == "F":
#                 f_finish[(k, s)] = end_t
#                 # 解锁下一个 stage 的 F 或尾 stage 的 Bx
#                 if s + 1 < S:
#                     if (s + 1, k) not in enqueued_F:
#                         enqueued_F.add((s + 1, k))
#                         ready.append(("F", s + 1, k))
#                 else:
#                     if (s, k) not in enqueued_Bx:
#                         enqueued_Bx.add((s, k))
#                         ready.append(("Bx", s, k))

#             elif op == "Bx":
#                 bx_finish[(k, s)] = end_t
#                 # 解锁上游 stage 的 Bx
#                 if s - 1 >= 0 and (s - 1, k) not in enqueued_Bx:
#                     enqueued_Bx.add((s - 1, k))
#                     ready.append(("Bx", s - 1, k))
#                 # 解锁本 stage 的 Bw
#                 if (s, k) not in enqueued_Bw:
#                     enqueued_Bw.add((s, k))
#                     ready.append(("Bw", s, k))

#             else:  # "Bw"
#                 bw_finish[(k, s)] = end_t
#                 # Bw 不再解锁其他事件

#         return events
# -*- coding: utf-8 -*-
from typing import List, Tuple, Dict
from ..events import Event
from ..params import ScheduleSpec


class DualPipeStrategy:
    """
    DualPipe：双端启动 + per-mini 反向填缝

    语义总结：
    - 前向 F：
      * 对每个 mini 的 micro 按索引分成左右两拨：
        - 左半（m < L）：从 stage 0 -> S-1 正向跑；
        - 右半（m >= L）：从 stage S-1 -> 0 反向跑。
      * 这样在 t=0 就会有：
        - 左半某个 micro 的 F( stage=0 )；
        - 右半某个 micro 的 F( stage=S-1 )；
        => stage0 和最后一层同时被注水，实现你要的“双流水起步”。

    - 反向 Bx / Bw：
      * Bx / Bw 的顺序严格是：同一 (mini, micro, stage) 上：
          F 先 -> Bx -> Bw。
      * Bx 沿 stage S-1 -> 0 回传（对所有 micro 相同，这是“正常反向”）。
      * Bw 必须在同一个 (n, m, s) 的 Bx 完成后才能跑，用来“吃泡”。

    - per-mini gating：
      * 对某个 mini n，当这个 mini 的所有 micro 在“出口 stage”上
        的 F 都完成时（左半出口是 S-1，右半出口是 0），
        才允许这个 mini 的 Bx / Bw 进入候选集。
        => “每个 minibatch 的所有 micro 跑完，就可以开始这个 minibatch 的反向”。

    - 优先级：
      * 来自 spec.manual["priority"]，比如 ["F", "Bx", "Bw"] 或 ["Bx", "F", "Bw"]。
      * 调度时按 (start_time, priority, -stage, mini, micro) 选事件，
        在满足依赖的前提下，用 Bx / Bw 在空隙里填缝，让时间轴更紧凑。
    """

    def build_fb_events(self, spec: ScheduleSpec) -> List[Event]:
        S, M, N = spec.num_stages, spec.num_micro, spec.num_mini
        lane_compute = spec.lanes["compute"]

        if S <= 0 or M <= 0 or N <= 0:
            return []

        # --------- 时长：F / Bx / Bw ---------
        Fdur = list(spec.forward)   # len = S
        BxDur = list(spec.bx)       # len = S
        BwDur = list(spec.bw)       # len = S

        # --------- 手工优先级配置 ---------
        manual = spec.manual or {}
        priority = manual.get("priority", ["F", "Bx", "Bw"])
        op_rank: Dict[str, int] = {op: i for i, op in enumerate(priority)}

        # --------- 依赖完成时间表 ---------
        # key = (mini, micro, stage)
        f_fin: Dict[Tuple[int, int, int], float] = {}
        bx_fin: Dict[Tuple[int, int, int], float] = {}
        bw_fin: Dict[Tuple[int, int, int], float] = {}

        # --------- stage 空闲时间 ---------
        stage_free = [0.0 for _ in range(S)]

        # --------- per-mini gating：前向完成后才开反向 ---------
        # micro 按 index 分左右两拨
        L = (M + 1) // 2  # 左半 micro: 0..L-1, 右半 micro: L..M-1

        def exit_stage_of_micro(m: int) -> int:
            # 左半 micro 的前向出口是 S-1，右半是 0
            return S - 1 if m < L else 0

        # 每个 mini 在“出口 stage”上完成前向的 micro 数量
        mini_exit_f_count = [0 for _ in range(N)]
        mini_fwd_done = [False for _ in range(N)]

        # --------- 待调度集合 ---------
        # 三种 op 的任务全集
        pendingF  = {(n, m, s) for n in range(N) for m in range(M) for s in range(S)}
        pendingBx = {(n, m, s) for n in range(N) for m in range(M) for s in range(S)}
        pendingBw = {(n, m, s) for n in range(N) for m in range(M) for s in range(S)}

        total_tasks = len(pendingF) + len(pendingBx) + len(pendingBw)
        scheduled = 0

        events: List[Event] = []

        # ---------- F 候选：双端启动 ----------
        def add_F_candidates(cands):
            # cands: List[ (start, op, n, m, s, dur) ]
            for (n, m, s) in list(pendingF):
                is_left = (m < L)
                if is_left:
                    # 左半：前向 0 -> S-1
                    if s == 0:
                        dep_t = 0.0
                    else:
                        prev_key = (n, m, s - 1)
                        if prev_key not in f_fin:
                            continue
                        dep_t = f_fin[prev_key]
                else:
                    # 右半：前向 S-1 -> 0（反向跑）
                    if s == S - 1:
                        dep_t = 0.0
                    else:
                        next_key = (n, m, s + 1)
                        if next_key not in f_fin:
                            continue
                        dep_t = f_fin[next_key]

                start = max(stage_free[s], dep_t)
                cands.append((start, "F", n, m, s, float(Fdur[s])))

        # ---------- Bx 候选：per-mini gating + Bx 在前 ----------
        def add_Bx_candidates(cands):
            for (n, m, s) in list(pendingBx):
                # 这个 mini 必须已经允许反向
                if not mini_fwd_done[n]:
                    continue
                # 本 stage 的 F 必须完成
                if (n, m, s) not in f_fin:
                    continue
                deps = [f_fin[(n, m, s)]]
                # Bx 链：从 S-1 -> 0，依赖下游 Bx(s+1)
                if s < S - 1:
                    down_key = (n, m, s + 1)
                    if down_key not in bx_fin:
                        continue
                    deps.append(bx_fin[down_key])
                dep_t = max(deps)
                start = max(stage_free[s], dep_t)
                cands.append((start, "Bx", n, m, s, float(BxDur[s])))

        # ---------- Bw 候选：严格在 Bx 之后 ----------
        def add_Bw_candidates(cands):
            for (n, m, s) in list(pendingBw):
                key = (n, m, s)
                if key not in bx_fin:
                    continue
                dep_t = bx_fin[key]
                start = max(stage_free[s], dep_t)
                cands.append((start, "Bw", n, m, s, float(BwDur[s])))

        # ---------- 主调度循环 ----------
        while scheduled < total_tasks:
            cands: List[Tuple[float, str, int, int, int, float]] = []

            add_F_candidates(cands)
            add_Bx_candidates(cands)
            add_Bw_candidates(cands)

            if not cands:
                raise RuntimeError("DualPipeStrategy deadlock: no schedulable candidates")

            # 选：开始时间 -> 优先级 -> stage 更靠后 -> mini/micro
            cands.sort(key=lambda x: (x[0], op_rank.get(x[1], 99), -x[4], x[2], x[3]))
            start, op, n, m, s, dur = cands[0]

            # 生成 Event
            ev = Event(
                stage=s,
                lane=lane_compute,
                start=start,
                dur=dur,
                op=op,
                mini=n,
                micro=m,
            )
            events.append(ev)

            end_t = start + dur
            stage_free[s] = end_t
            scheduled += 1

            if op == "F":
                pendingF.discard((n, m, s))
                f_fin[(n, m, s)] = end_t

                # 如果这是这个 micro 的“出口 stage”，更新该 mini 的前向完成计数
                exit_s = exit_stage_of_micro(m)
                if s == exit_s:
                    mini_exit_f_count[n] += 1
                    if mini_exit_f_count[n] == M:
                        mini_fwd_done[n] = True

            elif op == "Bx":
                pendingBx.discard((n, m, s))
                bx_fin[(n, m, s)] = end_t

            else:  # "Bw"
                pendingBw.discard((n, m, s))
                bw_fin[(n, m, s)] = end_t

        return events
