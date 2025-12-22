# SR_version1/strategies/onef1b.py
# -*- coding: utf-8 -*-
from typing import List, Tuple, Dict
from ..events import Event
from ..params import ScheduleSpec

class OneF1BStrategy:
    """
    1F1B (PipeDream-Flush) 纯调度：
      - 外层循环：minibatch（严格 minibatch 屏障）
      - 内层：microbatch（每个 micro 产生 F/B）
      - 依赖：
          F[s-1,m] -> F[s,m]
          F[s,m]   -> B[s,m]
          B[s+1,m] -> B[s,m]
      - 资源：每个 stage 同时最多运行一个 compute 事件
      - 选择（保证 1F1B 形态）：
          当 F 与 B 都可启动时，优先 B（贴近 steady 阶段的 1F1B 内存与时序）
          其次按最早 start；如仍相同，选更深的 stage（s 大），再选 micro 较小者
      - minibatch 屏障：
          下一个 minibatch 的所有 stage 起始时间 = 上一个 minibatch 完成时间的最大值
    """

    def build_fb_events(self, spec: ScheduleSpec) -> List[Event]:
        S, M, N  = spec.num_stages, spec.num_micro, spec.num_mini
        Fdur     = spec.forward      # List[float] len=S
        Bdur     = spec.backward     # List[float] len=S
        lane_cmp = spec.lanes.get("compute", 0)

        events: List[Event] = []

        # "更新屏障"基准时间：第一个 minibatch 从 t=0 开始
        barrier_time = 0.0

        for n in range(N):
            # 每个 minibatch 都从 barrier_time 起步，形成"minibatch 屏障"
            stage_free = [barrier_time] * S

            # 记录该 minibatch 内的依赖完成时间
            f_fin: Dict[Tuple[int, int, int], float] = {}  # (n, m, s) -> finish_time
            b_fin: Dict[Tuple[int, int, int], float] = {}

            # 初始可就绪的前向（stage 0 上的所有 micro）
            readyF: List[Tuple[int, int]] = [(0, m) for m in range(M)]
            readyB: List[Tuple[int, int]] = []

            # 该 minibatch 内共需调度 S*M 个 F 与 S*M 个 B
            while len(f_fin) < S * M or len(b_fin) < S * M:
                candidates: List[Tuple[str, int, int, float]] = []  # (type, s, m, start_time)

                # ---- 构造 F 候选 ----
                new_readyF: List[Tuple[int, int]] = []
                for (s, m) in readyF:
                    # 依赖：F[s-1, m] 完成（s=0 无此依赖）
                    dep = barrier_time if s == 0 else f_fin.get((n, m, s - 1), None)
                    if s == 0 or dep is not None:
                        start = max(stage_free[s], dep if dep is not None else barrier_time)
                        candidates.append(("F", s, m, start))
                    else:
                        new_readyF.append((s, m))
                readyF = new_readyF

                # ---- 构造 B 候选 ----
                new_readyB: List[Tuple[int, int]] = []
                for (s, m) in readyB:
                    # 依赖：F[s, m] 完成；以及（若 s < S-1）B[s+1, m] 完成
                    depF  = f_fin.get((n, m, s), None)
                    depUp = barrier_time if s == S - 1 else b_fin.get((n, m, s + 1), None)
                    if depF is not None and (s == S - 1 or depUp is not None):
                        start = max(stage_free[s],
                                    depF,
                                    depUp if depUp is not None else barrier_time)
                        candidates.append(("B", s, m, start))
                    else:
                        new_readyB.append((s, m))
                readyB = new_readyB

                if not candidates:
                    # 没有可立刻启动的事件：尝试推动就绪集（防御性推进）
                    progressed = False

                    # 优先推动 F：找第一个满足"前一层 F 已完成"的 F[s,m]
                    for m in range(M):
                        for s in range(S):
                            if (n, m, s) not in f_fin and (s == 0 or (n, m, s - 1) in f_fin):
                                readyF.append((s, m))
                                progressed = True
                                break
                        if progressed:
                            break

                    if not progressed:
                        # 再尝试推动 B：当尾层 F 完成后，B[S-1, m] 即可候选
                        for m in reversed(range(M)):
                            if (n, m, S - 1) in f_fin and (n, m, S - 1) not in b_fin:
                                readyB.append((S - 1, m))
                                progressed = True
                                break

                    if not progressed:
                        # 理论上不应发生；为避免死循环直接跳出
                        break
                    continue

                # ---- 选择策略：B 优先 / 最早 start / 深层优先 / micro 小优先 ----
                def priority(c):
                    typ, s, m, start = c
                    return (0 if typ == "B" else 1, start, -s, m)

                typ, s, m, start = min(candidates, key=priority)

                # ---- 发车 ----
                if typ == "F":
                    dur = Fdur[s]
                    events.append(Event(stage=s, lane=lane_cmp, start=start, dur=dur,
                                        op="F", micro=m, mini=n))
                    finish = start + dur
                    stage_free[s] = finish
                    f_fin[(n, m, s)] = finish

                    # 解锁下游前向或尾层反向
                    if s + 1 < S:
                        readyF.append((s + 1, m))
                    else:
                        # 尾层前向完成 → 可考虑反向起点
                        readyB.append((S - 1, m))

                else:  # "B"
                    dur = Bdur[s]
                    events.append(Event(stage=s, lane=lane_cmp, start=start, dur=dur,
                                        op="B", micro=m, mini=n))
                    finish = start + dur
                    stage_free[s] = finish
                    b_fin[(n, m, s)] = finish

                    # 解锁上游反向
                    if s - 1 >= 0:
                        readyB.append((s - 1, m))

            # 本 minibatch 完成时间 = 各 stage 的最后空闲时间的最大值
            barrier_time = max(stage_free)

        return events
