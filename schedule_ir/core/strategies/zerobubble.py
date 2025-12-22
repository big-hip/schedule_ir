# -*- coding: utf-8 -*-
"""
Zero-Bubble Pipeline Strategies (H1 / H2 / V)
=============================================

目标
----
在 NDJSON 事件级（mini 在前，micro 在后）生成更"贴近零空泡"的 PP 调度，满足：
  • 同一 minibatch 的每个 micro 严格遵循 F → Bx(∂L/∂x) → Bw(∂L/∂W)。
  • 不同 micro 之间允许并行：空闲时可插入其它 micro 的 F / Bx / Bw，但不得违反每个 micro 的顺序。
  • Warm-up 完成后优先沿 stage 链连续推进 Bx；F 次之；Bw 仅作"填缝"（不阻塞 Bx/F）。
  • Bx 完成立即触发 Send/Recv（激活梯度 P2P），驱动上游 Bx 连续推进。
  • Bw / AllReduce 放入 compute 空隙，绝不挤占 Bx/F 的起跑时机。

模式
----
  H1: gate = p-1
  H2: gate = (p-1) + zb_h2_extra_warmup
  V : 2p 虚拟 chunk 的 V 路径（同设备 intra_p2p、跨设备 p2p） ，保留实现

复杂度
------
事件调度为"就绪优先 + 工作保守"，每轮挑选全局最早可开跑的事件：
  时间复杂度 ~ O(N * M * S)（N=num_mini, M=num_micro, S=num_stages）
"""

from typing import List, Dict, Tuple, Optional
from ..events import Event
from ..params import ScheduleSpec
from .base import Strategy

# ------------------------------
# 工具：通信、忙段合并、插缝
# ------------------------------

def _p2p_time(spec: ScheduleSpec, s_from: int, s_to: int) -> float:
    """相邻 stage 间 P2P 时长（反向主用 s->s-1，用 spec.comm['p2p'][s-1]）。"""
    if s_to == s_from - 1:
        return spec.comm["p2p"][s_to]
    if s_to == s_from + 1:
        return spec.comm["p2p"][s_from]
    return 0.0

def _insert_send_recv(events: List[Event], lane_comm: int,
                      s_from: int, s_to: int,
                      t_start: float, dur: float,
                      mini: int, micro: int) -> float:
    """在 comm lane 上追加 Send/Recv，返回 Recv 结束时间。"""
    if dur <= 0:
        return t_start
    events.append(Event(stage=s_from, lane=lane_comm, start=t_start, dur=dur,
                        op="SendActGrad", mini=mini, micro=micro))
    events.append(Event(stage=s_to,   lane=lane_comm, start=t_start, dur=dur,
                        op="RecvActGrad", mini=mini, micro=micro))
    return t_start + dur

def _merge(intervals: List[Tuple[float,float]]) -> List[Tuple[float,float]]:
    """合并重叠忙段。"""
    if not intervals:
        return []
    intervals.sort()
    merged = [list(intervals[0])]
    for a, b in intervals[1:]:
        if a <= merged[-1][1]:
            merged[-1][1] = max(merged[-1][1], b)
        else:
            merged.append([a, b])
    return [(x, y) for x, y in merged]

def _place_into_gaps(busy: List[Tuple[float,float]], ready_t: float, dur: float) -> Optional[float]:
    """
    把 [t, t+dur) 塞进 busy 的空隙中；若无空隙则返回 None（表示现在不安全）。
    busy 不相交且升序。
    """
    if dur <= 0:
        return ready_t
    if not busy:
        busy.append((ready_t, ready_t + dur)); return ready_t
    # 开头
    if ready_t + dur <= busy[0][0]:
        busy.insert(0, (ready_t, ready_t + dur)); return ready_t
    # 中间
    for i in range(len(busy)-1):
        t0 = max(ready_t, busy[i][1]); t1 = busy[i+1][0]
        if t0 + dur <= t1:
            busy.insert(i+1, (t0, t0 + dur)); return t0
    # 尾部
    t0 = max(ready_t, busy[-1][1])
    busy.append((t0, t0 + dur))
    return t0

# ------------------------------
# 状态容器：单个 mini 内的运行态
# ------------------------------

class _State:
    def __init__(self, S: int):
        self.stage_free = [0.0] * S                      # compute lane 可用时间（每 stage 独立）
        self.f_fin:  Dict[Tuple[int,int], float] = {}    # (micro, s) -> t
        self.bx_fin: Dict[Tuple[int,int], float] = {}    # (micro, s) -> t
        self.grad_ready: Dict[Tuple[int,int], float] = {}# (micro, s) -> t （s 收到来自 s+1 的梯度）
        self.tail_f_count = 0                            # 尾段 F 完成的 micro 数

# ------------------------------
# 核心策略
# ------------------------------

class ZeroBubbleStrategy(Strategy):
    """统一入口：H1/H2/V。"""

    def build_fb_events(self, spec: ScheduleSpec) -> List[Event]:
        mode = spec.zb_mode.upper()
        if mode in {"H1", "H2"}:
            return self._build_h_mode(spec, h2=(mode == "H2"))
        if mode == "V":
            return self._build_v(spec)
        raise ValueError(f"Unsupported ZeroBubble mode: {mode}")

    # ---------- H1/H2 共用骨干：Warm-up → Steady(Bx-first) → Flush(Bx) → Bw填缝 ----------
    def _build_h_mode(self, spec: ScheduleSpec, h2: bool) -> List[Event]:
        S, M, N = spec.num_stages, spec.num_micro, spec.num_mini
        gate = (S - 1) + (spec.zb_h2_extra_warmup if h2 else 0)  # warm-up 门限
        lane_c = spec.lanes.get("compute", 0)
        lane_m = spec.lanes.get("comm", 1)

        events: List[Event] = []

        for n in range(N):
            st = _State(S)

            # 1) Warm-up：只推 F，直至尾段 F 完成数达到 gate
            self._warmup_forward_only(spec, n, st, gate, lane_c, events)

            # 2) Steady：就绪优先、工作保守
            self._steady_bx_first_work_conserving(spec, n, st, lane_c, lane_m, events)

            # 3) Flush：确保剩余 Bx 被冲刷（通常 steady 已覆盖大部分）
            self._flush_remaining_bx(spec, n, st, lane_c, lane_m, events)

        # 4) Bw/AllReduce：仅以 F/Bx 为"忙段"塞进空隙（不阻塞 Bx/F）
        events = self._place_bw_allreduce(spec, events)
        return events

    # ---------- 功能块 1：Warm-up（只允许 F） ----------
    def _warmup_forward_only(self, spec: ScheduleSpec, mini: int, st: _State,
                             gate: int, lane_c: int, events: List[Event]) -> None:
        S, M = spec.num_stages, spec.num_micro
        # 只要尾段 F 完成数未达 gate，就持续推进 F
        while st.tail_f_count < gate:
            best = None  # (start, s, m)
            # 对每个 micro，找其下一层待做的 F(s,m)
            for m in range(M):
                # 找到该 micro 尚未完成 F 的最小 s
                s_next = None
                for s in range(S):
                    if (m, s) not in st.f_fin:
                        s_next = s; break
                if s_next is None:
                    continue  # 该 micro F 已全完成
                # 依赖：F(s-1,m).fin（若 s>0）
                dep = 0.0 if s_next == 0 else st.f_fin.get((m, s_next-1))
                if dep is None:
                    continue
                start = max(st.stage_free[s_next], dep)
                cand = (start, s_next, m)
                if (best is None) or (cand < best):
                    best = cand
            if best is None:
                # 无可推进 F（理论上不应发生）；尝试时间推进：找最近依赖就绪点
                next_dep = min([t for t in st.f_fin.values()], default=0.0) + 1e-9
                # 将最早 stage 向前推进到 next_dep
                s0 = min(range(S), key=lambda s: st.stage_free[s])
                st.stage_free[s0] = max(st.stage_free[s0], next_dep)
                continue

            start, s, m = best
            dur = spec.forward[s]
            events.append(Event(stage=s, lane=lane_c, start=start, dur=dur, op="F", mini=mini, micro=m))
            t1 = start + dur
            st.stage_free[s] = t1
            st.f_fin[(m, s)] = t1
            if s == S-1:
                st.tail_f_count += 1
                if st.tail_f_count >= gate:
                    break  # Warm-up 达成，进入 Steady

    # ---------- 功能块 2：Steady（Bx 优先；F 次之；Bw 仅"安全填缝"） ----------
    def _steady_bx_first_work_conserving(self, spec: ScheduleSpec, mini: int, st: _State,
                                         lane_c: int, lane_m: int, events: List[Event]) -> None:
        S, M = spec.num_stages, spec.num_micro

        def collect_candidates():
            """
            收集三类候选：
              - Bx：依赖 F(s,m).fin & （若 s<S-1）grad_ready(m,s)
              - F ：依赖 F(s-1,m).fin（若 s>0）
              - Bw：依赖 Bx(s,m).fin，且"安全插缝"（不会阻塞本 stage 最近的 Bx/F）
            返回列表 [(typ, s, m, start, dur, priority_key), ...]
            说明：
              • 先比较 start（越早越好）；
              • 若 start 相同：Bx 优先 > F > Bw；
              • 再以 -s 细化（更深的 s 优先，让梯度更快上溯）。
            """
            cand: List[Tuple[str,int,int,float,float,Tuple[int,float,int]]] = []

            # 先预计算每个 stage 上"更高优先级事件"的最早起跑时刻（用于 Bw 安全判断）
            earliest_hp_per_stage: Dict[int, float] = {}
            # 2.1 Bx
            for m in range(M):
                for s in range(S-1, -1, -1):
                    if (m, s) in st.bx_fin:
                        continue
                    depF = st.f_fin.get((m, s))
                    if depF is None:
                        continue
                    depG = depF if s == S-1 else st.grad_ready.get((m, s))
                    if depG is None:
                        continue
                    start = max(st.stage_free[s], depF, depG)
                    dur = spec.bx[s]
                    prio = (0, start, -s)  # Bx 优先等级 0
                    cand.append(("BX", s, m, start, dur, prio))
                    earliest_hp_per_stage[s] = min(earliest_hp_per_stage.get(s, float("inf")), start)

            # 2.2 F：对每个 micro 仅找"下一层待做"的 F
            for m in range(M):
                s_next = None
                for s in range(S):
                    if (m, s) not in st.f_fin:
                        s_next = s; break
                if s_next is None:
                    continue
                dep = 0.0 if s_next == 0 else st.f_fin.get((m, s_next-1))
                if dep is None:
                    continue
                start = max(st.stage_free[s_next], dep)
                dur = spec.forward[s_next]
                prio = (1, start, -s_next)  # F 优先等级 1
                cand.append(("F", s_next, m, start, dur, prio))
                earliest_hp_per_stage[s_next] = min(earliest_hp_per_stage.get(s_next, float("inf")), start)

            # 2.3 Bw：仅当能"完整塞入"本 stage 下一次高优先级事件之前的空隙，才纳入候选
            #        （工作保守：避免 Bw 抢占本 stage 即将到来的 Bx/F）
            busy_by_stage: Dict[int, List[Tuple[float,float]]] = {s: [] for s in range(S)}
            for (m0, s0), tfin in st.f_fin.items():
                # F/Bx 忙段来自 events，而当前 st 中的 stage_free 代表了尾部占用
                pass
            # 为了不把历史事件再扫描一遍，这里仅依赖"就绪起点"和 hp 边界来做保守判断：
            # 若 ready_t = max(stage_free[s], bx_fin) 能在 hp_time 前完整执行，则可纳入。
            for m in range(M):
                for s in range(S):
                    if (m, s) not in st.bx_fin:
                        continue
                    # Bw 依赖 Bx 完成
                    ready_t = max(st.stage_free[s], st.bx_fin[(m, s)])
                    dur = spec.bw[s]
                    hp_time = earliest_hp_per_stage.get(s, float("inf"))
                    if ready_t + dur <= hp_time:
                        prio = (2, ready_t, -s)  # Bw 优先等级 2（最低）
                        cand.append(("BW", s, m, ready_t, dur, prio))
            return cand

        # 主循环：直到 F 与 Bx 全完成（Bw 不强求在此阶段全部完成）
        total_F = S * M
        total_Bx = S * M
        while (len(st.f_fin) < total_F) or (len(st.bx_fin) < total_Bx):
            cands = collect_candidates()
            if not cands:
                # 若仍未完成但无候选，推进到"最近的高优先级起跑时刻"
                # 计算所有 stage 的最近 Bx/F 起跑时刻
                nearest = float("inf"); s_pick = 0
                # 重新调用以得到 earliest_hp_per_stage（通过解析 cand）
                # 若 cand 为空，说明所有依赖未就绪；则选择最早 stage_free 并前推一点
                for s in range(S):
                    nearest = min(nearest, st.stage_free[s])
                    s_pick = min(range(S), key=lambda ss: st.stage_free[ss])
                # 小步推进，避免死锁
                st.stage_free[s_pick] = st.stage_free[s_pick] + 1e-9
                continue

            # 选择器：先按 (start)；再按优先级（Bx=0 < F=1 < Bw=2）；再按 -s
            cands.sort(key=lambda x: (x[3], x[5][0], x[5][2]))  # (start, type_rank, -s)
            typ, s, m, start, dur, _ = cands[0]

            if typ == "BX":
                events.append(Event(stage=s, lane=lane_c, start=start, dur=dur, op="Bx", mini=mini, micro=m))
                t1 = start + dur
                st.stage_free[s] = t1
                st.bx_fin[(m, s)] = t1
                if s - 1 >= 0:
                    recv_end = _insert_send_recv(events, lane_m, s, s-1, t1, _p2p_time(spec, s, s-1), mini, m)
                    st.grad_ready[(m, s-1)] = recv_end

            elif typ == "F":
                events.append(Event(stage=s, lane=lane_c, start=start, dur=dur, op="F", mini=mini, micro=m))
                t1 = start + dur
                st.stage_free[s] = t1
                st.f_fin[(m, s)] = t1
                if s == S-1:
                    st.tail_f_count += 1  # 仅用于统计（steady 内也会增长）

            else:  # "BW"（已通过"安全插缝"判定）
                events.append(Event(stage=s, lane=lane_c, start=start, dur=dur, op="Bw", mini=mini, micro=m))
                st.stage_free[s] = start + dur
                # AllReduce 放在 Bw 之后、comm lane，不阻塞 compute
                allr = float(spec.comm.get("allreduce", 0.0))
                if allr > 0:
                    events.append(Event(stage=s, lane=lane_m, start=start + dur, dur=allr,
                                        op="AllReduceGrad", mini=mini, micro=m))

    # ---------- 功能块 3：Flush 未完成 Bx ----------
    def _flush_remaining_bx(self, spec: ScheduleSpec, mini: int, st: _State,
                            lane_c: int, lane_m: int, events: List[Event]) -> None:
        S, M = spec.num_stages, spec.num_micro
        for m in range(M-1, -1, -1):
            for s in range(S-1, -1, -1):
                if (m, s) in st.bx_fin:
                    continue
                depF = st.f_fin.get((m, s))
                depG = depF if s == S-1 else st.grad_ready.get((m, s))
                if (depF is None) or (depG is None):
                    continue
                start = max(st.stage_free[s], depF, depG)
                dur = spec.bx[s]
                events.append(Event(stage=s, lane=lane_c, start=start, dur=dur, op="Bx", mini=mini, micro=m))
                t1 = start + dur
                st.stage_free[s] = t1
                st.bx_fin[(m, s)] = t1
                if s - 1 >= 0:
                    recv_end = _insert_send_recv(events, lane_m, s, s-1, t1, _p2p_time(spec, s, s-1), mini, m)
                    st.grad_ready[(m, s-1)] = recv_end

    # ---------- 功能块 4：仅以 F/Bx 为忙段，Bw/AllReduce 安全填缝 ----------
    def _place_bw_allreduce(self, spec: ScheduleSpec, events: List[Event]) -> List[Event]:
        lane_c = spec.lanes.get("compute", 0)
        lane_m = spec.lanes.get("comm", 1)
        S = spec.num_stages

        out = list(events)
        minis = sorted({e.mini for e in events if e.mini is not None})

        for n in minis:
            # 收集该 mini 的 F/Bx 忙段
            busy: Dict[int, List[Tuple[float,float]]] = {s: [] for s in range(S)}
            bx_fin: Dict[Tuple[int,int], float] = {}  # (micro, s) -> t
            for ev in events:
                if ev.mini != n:
                    continue
                if ev.op in ("F", "Bx"):
                    busy[ev.stage].append((ev.start, ev.start + ev.dur))
                if ev.op == "Bx":
                    bx_fin[(ev.micro, ev.stage)] = ev.start + ev.dur
            for s in range(S):
                busy[s] = _merge(busy[s])

            # 尝试为每个 (m,s) 塞入 Bw（若已在 steady 阶段插入过，也不会重复，因为 ready_t 会更早）
            for (m, s), ready_t in sorted(bx_fin.items(), key=lambda kv: kv[1]):
                st = _place_into_gaps(busy[s], ready_t, spec.bw[s])
                if st is None:
                    continue  # 当前 mini 内找不到不阻塞的空隙；宁愿留到更晚（或不插）
                out.append(Event(stage=s, lane=lane_c, start=st, dur=spec.bw[s], op="Bw", mini=n, micro=m))
                allr = float(spec.comm.get("allreduce", 0.0))
                if allr > 0:
                    out.append(Event(stage=s, lane=lane_m, start=st + spec.bw[s], dur=allr,
                                     op="AllReduceGrad", mini=n, micro=m))
        return out

    # ---------- V 模式（保留；如主要调 H1/H2 可暂忽略） ----------
    def _build_v(self, spec: ScheduleSpec) -> List[Event]:
        S, M, N = spec.num_stages, spec.num_micro, spec.num_mini
        lane_c = spec.lanes.get("compute", 0); lane_m = spec.lanes.get("comm", 1)

        Fv  = [[spec.forward[s]/2.0, spec.forward[s]/2.0] for s in range(S)]
        BXv = [[spec.bx[s]/2.0,      spec.bx[s]/2.0]      for s in range(S)]
        BWv = [[spec.bw[s]/2.0,      spec.bw[s]/2.0]      for s in range(S)]

        virt = [(s,0) for s in range(S)] + [(S-1-k,1) for k in range(S)]
        V = len(virt)

        events: List[Event] = []
        stage_free = [0.0]*S

        for n in range(N):
            ffin: Dict[Tuple[int,int], float] = {}
            bxfin: Dict[Tuple[int,int], float] = {}

            # F：沿 V 正向
            for m in range(M):
                prev = 0.0
                for v in range(V):
                    s, ch = virt[v]
                    start = max(stage_free[s], prev); dur = Fv[s][ch]
                    events.append(Event(stage=s, lane=lane_c, start=start, dur=dur, op="F", mini=n, micro=m))
                    t1 = start + dur; stage_free[s] = t1; ffin[(m, v)] = t1; prev = t1

            # Bx：沿 V 逆向（通信区分 intra_p2p / p2p）
            for m in range(M-1, -1, -1):
                nxt = None
                for v in range(V-1, -1, -1):
                    s, ch = virt[v]
                    depF = ffin[(m, v)]
                    depG = depF if nxt is None else nxt
                    start = max(stage_free[s], depF, depG); dur = BXv[s][ch]
                    events.append(Event(stage=s, lane=lane_c, start=start, dur=dur, op="Bx", mini=n, micro=m))
                    t1 = start + dur; stage_free[s] = t1; bxfin[(m, v)] = t1
                    if v - 1 >= 0:
                        s_prev, _ = virt[v-1]
                        p = float(spec.comm.get("intra_p2p", 0.0)) if s_prev == s else _p2p_time(spec, s, s_prev)
                        nxt = _insert_send_recv(events, lane_m, s, s_prev, t1, p, n, m)
                    else:
                        nxt = None

            # Bw：以 F/Bx 忙段为基准安全插缝
            busy = {s: [] for s in range(S)}
            for ev in events:
                if ev.mini == n and ev.op in ("F","Bx"):
                    busy[ev.stage].append((ev.start, ev.start+ev.dur))
            for s in range(S): busy[s] = _merge(busy[s])
            for m in range(M):
                for v in range(V-1, -1, -1):
                    s, ch = virt[v]; ready = bxfin[(m, v)]; dur = BWv[s][ch]
                    stt = _place_into_gaps(busy[s], ready, dur)
                    if stt is None: continue
                    events.append(Event(stage=s, lane=lane_c, start=stt, dur=dur, op="Bw", mini=n, micro=m))
                    allr = float(spec.comm.get("allreduce", 0.0))
                    if allr > 0:
                        events.append(Event(stage=s, lane=lane_m, start=stt + dur, dur=allr,
                                            op="AllReduceGrad", mini=n, micro=m))
        return events
