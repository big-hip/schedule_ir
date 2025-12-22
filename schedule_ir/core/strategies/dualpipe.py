from typing import List, Tuple, Dict, Any, Optional
from ..events import Event
from ..params import ScheduleSpec

class DualPipeStrategy:
    def build_fb_events(self, spec: ScheduleSpec) -> List[Event]:
        S, M, N = spec.num_stages, spec.num_micro, spec.num_mini
        lane_compute = spec.lanes.get("compute", 0)

        # --------- 可选手工参数（无则给合理默认） ---------
        manual: Dict[str, Any] = getattr(spec, "manual", {}) or {}
        prio = manual.get("priority", ["Bx","F","Bw"])
        prio_rank = {op:i for i,op in enumerate(prio)}
        bx_chain_strict = (manual.get("bx_chain","strict") == "strict")
        bw_after_bx = bool(manual.get("bw_after_bx", True))
        bx_share = float(manual.get("bx_share", 0.6))
        if not (0.0 < bx_share < 1.0): bx_share = 0.6
        lookahead = float(manual.get("bw_fill", {"mode":"safe","lookahead":60}).get("lookahead", 60.0))

        Fdur = list(spec.forward)
        Btot = list(spec.backward)
        BxDur = [Btot[s]*bx_share for s in range(S)]
        BwDur = [Btot[s]*(1.0-bx_share) for s in range(S)]

        # 把 micro 分成左右两组：左向(F_L) 0..L-1；右向(F_R) L..M-1
        L = (M + 1) // 2
        left_ids  = list(range(0, L))
        right_ids = list(range(L, M))

        events: List[Event] = []

        for n in range(N):
            stage_free = [0.0]*S

            # 完成时间表（按方向拆开）
            fL_fin: Dict[Tuple[int,int,int], float]  = {}  # (n, m, s)
            fR_fin: Dict[Tuple[int,int,int], float]  = {}
            bxL_fin: Dict[Tuple[int,int,int], float] = {}
            bxR_fin: Dict[Tuple[int,int,int], float] = {}
            bw_fin:  Dict[Tuple[int,int,int], float] = {}

            # 待调度集合
            FL  = {(m,s) for m in left_ids  for s in range(S)}      # F_L: 0->S-1
            FR  = {(m,s) for m in right_ids for s in range(S)}      # F_R: S-1->0（依赖反向）
            BxL = {(m,s) for m in left_ids  for s in range(S)}      # Bx_L: S-1->0
            BxR = {(m,s) for m in right_ids for s in range(S)}      # Bx_R: 0->S-1
            Bw  = {(m,s) for m in range(M)   for s in range(S)}     # Bw: per-stage 本地尾段

            total = len(FL)+len(FR)+len(BxL)+len(BxR)+len(Bw)

            while (len(fL_fin)+len(fR_fin)+len(bxL_fin)+len(bxR_fin)+len(bw_fin)) < total:
                cands: List[Tuple[str,str,int,int,float,float]] = []  # (op,dir,s,m,start,dur)

                # ---- F 左向：s 依赖 s-1 ----
                for (m,s) in list(FL):
                    dep = 0.0 if s==0 else fL_fin.get((n,m,s-1))
                    if s>0 and dep is None: continue
                    st = max(stage_free[s], dep)
                    cands.append(("F","L",s,m,st,Fdur[s]))

                # ---- F 右向：s 依赖 s+1（反序）----
                for (m,s) in list(FR):
                    dep = 0.0 if s==S-1 else fR_fin.get((n,m,s+1))
                    if s < S-1 and dep is None: continue
                    st = max(stage_free[s], dep)
                    cands.append(("F","R",s,m,st,Fdur[s]))

                # ---- Bx 左向链：依赖 F_L[s,m]，严格链还依赖 Bx_L[s+1,m] ----
                for (m,s) in list(BxL):
                    depF = fL_fin.get((n,m,s))
                    if depF is None: continue
                    if bx_chain_strict and s < S-1:
                        up = bxL_fin.get((n,m,s+1))
                        if up is None: continue
                        depF = max(depF, up)
                    st = max(stage_free[s], depF)
                    cands.append(("Bx","L",s,m,st,BxDur[s]))

                # ---- Bx 右向链：依赖 F_R[s,m]，严格链还依赖 Bx_R[s-1,m] ----
                for (m,s) in list(BxR):
                    depF = fR_fin.get((n,m,s))
                    if depF is None: continue
                    if bx_chain_strict and s > 0:
                        dn = bxR_fin.get((n,m,s-1))
                        if dn is None: continue
                        depF = max(depF, dn)
                    st = max(stage_free[s], depF)
                    cands.append(("Bx","R",s,m,st,BxDur[s]))

                # ---- Bw：默认依赖本 stage 的 Bx 完成（任一方向对应的那条 micro）----
                for (m,s) in list(Bw):
                    # 对于左/右各自的 m，只有对应方向的 Bx 完成才允许（这里简单合并到同一张表）
                    depBx = bxL_fin.get((n,m,s))
                    if depBx is None:
                        depBx = bxR_fin.get((n,m,s))
                    if bw_after_bx and depBx is None:
                        continue
                    st = max(stage_free[s], depBx or 0.0)
                    cands.append(("Bw","-",s,m,st,BwDur[s]))

                if not cands:
                    break  # 理论上不该发生

                # ---- 软拦 Bw（填缝）：若同 stage 的更高优先级(F/Bx)将在 lookahead 内就绪，则先让他们 ----
                earliest_high: Dict[int, Optional[float]] = {s:None for s in range(S)}
                for (op,di,s,m,st,du) in cands:
                    if op in ("F","Bx"):
                        t = earliest_high[s]
                        earliest_high[s] = st if t is None else min(t, st)

                allowed: List[Tuple[str,str,int,int,float,float]] = []
                for (op,di,s,m,st,du) in cands:
                    if op == "Bw":
                        eh = earliest_high[s]
                        if eh is not None and eh <= stage_free[s] + lookahead:
                            continue
                    allowed.append((op,di,s,m,st,du))
                if not allowed:
                    allowed = cands  # 防饥饿

                # ---- 关键 tie-break：让 F_L 偏左、F_R 偏右；Bx_L 偏右、Bx_R 偏左（更像 V）----
                def bias(op: str, di: str, s: int) -> int:
                    if op == "F":
                        return s if di == "L" else -s
                    if op == "Bx":
                        return -s if di == "L" else s
                    return 0

                op,di,s,m,st,du = min(
                    allowed,
                    key=lambda x: (x[4], prio_rank.get(x[0], 99), bias(x[0], x[1], x[2]), x[3])
                )

                # 生成事件（op 仍用 "F"/"Bx"/"Bw"）
                events.append(Event(stage=s, lane=lane_compute, start=st, dur=du, op=op, micro=m, mini=n))
                stage_free[s] = st + du

                # 标记完成 & 出队
                if op == "F":
                    if di == "L":
                        FL.discard((m,s));  fL_fin[(n,m,s)] = stage_free[s]
                    else:
                        FR.discard((m,s));  fR_fin[(n,m,s)] = stage_free[s]
                elif op == "Bx":
                    if di == "L":
                        BxL.discard((m,s)); bxL_fin[(n,m,s)] = stage_free[s]
                    else:
                        BxR.discard((m,s)); bxR_fin[(n,m,s)] = stage_free[s]
                else:
                    Bw.discard((m,s));     bw_fin[(n,m,s)]  = stage_free[s]

        return events
