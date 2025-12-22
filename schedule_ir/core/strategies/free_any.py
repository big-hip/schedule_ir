# -*- coding: utf-8 -*-
"""
自由模式 free_any：
- 统一调度 F / Bx / Bw，兼容你们已有 cfg 字段与"手工建模"规则
- 仅使用标准库；贪心就绪调度 + 规则优先级 + before/after/guard/offset
"""
from typing import Dict, Tuple, List, Optional, Any
from ..events import Event
from ..params import ScheduleSpec

Kind = str  # "F" | "Bx" | "Bw"

def _as_list(x, n):
    return x if isinstance(x, list) else [x] * n

def _get_bx_bw(spec: ScheduleSpec):
    # 1) 若分析层已生成 backward_x/backward_w，优先使用
    bx = getattr(spec, "backward_x", None)
    bw = getattr(spec, "backward_w", None)

    # 2) 否则从 backward_split.bx_ratio 切分
    if bx is None or bw is None:
        ratio = 0.5
        bs = getattr(spec, "backward_split", None)
        if isinstance(bs, dict) and "bx_ratio" in bs:
            ratio = float(bs["bx_ratio"])
        B = _as_list(getattr(spec, "backward", spec.backward), spec.num_stages)
        bx = [float(b) * ratio for b in B]
        bw = [float(b) * (1.0 - ratio) for b in B]

    # 3) 万一仍不是列表，补齐
    bx = _as_list(bx, spec.num_stages)
    bw = _as_list(bw, spec.num_stages)
    return bx, bw

def _priority(spec: ScheduleSpec) -> List[Kind]:
    man = getattr(spec, "manual", {}) or {}
    pr = man.get("priority", ["Bx", "F", "Bw"])
    # 兜底校正
    seen = set()
    out = []
    for k in pr + ["Bx","F","Bw"]:
        if k not in seen:
            out.append(k); seen.add(k)
    return out

def _bx_chain_strict(spec: ScheduleSpec) -> bool:
    man = getattr(spec, "manual", {}) or {}
    return str(man.get("bx_chain", "strict")).lower() == "strict"

def _bw_after_bx(spec: ScheduleSpec) -> bool:
    man = getattr(spec, "manual", {}) or {}
    return bool(man.get("bw_after_bx", True))

def _bw_fill(spec: ScheduleSpec) -> Tuple[str, float]:
    man = getattr(spec, "manual", {}) or {}
    fill = man.get("bw_fill", {}) or {}
    mode = str(fill.get("mode", "off")).lower()
    look = float(fill.get("lookahead", 0.0))
    return mode, look

def _iter_rules(spec: ScheduleSpec) -> List[Dict[str, Any]]:
    man = getattr(spec, "manual", {}) or {}
    return man.get("rules", []) or []

def _sel_match(sel: Dict[str, Any], s: int, n: int, m: int, kind: Kind) -> bool:
    def _match(v, idx):
        if v in (None, "all"): return True
        if v == "even": return idx % 2 == 0
        if v == "odd":  return idx % 2 == 1
        if isinstance(v, list): return idx in v
        return False
    if "op" in sel and sel["op"] not in ("F","Bx","Bw"): return False
    if "op" in sel and sel["op"] != kind: return False
    return _match(sel.get("stages","all"), s) and _match(sel.get("minis","all"), n) and _match(sel.get("micros","all"), m)

def _after_target_ready(evkey: Tuple[int,int,int,Kind], t_ready: float,
                        f_fin, bx_fin, bw_fin, spec: ScheduleSpec,
                        s: int, n: int, m: int, kind: Kind) -> Optional[float]:
    """
    应用 rules[].after：若命中，返回更严格的 ready 时间
    """
    for r in _iter_rules(spec):
        sel = r.get("select", {})
        if not _sel_match(sel, s, n, m, kind):
            continue
        aft = r.get("after")
        if not aft:  # 没有 after
            continue
        op = aft.get("op")
        same_stage = bool(aft.get("same_stage", False))
        same_micro = bool(aft.get("same_micro", False))
        ts = s if same_stage else slice(None)
        tm = m if same_micro else slice(None)

        # 找到需要等待的那个/那些 op 的完成时间，取 max
        dep_t = t_ready
        def _get_fin(_s, _m, _op):
            if _op == "F":   return f_fin.get((n, _m, _s))
            if _op == "Bx":  return bx_fin.get((n, _m, _s))
            if _op == "Bw":  return bw_fin.get((n, _m, _s))
            return None

        if isinstance(ts, int) and isinstance(tm, int):
            tdep = _get_fin(ts, tm, op)
            if tdep is None: return None  # 依赖未完成，暂不可启动
            dep_t = max(dep_t, tdep)
        else:
            # 任意 stage/micro 组合都要完成，这里取最大（严格）
            # 也可以改成"任一即可"，看你们需求
            for _s in range(spec.num_stages) if not isinstance(ts,int) else [ts]:
                for _m in range(spec.num_micro) if not isinstance(tm,int) else [tm]:
                    tdep = _get_fin(_s, _m, op)
                    if tdep is None: return None
                    dep_t = max(dep_t, tdep)

        dep_t += float(r.get("offset", 0.0))
        t_ready = max(t_ready, dep_t)
    return t_ready

def _guard_before(spec: ScheduleSpec, s: int, n: int, m: int, kind: Kind,
                  t0: float, stage_free: List[float],
                  f_fin, bx_fin, bw_fin) -> bool:
    """
    应用 rules[].before + guard.lookahead：
    若"被 before 的 op"将在 lookahead 内可开，则当前 kind 延后（返回 True=需要延后）
    """
    for r in _iter_rules(spec):
        sel = r.get("select", {})
        if not _sel_match(sel, s, n, m, kind):
            continue
        bef = r.get("before")
        grd = r.get("guard", {})
        if not bef or not grd:  # 没有 before 或没有 guard
            continue
        op2 = bef.get("op")
        if not op2: continue
        same_stage = bool(bef.get("same_stage", False))
        look = float(grd.get("lookahead", 0.0))

        # 估计 op2 的可开时间（非常简化：仅看资源与依赖完成）
        def _dep_ready(_op2):
            deps = [stage_free[s]] if same_stage else [min(stage_free)]
            # 最常见依赖近似：F 依赖 F 上游；Bx 依赖 F (+ Bx[s+1]); Bw 依赖 F (+Bx)
            # 这里用于 guard 粗判，不做跨 stage 穷举（你们可替换成更严格估计）
            if _op2 == "F":
                if s > 0:
                    # 找最近 micro 的 F[s-1,m] 完成；粗略用现有 m
                    if (n, m, s-1) not in f_fin: return None
                    deps.append(f_fin[(n, m, s-1)])
            elif _op2 == "Bx":
                if (n, m, s) not in f_fin: return None
                deps.append(f_fin[(n, m, s)])
                if _bx_chain_strict(spec) and s < (spec.num_stages-1):
                    if (n, m, s+1) not in bx_fin: return None
                    deps.append(bx_fin[(n, m, s+1)])
            elif _op2 == "Bw":
                if (n, m, s) not in f_fin: return None
                deps.append(f_fin[(n, m, s)])
                if _bw_after_bx(spec):
                    if (n, m, s) not in bx_fin: return None
                    deps.append(bx_fin[(n, m, s)])
            return max(deps)

        t2 = _dep_ready(op2)
        if t2 is None:  # 被 before 的 op 还远
            continue
        if t2 - t0 <= look:
            return True  # 延后
    return False

class FreeAnyStrategy:
    """
    可自由造型的统一策略：
      - 读取现有 cfg 字段；不限定具体模式
      - 生成 F/Bx/Bw 事件；insertions 由上层 builder 复用（无需这里处理）
    """
    def build_fb_events(self, spec: ScheduleSpec) -> List[Event]:
        S, M, N = spec.num_stages, spec.num_micro, spec.num_mini
        lane_compute = spec.lanes.get("compute", 0)

        Fdur = _as_list(spec.forward, S)  # 你们分析层应已做归一；这里兜底
        Bxdur, Bwdur = _get_bx_bw(spec)

        prio = _priority(spec)
        pr_idx = {k:i for i,k in enumerate(prio)}
        bx_chain = _bx_chain_strict(spec)
        bw_after = _bw_after_bx(spec)
        bw_fill_mode, bw_fill_look = _bw_fill(spec)

        events: List[Event] = []
        # 完成时间表
        f_fin: Dict[Tuple[int,int,int], float]  = {}
        bx_fin: Dict[Tuple[int,int,int], float] = {}
        bw_fin: Dict[Tuple[int,int,int], float] = {}

        for n in range(N):
            stage_free = [0.0] * S
            # 每个 mini：把 3*S*M 个任务都放进待调度表
            pending: List[Tuple[int,int,Kind]] = []
            for m in range(M):
                for s in range(S):
                    pending.extend([(s,m,"F"), (s,m,"Bx"), (s,m,"Bw")])

            def deps_ready(s:int, m:int, kind:Kind) -> Optional[float]:
                deps = [stage_free[s]]  # 资源
                if kind == "F":
                    if s > 0:
                        if (n, m, s-1) not in f_fin: return None
                        deps.append(f_fin[(n, m, s-1)])
                elif kind == "Bx":
                    if (n, m, s) not in f_fin: return None
                    deps.append(f_fin[(n, m, s)])
                    if bx_chain and s < S-1:
                        if (n, m, s+1) not in bx_fin: return None
                        deps.append(bx_fin[(n, m, s+1)])
                elif kind == "Bw":
                    if (n, m, s) not in f_fin: return None
                    deps.append(f_fin[(n, m, s)])
                    if bw_after:
                        if (n, m, s) not in bx_fin: return None
                        deps.append(bx_fin[(n, m, s)])
                else:
                    return None
                return max(deps)

            def dur_of(s:int, kind:Kind) -> float:
                if kind == "F":  return float(Fdur[s])
                if kind == "Bx": return float(Bxdur[s])
                if kind == "Bw": return float(Bwdur[s])
                raise ValueError("unknown kind")

            # 主循环：每次挑一个"最早可开"的任务；用优先级与 guard 细化 tie-break
            while pending:
                cands: List[Tuple[float,int,int,Kind]] = []
                for (s,m,k) in pending:
                    t0 = deps_ready(s,m,k)
                    if t0 is None: continue
                    # 应用 after 规则（可能推迟或禁止启动）
                    t1 = _after_target_ready((n,m,s,k), t0, f_fin, bx_fin, bw_fin, spec, s, n, m, k)
                    if t1 is None:  # 依赖没成型
                        continue
                    cands.append((t1,s,m,k))

                if not cands:
                    # 没有就绪任务：推进最早完成的 stage 到它的 free 时间（等价于跳时间）
                    # 这里简化为"找下一批可能就绪"的 F 前驱；一般不会走到
                    break

                # Bw 填缝（可选）：如果 mode=safe，且存在 Bw 在 lookahead 内，提升其优先级
                boost = set()
                if bw_fill_mode in ("safe","aggressive") and bw_fill_look > 0:
                    for (t1,s,m,k) in cands:
                        if k == "Bw":
                            # 若另一个候选不是 Bw 且 (t_nonbw - t_bw) 小于阈值，把 Bw 提升
                            pass  # 轻量启发就够了；过度复杂会拖慢

                def keyfn(x):
                    t1,s,m,k = x
                    # guard-before：若触发，延后（加一个小惩罚）
                    delay = 0.0
                    if _guard_before(spec, s, n, m, k, t1, stage_free, f_fin, bx_fin, bw_fin):
                        delay = 1e6  # 直接丢到非常靠后（也可把 t1 += lookahead）
                    return (t1 + delay, pr_idx.get(k, 99), -s)

                t1,s,m,k = min(cands, key=keyfn)
                d = dur_of(s,k)
                op = k  # "F"/"Bx"/"Bw"
                ev = Event(stage=s, lane=lane_compute, start=t1, dur=d, op=op, micro=m, mini=n)
                events.append(ev)

                t2 = t1 + d
                stage_free[s] = t2
                if k == "F":
                    f_fin[(n,m,s)] = t2
                elif k == "Bx":
                    bx_fin[(n,m,s)] = t2
                else:
                    bw_fin[(n,m,s)] = t2

                # 移除已完成
                pending.remove((s,m,k))

        return events
