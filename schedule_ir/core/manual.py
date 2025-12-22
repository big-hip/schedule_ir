# ppflow/manual.py (v2)
# -*- coding: utf-8 -*-
from typing import List, Dict, Any, Optional, Tuple
from .events import Event
from .params import ScheduleSpec
from .utils import select_indices

# 仅对 compute 事件做微调；Idle/comm/custom 不动
COMPUTE_OPS = {"F", "Bx", "Bw"}

# ---------- 基础工具 ----------
def _ends(ev: Event) -> float:
    return ev.start + ev.dur

def _priority_index(priority: List[str], op: str) -> int:
    try:
        return priority.index(op)
    except ValueError:
        return len(priority)

def _group_by_stage(events: List[Event]) -> Dict[int, List[Event]]:
    g: Dict[int, List[Event]] = {}
    for e in events:
        g.setdefault(e.stage, []).append(e)
    for s in g:
        g[s].sort(key=lambda x: (x.start, _ends(x)))
    return g

def _find_targets(events: List[Event], sel: Dict[str, Any], spec: ScheduleSpec) -> List[Event]:
    stages = set(select_indices(sel.get("stages", "all"), spec.num_stages))
    minis  = sel.get("minis", "all")
    micros = sel.get("micros", "all")
    ops    = sel.get("op", None)
    if isinstance(ops, str):
        ops = [ops]

    ok_minis  = set(select_indices(minis,  spec.num_mini))  if minis  != "all" else None
    ok_micros = set(select_indices(micros, spec.num_micro)) if micros != "all" else None

    out = []
    for e in events:
        if e.stage not in stages: continue
        if ops is not None and e.op not in ops: continue
        if ok_minis  is not None and (e.mini  not in ok_minis):  continue
        if ok_micros is not None and (e.micro not in ok_micros): continue
        out.append(e)
    return out

def _find_anchor(events: List[Event], cond: Dict[str, Any], host: Event, mode: str) -> Optional[Event]:
    """
    cond: {op: "Bx"/"Bw"/"F", same_micro: bool, same_stage: bool}
    mode: "after" -> 取"结束时间最大"的锚点；"before" -> 取"开始时间最小"的锚点
    """
    op  = cond.get("op")
    smi = cond.get("same_micro", False)
    sst = cond.get("same_stage", False)

    def _same(ev: Event, other: Event) -> bool:
        if sst and ev.stage != other.stage: return False
        if smi and (ev.micro != other.micro or ev.mini != other.mini): return False
        return True

    cands = [e for e in events if (op is None or e.op == op) and _same(e, host)]
    if not cands: return None
    return max(cands, key=_ends) if mode == "after" else min(cands, key=lambda x: x.start)

def _get_budget_for(op: str, budgets: Dict[str, Dict[str, float]]) -> Tuple[float, float]:
    b = budgets.get(op, {})
    adv = float(b.get("advance", float("inf")))  # 允许最大提前
    delay = float(b.get("delay", float("inf")))  # 允许最大延后
    return adv, delay

def _clamp_to_budget(orig_start: float, desired: float, adv: float, delay: float) -> float:
    earliest = orig_start - adv
    latest   = orig_start + delay
    if desired < earliest: return earliest
    if desired > latest:   return latest
    return desired

def _shift_without_overlap_with_budget(
    events_stage: List[Event],
    ev: Event,
    desired_start: float,
    adv: float,
    delay: float,
    orig_start: float,
):
    """
    只移动 ev（可编辑），其他事件（包括 F 或不可编辑）视为障碍。
    1) 先按预算夹住 desired_start
    2) 再右移直到不与同 stage 其他事件重叠
    """
    t = _clamp_to_budget(orig_start, desired_start, adv, delay)
    others = [x for x in events_stage if x is not ev]
    changed = True
    while changed:
        changed = False
        for o in sorted(others, key=lambda x: x.start):
            if t < _ends(o) and t + ev.dur > o.start:
                t = _ends(o)  # 只向右挪，保持拓扑安全
                changed = True
    ev.start = t

def _f_barriers_by_mini(events: List[Event]) -> Dict[int, float]:
    """同 mini 的 F 阶段结束屏障：该 mini 所有 F 的结束时间最大值"""
    barriers: Dict[int, float] = {}
    for e in events:
        if e.op != "F" or e.mini is None: 
            continue
        barriers[e.mini] = max(barriers.get(e.mini, 0.0), _ends(e))
    return barriers

def _has_higher_priority_coming(events_stage: List[Event], base_start: float, horizon: float, priority: List[str], cur_op: str) -> bool:
    """在 [base_start, base_start+horizon) 内，是否有比 cur_op 更高优先级的事件将到来"""
    cur_rank = _priority_index(priority, cur_op)
    window_end = base_start + horizon
    for o in events_stage:
        if o.start >= base_start and o.start < window_end:
            if _priority_index(priority, o.op) < cur_rank:
                return True
    return False

# ---------- 主体 ----------
class ManualTuner:
    """
    v2：只按"scope + budget + gap_fill"对 Bx/Bw 做保守微调，缩小空泡。
    - F 锁定（可配置），不动；不可编辑事件是"障碍"
    - 预算：每个 op 的最大提前/延后
    - gap_fill：仅把可编辑的 Bx/Bw 尝试塞入足够大的空隙
    - 规则：after/before/offset/guard 只作用在可编辑事件上
    - 相位屏障：同 mini 的 Bx/Bw 不早于该 mini 全部 F 完成
    """
    def __init__(self, spec: ScheduleSpec):
        self.spec = spec
        m = spec.manual or {}

        # 全局优先级：默认 F > Bx > Bw（不会把 B 推到 F 之前；再有屏障兜底）
        self.priority: List[str] = m.get("priority", ["F","Bx","Bw"])

        # 作用域/预算
        scope = m.get("scope", {})
        self.editable_ops = set(scope.get("editable_ops", ["Bx","Bw"]))  # 默认只动反向
        self.lock_forward = bool(scope.get("lock_forward", True))
        self.within_stage_only = bool(scope.get("within_stage_only", True))  # 目前仅本 stage 内细排
        self.move_budget: Dict[str, Dict[str, float]] = m.get("move_budget", {})

        # 规则、链、屏障
        self.rules: List[Dict[str, Any]] = m.get("rules", [])
        self.bx_chain: str                = m.get("bx_chain", "strict")
        pb = m.get("phase_barrier", True)
        self.phase_barrier_enabled = (pb is True) or (isinstance(pb, dict) and pb.get("b_after_all_f", True))

        # gap_fill（新）——兼容旧 bw_fill
        gf = m.get("gap_fill")
        if not gf:
            bwf = m.get("bw_fill", {"mode": "safe", "lookahead": 60})
            gf = {
                "enable": True,
                "ops": ["Bw"],
                "gap_min": 0.0,
                "pack_direction": "left",
                "gap_order": "earliest_first",
                "pick": "earliest_ready",
                "max_per_gap": 1,
                "lookahead": float(bwf.get("lookahead", 60.0)),
            }
        self.gap_fill = gf

        # pin（可选）：满足 select 的事件彻底不动（从 editable 中剔除）
        self.pin_specs: List[Dict[str, Any]] = m.get("pin", [])

    def apply(self, events: List[Event]) -> List[Event]:
        # 只处理 compute；其余事件原样返回
        compute = [e for e in events if e.op in COMPUTE_OPS]
        others  = [e for e in events if e.op not in COMPUTE_OPS]

        # 过滤可编辑集合：按 scope & pin
        editable = [e for e in compute if e.op in self.editable_ops]
        if self.lock_forward:
            editable = [e for e in editable if e.op != "F"]

        if self.pin_specs:
            fixed = set()
            for ps in self.pin_specs:
                for ev in _find_targets(compute, ps.get("select", {}), self.spec):
                    fixed.add(ev)
            editable = [e for e in editable if e not in fixed]

        pinned = [e for e in compute if e not in editable]
        orig_start: Dict[int, float] = {id(e): e.start for e in editable}

        # 以 stage 为桶操作（pinned 作为障碍，仅移动 editable）
        by_stage = _group_by_stage(editable + pinned)

        # 1) 规则（只作用于 editable）
        self._apply_manual_rules(editable, pinned, by_stage, orig_start)

        # 2) gap_fill（只把可编辑的 Bx/Bw 塞入空隙）
        self._apply_gap_fill(editable, pinned, by_stage, orig_start)

        # 3) Bx 链粘连（strict/relaxed，仅动 Bx）
        self._apply_bx_chain(editable, pinned, by_stage, orig_start)

        # 4) 相位屏障（兜底：B 不早于同 mini 的所有 F 完成）
        if self.phase_barrier_enabled:
            self._enforce_phase_barrier(editable, pinned, by_stage, orig_start)

        # 拼回 + 保持原有非 compute 事件
        out = []
        for s, arr in by_stage.items():
            out.extend(arr)
        return out + others

    # ---------- 规则（after/before/offset/guard） ----------
    def _apply_manual_rules(self, editable, pinned, by_stage, orig_start):
        if not self.rules: return
        all_events = [e for arr in by_stage.values() for e in arr]
        for rule in self.rules:
            tars = [e for e in _find_targets(all_events, rule.get("select", {}), self.spec) if e in editable]
            if not tars:
                continue
            if "after" in rule:
                cond = rule["after"]; offset = float(rule.get("offset", 0.0))
                for ev in tars:
                    anchor = _find_anchor(all_events, cond, ev, mode="after")
                    if not anchor: continue
                    desired = _ends(anchor) + offset
                    arr = by_stage[ev.stage]
                    adv, delay = _get_budget_for(ev.op, self.move_budget)
                    _shift_without_overlap_with_budget(arr, ev, desired, adv, delay, orig_start[id(ev)])
            if "before" in rule:
                cond = rule["before"]; offset = float(rule.get("offset", 0.0))
                for ev in tars:
                    anchor = _find_anchor(all_events, cond, ev, mode="before")
                    if not anchor: continue
                    desired = max(0.0, anchor.start - ev.dur - offset)
                    arr = by_stage[ev.stage]
                    adv, delay = _get_budget_for(ev.op, self.move_budget)
                    _shift_without_overlap_with_budget(arr, ev, desired, adv, delay, orig_start[id(ev)])

    # ---------- gap_fill：在空隙里塞 Bx/Bw ----------
    def _apply_gap_fill(self, editable, pinned, by_stage, orig_start):
        gf = self.gap_fill or {}
        if not gf.get("enable", False):
            return
        ops_allowed = set(gf.get("ops", ["Bw"]))
        gap_min = float(gf.get("gap_min", 0.0))
        pack_direction = gf.get("pack_direction", "left")   # "left" | "right"
        gap_order = gf.get("gap_order", "largest_first")    # "largest_first" | "earliest_first"
        pick = gf.get("pick", "shortest_first")             # "shortest_first" | "earliest_ready"
        max_per_gap = int(gf.get("max_per_gap", 1))
        lookahead = float(gf.get("lookahead", 60.0))

        for s, arr in by_stage.items():
            arr.sort(key=lambda x: x.start)
            # 1) 计算空隙
            gaps: List[Tuple[float, float]] = []
            t = 0.0
            for e in arr:
                if e.start > t and (e.start - t) >= gap_min:
                    gaps.append((t, e.start))
                t = max(t, _ends(e))
            if not gaps:
                continue

            # 2) 空隙排序
            if gap_order == "largest_first":
                gaps.sort(key=lambda g: (g[1] - g[0]), reverse=True)
            else:
                gaps.sort(key=lambda g: g[0])

            # 3) 候选事件（本 stage、可编辑、op 允许），且不在任何空隙内部
            def _inside_gap(e, g): return e.start >= g[0] and _ends(e) <= g[1]
            cands = [e for e in arr if (e in editable and e.op in ops_allowed)]
            cands = [e for e in cands if not any(_inside_gap(e, g) for g in gaps)]

            # 4) 逐个空隙尝试放置
            for (gs, ge) in gaps:
                placed = 0
                # 候选选择策略
                if pick == "shortest_first":
                    cands.sort(key=lambda e: e.dur)
                else:
                    cands.sort(key=lambda e: e.start)  # 近似 earliest_ready

                for e in list(cands):
                    if placed >= max_per_gap:
                        break
                    if e.dur > (ge - gs):
                        continue
                    # 保护：空隙右侧 lookahead 内将有更高优先级（F/Bx）就谨慎跳过
                    if _has_higher_priority_coming(arr, ge, lookahead, self.priority, e.op):
                        continue
                    # 相位屏障：B 不得早于该 mini 的 F 完成
                    barriers = _f_barriers_by_mini(arr)
                    btime = barriers.get(e.mini, 0.0) if e.mini is not None else 0.0
                    desired = max(gs, btime) if pack_direction == "left" else max(gs, btime, ge - e.dur)

                    adv, delay = _get_budget_for(e.op, self.move_budget)
                    _shift_without_overlap_with_budget(arr, e, desired, adv, delay, orig_start[id(e)])
                    placed += 1
                    cands.remove(e)

    # ---------- Bx 链粘连（只动 Bx） ----------
    def _apply_bx_chain(self, editable, pinned, by_stage, orig_start):
        mode = self.bx_chain
        if mode not in {"strict", "relaxed"}:
            return
        # (mini, micro) -> 该 micro 的 Bx 列表（按 stage 递增）
        bx_map: Dict[Tuple[int,int], List[Event]] = {}
        for s, arr in by_stage.items():
            for e in arr:
                if e.op == "Bx" and e in editable and e.mini is not None and e.micro is not None:
                    bx_map.setdefault((e.mini, e.micro), []).append(e)

        for key, seq in bx_map.items():
            seq.sort(key=lambda x: x.stage)
            for i in range(1, len(seq)):
                prev, cur = seq[i-1], seq[i]
                target = _ends(prev)
                arr = by_stage[cur.stage]
                adv, delay = _get_budget_for(cur.op, self.move_budget)
                if cur.start > target:
                    _shift_without_overlap_with_budget(arr, cur, target, adv, delay, orig_start[id(cur)])

    # ---------- 相位屏障：同 mini 的 Bx/Bw 不早于该 mini 的 F 完成 ----------
    def _enforce_phase_barrier(self, editable, pinned, by_stage, orig_start):
        barriers = _f_barriers_by_mini([e for arr in by_stage.values() for e in arr])
        if not barriers: return
        for s, arr in by_stage.items():
            for e in arr:
                if e not in editable:  # 只对可编辑的 Bx/Bw 兜底
                    continue
                if e.op in {"Bx","Bw"} and e.mini is not None:
                    b = barriers.get(e.mini, 0.0)
                    if e.start < b:
                        adv, delay = _get_budget_for(e.op, self.move_budget)
                        _shift_without_overlap_with_budget(arr, e, b, adv, delay, orig_start[id(e)])
