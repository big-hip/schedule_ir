# -*- coding: utf-8 -*-
from typing import List, Dict, Any
from .events import Event
from .params import ScheduleSpec
from .utils import select_indices

# ============================================================
# Helper: 判断 rule 是否适用于某个事件
# ============================================================
def _applies(rule: Dict[str, Any], ev: Event, spec: ScheduleSpec) -> bool:
    """判断一条插入规则是否适用于当前事件，空规则会安全返回 False"""
    if not rule or not isinstance(rule, dict):
        return False

    # phase 匹配
    phase = rule.get("phase")
    if phase == "forward" and ev.op != "F":
        return False
    if phase == "backward" and ev.op not in ("B", "Bx", "Bw"): 
        return False
    if phase == "idle" and ev.op != "Idle":
        return False

    # selector 匹配
    try:
        ss = rule.get("stage_selector", "all")
        ns = rule.get("mini_selector", "all")   # mini = 大批
        ms = rule.get("micro_selector", "all")  # micro = 子批

        if ev.stage not in set(select_indices(ss, spec.num_stages)):
            return False
        if ev.mini is not None and ev.mini not in set(select_indices(ns, spec.num_mini)):
            return False
        if ev.micro is not None and ev.micro not in set(select_indices(ms, spec.num_micro)):
            return False
    except Exception:
        # 配置异常（如 selector 类型错误），直接跳过
        return False

    return True


# ============================================================
# Helper: 计算插入算子的持续时间
# ============================================================
def _duration_of(rule: Dict[str, Any], host_dur: float) -> float:
    d = rule.get("duration", {})
    if not d:
        # 没有定义 duration，则默认使用 host 时长的 10%
        return 0.1 * host_dur
    if "fixed" in d:
        return float(d["fixed"])
    if "ratio_of_host" in d:
        return float(d["ratio_of_host"]) * float(host_dur)
    # 不合法时返回最小正值而不是抛错
    return max(1e-6, 0.1 * host_dur)


# ============================================================
# Helper: 计算插入算子的起始时间
# ============================================================
def _anchor_time(rule: Dict[str, Any], host: Event) -> float:
    anchor = rule.get("anchor", "host_start")
    if anchor == "host_start":
        return host.start
    if anchor == "host_end":
        return host.start + host.dur
    if anchor == "gap_start":
        # 若 host 是 Idle，则取 start，否则取 end
        return host.start if host.op == "Idle" else (host.start + host.dur)
    # 未知 anchor，默认使用 host_start
    return host.start


# ============================================================
# 主函数：执行插入逻辑
# ============================================================
def apply_insertions(spec: ScheduleSpec, events: List[Event]) -> List[Event]:
    """根据 spec.insertions 对原事件进行扩展，支持空/不完整配置"""
    if not getattr(spec, "insertions", None):
        return events

    lane_map = getattr(spec, "lanes", {"custom": 2})
    out = list(events)

    for rule in spec.insertions:
        # 跳过空或不合法的 rule
        if not rule or "op" not in rule:
            continue

        lane_id = lane_map.get(rule.get("lane", "custom"), 2)
        op_name = rule["op"]

        for ev in events:
            # 仅在匹配的事件上插入
            if not _applies(rule, ev, spec):
                continue

            try:
                start0 = _anchor_time(rule, ev) + float(rule.get("offset", 0.0))
                dur0 = _duration_of(rule, ev.dur if ev.dur else 1.0)
            except Exception:
                continue  # 若计算出错则跳过该插入

            inserted = Event(
                stage=ev.stage,
                lane=lane_id,
                start=start0,
                dur=dur0,
                op=op_name,
                mini=ev.mini,
                micro=ev.micro,

            )
            out.append(inserted)

    return out

