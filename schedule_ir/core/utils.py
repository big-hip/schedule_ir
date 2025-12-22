# -*- coding: utf-8 -*-
from typing import Iterable, List, Optional, Dict, Any

def select_indices(selector: Any, total: int) -> List[int]:
    """
    选择器统一实现：
      - "all": 全部 [0..total-1]
      - "even"/"odd": 偶/奇
      - list: 指定列表（越界会过滤）
    """
    if selector == "all" or selector is None:
        return list(range(total))
    if selector == "even":
        return [i for i in range(total) if i % 2 == 0]
    if selector == "odd":
        return [i for i in range(total) if i % 2 == 1]
    if isinstance(selector, list):
        return [i for i in selector if 0 <= i < total]
    raise ValueError(f"unsupported selector: {selector}")

def sorted_by_start_then_stage(events):
    return sorted(events, key=lambda e: (e.start, e.stage))

def assign_event_ids(events):
    for i, ev in enumerate(events):
        ev.event_id = i
    return events

