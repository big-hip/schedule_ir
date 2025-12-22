# -*- coding: utf-8 -*-
from dataclasses import dataclass, asdict
from typing import Optional, Dict, Any
import json

@dataclass
class Event:
    stage: int
    lane: int
    start: float
    dur: float
    op: str
    mini: Optional[int]
    micro: Optional[int]
    
    # event_id 在最终统一编号时赋值
    event_id: Optional[int] = None

    def to_dict(self) -> Dict[str, Any]:
        d = asdict(self)
        # 确保字段顺序（可选）
        keys = ["event_id", "stage", "lane", "start", "dur", "op", "mini" ,"micro"]
        return {k: d.get(k, None) for k in keys}

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), ensure_ascii=False)

