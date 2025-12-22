# -*- coding: utf-8 -*-
from typing import Any, Dict, List, Union

Number = Union[int, float]

def _ensure_list(x: Union[Number, List[Number]], n: int, name: str) -> List[Number]:
    if isinstance(x, list):
        if len(x) != n:
            raise ValueError(f"{name} length must be {n}, got {len(x)}")
        return x
    return [x] * n

class ScheduleSpec:
    """
    统一参数对象：从 dict(JSON/YAML) 构造并归一化。
    仅使用标准库。
    """
    def __init__(self, cfg: Dict[str, Any]):
        self.name: str = cfg.get("name", "pp_run")
        self.strategy: str = cfg["strategy"]  # gpipe | pipedream_flush | interleaved

        self.num_stages: int = int(cfg["num_stages"])
        self.num_micro: int = int(cfg["num_micro"])
        self.num_mini: int  = int(cfg.get("num_mini", 1))
        self.time_unit: str = cfg.get("time_unit", "us")

        # lanes
        lanes = cfg.get("lanes", {"compute": 0, "comm": 1, "custom": 2})
        self.lanes: Dict[str, int] = dict(lanes)

        # durations
        sd = cfg["stage_durations"]
        self.forward: List[Number]  = _ensure_list(sd["forward"],  self.num_stages, "stage_durations.forward")
        self.backward: List[Number] = _ensure_list(sd["backward"], self.num_stages, "stage_durations.backward")

        # comm (optional)
        self.comm: Dict[str, Any] = cfg.get("comm", {})
        if self.comm:
            if "send" in self.comm: self.comm["send"] = _ensure_list(self.comm["send"], self.num_stages-1, "comm.send")
            if "recv" in self.comm: self.comm["recv"] = _ensure_list(self.comm["recv"], self.num_stages-1, "comm.recv")

        # insertions
        self.insertions: List[Dict[str, Any]] = cfg.get("insertions", [])

        # emit
        self.emit: Dict[str, Any] = cfg.get("emit", {"include_comments": True})
        # zb_mode
        self.zb_mode = str(cfg.get("zb_mode", "H1")).upper()  # "H1" | "H2" | "V"

        # backward 拆分：优先使用显式 bx/bw，否则用比例从 backward 拆
        bx = cfg.get("stage_durations", {}).get("bx")
        bw = cfg.get("stage_durations", {}).get("bw")
        split = cfg.get("backward_split", {"bx_ratio": 0.5})  # 也可传 {"bx_ratio": 0.4} 等

        if bx is not None and bw is not None:
            self.bx = _ensure_list(bx, self.num_stages, "stage_durations.bx")
            self.bw = _ensure_list(bw, self.num_stages, "stage_durations.bw")
        else:
            r = float(split.get("bx_ratio", 0.5))
            self.bx = [r * t for t in self.backward]
            self.bw = [(1.0 - r) * t for t in self.backward]

        # 通信时长：activation grad 的点对点；weight grad 的 all-reduce
        # p2p 可以是单值或 len=S-1 的数组；allreduce 可以是单值
        self.comm.setdefault("p2p", 0)        # activation grad P2P（Send/Recv 对）
        self.comm.setdefault("allreduce", 0)  # weight grad 规约
        if isinstance(self.comm["p2p"], list):
            if len(self.comm["p2p"]) != self.num_stages - 1:
                raise ValueError("comm.p2p length must be num_stages-1")
        else:
            self.comm["p2p"] = [self.comm["p2p"]] * (self.num_stages - 1)

        # H2 的额外 warmup（比 1F1B 的 p-1 多做多少个 micro 的前向；经验上取 p）
        self.zb_h2_extra_warmup = int(cfg.get("zb_h2_extra_warmup", self.num_stages))

        # ZB-V：同一物理 stage 上两个虚拟 chunk 的"体内"P2P开销（默认 0）
        self.comm.setdefault("intra_p2p", 0.0)
        #manual的接口
        self.manual = cfg.get("manual")  # 若未提供，则为 None
                # free_any: 直接采用 cfg 段落（ops/durations/dependencies/manual）
        
        self._validate()

    def _validate(self):
        if self.num_stages <= 0 or self.num_micro <= 0 or self.num_mini <= 0:
            raise ValueError("num_stages/num_micro/num_mini must be positive.")
        if self.strategy not in {"gpipe", "pipedream_flush", "interleaved","pipedream_2bw","zerobubble","1f1b","dualpipe","xpipe","free_any"}:
            raise ValueError(f"unknown strategy: {self.strategy}")
        if self.zb_mode not in {"H1", "H2", "V"}:
            raise ValueError(f"unknown ZeroBubble mode: {self.zb_mode}")
        # lane 唯一性校验
        seen = set()
        for k, v in self.lanes.items():
            if v in seen:
                raise ValueError(f"lane id duplicated: {v}")
            seen.add(v)

        

