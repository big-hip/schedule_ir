# -*- coding: utf-8 -*-
from typing import List
from ..events import Event
from ..params import ScheduleSpec

class Strategy:
    """调度策略抽象类：只负责 F/B 的时序与占用（不管通信与插入算子/空泡）"""
    def build_fb_events(self, spec: ScheduleSpec) -> List[Event]:
        raise NotImplementedError

class StrategyFactory:
    @staticmethod
    def make(name: str) -> "Strategy":
        if name == "gpipe":
            from .gpipe import GPipeStrategy
            return GPipeStrategy()
        if name == "pipedream_flush":
            from .pipedream_flush import OneFOneBStrategy
            return OneFOneBStrategy()
        if name == "interleaved":
            from .interleaved import InterleavedStrategy
            return InterleavedStrategy()
        if name == "pipedream_2bw":
            from .pipedream_2bw import PipeDream2BWStrategy
            return PipeDream2BWStrategy()
        if name == "zerobubble":
            from .zerobubble import ZeroBubbleStrategy
            return ZeroBubbleStrategy()
        if name == "1f1b":
            from .onef1b import OneF1BStrategy
            return OneF1BStrategy()
        if name == "dualpipe":
            from .dualpipe import DualPipeStrategy
            return DualPipeStrategy()
        if name == "xpipe":
            from .Xpipe import XPipeStrategy
            return XPipeStrategy()
        if name == "free_any":
            from .free_any import FreeAnyStrategy
            return FreeAnyStrategy()

        raise ValueError(f"unknown strategy: {name}")


##T_stage = FLOPs_stage / (GPU_FLOPS * η)  (η≈0.3~0.6（效率）)
