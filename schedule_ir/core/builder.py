# -*- coding: utf-8 -*-
from typing import List
from .events import Event
from .params import ScheduleSpec
from .strategies.base import StrategyFactory
from .insertions import apply_insertions
from .idle import synthesize_idle
from .utils import sorted_by_start_then_stage, assign_event_ids
from .manual import ManualTuner
def build(spec: ScheduleSpec, add_comm: bool = False) -> List[Event]:
    """
    主入口：
      1) 策略生成 F/B
      2) （可选）插入通信算子（建议用 insertions 统一实现 Send/Recv/AllReduce）
      3) 应用通用插入算子规则
      4) 合成 Idle
      5) 排序并编号
    """
    strat = StrategyFactory.make(spec.strategy)
    events = strat.build_fb_events(spec)

    # 提示：通信也可通过 insertions 统一添加（建议）
    # if add_comm: events = add_comm_events(spec, events)

    events = apply_insertions(spec, events)
    #插入manual的
    if spec.manual:                 # 只有提供了 manual 参数才执行
        tuner = ManualTuner(spec)
        events = tuner.apply(events)
    events = synthesize_idle(events, spec.num_stages)

    events = sorted_by_start_then_stage(events)
    events = assign_event_ids(events)
    return events

