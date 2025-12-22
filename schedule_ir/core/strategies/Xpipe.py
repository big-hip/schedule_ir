# ppflow/strategies/xpipe.py
# -*- coding: utf-8 -*-
from typing import List, Tuple, Optional
from ..events import Event
from ..params import ScheduleSpec

class XPipeStrategy:
    """
    XPipe 调度（跨 mini 异步交错，无全局 flush）。
    约束：
      - 输入/输出接口保持不变：build_fb_events(spec)->List[Event]；Event 字段不变
      - 仅改中间"生成逻辑"，修复潜在死循环
    思路：
      - 显式构建待调度集合 pending = {F(s,k), B(s,k)}；每轮选择一个最早可开的任务执行
      - 依赖满足即可候选；若无候选则属于逻辑不可能（因 F(0,k) 总能逐步就绪）
      - 优先级：F@stage0 注水最高，其次 B，其次普通 F；同起点先更深 s，再更小 k
    记号：
      - 全局 micro 索引 k ∈ [0, N*M)，mini = k//M，micro = k%M
    """
    def build_fb_events(self, spec: ScheduleSpec) -> List[Event]:
        S: int = int(spec.num_stages)
        M: int = int(spec.num_micro)
        N: int = int(getattr(spec, "num_mini", 1))

        # 时长（逐 stage 列表）
        Fdur = spec.forward if isinstance(spec.forward, list) else [spec.forward] * S
        Bdur = spec.backward if isinstance(spec.backward, list) else [spec.backward] * S
        lane_compute = spec.lanes.get("compute", 0)

        total = N * M  # 全局 micro 数
        events: List[Event] = []

        # 资源与完成时间
        stage_free = [0.0] * S               # 每个 stage 可用时间
        f_finish = {}  # (k, s) -> t
        b_finish = {}  # (k, s) -> t

        # 待调度集合（显式列出所有 F/B 任务，保证每轮至少调度一个 → 无死循环）
        pending = []  # 列表项：(kind, s, k)；kind ∈ {"F","B"}
        for k in range(total):
            for s in range(S):
                pending.append(("F", s, k))
                pending.append(("B", s, k))

        # 依赖就绪时间计算；就绪则返回最早可开时间，否则返回 None
        def ready_time(kind: str, s: int, k: int):
            deps = [stage_free[s]]
            if kind == "F":
                if s == 0:
                    # 注水：只需本 stage 空闲
                    return max(deps)
                # 依赖前一 stage 的 F 完成
                if (k, s-1) not in f_finish:
                    return None
                deps.append(f_finish[(k, s-1)])
                return max(deps)

            # kind == "B"
            # 依赖本层 F 完成
            if (k, s) not in f_finish:
                return None
            deps.append(f_finish[(k, s)])
            # 依赖下一层 B 完成（尾层例外）
            if s < S - 1:
                if (k, s+1) not in b_finish:
                    return None
                deps.append(b_finish[(k, s+1)])
            return max(deps)

        # 选择器优先级
        def pri(kind: str, s: int, k: int, start: float):
            inject = (kind == "F" and s == 0)
            return (-1 if inject else (0 if kind == "B" else 1), start, -s, k)

        # 主循环：每轮调度 1 个任务（pending 严格减少 → 必终止）
        while pending:
            candidates: List[tuple] = []  # (start, kind, s, k)

            # 枚举可开任务
            for (kind, s, k) in pending:
                t0 = ready_time(kind, s, k)
                if t0 is not None:
                    candidates.append((t0, kind, s, k))

            if not candidates:
                # 防御：选一个最早的 F@stage0 作为注水候选
                best = None
                for (kind, s, k) in pending:
                    if kind == "F" and s == 0:
                        t0 = stage_free[0]
                        cand = (t0, kind, s, k)
                        if best is None or pri(*cand[1:], cand[0]) < pri(*best[1:], best[0]):
                            best = cand
                if best is None:
                    # 极端情况下仍无（理论上不可能），直接中断，避免死循环
                    break
                candidates.append(best)

            # 选出优先级最高的候选
            start, kind, s, k = min(candidates, key=lambda c: pri(c[1], c[2], c[3], c[0]))
            mini = k // M
            micro = k % M

            # 执行并记录
            if kind == "F":
                dur = float(Fdur[s])
                ev = Event(stage=s, lane=lane_compute, start=start, dur=dur,
                           op="F", mini=mini, micro=micro)
                events.append(ev)
                finish = start + dur
                stage_free[s] = finish
                f_finish[(k, s)] = finish
            else:
                dur = float(Bdur[s])
                ev = Event(stage=s, lane=lane_compute, start=start, dur=dur,
                           op="B", mini=mini, micro=micro)
                events.append(ev)
                finish = start + dur
                stage_free[s] = finish
                b_finish[(k, s)] = finish

            # 移除该任务
            pending.remove((kind, s, k))

        return events
