#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
封装入口：直接用 Eval.Evaluate() 给出的 time_list 生成流水线调度。

核心函数：
    generate_from_time_list(time_list, strategy="gpipe", ...)
仅需提供 time_list（Eval.Evaluate 返回的第二个值）与策略等少量参数。
"""
from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Dict, List, Optional, Sequence

from schedule_ir.core.params import ScheduleSpec
from schedule_ir.core.builder import build
from schedule_ir.core.ndjson_writer import write_ndjson
from schedule_ir.utils.plot import plot_pp_gantt


def _compute_stage_durations(time_list: Sequence[float], P: int, T: int, S: int) -> List[float]:
    """
    根据 dist_config 中的并行度信息，按设备编号间差分得到每个 stage 的耗时。
    device_number = i * T * S
    """
    if P <= 0:
        raise ValueError("num_stages(P) 必须大于 0")
    device_indices = [i * T * S for i in range(P)]
    if max(device_indices) >= len(time_list):
        raise ValueError("time_list 长度不足以覆盖所有 stage 设备索引")

    durations: List[float] = []
    prev_time: Optional[float] = None
    for idx in device_indices:
        current = float(time_list[idx])
        if prev_time is None:
            durations.append(current)
        else:
            delta = current - prev_time
            if delta <= 0:
                raise ValueError("计算得到的相邻 stage 时间差必须为正")
            durations.append(delta)
        prev_time = current
    return durations


def _load_dist_config(env_key: str) -> Dict:
    cfg_str = os.getenv(env_key)
    if cfg_str is None:
        raise RuntimeError(f"环境变量 {env_key} 没有设置")
    try:
        return json.loads(cfg_str)
    except json.JSONDecodeError as e:
        raise RuntimeError(f"{env_key} 环境变量内容不是合法 JSON: {e}") from e


def generate_from_time_list(
    time_list: Sequence[float],
    strategy: str = "gpipe",
    num_mini: int = 1,
    num_micro: int = 4,
    lanes: Optional[Dict[str, int]] = None,
    insertions: Optional[List[Dict]] = None,
    extra_cfg: Optional[Dict] = None,
    dist_env_key: str = "dist_config",
    output_ndjson: str | Path = "out.ndjson",
    output_plot: Optional[str | Path] = "pp_gantt.png",
    include_comments: bool = True,
) -> None:
    """
    主入口：用 Eval.Evaluate 返回的 time_list + 环境中的 dist_config 生成调度并保存文件。

    参数：
        time_list: Eval.Evaluate() 返回的时间列表。
        strategy: 流水策略（gpipe / pipedream_flush / interleaved / ...）。
        num_mini: mini 批次数。
        num_micro: micro 批次数。
        lanes: 可选，通道映射；缺省 compute=0, comm=1, custom=2。
        insertions: 可选，自定义算子列表。
        extra_cfg: 可选，附加字段（如 meta.virtual_chunks, manual, comm 等）。
        dist_env_key: 环境变量名，默认为 dist_config。
        output_ndjson: 输出 ndjson 路径，默认 "out.ndjson"。
        output_plot: 甘特图输出路径，默认 "pp_gantt.png"；设为 None 时不绘制。
        include_comments: ndjson 是否包含中文注释行。
    
    说明：
        函数调用后会自动保存两个文件：
        1. NDJSON 文件：包含调度事件数据
        2. 甘特图文件：可视化的调度图表（如果 output_plot 不为 None）
    """
    dist_cfg = _load_dist_config(dist_env_key)
    pd = dist_cfg["prefill"]["parallel_degree"]
    P = int(pd["P"])
    T = int(pd["T"])
    S = int(pd["S"])

    stage_durations = _compute_stage_durations(time_list, P=P, T=T, S=S)

    lanes = lanes or {"compute": 0, "comm": 1, "custom": 2}
    cfg = {
        "name": f"pp_{strategy}_from_eval",
        "strategy": strategy,
        "num_stages": P,
        "num_mini": num_mini,
        "num_micro": num_micro,
        "lanes": lanes,
        "stage_durations": {"forward": stage_durations, "backward": stage_durations},
        "insertions": insertions or [],
        "emit": {"include_comments": include_comments},
    }
    if extra_cfg:
        cfg.update(extra_cfg)

    spec = ScheduleSpec(cfg)
    events = build(spec)

    # 保存 NDJSON 文件
    output_ndjson = Path(output_ndjson)
    output_ndjson.parent.mkdir(parents=True, exist_ok=True)
    with output_ndjson.open("w", encoding="utf-8") as f:
        write_ndjson(f, events, include_comments=include_comments)
    print(f"✅ NDJSON 文件已保存: {output_ndjson} ({len(events)} 个事件)")

    # 保存甘特图文件
    if output_plot:
        output_plot = Path(output_plot)
        output_plot.parent.mkdir(parents=True, exist_ok=True)
        plot_pp_gantt(events, output_file=str(output_plot))
        print(f"✅ 甘特图已保存: {output_plot}")


if __name__ == "__main__":
    # 典型用法示例：假设外部已执行 time, time_list = Eval.Evaluate()
    # 这里使用占位示例数据与 dist_config 环境变量
    import argparse

    parser = argparse.ArgumentParser(description="用 Eval.Evaluate 的 time_list 直接生成调度")
    parser.add_argument("--strategy", type=str, default="gpipe", help="流水策略")
    parser.add_argument("--output-ndjson", type=Path, default=Path("out.ndjson"), help="ndjson 输出")
    parser.add_argument("--output-plot", type=Path, default=Path("pp_gantt.png"), help="甘特图输出，设为空字符串可禁用")
    parser.add_argument("--dist-env-key", type=str, default="dist_config", help="并行度环境变量名")
    args = parser.parse_args()

    # 示例占位：真实使用时请替换为 Eval.Evaluate() 的返回值
    sample_time_list = [100, 220, 360, 520, 700, 890, 1100, 1320]

    output_plot = None if str(args.output_plot) == "" else args.output_plot
    generate_from_time_list(
        time_list=sample_time_list,
        strategy=args.strategy,
        output_ndjson=args.output_ndjson,
        output_plot=output_plot,
        dist_env_key=args.dist_env_key,
    )

