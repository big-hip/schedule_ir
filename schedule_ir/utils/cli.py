#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
简单的调度生成入口：
- 读取基础配置（JSON）
- 可选：通过 time_list + stage_indices 自动推算 stage_durations
- 生成 out.ndjson，并可直接绘制甘特图
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Iterable, List, Sequence

from schedule_ir.core.params import ScheduleSpec
from schedule_ir.core.builder import build
from schedule_ir.core.ndjson_writer import write_ndjson
from schedule_ir.utils.plot import plot_pp_gantt


CONFIG_DIR = Path("configs")


def _parse_number_list(raw: str) -> List[float]:
    """
    支持两种输入：
      1) 逗号分隔字符串: "10,20,35"
      2) 文件路径: 内容可以是 JSON 数组或换行/逗号分隔数字
    """
    path = Path(raw)
    if path.exists():
        text = path.read_text(encoding="utf-8").strip()
        try:
            data = json.loads(text)
            if isinstance(data, Iterable):
                return [float(x) for x in data]
        except json.JSONDecodeError:
            pass
        # fallback: split by comma/newline
        tokens = [t for t in text.replace("\n", ",").split(",") if t.strip()]
        return [float(t) for t in tokens]
    return [float(t) for t in raw.split(",") if t.strip()]


def _parse_stage_indices(raw: str | None, num_stages: int) -> List[int]:
    if raw is None:
        return list(range(num_stages))
    return [int(x) for x in raw.split(",") if x.strip()]


def _derive_stage_durations(time_list: Sequence[float], stage_indices: Sequence[int]) -> List[float]:
    if len(stage_indices) == 0:
        raise ValueError("stage_indices 不能为空")
    if any(idx < 0 or idx >= len(time_list) for idx in stage_indices):
        raise ValueError("stage_indices 中存在越界下标")
    if len(stage_indices) != len(set(stage_indices)):
        raise ValueError("stage_indices 不能包含重复值")
    if sorted(stage_indices) != list(stage_indices):
        raise ValueError("stage_indices 必须按时间顺序递增")

    durations: List[float] = []
    prev = None
    for idx in stage_indices:
        current = float(time_list[idx])
        if prev is None:
            durations.append(current)
        else:
            delta = current - prev
            if delta <= 0:
                raise ValueError("time_list 中对应的时间差必须为正")
            durations.append(delta)
        prev = current
    return durations


def _resolve_config(path_or_name: Path) -> Path:
    """允许传入文件路径或简写名字（自动在 configs/ 下寻找同名 json）"""
    if path_or_name.exists():
        return path_or_name
    candidate = CONFIG_DIR / f"{path_or_name}.json"
    if candidate.exists():
        return candidate
    raise FileNotFoundError(f"未找到配置文件：{path_or_name}")


def build_cfg(base_cfg_path: Path, time_list_arg: str | None, stage_indices_arg: str | None) -> dict:
    cfg = json.loads(base_cfg_path.read_text(encoding="utf-8"))

    if time_list_arg:
        time_list = _parse_number_list(time_list_arg)
        stage_indices = _parse_stage_indices(stage_indices_arg, cfg["num_stages"])
        if len(stage_indices) != cfg["num_stages"]:
            raise ValueError("stage_indices 长度必须等于 num_stages")
        durations = _derive_stage_durations(time_list, stage_indices)
        cfg.setdefault("stage_durations", {})
        cfg["stage_durations"]["forward"] = durations
        cfg["stage_durations"]["backward"] = cfg["stage_durations"].get("backward", durations)
    return cfg


def main():
    parser = argparse.ArgumentParser(description="生成流水线调度 ndjson 并绘制甘特图")
    parser.add_argument(
        "--config",
        type=Path,
        default=CONFIG_DIR / "gpipe.json",
        help="配置文件路径或简写（若无扩展名，则会在 configs/ 下补 .json）",
    )
    parser.add_argument(
        "--list-configs",
        action="store_true",
        help="列出内置 configs 目录中的所有示例并退出",
    )
    parser.add_argument(
        "--time-list",
        type=str,
        default=None,
        help="逗号分隔的时间列表或文件路径，用来推算 stage_durations",
    )
    parser.add_argument(
        "--stage-indices",
        type=str,
        default=None,
        help="逗号分隔的下标列表，对应 time_list 中各 stage 的定位，缺省为 0..S-1",
    )
    parser.add_argument(
        "--output-ndjson",
        type=Path,
        default=Path("out.ndjson"),
        help="ndjson 输出文件",
    )
    parser.add_argument(
        "--output-plot",
        type=Path,
        default=Path("pp_gantt.png"),
        help="甘特图输出文件",
    )
    parser.add_argument(
        "--no-plot",
        action="store_true",
        help="仅生成 ndjson，不绘制图像",
    )
    args = parser.parse_args()

    if args.list_configs:
        if not CONFIG_DIR.exists():
            print("未找到 configs 目录")
            return
        files = sorted(CONFIG_DIR.glob("*.json"))
        if not files:
            print("configs 目录为空")
            return
        print("可用配置：")
        for p in files:
            print(f"- {p.name}")
        return

    cfg_path = _resolve_config(args.config)
    cfg = build_cfg(cfg_path, args.time_list, args.stage_indices)

    spec = ScheduleSpec(cfg)
    events = build(spec)
    args.output_ndjson.parent.mkdir(parents=True, exist_ok=True)
    args.output_plot.parent.mkdir(parents=True, exist_ok=True)

    with open(args.output_ndjson, "w", encoding="utf-8") as f:
        write_ndjson(f, events, include_comments=spec.emit.get("include_comments", True))
    print(f"✅ ndjson 已生成: {args.output_ndjson} ({len(events)} events)")

    if not args.no_plot:
        plot_pp_gantt(events, output_file=str(args.output_plot))
        print(f"✅ 甘特图已保存: {args.output_plot}")


if __name__ == "__main__":
    main()

