# -*- coding: utf-8 -*-
"""
可视化 PP 调度 ndjson 的甘特图
- 每个 stage 在同一水平线上
- 不同 op 用不同颜色
- 标注 mini/micro 批次 (mnX mcY)
"""

import json
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from collections import defaultdict
from typing import Any, Dict

from schedule_ir.core.events import Event

# -------------- 可配置参数 --------------
NDJSON_FILE = "out.ndjson"  # 默认 ndjson 路径
OUTPUT_FILE = "pp_gantt.png"  # 默认图像路径
FIG_SIZE = (14, 6)  # 图尺寸
COLOR_MAP = {
    "F": "#1f77b4",        # 蓝色 Forward
    "B": "#ff7f0e",        # 橙色 Backward
    "Idle": "#d3d3d3",     # 灰色 空泡
    "Cast": "#2ca02c",     # 绿色 自定义算子
    "Send": "#9467bd",     # 紫色 通信
    "Recv": "#8c564b",     # 棕色 通信
    "AllReduce": "#e377c2",# 粉色 通信
    "Bx": "#17becf",            # 输入梯度
    "Bw": "#bcbd22",            # 权重梯度
    "SendActGrad": "#9467bd",   # P2P 发送
    "RecvActGrad": "#8c564b",   # P2P 接收
    "AllReduceGrad": "#e377c2", # 也为粉色，和allreduce通信一致

    # 其他未知 op 随机颜色
}
# --------------------------------------

def load_ndjson(file_path):
    """读取 ndjson 文件为事件列表"""
    events = []
    with open(file_path, "r", encoding="utf-8") as f:
        for line in f:
            if line.startswith("//") or not line.strip():
                continue
            events.append(json.loads(line))
    return events


def _as_mapping(ev: Any) -> Dict[str, Any]:
    """兼容 dict 或 Event 对象"""
    if isinstance(ev, Event):
        return ev.to_dict()
    if isinstance(ev, dict):
        return ev
    raise TypeError(f"Unsupported event type: {type(ev)}")


def plot_pp_gantt(events, output_file=OUTPUT_FILE, figsize=FIG_SIZE):
    """绘制甘特图"""
    events_map = [_as_mapping(e) for e in events]
    if len(events_map) == 0:
        raise ValueError("events 为空，无法绘制")

    # 按 stage 分组
    stages = sorted({e["stage"] for e in events_map})
    grouped = defaultdict(list)
    for e in events_map:
        grouped[e["stage"]].append(e)

    fig, ax = plt.subplots(figsize=figsize)

    legend_handles = {}
    y_ticks = []
    y_labels = []

    for i, stage in enumerate(stages):
        y = i  # 每个 stage 在一行
        y_ticks.append(y)
        y_labels.append(f"Stage {stage}")

        events_stage = sorted(grouped[stage], key=lambda x: x["start"])

        for ev in events_stage:
            op = ev["op"]
            color = COLOR_MAP.get(op, "#"+("%06x" % (hash(op) & 0xFFFFFF)))
            start = ev["start"]
            dur = ev["dur"]
            rect = plt.Rectangle((start, y - 0.4), dur, 0.8, color=color, alpha=0.8)
            ax.add_patch(rect)

            # 添加文本标签 mnX mcY
            mini = ev.get("mini")
            micro = ev.get("micro")
            if mini is not None and micro is not None and op not in ("Idle",):
                ax.text(
                    start + dur / 2,
                    y,
                    f"mn{mini} mc{micro}",
                    ha="center",
                    va="center",
                    fontsize=7,
                    color="white" if op != "Idle" else "black",
                )

            # 图例
            if op not in legend_handles:
                legend_handles[op] = mpatches.Patch(color=color, label=op)

    ax.set_yticks(y_ticks)
    ax.set_yticklabels(y_labels)
    ax.set_xlabel("Time (frames or μs)")
    ax.set_ylabel("Pipeline Stages")
    ax.set_title("Pipeline Parallel Gantt Chart")
    bar_half = 0.4  
    pad = bar_half + 0.2  
    ax.set_ylim(-pad, len(stages)-1 + pad)

    # 自动确定横轴范围
    all_end = max(e["start"] + e["dur"] for e in events_map)
    ax.set_xlim(0, all_end * 1.05)

    ax.legend(handles=list(legend_handles.values()), bbox_to_anchor=(1.05, 1), loc='upper left')
    ax.grid(True, axis="x", linestyle="--", alpha=0.3)

    plt.tight_layout()
    plt.savefig(output_file, dpi=300)
    print(f"✅ Gantt 图已保存到: {output_file}")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="将 ndjson 可视化为甘特图")
    parser.add_argument("--input", type=str, default=NDJSON_FILE, help="ndjson 输入路径")
    parser.add_argument("--output", type=str, default=OUTPUT_FILE, help="输出图片路径")
    args = parser.parse_args()

    events = load_ndjson(args.input)
    plot_pp_gantt(events, output_file=args.output)

