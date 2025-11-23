"""Caption trajectories in parallel with limit control."""

import os
import glob
import json
import shutil
import multiprocessing
from functools import partial
from pathlib import Path

import hydra
import numpy as np
from omegaconf import DictConfig
import torch
from tqdm import tqdm

from helper.files import save_txt
# from helper.progress import PROGRESS # Multiprocessing 建議改用 tqdm
from processing.captioning import caption_trajectories
from processing.segmentation import segment_rigidbody_trajectories

# ------------------------------------------------------------------------------------- #


def default_serializer(obj):
    if isinstance(obj, np.int64):
        return int(obj)
    raise TypeError(f"Type {type(obj)} not serializable")


def process_single_trajectory(traj_path, config, output_dir):
    """
    單一軌跡處理函數 (Worker Function)
    """
    try:
        # 1. 計算路徑與名稱
        traj_path_str = str(traj_path)
        parts = traj_path_str.split("/")
        # 假設路徑結構為 .../ClipID/shotID_transforms_cleaning.json
        # traj_name 變成 ClipID/shotID
        traj_name = f"{parts[-2]}/{parts[-1].replace('_transforms_cleaning.json', '')}"

        cam_segment_path = output_dir / "cam_segments" / (traj_name + "_tag.json")
        caption_path = output_dir / "cam_segments" / (traj_name + "_caption.txt")

        # 2. 檢查輸出是否存在 (Double check，防止 race condition)
        if os.path.exists(caption_path) and not config.overwrite:
            return  # Skip
        print(f"[{os.getpid()}] Processing: {traj_name}")
        # 建立父目錄
        cam_segment_path.parent.mkdir(parents=True, exist_ok=True)

        # 3. 載入資料
        traj = []
        with open(traj_path, "r") as f:
            data = json.load(f)["frames"]
        for frame in data:
            traj.append(frame["transform_matrix"])
        traj = torch.tensor(traj)

        # 4. Segmentation (CPU 計算)
        cam_segments = segment_rigidbody_trajectories(
            traj,
            cam_static_threshold=config.cam.static_threshold,
            cam_diff_threshold=config.cam.diff_threshold,
            angular_static_threshold=config.cam.angular_static_threshold,
            fps=config.fps,
            min_chunk_size=config.min_chunk_size,
            smoothing_window_size=config.smoothing_window_size,
        )

        with open(cam_segment_path, "w") as f:
            json.dump(cam_segments, f, default=default_serializer)

        # 5. Captioning (API 呼叫)
        caption = caption_trajectories(
            cam_segments=cam_segments,
            context_prompt=config.llm.context,
            instruction_prompt=config.llm.instruction,
            constraint_prompt=config.llm.constraint,
            demonstration_prompt=config.llm.demonstration,
        )
        save_txt(caption, caption_path)

    except Exception as e:
        print(f"\n[Error] Failed to process {traj_path}: {e}")

# ------------------------------------------------------------------------------------- #


@hydra.main(
    version_base=None,
    config_path="./configs/captioning/",
    config_name="caption_cam+char.yaml",
)
def launch_captioning(config: DictConfig):
    print("Configuration:\n", config)
    data_dir = Path(config.data_dir)
    output_dir = Path(config.output_dir)

    # 1. 搜尋所有檔案
    pattern = "*/*_transforms_cleaning.json"
    print(f"Scanning {data_dir} for {pattern}...")
    traj_paths = glob.glob(os.path.join(data_dir, pattern))
    traj_paths = sorted(traj_paths, reverse=config.reverse)
    print(f"Total trajectories found: {len(traj_paths)}")

    # 2. 過濾掉已經處理過的檔案 (Smart Filter)
    # 這樣如果不小心斷掉，下次跑的時候 limit=100 還是會跑 100 個新的，而不是跑過的
    if not config.overwrite:
        pending_paths = []
        print("Filtering processed files...")
        for p in traj_paths:
            parts = p.split("/")
            traj_name = f"{parts[-2]}/{parts[-1].replace('_transforms_cleaning.json', '')}"
            caption_path = output_dir / "cam_segments" / (traj_name + "_caption.txt")
            if not os.path.exists(caption_path):
                pending_paths.append(p)

        traj_paths = pending_paths
        print(f"Trajectories remaining to process: {len(traj_paths)}")

    # 3. 應用數量限制 (Limit)
    # 你可以透過命令列傳入 limit，例如 python script.py limit=100
    limit = config.get("limit", None)  # 預設 None 表示跑全部
    if limit is not None:
        limit = int(limit)
        traj_paths = traj_paths[:limit]
        print(f"--- Limit applied: Processing top {limit} trajectories ---")

    if len(traj_paths) == 0:
        print("No files to process.")
        return

    # 4. 設定並行 Worker 數量
    # 如果 config 裡有 num_workers 就用，沒有就預設 CPU 核心數 - 24 (保留資源)
    num_workers = 24
    # 注意：因為這裡會呼叫 OpenAI API，如果設太大 (如 >10)，可能會觸發 429 Rate Limit
    # 建議設在 4 ~ 8 之間
    if num_workers > 8:
        print(f"Warning: High worker count ({num_workers}) might hit OpenAI API rate limits.")

    print(f"Starting parallel processing with {num_workers} workers...")

    # 5. 開始並行處理
    # 使用 partial 固定 config 和 output_dir 參數
    worker_func = partial(process_single_trajectory, config=config, output_dir=output_dir)

    with multiprocessing.Pool(num_workers) as pool:
        # 使用 tqdm 顯示進度條
        list(tqdm(pool.imap_unordered(worker_func, traj_paths),
                  total=len(traj_paths),
                  desc="Captioning Trajectories"))

    print("\nBatch processing completed.")


if __name__ == "__main__":
    launch_captioning()
