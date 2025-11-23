import os
import glob
import multiprocessing
from functools import partial
from tqdm import tqdm
from PIL import Image

# 嘗試從 Filtering.py 導入 get_images
# 請確保 Filtering.py 在同一目錄下，且 get_images 函數是可被 import 的
try:
    from Filtering import get_images
except ImportError:
    print("Error: Could not import 'get_images' from 'Filtering.py'.")
    print("Please ensure 'Filtering.py' is in the same directory.")
    exit(1)

# ==========================================
# Worker Function: 圖片合成邏輯 (從原本的 nested function 提取出來)
# ==========================================


def worker_combine_images(task_args):
    """
    用於並行合成圖片的 Worker 函數
    task_args: (shot_dir, output_path, all_width)
    """
    shot_dir, output_path, all_width = task_args

    try:
        # 找出該 Shot 資料夾下所有的 jpg
        images_names = sorted([f for f in os.listdir(shot_dir) if f.lower().endswith('.jpg')])

        # 如果圖片少於 16 張，無法合成 4x4
        if len(images_names) < 16:
            # print(f"Skipping {shot_dir}: Not enough images ({len(images_names)})")
            return

        # 均勻選取 16 張圖片
        selected_paths = []
        step = len(images_names) // 16
        for i in range(16):
            selected_paths.append(os.path.join(shot_dir, images_names[i * step]))

        # --- 開始合成 (原本 combine_images 的邏輯) ---
        images = [Image.open(p) for p in selected_paths]

        # 計算目標寬高
        single_width = all_width // 4
        aspect_ratios = [img.width / img.height for img in images]
        single_heights = [int(single_width / ar) for ar in aspect_ratios]

        # 計算每一列的高度 (取該列最大值)
        row_heights = [max(single_heights[i*4: (i+1)*4]) for i in range(4)]
        total_height = sum(row_heights)

        # 建立新畫布
        new_image = Image.new('RGB', (all_width, total_height))

        y_offset = 0
        for i in range(4):  # Row
            x_offset = 0
            row_max_height = row_heights[i]
            for j in range(4):  # Col
                idx = i * 4 + j
                # Resize
                resized_img = images[idx].resize((single_width, single_heights[idx]))
                # Paste
                new_image.paste(resized_img, (x_offset, y_offset))
                x_offset += single_width
            y_offset += row_max_height

        # 建立輸出目錄並存檔
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        new_image.save(output_path)

    except Exception as e:
        print(f"Error combining {shot_dir}: {e}")

# ==========================================
# Worker Function: 影片截圖 (Wrapper)
# ==========================================


def worker_extract_frames(shot_path):
    try:
        # get_images 本身會檢查是否已存在，並處理截圖
        get_images(shot_path)
    except Exception as e:
        print(f"Error extracting {shot_path}: {e}")

# ==========================================
# Main Process
# ==========================================


def main():
    # 設定並行核心數 (保留 1-2 個核心給系統，避免卡死)
    num_workers = max(1, os.cpu_count() - 24)
    print(f"Using {num_workers} workers for parallel processing.")

    # -------------------------------------------------
    # 步驟 1: 並行提取截圖 (Extract Frames)
    # -------------------------------------------------
    print("\n[Step 1/2] Scanning videos for frame extraction...")
    # data_dir = "./DATA/Shots"
    # video_tasks = []

    # 蒐集所有 mp4 檔案路徑
    # if os.path.exists(data_dir):
    #     for folder in sorted(os.listdir(data_dir)):
    #         folder_path = os.path.join(data_dir, folder)
    #         if not os.path.isdir(folder_path):
    #             continue

    #         for shot in sorted(os.listdir(folder_path)):
    #             shot_path = os.path.join(folder_path, shot)
    #             if shot_path.endswith('.mp4'):
    #                 video_tasks.append(shot_path)

    # print(f"Found {len(video_tasks)} videos. Starting extraction...")

    # # 開始並行處理
    # with multiprocessing.Pool(num_workers) as pool:
    #     # 使用 imap_unordered 配合 tqdm 顯示進度條
    #     list(tqdm(pool.imap_unordered(worker_extract_frames, video_tasks),
    #               total=len(video_tasks),
    #               desc="Extracting Frames"))

    # -------------------------------------------------
    # 步驟 2: 並行合成網格圖 (Combine Images)
    # -------------------------------------------------
    print("\n[Step 2/2] Scanning folders for image combination...")
    images_dir = "./DATA/Images"
    caption_dir = "./DATA/Captions"
    all_width = 900
    combine_tasks = []

    if os.path.exists(images_dir):
        for folder in sorted(os.listdir(images_dir)):
            folder_path = os.path.join(images_dir, folder)
            if not os.path.isdir(folder_path):
                continue

            for shot in sorted(os.listdir(folder_path)):
                shot_dir = os.path.join(folder_path, shot)
                if not os.path.isdir(shot_dir):
                    continue

                # 定義輸出路徑
                output_path = os.path.join(caption_dir, f"{folder}/{shot}.png")

                # 如果輸出圖已經存在，就跳過加入任務列表
                if os.path.exists(output_path):
                    continue

                combine_tasks.append((shot_dir, output_path, all_width))

    print(f"Found {len(combine_tasks)} shots needing combination. Starting processing...")

    # 開始並行處理
    with multiprocessing.Pool(num_workers) as pool:
        list(tqdm(pool.imap_unordered(worker_combine_images, combine_tasks),
                  total=len(combine_tasks),
                  desc="Combining Images"))

    print("\nAll done! Images are ready in ./DATA/Captions/")


if __name__ == "__main__":
    main()
