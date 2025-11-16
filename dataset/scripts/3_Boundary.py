import re
import os
import subprocess
import pandas as pd
from datetime import datetime
from concurrent.futures import ProcessPoolExecutor, as_completed
from functools import partial
import multiprocessing
from scenedetect import detect, AdaptiveDetector


def time_to_seconds(time_str):
    h, m, s = map(float, time_str.split(':'))
    return h * 3600 + m * 60 + s


def _detect_one_video(video_tuple, data_dir):
    """
    單支影片場景偵測（子進程執行）。
    保持與原本 BoundaryDetection 相同的篩選：10–20 秒。
    回傳 list[tuple]: (ClipID, YouTubeID, CropSize, StartTime, EndTime)
    """
    VideoID, YouTubeID, CropSize = video_tuple
    video_path = os.path.join(data_dir, f"{VideoID}.mp4")
    if not os.path.exists(video_path):
        return VideoID, [], f"Video file does not exist: {video_path}"

    try:
        scene_list = detect(video_path, AdaptiveDetector())
    except Exception as e:
        return VideoID, [], f"Scene detect failed: {VideoID}, err={e}"

    metalist = []
    pattern = r'(\d{2}:\d{2}:\d{2}\.\d{3})'
    for idx, scene in enumerate(scene_list):
        time_stamps = re.findall(pattern, str(scene))
        if len(time_stamps) < 2:
            continue

        start_time = time_stamps[0]
        end_time = time_stamps[1]

        start_seconds = time_to_seconds(start_time)
        end_seconds = time_to_seconds(end_time)
        seconds = end_seconds - start_seconds

        if seconds < 10 or seconds > 20:
            continue

        ShotID = f"shot_{idx:04d}"
        ClipID = VideoID.split('.')[0] + '/' + ShotID
        metalist.append((ClipID, YouTubeID, CropSize, start_time, end_time))

    return VideoID, metalist, None


def BoundaryDetection(max_workers=None):
    """
    並行化的 BoundaryDetection：
    - 多進程針對每支影片做場景偵測
    - 主程序最後一次性把所有結果 append 到 metadata.csv
    """
    data_dir = "./DATA/crop"
    metadata = "./metadata.csv"

    if os.path.exists(metadata):
        df = pd.read_csv(metadata)
    else:
        df = pd.DataFrame(columns=['ClipID', 'YouTubeID', 'CropSize', 'StartTime', 'EndTime'])

    ClipIDs = df['ClipID'].tolist()
    YouTubeIDs = df['YouTubeID'].tolist()
    CropSizes = df['CropSize'].tolist()

    clip_dict = {
        clip_id.split('/')[0]: {'YouTubeID': yt_id, 'CropSize': crop_size}
        for clip_id, yt_id, crop_size in zip(ClipIDs, YouTubeIDs, CropSizes)
    }

    # 如果 metadata 暫時是空的，則不會有任何 VideoID 要跑偵測（保持原行為）
    video_tuples = []
    for VideoID in sorted(clip_dict.keys()):
        video_tuples.append((VideoID, clip_dict[VideoID]['YouTubeID'], clip_dict[VideoID]['CropSize']))

    if not video_tuples:
        print("No videos to process for BoundaryDetection.")
        return

    if max_workers is None:
        # 堆疊場景偵測屬 CPU-bound；先以實體核心數做 baseline（簡化估計：cpu_count() // 2）
        cpu_cnt = os.cpu_count() or 8
        max_workers = max(4, cpu_cnt // 4)  # 保守一些，避免偵測器同步成本過高

    print(f"BoundaryDetection: using max_workers={max_workers}")
    all_results = []
    errors = []

    with ProcessPoolExecutor(max_workers=max_workers) as ex:
        futures = {ex.submit(_detect_one_video, vt, data_dir): vt[0] for vt in video_tuples}
        for fut in as_completed(futures):
            VideoID = futures[fut]
            try:
                vid, metalist, err = fut.result()
                if err:
                    print(err)
                if metalist:
                    all_results.extend(metalist)
                    print(f"Metadata for VideoID {VideoID} prepared: {len(metalist)} shots.")
                else:
                    print(f"No valid scenes found for VideoID {VideoID}.")
            except Exception as e:
                errors.append((VideoID, str(e)))
                print(f"Error processing {VideoID}: {e}")

    if all_results:
        new_data = pd.DataFrame(all_results, columns=['ClipID', 'YouTubeID', 'CropSize', 'StartTime', 'EndTime'])
        # 以 append 方式維持原有檔案格式
        header_needed = not os.path.exists(metadata) or (os.path.getsize(metadata) == 0 and len(df) == 0)
        new_data.to_csv(metadata, mode='a', header=header_needed, index=False)
        print(f"Appended {len(all_results)} rows to metadata.")
    else:
        print("No new metadata to append.")


def _ffmpeg_split_one(clip_id, st, et, ffmpeg_threads):
    """
    單段剪輯（子進程執行）。
    回傳 (clip_id, output_path, ok, msg)
    """
    video_id = clip_id.split('/')[0]
    shot_id = clip_id.split('/')[1]
    video_path = f"./DATA/crop/{video_id}.mp4"
    output_dir = f"./DATA/Shots/{video_id}"
    os.makedirs(output_dir, exist_ok=True)
    output_path = os.path.join(output_dir, f"{shot_id}.mp4")

    if os.path.exists(output_path):
        return clip_id, output_path, True, "exists"

    command = [
        'ffmpeg',
        '-y',                    # 覆寫輸出（以免殘檔造成互斥）
        '-threads', str(ffmpeg_threads),
        '-i', video_path,
        '-ss', str(st),
        '-to', str(et),
        '-c:v', 'libx264',
        '-c:a', 'aac',
        '-strict', 'experimental',
        output_path
    ]

    try:
        completed = subprocess.run(
            command,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=True,
            text=True
        )
        return clip_id, output_path, True, "ok"
    except subprocess.CalledProcessError as e:
        return clip_id, output_path, False, f"ffmpeg failed: rc={e.returncode}, stderr={e.stderr[-500:]}"


def BoundarySplit(max_workers=None, ffmpeg_threads=2, dry_run=False):
    """
    並行化的 BoundarySplit：
    - 以多進程平行呼叫 ffmpeg 進行分割/轉碼
    - ffmpeg_threads 預設為 2（配合你機器 224 邏輯執行緒，利於更高併發）
    - 可用 dry_run 測試不實際執行 ffmpeg
    """
    os.makedirs('./DATA/Shots', exist_ok=True)
    metadata = "./metadata.csv"
    if not os.path.exists(metadata):
        print("metadata.csv not found.")
        return

    df = pd.read_csv(metadata)
    ClipIDs = df['ClipID'].tolist()
    StartTimes = df['StartTime'].tolist()
    EndTimes = df['EndTime'].tolist()

    tasks = list(zip(ClipIDs, StartTimes, EndTimes))
    if not tasks:
        print("No tasks found in metadata.")
        return

    if max_workers is None:
        # 依照前面建議：threads=2 時，workers ≈ 96–112
        cpu_cnt = os.cpu_count() or 8
        # 估算：workers ≈ 邏輯執行緒 / ffmpeg_threads * 0.85
        est = int((cpu_cnt / max(1, ffmpeg_threads)) * 0.85)
        max_workers = max(4, est)

    print(f"BoundarySplit: using max_workers={max_workers}, ffmpeg_threads={ffmpeg_threads}, tasks={len(tasks)}")

    if dry_run:
        for clip_id, st, et in tasks[:10]:
            print(f"[DRY-RUN] {clip_id} {st}->{et}")
        return

    ok_cnt = 0
    skip_cnt = 0
    fail_cnt = 0

    worker = partial(_ffmpeg_split_one, ffmpeg_threads=ffmpeg_threads)
    with ProcessPoolExecutor(max_workers=max_workers) as ex:
        futures = {ex.submit(worker, clip_id, st, et): clip_id for clip_id, st, et in tasks}
        for fut in as_completed(futures):
            clip_id = futures[fut]
            try:
                cid, outp, ok, msg = fut.result()
                if ok and msg == "exists":
                    print(f"Shot already exists: {outp}, skipping...")
                    skip_cnt += 1
                elif ok:
                    print(f"Scene {cid}: Saved shot to {outp}")
                    ok_cnt += 1
                else:
                    print(f"Scene {cid}: FAILED -> {msg}")
                    fail_cnt += 1
            except Exception as e:
                print(f"Scene {clip_id}: FAILED -> {e}")
                fail_cnt += 1

    print(f"Done. ok={ok_cnt}, skip={skip_cnt}, fail={fail_cnt}")


if __name__ == "__main__":
    """
    Boundary Detection：保留 10–20 秒，資訊寫入 metadata.csv（並行）
    Boundary Split：依 metadata.csv 切出分鏡（並行）
    """
    # 並行跑偵測（必要時才開）
    # BoundaryDetection(max_workers=None)

    # 並行分割（建議先從 threads=2, max_workers=96 開始）
    BoundarySplit(max_workers=96, ffmpeg_threads=2)
