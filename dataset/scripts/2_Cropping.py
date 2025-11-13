import os
import shutil
import subprocess
import pandas as pd

from collections import Counter
from concurrent.futures import ProcessPoolExecutor, as_completed
from functools import partial


def get_most_common_crop(crop_params_list):
    counter = Counter(crop_params_list)
    most_common_crop, count = counter.most_common(1)[0]
    return most_common_crop


def detect_black_borders(video_path):
    command = [
        "ffmpeg",
        "-hide_banner",
        "-ss", "900",
        "-threads", "4",
        "-i", video_path,
        "-vf", "cropdetect",
        "-t", "60",
        "-an",
        "-f", "null",
        "-"
    ]
    result = subprocess.run(command, stderr=subprocess.PIPE, text=True)
    output = result.stderr

    crop_params_list = []
    for line in output.splitlines():
        if 'crop=' in line:
            crop_params = line.split('crop=')[1].split(' ')[0]
            crop_params_list.append(crop_params)
    crop_params = get_most_common_crop(crop_params_list)
    return crop_params


def crop_video(input_video, output_video, crop_params):
    crop_values = crop_params.split(':')
    width = crop_values[0]
    height = crop_values[1]
    x = crop_values[2]
    y = crop_values[3]

    command = [
        'ffmpeg',
        '-hide_banner',
        '-y',
        "-threads", "4",
        '-i', input_video,
        '-vf', f'crop={width}:{height}:{x}:{y}',
        '-c:a', 'copy',
        output_video
    ]
    subprocess.run(command)


def get_CropSize_worker(args):
    # worker 僅負責單一 clip 的偵測與回傳結果，不改變原本邏輯
    clip_id, data_root = args
    video_path = os.path.join(data_root, f"{clip_id.split('/')[0]}.mp4")

    if not os.path.exists(video_path):
        return (clip_id, None, f"Video file {video_path} does not exist, skipping crop detection.")

    try:
        crop_params = detect_black_borders(video_path)
        if crop_params:
            msg = f"Detected crop parameters for {clip_id}: {crop_params}"
            return (clip_id, crop_params, msg)
        else:
            msg = f"No black borders detected for {clip_id}."
            return (clip_id, None, msg)
    except Exception as e:
        return (clip_id, None, f"Error detecting {clip_id}: {e}")


def get_CropSize(max_workers=4):
    data_root = "./DATA/raw"
    metadata = "./metadata.csv"

    df = pd.read_csv(metadata)

    clip_ids = df['ClipID'].tolist()
    # 準備需要偵測的清單（維持你原本「若已存在就跳過」的邏輯）
    tasks = []
    for clip_id in clip_ids:
        if pd.notna(df.loc[df['ClipID'] == clip_id, 'CropSize'].values[0]):
            print(f"CropSize already exists for {clip_id}, skipping...")
            continue
        video_path = os.path.join(data_root, f"{clip_id.split('/')[0]}.mp4")
        if not os.path.exists(video_path):
            print(f"Video file {video_path} does not exist, skipping crop detection.")
            continue
        tasks.append((clip_id, data_root))

    # 平行偵測
    if tasks:
        with ProcessPoolExecutor(max_workers=max_workers) as ex:
            futures = {ex.submit(get_CropSize_worker, t): t[0] for t in tasks}
            for fut in as_completed(futures):
                clip_id = futures[fut]
                try:
                    cid, crop_params, msg = fut.result()
                    print(msg)
                    if cid == clip_id:
                        if crop_params:
                            df.loc[df['ClipID'] == clip_id, 'CropSize'] = crop_params
                        else:
                            df.loc[df['ClipID'] == clip_id, 'CropSize'] = None
                except Exception as e:
                    print(f"Error on {clip_id}: {e}")

    df.to_csv(metadata, index=False)
    print(f"Updated metadata has been saved to '{metadata}'.")


def Remove_black_borders_worker(args):
    # worker 僅負責單一 clip 的裁切/搬移與回傳結果，不改變原本邏輯
    clip_id, data_root, crop_root, crop_params = args
    video_path = os.path.join(data_root, f"{clip_id.split('/')[0]}.mp4")
    crop_path = os.path.join(crop_root, f"{clip_id.split('/')[0]}.mp4")

    if not os.path.exists(video_path):
        return f"Video file {video_path} does not exist, skipping crop operation."

    if os.path.exists(crop_path):
        return f"Crop file {crop_path} already exists, skipping crop operation."

    w, h, x, y = crop_params.split(':')
    if int(x) + int(y) <= 6:
        msg = f"No significant black borders detected for {clip_id}, skipping crop operation."
        try:
            shutil.move(video_path, crop_path)
        except Exception as e:
            return f"{msg} But move failed: {e}"
        return msg

    try:
        crop_video(video_path, crop_path, crop_params)
        return f"Cropped {clip_id} with {crop_params}"
    except Exception as e:
        return f"Error cropping {clip_id}: {e}"


def Remove_black_borders(max_workers=4):
    data_root = "./DATA/raw"
    crop_root = "./DATA/crop"
    os.makedirs(crop_root, exist_ok=True)
    metadata = "./metadata.csv"

    df = pd.read_csv(metadata)
    clip_ids = df['ClipID'].tolist()

    # 收集需處理的任務（保留你的原始判斷與輸出文字）
    tasks = []
    for clip_id in clip_ids:
        video_path = os.path.join(data_root, f"{clip_id.split('/')[0]}.mp4")
        print("Starting processing", video_path)
        if not os.path.exists(video_path):
            print(f"Video file {video_path} does not exist, skipping crop operation.")
            continue
        crop_path = os.path.join(crop_root, f"{clip_id.split('/')[0]}.mp4")
        if os.path.exists(crop_path):
            print(f"Crop file {crop_path} already exists, skipping crop operation.")
            continue
        crop_params = df.loc[df['ClipID'] == clip_id, 'CropSize'].values[0]
        # 按你原始邏輯直接進入 worker，並在 worker 內做一樣的判斷與行為
        tasks.append((clip_id, data_root, crop_root, crop_params))

    # 平行處理
    if tasks:
        with ProcessPoolExecutor(max_workers=max_workers) as ex:
            futures = {ex.submit(Remove_black_borders_worker, t): t[0] for t in tasks}
            for fut in as_completed(futures):
                clip_id = futures[fut]
                try:
                    msg = fut.result()
                    print(msg)
                except Exception as e:
                    print(f"Error on {clip_id}: {e}")


if __name__ == "__main__":
    '''Detect black borders, store CropSize in metadata.csv'''
    # get_CropSize(max_workers=4)

    '''Remove black borders from videos based on metadata.csv, store shots in DATA/crop'''
    Remove_black_borders(max_workers=32)
