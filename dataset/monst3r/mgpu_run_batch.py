import os
import json
import random
import argparse
import multiprocessing as mp
from functools import partial


def process_clip_on_gpu(clip, input_dir, output_dir, gpu_id, start, end):
    """在指定GPU上處理單個clip"""
    clip_path = os.path.join(input_dir, clip)
    output_path = os.path.join(output_dir, clip)
    check_path = os.path.join(output_path, "NULL")

    if os.path.exists(check_path):
        print(f"[GPU {gpu_id}] Skip {check_path}")
        return

    os.makedirs(output_path, exist_ok=True)

    # 設置CUDA_VISIBLE_DEVICES來指定GPU
    env = os.environ.copy()
    env['CUDA_VISIBLE_DEVICES'] = str(gpu_id)

    cmd = f"python run_single.py --input_dir {clip_path} --output_dir {output_path} --range {start},{end}"
    print(f"[GPU {gpu_id}] {cmd}")
    os.system(cmd)


def main():
    parser = argparse.ArgumentParser(description="Process a range of numbers.")
    parser.add_argument('--range', type=str, help='Range of values in the form start,end')
    parser.add_argument('--num_gpus', type=int, default=8, help='Number of GPUs to use')
    args = parser.parse_args()

    if args.range:
        start, end = map(int, args.range.split(','))
        print(f"Start: {start}, End: {end}")

    input_dir = "../DATA/Images"
    output_dir = "../DATA/Monst3r"
    os.makedirs(output_dir, exist_ok=True)

    # 收集所有clips
    clip_list = []
    for video in os.listdir(input_dir):
        video_path = os.path.join(input_dir, video)
        for shot in os.listdir(video_path):
            shot_path = os.path.join(video_path, shot)
            clip_list.append(f"{video}/{shot}")

    clip_list = sorted(clip_list)
    clip_list = clip_list[start:end:1]
    random.shuffle(clip_list)

    print(f"Total clips: {len(clip_list)}")
    print(f"Using {args.num_gpus} GPUs")

    # 為每個clip分配GPU ID
    clips_with_gpu = [(clip, i % args.num_gpus) for i, clip in enumerate(clip_list)]

    # 使用進程池並行處理
    process_func = partial(
        process_clip_on_gpu,
        input_dir=input_dir,
        output_dir=output_dir,
        start=start,
        end=end
    )

    with mp.Pool(processes=args.num_gpus) as pool:
        pool.starmap(lambda clip, gpu_id: process_clip_on_gpu(
            clip, input_dir, output_dir, gpu_id, start, end
        ), clips_with_gpu)


if __name__ == '__main__':
    main()
