import os
import json
import random
import argparse
import multiprocessing as mp


def process_clip_on_gpu(args_tuple):
    """在指定GPU上處理單個clip"""
    clip, gpu_id, input_dir, output_dir, start, end = args_tuple

    clip_path = os.path.join(input_dir, clip)
    output_path = os.path.join(output_dir, clip)
    check_path = os.path.join(output_path, "NULL")

    if os.path.exists(check_path):
        print(f"[GPU {gpu_id}] Skip {check_path}")
        return f"Skipped: {clip}"

    os.makedirs(output_path, exist_ok=True)

    # 設置CUDA_VISIBLE_DEVICES來指定GPU
    env = os.environ.copy()
    env['CUDA_VISIBLE_DEVICES'] = str(gpu_id)

    cmd = f"CUDA_VISIBLE_DEVICES={gpu_id} python run_single.py --input_dir {clip_path} --output_dir {output_path} --range {start},{end}"
    print(f"[GPU {gpu_id}] {cmd}")

    result = os.system(cmd)

    if result == 0:
        return f"Completed: {clip} on GPU {gpu_id}"
    else:
        return f"Failed: {clip} on GPU {gpu_id}"


def main():
    parser = argparse.ArgumentParser(description="Process a range of numbers.")
    parser.add_argument('--range', type=str, help='Range of values in the form start,end')
    parser.add_argument('--num_gpus', type=int, default=8, help='Number of GPUs to use')
    args = parser.parse_args()

    start, end = 0, None
    if args.range:
        start, end = map(int, args.range.split(','))
        print(f"Start: {start}, End: {end}")

    input_dir = "../DATA/Images"
    output_dir = "../DATA/Monst3r"
    os.makedirs(output_dir, exist_ok=True)

    # 收集所有clips
    clip_list = []
    for video in sorted(os.listdir(input_dir)):
        video_path = os.path.join(input_dir, video)
        if not os.path.isdir(video_path):
            continue
        for shot in sorted(os.listdir(video_path)):
            shot_path = os.path.join(video_path, shot)
            if os.path.isdir(shot_path):
                clip_list.append(f"{video}/{shot}")

    clip_list = sorted(clip_list)
    if end is not None:
        clip_list = clip_list[start:end]
    else:
        clip_list = clip_list[start:]

    random.shuffle(clip_list)

    print(f"Total clips: {len(clip_list)}")
    print(f"Using {args.num_gpus} GPUs")

    # 為每個clip準備參數
    tasks = []
    for i, clip in enumerate(clip_list):
        gpu_id = i % args.num_gpus
        tasks.append((clip, gpu_id, input_dir, output_dir, start, end if end else len(clip_list)))

    # 使用進程池並行處理
    print(f"Starting parallel processing with {args.num_gpus} workers...")
    with mp.Pool(processes=args.num_gpus) as pool:
        results = pool.map(process_clip_on_gpu, tasks)

    # 打印結果統計
    print("\n" + "="*50)
    print("Processing Summary:")
    print("="*50)
    completed = sum(1 for r in results if r.startswith("Completed"))
    skipped = sum(1 for r in results if r.startswith("Skipped"))
    failed = sum(1 for r in results if r.startswith("Failed"))

    print(f"Completed: {completed}")
    print(f"Skipped: {skipped}")
    print(f"Failed: {failed}")
    print(f"Total: {len(results)}")


if __name__ == '__main__':
    # 設置multiprocessing的啟動方法
    mp.set_start_method('spawn', force=True)
    main()
