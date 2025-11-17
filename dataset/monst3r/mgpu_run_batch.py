import os
import json
import random
import argparse
import multiprocessing as mp
import subprocess
import torch
import time


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

    # 使用環境變量讓進程只看到一個GPU
    env = os.environ.copy()
    env['CUDA_VISIBLE_DEVICES'] = str(gpu_id)
    # 限制CPU線程數，減少CPU負載
    env['OMP_NUM_THREADS'] = '24'
    env['MKL_NUM_THREADS'] = '24'
    env['NUMEXPR_NUM_THREADS'] = '24'

    cmd = [
        'python', 'run_single.py',
        '--input_dir', clip_path,
        '--output_dir', output_path,
        '--range', f'{start},{end}',
        '--device', 'cuda:0'
    ]

    print(f"[GPU {gpu_id}] Processing {clip}")
    start_time = time.time()

    try:
        result = subprocess.run(
            cmd,
            capture_output=True,  # 改為True，減少輸出
            text=True,
            env=env
        )

        elapsed = time.time() - start_time

        if result.returncode == 0:
            print(f"[GPU {gpu_id}] ✓ Completed {clip} in {elapsed:.1f}s")
            return f"Completed: {clip} on GPU {gpu_id}"
        else:
            print(f"[GPU {gpu_id}] ✗ Failed {clip}")
            if result.stderr:
                print(f"Error: {result.stderr[:200]}")
            return f"Failed: {clip} on GPU {gpu_id} (exit code: {result.returncode})"
    except Exception as e:
        print(f"[GPU {gpu_id}] ✗ Error {clip}: {str(e)}")
        return f"Error: {clip} on GPU {gpu_id} - {str(e)}"


def worker_process(gpu_id, clips, input_dir, output_dir, start, end, result_queue):
    """每個GPU一個工作進程，順序處理分配給它的clips"""
    print(f"[GPU {gpu_id}] Worker started with {len(clips)} clips")
    results = []

    for i, clip in enumerate(clips, 1):
        print(f"\n[GPU {gpu_id}] === Progress: {i}/{len(clips)} ===")
        result = process_clip_on_gpu((clip, gpu_id, input_dir, output_dir, start, end))
        results.append(result)
        result_queue.put(result)

    print(f"[GPU {gpu_id}] Worker finished")
    return results


def main():
    parser = argparse.ArgumentParser(description="Process a range of numbers.")
    parser.add_argument('--range', type=str, help='Range of values in the form start,end')
    parser.add_argument('--num_gpus', type=int, default=None, help='Number of GPUs to use')
    parser.add_argument('--max_workers', type=int, default=None, help='Max concurrent workers (default: num_gpus)')
    args = parser.parse_args()

    # 自動檢測可用的GPU數量
    available_gpus = torch.cuda.device_count()
    if args.num_gpus is None:
        num_gpus = available_gpus
    else:
        num_gpus = min(args.num_gpus, available_gpus)

    # 限制最大並發數
    max_workers = args.max_workers if args.max_workers else num_gpus
    max_workers = min(max_workers, num_gpus)

    if num_gpus == 0:
        print("ERROR: No CUDA GPUs available!")
        return

    print(f"Available GPUs: {available_gpus}")
    print(f"Using GPUs: {num_gpus}")
    print(f"Max concurrent workers: {max_workers}")

    start, end = 0, None
    if args.range:
        start, end = map(int, args.range.split(','))
        print(f"Range: {start} to {end}")

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

    print(f"Total clips to process: {len(clip_list)}")

    # 將clips分配給各個GPU
    clips_per_gpu = [[] for _ in range(max_workers)]
    for i, clip in enumerate(clip_list):
        gpu_id = i % max_workers
        clips_per_gpu[gpu_id].append(clip)

    # 打印分配情況
    print("\nWork distribution:")
    for gpu_id in range(max_workers):
        print(f"  GPU {gpu_id}: {len(clips_per_gpu[gpu_id])} clips")

    # 創建結果隊列
    result_queue = mp.Queue()

    # 為每個GPU啟動一個進程
    processes = []
    start_time = time.time()

    for gpu_id in range(max_workers):
        if len(clips_per_gpu[gpu_id]) > 0:
            p = mp.Process(
                target=worker_process,
                args=(gpu_id, clips_per_gpu[gpu_id], input_dir, output_dir,
                      start, end if end else len(clip_list), result_queue)
            )
            p.start()
            processes.append(p)
            print(f"✓ Started worker for GPU {gpu_id}")

    print(f"\n{'='*60}")
    print("All workers started. Processing...")
    print(f"{'='*60}\n")

    # 等待所有進程完成
    for p in processes:
        p.join()

    total_time = time.time() - start_time

    # 收集結果
    results = []
    while not result_queue.empty():
        results.append(result_queue.get())

    # 打印結果統計
    print("\n" + "="*60)
    print("PROCESSING SUMMARY")
    print("="*60)
    completed = sum(1 for r in results if r.startswith("Completed"))
    skipped = sum(1 for r in results if r.startswith("Skipped"))
    failed = sum(1 for r in results if r.startswith("Failed"))

    print(f"Completed: {completed}")
    print(f"Skipped:   {skipped}")
    print(f"Failed:    {failed}")
    print(f"Total:     {len(results)}")
    print(f"Time:      {total_time/60:.1f} minutes")
    if completed > 0:
        print(f"Avg time:  {total_time/completed:.1f}s per clip")
    print("="*60)


if __name__ == '__main__':
    mp.set_start_method('spawn', force=True)
    main()
