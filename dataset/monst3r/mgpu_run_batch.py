import os
import json
import random
import argparse
import multiprocessing as mp
import subprocess
import torch


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

    # 在子進程中，cuda:0 就是實際的 gpu_id
    cmd = [
        'python', 'run_single.py',
        '--input_dir', clip_path,
        '--output_dir', output_path,
        '--range', f'{start},{end}',
        '--device', 'cuda:0'  # 總是使用 cuda:0，因為只有一個可見GPU
    ]

    print(f"[GPU {gpu_id}] Processing {clip}")

    try:
        result = subprocess.run(
            cmd,
            capture_output=False,
            text=True,
            env=env  # 傳遞修改後的環境變量
        )

        if result.returncode == 0:
            return f"Completed: {clip} on GPU {gpu_id}"
        else:
            return f"Failed: {clip} on GPU {gpu_id} (exit code: {result.returncode})"
    except Exception as e:
        return f"Error: {clip} on GPU {gpu_id} - {str(e)}"


def worker_process(gpu_id, clips, input_dir, output_dir, start, end, result_queue):
    """每個GPU一個工作進程，順序處理分配給它的clips"""
    results = []
    for i, clip in enumerate(clips, 1):
        print(f"[GPU {gpu_id}] Progress: {i}/{len(clips)}")
        result = process_clip_on_gpu((clip, gpu_id, input_dir, output_dir, start, end))
        results.append(result)
        result_queue.put(result)
    return results


def main():
    parser = argparse.ArgumentParser(description="Process a range of numbers.")
    parser.add_argument('--range', type=str, help='Range of values in the form start,end')
    parser.add_argument('--num_gpus', type=int, default=None, help='Number of GPUs to use (default: all available)')
    args = parser.parse_args()

    # 自動檢測可用的GPU數量
    available_gpus = torch.cuda.device_count()
    if args.num_gpus is None:
        num_gpus = available_gpus
    else:
        num_gpus = min(args.num_gpus, available_gpus)

    if num_gpus == 0:
        print("ERROR: No CUDA GPUs available!")
        return

    print(f"Available GPUs: {available_gpus}")
    print(f"Using GPUs: {num_gpus}")

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

    # 將clips分配給各個GPU
    clips_per_gpu = [[] for _ in range(num_gpus)]
    for i, clip in enumerate(clip_list):
        gpu_id = i % num_gpus
        clips_per_gpu[gpu_id].append(clip)

    # 打印分配情況
    for gpu_id in range(num_gpus):
        print(f"GPU {gpu_id}: {len(clips_per_gpu[gpu_id])} clips")

    # 創建結果隊列
    result_queue = mp.Queue()

    # 為每個GPU啟動一個進程
    processes = []
    for gpu_id in range(num_gpus):
        if len(clips_per_gpu[gpu_id]) > 0:
            p = mp.Process(
                target=worker_process,
                args=(gpu_id, clips_per_gpu[gpu_id], input_dir, output_dir,
                      start, end if end else len(clip_list), result_queue)
            )
            p.start()
            processes.append(p)
            print(f"Started worker process for GPU {gpu_id}")

    # 等待所有進程完成
    for p in processes:
        p.join()

    # 收集結果
    results = []
    while not result_queue.empty():
        results.append(result_queue.get())

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
    mp.set_start_method('spawn', force=True)
    main()
