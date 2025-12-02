# -*- coding: utf-8 -*-
"""
Step 1: DataDoP Dataset NPY Intermediate Format Generation

This script converts the DataDoP dataset into intermediate .npy format for RLDS conversion.
Unlike ET-VLA which processes videos, DataDoP uses cleaned trajectory JSON files with
120 interpolated camera poses.

Author: Gemini (Adapted from ET-VLA conversion)
Date: 2025-12-03

Usage:
    # Full conversion with train/val/test splits (80/10/10)
    python generate_datadop_npy.py --data_root ../../DATA --output_dir ../../DATA/RLDS/datadop-npy/splits --val_split 0.1 --test_split 0.1 --seed 42
    
    # Test mode with 20 random samples
    python generate_datadop_npy.py --data_root ../../DATA --output_dir ../../DATA/RLDS/datadop-npy-test/splits --test_samples 20 --seed 42
"""

from scipy.spatial.transform import Rotation
from tqdm import tqdm
import cv2
import numpy as np
import os
import json
import argparse
import random
import shutil
from pathlib import Path

# Set random seed for reproducibility
RANDOM_SEED = 42


# === Helper Functions ===


def matrix_to_pos_quat(matrix):
    """Convert 4x4 pose matrix to 7D vector [pos, quat(xyzw)]"""
    pos = matrix[:3, 3]
    # Scipy quat is [x, y, z, w]
    quat = Rotation.from_matrix(matrix[:3, :3]).as_quat()
    return np.concatenate([pos, quat]).astype(np.float32)


def calculate_relative_pose_7d(pose_start, pose_end):
    """
    Calculate relative transformation from pose_start to pose_end as 7D action.
    Returns: [dx, dy, dz, d_roll, d_pitch, d_yaw, gripper_placeholder]
    """
    T_start_inv = np.linalg.inv(pose_start)
    T_delta = T_start_inv @ pose_end

    translation_delta = T_delta[:3, 3]
    # 'xyz' euler angles in radians
    euler_delta = Rotation.from_matrix(T_delta[:3, :3]).as_euler('xyz', degrees=False)

    # Add 7th dimension as placeholder (for gripper/zoom)
    gripper_placeholder = np.array([-1.0], dtype=np.float32)

    return np.concatenate([translation_delta, euler_delta, gripper_placeholder]).astype(np.float32)


def find_valid_samples(data_root, test_samples=None):
    """
    Scan DataDoP dataset and find all valid samples with complete data.

    Args:
        data_root: Path to DataDoP root directory (should contain Monst3r/ and Dataset/)
        test_samples: If provided, limit to this many random samples for testing

    Returns:
        List of valid sample dictionaries
    """
    print(">>> Step 1/3: Scanning for valid DataDoP samples...")

    monst3r_dir = data_root / 'Monst3r'
    dataset_dir = data_root / 'Dataset'

    if not monst3r_dir.exists():
        print(f"Error: Monst3r directory not found at {monst3r_dir}")
        return []

    if not dataset_dir.exists():
        print(f"Error: Dataset directory not found at {dataset_dir}")
        return []

    valid_samples = []
    scan_limit = test_samples * 20 if test_samples else None

    # Get all video directories in Monst3r
    video_dirs = sorted([d for d in monst3r_dir.iterdir() if d.is_dir()])

    with tqdm(total=scan_limit if scan_limit else None, desc="Scanning samples", unit="samples") as pbar:
        for video_dir in video_dirs:
            video_id = video_dir.name

            # Look for shot directories
            shot_dirs = sorted([d for d in video_dir.iterdir() if d.is_dir() and d.name.startswith('shot_')])

            for shot_dir in shot_dirs:
                shot_id = shot_dir.name
                sample_name = f"{video_id}/{shot_id}"

                # Check for Monst3r NULL directory with frames
                null_dir = shot_dir / 'NULL'
                if not null_dir.exists():
                    continue

                # Check for required files in Dataset directory
                dataset_sample_dir = dataset_dir / video_id
                if not dataset_sample_dir.exists():
                    continue

                transforms_path = dataset_sample_dir / f"{shot_id}_transforms_cleaning.json"
                caption_path = dataset_sample_dir / f"{shot_id}_caption.json"

                if not (transforms_path.exists() and caption_path.exists()):
                    continue

                # Load caption to create instruction
                try:
                    with open(caption_path, 'r', encoding='utf-8') as f:
                        caption_data = json.load(f)

                    # Use "Concise Interaction" as primary instruction
                    if 'Concise Interaction' in caption_data:
                        instruction = caption_data['Concise Interaction']
                    elif 'Movement' in caption_data:
                        instruction = caption_data['Movement']
                    else:
                        continue

                    valid_samples.append({
                        'id': sample_name,
                        'instruction': instruction,
                        'transforms_path': transforms_path,
                        'frames_dir': null_dir,
                        'caption_data': caption_data
                    })

                    if pbar:
                        pbar.update(1)

                    if scan_limit and len(valid_samples) >= scan_limit:
                        if pbar.total is not None and pbar.n < pbar.total:
                            pbar.update(pbar.total - pbar.n)
                        print(f"\n>>> Step 1/3 Complete! Scanned {len(valid_samples)} samples.")
                        return valid_samples

                except (json.JSONDecodeError, KeyError) as e:
                    continue

    print(f"\n>>> Step 1/3 Complete! Found {len(valid_samples)} valid samples.")
    return valid_samples


def process_single_sample(sample, output_path):
    """
    Process a single DataDoP sample and save as .npy file.
    Each video frame is paired with its corresponding trajectory action.

    Args:
        sample: Dictionary containing sample information
        output_path: Path to save the .npy file
    """
    try:
        # Load trajectory data
        with open(sample['transforms_path'], 'r') as f:
            transforms_data = json.load(f)

        frames_data = transforms_data['frames']
        if len(frames_data) < 2:
            return

        # Get list of frame images from Monst3r NULL directory
        frames_dir = sample['frames_dir']
        frame_files = sorted(frames_dir.glob('frame_*.png'))

        if len(frame_files) == 0:
            print(f"Warning: No frame images found in {frames_dir}")
            return

        # Match number of frames with trajectory length
        # Trajectory has 120 frames, but video might have different number
        # We'll use the minimum of the two
        num_frames = min(len(frame_files), len(frames_data))

        if num_frames < 2:
            return

        # Process each frame with its corresponding trajectory
        episode_steps = []
        for t in range(num_frames - 1):
            # Load the specific frame image
            frame_path = frame_files[t]
            frame_image = cv2.imread(str(frame_path))

            if frame_image is None:
                print(f"Warning: Could not load frame {frame_path}")
                continue

            # Convert BGR to RGB and resize
            frame_rgb = cv2.cvtColor(frame_image, cv2.COLOR_BGR2RGB)
            resized_frame = cv2.resize(frame_rgb, (224, 224), interpolation=cv2.INTER_AREA)

            # Get transformation matrices for current and next frame
            transform_t = np.array(frames_data[t]['transform_matrix'])
            transform_t_next = np.array(frames_data[t + 1]['transform_matrix'])

            # Convert to state (pos + quat)
            state_t = matrix_to_pos_quat(transform_t)

            # Calculate relative action (what to do from current frame to reach next)
            action_t = calculate_relative_pose_7d(transform_t, transform_t_next)

            episode_steps.append({
                'image': resized_frame,  # Individual frame for each timestep
                'state': state_t,
                'action': action_t,
                'language_instruction': sample['instruction'],
                'caption_data': sample['caption_data']
            })

        if len(episode_steps) == 0:
            print(f"Warning: No valid steps generated for {sample['id']}")
            return

        # Save as .npy file
        np.save(output_path, episode_steps, allow_pickle=True)

    except Exception as e:
        print(f"Error processing {sample['id']}: {e}")


def main(args):
    """Main execution function"""
    # Set random seed for reproducibility
    random.seed(args.seed)
    print(f"Random seed set to: {args.seed}")

    data_root = Path(args.data_root)
    output_dir = Path(args.output_dir)

    # Find valid samples
    sample_pool = find_valid_samples(data_root, args.test_samples)
    if not sample_pool:
        print("No valid samples found. Exiting.")
        return

    print(f"\n>>> Step 2/3: Preparing output directory and sample splits...")
    if output_dir.exists():
        print(f"  Warning: Output directory {output_dir} exists, will be removed.")
        shutil.rmtree(output_dir)

    os.makedirs(output_dir, exist_ok=True)

    # Shuffle samples with fixed seed for reproducibility
    random.shuffle(sample_pool)

    # Create splits
    if args.test_samples:
        print(f"  Test mode: Randomly selecting {args.test_samples} samples from pool of {len(sample_pool)}.")
        train_dir = output_dir / 'train'
        val_dir = output_dir / 'val'
        test_dir = output_dir / 'test'
        train_dir.mkdir(exist_ok=True)
        val_dir.mkdir(exist_ok=True)
        test_dir.mkdir(exist_ok=True)

        # 60/20/20 ratio for train:val:test in test mode
        total_samples = min(args.test_samples, len(sample_pool))
        train_count = int(total_samples * 0.6)
        val_count = int(total_samples * 0.2)
        test_count = total_samples - train_count - val_count

        train_samples = sample_pool[:train_count]
        val_samples = sample_pool[train_count:train_count + val_count]
        test_samples = sample_pool[train_count + val_count:total_samples]

        splits = {
            'train': (train_samples, train_dir),
            'val': (val_samples, val_dir),
            'test': (test_samples, test_dir)
        }
        print(f"  Train: {len(train_samples)}, Val: {len(val_samples)}, Test: {len(test_samples)}")

    else:
        print(f"  Full conversion mode - Val split: {args.val_split}, Test split: {args.test_split}")
        train_dir = output_dir / 'train'
        val_dir = output_dir / 'val'
        test_dir = output_dir / 'test'
        train_dir.mkdir(exist_ok=True)
        val_dir.mkdir(exist_ok=True)
        test_dir.mkdir(exist_ok=True)

        # Calculate split indices
        total_samples = len(sample_pool)
        test_count = int(total_samples * args.test_split)
        val_count = int(total_samples * args.val_split)
        train_count = total_samples - test_count - val_count

        # Split: train / val / test
        train_samples = sample_pool[:train_count]
        val_samples = sample_pool[train_count:train_count + val_count]
        test_samples = sample_pool[train_count + val_count:]

        splits = {
            'train': (train_samples, train_dir),
            'val': (val_samples, val_dir),
            'test': (test_samples, test_dir)
        }
        print(f"  Train: {len(train_samples)} ({train_count/total_samples*100:.1f}%)")
        print(f"  Val: {len(val_samples)} ({val_count/total_samples*100:.1f}%)")
        print(f"  Test: {len(test_samples)} ({test_count/total_samples*100:.1f}%)")

    print(f"\n>>> Step 3/3: Converting to .npy format...")
    for split_name, (samples, split_dir) in splits.items():
        for sample in tqdm(samples, desc=f"Processing {split_name} set"):
            # Create safe filename from sample ID
            safe_id = sample['id'].replace('/', '_')
            output_path = split_dir / f"{safe_id}.npy"
            process_single_sample(sample, output_path)

    print(f"\n>>> All done! Intermediate .npy files saved to: {output_dir}")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Step 1: Convert DataDoP dataset to intermediate .npy format.")
    parser.add_argument('--data_root', type=str, required=True, help='Path to DataDoP root directory.')
    parser.add_argument('--output_dir', type=str, required=True, help='Output directory for .npy files.')
    parser.add_argument('--val_split', type=float, default=0.1, help='Validation split ratio (e.g., 0.1 for 10%).')
    parser.add_argument('--test_split', type=float, default=0.1, help='Test split ratio (e.g., 0.1 for 10%).')
    parser.add_argument('--test_samples', type=int, help='Test mode: process only N random samples.')
    parser.add_argument('--seed', type=int, default=42, help='Random seed for reproducibility (default: 42).')

    args = parser.parse_args()
    main(args)
