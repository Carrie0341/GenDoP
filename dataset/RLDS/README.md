# DataDoP to RLDS Conversion Guide

This guide explains how to convert the DataDoP dataset to RLDS (TFRecord) format for OpenVLA fine-tuning.

## Overview

The conversion process consists of two steps:

1. **Step 1**: Convert DataDoP data to intermediate `.npy` format
2. **Step 2**: Build TFDS dataset from `.npy` files to RLDS TFRecord format

## Prerequisites

```bash
pip install tensorflow-datasets numpy opencv-python scipy tqdm
```

## Data Structure

DataDoP dataset should be organized as:

```
DATA/
└── Dataset/
    └── <VideoID>/
        ├── shot_XXXX_rgb.png
        ├── shot_XXXX_depth.npy
        ├── shot_XXXX_intrinsics.txt
        ├── shot_XXXX_traj.txt
        ├── shot_XXXX_transforms_cleaning.json  # 120 interpolated camera poses
        └── shot_XXXX_caption.json              # Multi-level captions
```

## Step 1: Generate Intermediate NPY Files

### Test Mode (Recommended First)

Test with a small subset to verify the conversion works:

```bash
cd dataset/scripts
python generate_datadop_npy.py \
    --data_root ../DATA \
    --output_dir ../DATA/datadop-npy-test/splits \
    --test_samples 20
```

This will:

-   Randomly select 20 samples from the dataset
-   Split them 60/40 into train/val
-   Save `.npy` files to `../DATA/datadop-npy-test/splits/`

### Full Dataset Conversion

Once testing is successful, convert the full dataset:

```bash
python generate_datadop_npy.py \
    --data_root ../DATA \
    --output_dir ../DATA/datadop-npy/splits \
    --val_split 0.1
```

This will:

-   Process all valid DataDoP samples
-   Create 90/10 train/val split
-   Save `.npy` files to `../DATA/datadop-npy/splits/`

### Arguments

-   `--data_root`: Path to DataDoP root directory containing `Dataset/` folder
-   `--output_dir`: Output directory for intermediate `.npy` files
-   `--val_split`: Validation split ratio (default: 0.1 for 10%)
-   `--test_samples`: Optional, number of random samples for testing

## Step 2: Build TFDS Dataset

### Test Dataset

Build the test dataset first:

```bash
cd dataset/scripts
tfds build \
    --config=datadop_vla_test \
    --data_dir=../DATA/TFDS \
    --manual_dir=../DATA/datadop-npy-test/splits
```

### Full Dataset

Build the full dataset:

```bash
tfds build \
    --config=datadop_vla \
    --data_dir=../DATA/TFDS \
    --manual_dir=../DATA/datadop-npy/splits
```

### TFDS Arguments

-   `--config`: Dataset configuration (`datadop_vla` or `datadop_vla_test`)
-   `--data_dir`: Output directory for TFRecord files
-   `--manual_dir`: Input directory containing `.npy` files from Step 1

## Verification

### Verify NPY Files

```python
import numpy as np

# Load a sample .npy file
data = np.load('DATA/datadop-npy-test/splits/train/1_0000_shot_0070.npy', allow_pickle=True)

print(f"Number of timesteps: {len(data)}")
print(f"Image shape: {data[0]['image'].shape}")  # Should be (224, 224, 3)
print(f"State shape: {data[0]['state'].shape}")  # Should be (7,)
print(f"Action shape: {data[0]['action'].shape}")  # Should be (7,)
print(f"Instruction: {data[0]['language_instruction']}")
```

### Verify TFDS Dataset

```python
import tensorflow_datasets as tfds

# Load the dataset
ds = tfds.load('datadop_vla_test', data_dir='DATA/TFDS', split='train')

# Inspect a sample
for episode in ds.take(1):
    print("Episode keys:", episode.keys())
    print("Number of steps:", len(episode['steps']))

    for step in episode['steps'].take(1):
        print("Step keys:", step.keys())
        print("Image shape:", step['observation']['image'].shape)
        print("State shape:", step['observation']['state'].shape)
        print("Action shape:", step['action'].shape)
        print("Instruction:", step['language_instruction'].numpy().decode())
```

## Dataset Features

### Observation

-   **image**: 224×224×3 RGB image (JPEG encoded)
-   **state**: 7D camera pose [x, y, z, qx, qy, qz, qw]

### Action

-   7D relative camera movement [dx, dy, dz, d_roll, d_pitch, d_yaw, placeholder]

### Language Instruction

-   Formatted from DataDoP captions (prioritizes "Concise Interaction")

### Episode Metadata

-   Original file path
-   Movement caption
-   Detailed interaction caption
-   Concise interaction caption

## Key Differences from ET-VLA

1. **Frame-by-Frame Processing**: Each individual video frame from `Monst3r/*/shot_*/NULL/frame_*.png` is used as input
2. **Trajectory-based Actions**: Actions computed from consecutive camera poses in cleaned trajectories
3. **Multi-level Captions**: DataDoP provides Movement, Detailed, and Concise captions
4. **Autoregressive Training**: Each frame is paired with the action to reach the next frame, suitable for OpenVLA's autoregressive model

## Troubleshooting

### "No valid samples found"

-   Check that `--data_root` points to the correct directory
-   Verify the `Dataset/` subdirectory exists
-   Ensure files follow the naming convention: `shot_XXXX_transforms_cleaning.json`, etc.

### "Cannot load .npy file"

-   Verify Step 1 completed successfully
-   Check that `.npy` files exist in the `--manual_dir` path

### TFDS build fails

-   Ensure `datadop_vla_dataset.py` is in the current directory or PYTHONPATH
-   Verify TensorFlow Datasets is properly installed
-   Check that `--manual_dir` points to the directory containing train/val subdirectories

## Next Steps

After successful conversion, use the TFDS dataset with OpenVLA:

```python
import tensorflow_datasets as tfds

# Load for training
train_ds = tfds.load('datadop_vla', data_dir='DATA/TFDS', split='train')
val_ds = tfds.load('datadop_vla', data_dir='DATA/TFDS', split='val')

# Use with OpenVLA training pipeline
# ... (follow OpenVLA documentation)
```
