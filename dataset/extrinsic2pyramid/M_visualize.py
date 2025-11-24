from util.camera_pose_visualizer import CameraPoseVisualizer
import tempfile  # 用於解決並行時的檔案名稱衝突
from concurrent.futures import ProcessPoolExecutor, as_completed
from PIL import Image
from scipy.spatial.transform import Rotation as R
import numpy as np
from torch import tensor
import matplotlib.pyplot as plt
import os
import glob
import tqdm
import json
import torch
import matplotlib
# 設定 Matplotlib 後端為非互動模式，這對於多進程繪圖至關重要
matplotlib.use('Agg')
# 假設 util 存在於您的環境中

# 新增並行處理需要的庫


def quaternion_to_matrix(quaternions):
    """
    Convert rotations given as quaternions to rotation matrices.
    Args:
        quaternions: quaternions with real part first,
            as numpy array of shape (..., 4).
    Returns:
        Rotation matrices as numpy array of shape (..., 3, 3).
    """
    r, i, j, k = np.split(quaternions, 4, axis=-1)
    two_s = 2.0 / np.sum(quaternions ** 2, axis=-1)

    o = np.concatenate(
        (
            1 - two_s * (j * j + k * k),
            two_s * (i * j - k * r),
            two_s * (i * k + j * r),
            two_s * (i * j + k * r),
            1 - two_s * (i * i + k * k),
            two_s * (j * k - i * r),
            two_s * (i * k - j * r),
            two_s * (j * k + i * r),
            1 - two_s * (i * i + j * j),
        ),
        axis=-1
    )

    return o.reshape(*quaternions.shape[:-1], 3, 3)


def matrix_to_quaternion(M):
    """
    Matrix-to-quaternion conversion method. Equation taken from 
    https://www.euclideanspace.com/maths/geometry/rotations/conversions/matrixToQuaternion/index.htm
    Args:
        M: rotation matrices, (... x 3 x 3)
    Returns:
        q: quaternion of shape (... x 4)
    """
    prefix_shape = M.shape[:-2]
    Ms = M.reshape(-1, 3, 3)

    trs = 1 + Ms[:, 0, 0] + Ms[:, 1, 1] + Ms[:, 2, 2]

    Qs = []

    for i in range(Ms.shape[0]):
        M = Ms[i]
        tr = trs[i]
        if tr > 0:
            r = np.sqrt(tr) / 2.0
            x = (M[2, 1] - M[1, 2]) / (4 * r)
            y = (M[0, 2] - M[2, 0]) / (4 * r)
            z = (M[1, 0] - M[0, 1]) / (4 * r)
        elif (M[0, 0] > M[1, 1]) and (M[0, 0] > M[2, 2]):
            S = np.sqrt(1.0 + M[0, 0] - M[1, 1] - M[2, 2]) * 2  # S = 4 * qx
            r = (M[2, 1] - M[1, 2]) / S
            x = 0.25 * S
            y = (M[0, 1] + M[1, 0]) / S
            z = (M[0, 2] + M[2, 0]) / S
        elif M[1, 1] > M[2, 2]:
            S = np.sqrt(1.0 + M[1, 1] - M[0, 0] - M[2, 2]) * 2  # S = 4 * qy
            r = (M[0, 2] - M[2, 0]) / S
            x = (M[0, 1] + M[1, 0]) / S
            y = 0.25 * S
            z = (M[1, 2] + M[2, 1]) / S
        else:
            S = np.sqrt(1.0 + M[2, 2] - M[0, 0] - M[1, 1]) * 2  # S = 4 * qz
            r = (M[1, 0] - M[0, 1]) / S
            x = (M[0, 2] + M[2, 0]) / S
            y = (M[1, 2] + M[2, 1]) / S
            z = 0.25 * S

        Q = np.stack([r, x, y, z], axis=-1)
        Qs.append(Q)

    return np.stack(Qs, axis=0).reshape(*prefix_shape, 4)


def convert_viser_poses_to_new_coordinate_system(quaternions, positions):
    quaternions = np.array(quaternions)
    positions = np.array(positions)

    matrices = []

    for q, p in zip(quaternions, positions):
        q_wxyz = q

        rotation = quaternion_to_matrix(q_wxyz)

        matrix = np.eye(4)
        matrix[:3, :3] = rotation
        matrix[:3, 3] = p

        matrices.append(matrix)
    return np.array(matrices)


def draw_json(json_path, vis_name_base):
    """
    修改後的 draw_json 函數
    1. 接收 json_path
    2. 使用 tempfile 避免多進程寫入同一個 'front_view.png' 導致衝突
    """
    vis_path = json_path.replace("_transforms", "_traj").replace(".json", ".png")

    # 讀取數據
    try:
        data = json.load(open(json_path))['frames']
    except Exception as e:
        print(f"Error reading {json_path}: {e}")
        return

    poses = []
    for frame in data:
        poses.append(frame['transform_matrix'])

    poses = [poses[i] for i in range(0, len(poses), 2)]

    if not poses:
        print(f"No poses found in {json_path}")
        return

    c2ws = torch.tensor(poses)

    ref_w2c = torch.inverse(c2ws[:1])
    c2ws = ref_w2c.repeat(c2ws.shape[0], 1, 1) @ c2ws

    rangesize = torch.max(torch.abs(torch.tensor(c2ws[:, :3, 3]))) * 1.1
    c2ws = c2ws.numpy()

    num_matrices = c2ws.shape[0]
    colors = plt.cm.rainbow(np.linspace(1, 0, num_matrices))

    views = [
        {'elev': 90, 'azim': -90, 'name': 'front'},
        {'elev': 180, 'azim': -90, 'name': 'top'},
        {'elev': 0, 'azim': 0, 'name': 'side'}
    ]

    # 使用 TemporaryDirectory 創建唯一的暫存目錄
    # 這樣不同的進程不會互相覆蓋 front_view.png 等中間文件
    with tempfile.TemporaryDirectory() as temp_dir:
        image_paths = []

        for view in views:
            fig = plt.figure(figsize=(12, 12))
            visualizer = CameraPoseVisualizer([-rangesize, rangesize], [-rangesize, rangesize], [-rangesize, rangesize])

            for i in range(num_matrices):
                color = colors[i]
                visualizer.extrinsic2pyramid(c2ws[i], color, rangesize / 4)

            visualizer.ax.view_init(elev=view['elev'], azim=view['azim'])

            # 將中間文件存入唯一的 temp_dir
            image_path = os.path.join(temp_dir, f"{view['name']}_view.png")
            visualizer.save(image_path)
            image_paths.append(image_path)

            # 重要：關閉 figure 以釋放內存
            plt.close(fig)

        # 讀取並處理圖片
        images = [Image.open(img_path) for img_path in image_paths]
        images[-1] = images[-1].rotate(90, expand=True)

        images = [img.crop((420, 420, 1980, 1980)) for img in images]
        images_resized = [img.resize((341, 341)) for img in images]

        combined_image = np.concatenate([np.array(img) for img in images_resized], axis=1)

        final_image = Image.fromarray(combined_image)
        final_image.save(vis_path)

    # print(f"Combined image saved at {vis_path}") # Optional: reduce spam


def process_single_task(name, dataset_dir, visname):
    """
    這是提交給 ProcessPoolExecutor 的單一任務封裝函數
    """
    json_file = f"{dataset_dir}/{name}_transforms_cleaning.json"
    vis_path = json_file.replace("_transforms", "_traj").replace(".json", ".png")

    if os.path.exists(vis_path):
        return  # Skip

    draw_json(json_file, visname)


if __name__ == "__main__":
    valid_name_list = []
    visname = 'vis0'  # 此變數在並行版本中僅作為保留參數，實際中間檔會存於 temp
    dataset_dir = "./DATA/Dataset"
    dataset_list = "./DATA/DataDoP_valid.txt"

    with open(dataset_list, 'r') as f:
        lines = f.readlines()
        for line in lines:
            valid_name_list.append(line.strip())

    print("#valid_name_list:", len(valid_name_list))

    # 準備任務列表
    tasks = []
    for name in valid_name_list:
        tasks.append((name, dataset_dir, visname))

    # 設定並行進程數 (預設使用所有可用 CPU 核心)
    # 若要限制核心數，可設定 max_workers=4
    max_workers = os.cpu_count()
    print(f"Starting processing with {max_workers} CPUs...")

    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        # 提交任務
        futures = [executor.submit(process_single_task, *task) for task in tasks]

        # 使用 tqdm 顯示進度
        for _ in tqdm.tqdm(as_completed(futures), total=len(futures)):
            pass
