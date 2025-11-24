import os
import sys
import json
import glob
import math

import tqdm
import torch
import shutil
from PIL import Image
import numpy as np
from processing.cleaning import clean_trajectories
import multiprocessing
from functools import partial

root = "./DATA"
dataset_dir = f"{root}/Dataset"
dataset_list = "./DATA/DataDoP_valid.txt"


def process_single_build(name):
    """處理單個資料的 build 邏輯"""
    old_shot = f"{root}/Monst3r/{name}/NULL"
    if not os.path.exists(old_shot):
        return

    depth_path = f"{dataset_dir}/{name}_depth.npy"
    rgb_path = f"{dataset_dir}/{name}_rgb.png"
    info_path = f"{dataset_dir}/{name}_info.json"
    intrinsics_path = f"{dataset_dir}/{name}_intrinsics.txt"
    traj_path = f"{dataset_dir}/{name}_traj.txt"

    # 為了避免多進程同時創建目錄導致的 Race Condition，這裡可以先不做makedirs
    # 或者接受它可能拋出錯誤 (exist_ok=True 通常很安全)
    try:
        os.makedirs(os.path.dirname(depth_path), exist_ok=True)
    except FileExistsError:
        pass

    if os.path.exists(depth_path) and os.path.exists(rgb_path) and \
       os.path.exists(intrinsics_path) and os.path.exists(traj_path) and \
       os.path.exists(info_path):
        return  # Skip

    get_data(name)


def process_single_pose_check(name):
    """處理單個資料的 pose check 邏輯"""
    if os.path.exists(f"{dataset_dir}/{name}_transforms.json"):
        return  # Skip
    # 設定 PyTorch 在每個子進程只用 1 個執行緒，避免過度競爭
    torch.set_num_threads(1)
    pose_normalize(name)


def process_single_pose_clean(name):
    """處理單個資料的 pose clean 邏輯"""
    if os.path.exists(f"{dataset_dir}/{name}_transforms_cleaning.json"):
        return  # Skip
    torch.set_num_threads(1)
    pose_clean_normalize(name)


def get_black_area_ratio(image_path, threshold=50):
    image = Image.open(image_path).convert('L')
    img_array = np.array(image)
    black_pixels = np.sum(img_array < threshold)
    total_pixels = img_array.size
    return black_pixels / total_pixels


def get_init_idx(name):
    Image_dir = f"{root}/Monst3r/{name}/NULL/"
    search_pattern = os.path.join(Image_dir, "frame_*.png")
    image_files = sorted(glob.glob(search_pattern))
    init_idx = 0
    if len(image_files) < 15:
        return -1
    for idx in range(15):
        image_file = image_files[idx]
        black = get_black_area_ratio(image_file)
        if black >= 1:
            init_idx = int(image_file.split('_')[-1].split('.')[0]) + 1
    return init_idx


def get_image(name, idx):
    image_path = f"{root}/Monst3r/{name}/NULL/frame_{str(idx).zfill(4)}.png"
    new_path = f"{dataset_dir}/{name}_rgb.png"
    shutil.copy(image_path, new_path)


def get_depth(name, idx):
    image_path = f"{root}/Monst3r/{name}/NULL/frame_depth_{str(idx).zfill(4)}.npy"
    new_path = f"{dataset_dir}/{name}_depth.npy"
    shutil.copy(image_path, new_path)


def get_pose(name, idx):
    pose_intrinsics_path = f"{root}/Monst3r/{name}/NULL/pred_intrinsics.txt"
    new_intrinsics_path = f"{dataset_dir}/{name}_intrinsics.txt"
    with open(pose_intrinsics_path, 'r') as f:
        intrinsics = f.readlines()
        final_intrinsics = intrinsics[idx:]
    with open(new_intrinsics_path, 'w') as f:
        f.writelines(final_intrinsics)

    pose_traj_path = f"{root}/Monst3r/{name}/NULL/pred_traj.txt"
    new_traj_path = f"{dataset_dir}/{name}_traj.txt"
    with open(pose_traj_path, 'r') as f:
        trajs = f.readlines()
        final_trajs = trajs[idx:]
    with open(new_traj_path, 'w') as f:
        f.writelines(final_trajs)


def get_data(name):
    idx = get_init_idx(name)
    if idx == -1:
        return
    get_image(name, idx)
    get_depth(name, idx)
    get_pose(name, idx)


def build_dataset():
    valid_name_list = []
    with open(dataset_list, 'r') as f:
        lines = f.readlines()
        for line in lines:
            valid_name_list.append(line.strip())
    print("# valid_name_list: ", len(valid_name_list))

    # 使用 CPU 核心數量的進程數
    num_workers = os.cpu_count()-24
    print(f"Using {num_workers} workers for parallel processing.")

    with multiprocessing.Pool(num_workers) as pool:
        # 使用 imap_unordered 可以讓 tqdm 更平滑地顯示進度
        list(tqdm.tqdm(pool.imap_unordered(process_single_build, valid_name_list),
                       total=len(valid_name_list),
                       desc="Building dataset (Parallel)"))

    print("Dataset building completed.")


def quaternion_to_matrix(quaternions):
    """
    Convert rotations given as quaternions to rotation matrices.
    Args:
        quaternions: quaternions with real part first,
            as tensor of shape (..., 4).
    Returns:
        Rotation matrices as tensor of shape (..., 3, 3).
    """
    r, i, j, k = torch.unbind(quaternions, -1)
    two_s = 2.0 / (quaternions * quaternions).sum(-1)

    o = torch.stack(
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
        -1,
    )
    return o.reshape(quaternions.shape[:-1] + (3, 3))


def matrix_to_quaternion(M: torch.Tensor) -> torch.Tensor:
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
            r = torch.sqrt(tr) / 2.0
            x = (M[2, 1] - M[1, 2]) / (4 * r)
            y = (M[0, 2] - M[2, 0]) / (4 * r)
            z = (M[1, 0] - M[0, 1]) / (4 * r)
        elif (M[0, 0] > M[1, 1]) and (M[0, 0] > M[2, 2]):
            S = torch.sqrt(1.0 + M[0, 0] - M[1, 1] - M[2, 2]) * 2  # S=4*qx
            r = (M[2, 1] - M[1, 2]) / S
            x = 0.25 * S
            y = (M[0, 1] + M[1, 0]) / S
            z = (M[0, 2] + M[2, 0]) / S
        elif M[1, 1] > M[2, 2]:
            S = torch.sqrt(1.0 + M[1, 1] - M[0, 0] - M[2, 2]) * 2  # S=4*qy
            r = (M[0, 2] - M[2, 0]) / S
            x = (M[0, 1] + M[1, 0]) / S
            y = 0.25 * S
            z = (M[1, 2] + M[2, 1]) / S
        else:
            S = torch.sqrt(1.0 + M[2, 2] - M[0, 0] - M[1, 1]) * 2  # S=4*qz
            r = (M[1, 0] - M[0, 1]) / S
            x = (M[0, 2] + M[2, 0]) / S
            y = (M[1, 2] + M[2, 1]) / S
            z = 0.25 * S
        Q = torch.stack([r, x, y, z], dim=-1)
        Qs += [Q]

    return torch.stack(Qs, dim=0).reshape(*prefix_shape, 4)


def quaternion_slerp(
    q0, q1, fraction, spin: int = 0, shortestpath: bool = True
):
    """Return spherical linear interpolation between two quaternions.
    Args:
        quat0: first quaternion
        quat1: second quaternion
        fraction: how much to interpolate between quat0 vs quat1 (if 0, closer to quat0; if 1, closer to quat1)
        spin: how much of an additional spin to place on the interpolation
        shortestpath: whether to return the short or long path to rotation
    """
    d = (q0 * q1).sum(-1)
    if shortestpath:
        # invert rotation
        d[d < 0.0] = -d[d < 0.0]
        q1[d < 0.0] = q1[d < 0.0]

    d = d.clamp(0, 1.0)

    angle = torch.acos(d) + spin * math.pi
    isin = 1.0 / (torch.sin(angle) + 1e-10)
    q0_ = q0 * torch.sin((1.0 - fraction) * angle) * isin
    q1_ = q1 * torch.sin(fraction * angle) * isin

    q = q0_ + q1_
    q[angle < 1e-5, :] = q0

    return q


def sample_from_two_pose(pose_a, pose_b, fraction, noise_strengths=[0, 0]):
    """
    Args:
        pose_a: first pose
        pose_b: second pose
        fraction
    """
    def is_valid_rotation_matrix(matrix):
        should_be_identity = torch.matmul(matrix, matrix.transpose(-1, -2))
        identity = torch.eye(3, device=matrix.device).expand_as(should_be_identity)
        return torch.allclose(should_be_identity, identity, atol=1e-6) and torch.allclose(torch.det(matrix), torch.ones_like(matrix))

    quat_a = matrix_to_quaternion(pose_a[..., :3, :3])
    quat_b = matrix_to_quaternion(pose_b[..., :3, :3])
    dot = torch.sum(quat_a * quat_b, dim=-1, keepdim=True)
    quat_b = torch.where(dot < 0, -quat_b, quat_b)

    cos_theta = torch.sum(quat_a * quat_b, dim=-1, keepdim=True)
    slerp_condition = cos_theta.abs() < 0.9995
    slerp_quat = quaternion_slerp(quat_a, quat_b, fraction)
    lerp_quat = (1 - fraction) * quat_a + fraction * quat_b
    lerp_quat = lerp_quat / lerp_quat.norm(dim=-1, keepdim=True)
    quaternion = torch.where(slerp_condition, slerp_quat, lerp_quat)

    quaternion = torch.nn.functional.normalize(quaternion + torch.randn_like(quaternion) * noise_strengths[0], dim=-1)

    R = quaternion_to_matrix(quaternion)
    T = (1 - fraction) * pose_a[..., :3, 3] + fraction * pose_b[..., :3, 3]
    T = T + torch.randn_like(T) * noise_strengths[1]

    new_pose = pose_a.clone()
    new_pose[..., :3, :3] = R
    new_pose[..., :3, 3] = T

    assert is_valid_rotation_matrix(R), "Invalid rotation matrix"
    return new_pose


def sample_from_dense_cameras(dense_cameras, t, noise_strengths=[0, 0, 0, 0]):
    B, N, C = dense_cameras.shape
    B, M = t.shape

    left = torch.floor(t * (N-1)).long().clamp(0, N-2)
    right = left + 1
    fraction = t * (N-1) - left
    a = torch.gather(dense_cameras, 1, left[..., None].repeat(1, 1, C))
    b = torch.gather(dense_cameras, 1, right[..., None].repeat(1, 1, C))

    new_pose = sample_from_two_pose(a[:, :, :12].reshape(B, M, 3, 4),
                                    b[:, :, :12].reshape(B, M, 3, 4), fraction, noise_strengths=noise_strengths[:2])

    new_ins = (1 - fraction) * a[:, :, 12:] + fraction * b[:, :, 12:]

    return torch.cat([new_pose.reshape(B, M, 12), new_ins], dim=2)


def convert_viser_poses_to_new_coordinate_system(quaternions, positions):
    quaternions = torch.tensor(quaternions)
    positions = torch.tensor(positions)

    matrices = []

    for q, p in zip(quaternions, positions):
        q_wxyz = q
        rotation = quaternion_to_matrix(q_wxyz)

        matrix = np.eye(4)
        matrix[:3, :3] = rotation
        matrix[:3, 3] = p  # 将位置向量放入矩阵中
        matrix[:3, 1:3] *= -1

        if not np.all(np.isfinite(matrix)):
            print("Invalid rotation matrix:", rotation)
        matrices.append(matrix[:3, :])
    return np.array(matrices)


def pose_normalize(name):
    torch.set_printoptions(profile="full")
    np.set_printoptions(threshold=np.inf)

    transforms_path = f"{dataset_dir}/{name}_transforms.json"

    traj_path = f"{dataset_dir}/{name}_traj.txt"

    intri_path = f"{dataset_dir}/{name}_intrinsics.txt"
    intri = np.loadtxt(intri_path)[0]

    # Extract the intrinsic parameters (focal length and principal point)
    f_x, _, c_x = intri[0], intri[1], intri[2]  # First row
    _, f_y, c_y = intri[3], intri[4], intri[5]  # Second row

    # Assuming the image resolution (w, h) can be inferred from c_x, c_y
    w = 2 * int(c_x)  # Image width inferred from the principal point c_x
    h = 2 * int(c_y)  # Image height inferred from the principal point c_y

    # Create a dictionary of intrinsic parameters
    transforms_dict = {
        "w": w,
        "h": h,
        "fl_x": f_x,  # Focal length in x direction
        "fl_y": f_y,  # Focal length in y direction
        "cx": c_x,    # Principal point in x
        "cy": c_y,     # Principal point in y
        'frames': []
    }

    trajs = np.loadtxt(traj_path)
    quaternions = []
    positions = []
    for traj in trajs:
        positions.append(traj[1:4])
        quaternions.append(traj[4:8])

    traj_matrix = convert_viser_poses_to_new_coordinate_system(quaternions, positions)
    traj_tensor = torch.tensor(traj_matrix, dtype=torch.float32)
    traj_tensor = traj_tensor.view(1, -1, 12)
    camera_list = []
    for i in range(120):
        t = torch.full((1, 1), fill_value=i/120)
        camera = sample_from_dense_cameras(traj_tensor, t)
        camera_list.append(camera[0])
    camera_tensor = torch.cat(camera_list, dim=0)  # Concatenate along the batch dimension (dim=0)
    camera_numpy = camera_tensor.clone().cpu().numpy()
    # transform_matrixs = []
    for idx, row in enumerate(camera_numpy):
        RT = row.reshape(3, 4)
        transform_matrix = np.vstack([RT, [0, 0, 0, 1]])
        transform_matrix_list = transform_matrix.tolist()
        # Prepare frame data
        frame_data = {
            "transform_matrix": transform_matrix_list,
            "monst3r_im_id": idx + 1  # Assuming colmap_im_id is an index starting from 1
        }
        transforms_dict['frames'].append(frame_data)

    # Save the transforms dictionary to a JSON file
    with open(transforms_path, 'w') as f:
        json.dump(transforms_dict, f, indent=4)


def pose_clean_normalize(name):
    torch.set_printoptions(profile="full")
    np.set_printoptions(threshold=np.inf)

    transforms_path = f"{dataset_dir}/{name}_transforms_cleaning.json"

    traj_path = f"{dataset_dir}/{name}_traj.txt"

    intri_path = f"{dataset_dir}/{name}_intrinsics.txt"
    intri = np.loadtxt(intri_path)[0]

    # Extract the intrinsic parameters (focal length and principal point)
    f_x, _, c_x = intri[0], intri[1], intri[2]  # First row
    _, f_y, c_y = intri[3], intri[4], intri[5]  # Second row

    # Assuming the image resolution (w, h) can be inferred from c_x, c_y
    w = 2 * int(c_x)  # Image width inferred from the principal point c_x
    h = 2 * int(c_y)  # Image height inferred from the principal point c_y

    # Create a dictionary of intrinsic parameters
    transforms_dict = {
        "w": w,
        "h": h,
        "fl_x": f_x,  # Focal length in x direction
        "fl_y": f_y,  # Focal length in y direction
        "cx": c_x,    # Principal point in x
        "cy": c_y,     # Principal point in y
        'frames': []
    }

    trajs = np.loadtxt(traj_path)
    quaternions = []
    positions = []
    for traj in trajs:
        positions.append(traj[1:4])
        quaternions.append(traj[4:8])

    traj_matrix = convert_viser_poses_to_new_coordinate_system(quaternions, positions)
    traj_tensor = torch.tensor(traj_matrix, dtype=torch.float32)
    bottom_row = torch.tensor([[0, 0, 0, 1]], dtype=traj_tensor.dtype).repeat(traj_tensor.size(0), 1, 1)
    traj_tensor = torch.cat((traj_tensor, bottom_row), dim=1)
    traj_tensor = clean_trajectories(traj_tensor)
    traj_tensor = traj_tensor[0][1][:, :3, :]
    traj_tensor = traj_tensor.view(1, -1, 12)
    camera_list = []
    for i in range(120):
        t = torch.full((1, 1), fill_value=i/120)
        camera = sample_from_dense_cameras(traj_tensor, t)
        camera_list.append(camera[0])
    camera_tensor = torch.cat(camera_list, dim=0)  # Concatenate along the batch dimension (dim=0)
    camera_numpy = camera_tensor.clone().cpu().numpy()
    for idx, row in enumerate(camera_numpy):
        RT = row.reshape(3, 4)
        transform_matrix = np.vstack([RT, [0, 0, 0, 1]])
        transform_matrix_list = transform_matrix.tolist()
        # Prepare frame data
        frame_data = {
            "transform_matrix": transform_matrix_list,
            "monst3r_im_id": idx + 1  # Assuming colmap_im_id is an index starting from 1
        }
        transforms_dict['frames'].append(frame_data)

    # Save the transforms dictionary to a JSON file
    with open(transforms_path, 'w') as f:
        json.dump(transforms_dict, f, indent=4)


def pose_check():
    valid_name_list = []
    with open(dataset_list, 'r') as f:
        lines = f.readlines()
        for line in lines:
            valid_name_list.append(line.strip())
    print(len(valid_name_list))

    num_workers = os.cpu_count()

    with multiprocessing.Pool(num_workers) as pool:
        list(tqdm.tqdm(pool.imap_unordered(process_single_pose_check, valid_name_list),
                       total=len(valid_name_list),
                       desc="Pose Check (Parallel)"))


def pose_clean():
    valid_name_list = []
    with open(dataset_list, 'r') as f:
        lines = f.readlines()
        for line in lines:
            valid_name_list.append(line.strip())
    print("#valid_name_list:", len(valid_name_list))

    num_workers = os.cpu_count()

    with multiprocessing.Pool(num_workers) as pool:
        list(tqdm.tqdm(pool.imap_unordered(process_single_pose_clean, valid_name_list),
                       total=len(valid_name_list),
                       desc="Pose Clean (Parallel)"))


def process_single_caption_folder(folder):
    """處理單個資料夾的 Caption 轉換"""
    source_dir = f"{root}/Tagging/cam_segments"
    target_dir = f'{root}/Dataset'

    folder_path = os.path.join(source_dir, folder)
    if not os.path.isdir(folder_path):
        return

    # 確保目標資料夾存在 (如果是結構化的)
    # 如果 target_dir/{folder} 不存在，寫入檔案會報錯，所以先建立
    target_folder_path = os.path.join(target_dir, folder)
    os.makedirs(target_folder_path, exist_ok=True)

    files = os.listdir(folder_path)
    txts = [f for f in files if f.lower().endswith('_caption.txt')]

    for txt_file in txts:
        try:
            name_base = txt_file.replace('_caption.txt', '')
            txt_path = os.path.join(folder_path, txt_file)
            rela_path = os.path.join(folder_path, name_base + '_relationship.txt')

            # 注意：這裡根據你的原始邏輯，new_path 是 dataset_dir/folder/name_caption.json
            new_path = os.path.join(target_folder_path, name_base + '_caption.json')

            if not os.path.exists(rela_path):
                continue

            with open(txt_path, 'r') as f:
                caption = f.read().strip()
            with open(rela_path, 'r') as f:
                rela = f.readlines()

            data = {}
            data['Movement'] = caption
            for line in rela:
                if '**Detailed**' in line:
                    data['Detailed Interaction'] = line.replace('**Detailed**: ', '').strip()
                elif '**Concise**' in line:
                    data['Concise Interaction'] = line.replace('**Concise**: ', '').strip()

            # 簡易檢查
            if len(data) != 3:
                # 可以在這裡 log 錯誤，但避免 print 影響進度條
                pass

            with open(new_path, 'w') as f:
                json.dump(data, f, indent=4)
        except Exception as e:
            print(f"Error processing {folder}/{txt_file}: {e}")


def get_captions():
    source_dir = f"{root}/Tagging/cam_segments"
    if not os.path.exists(source_dir):
        print(f"Directory not found: {source_dir}")
        return

    folders = sorted(os.listdir(source_dir))

    num_workers = max(1, os.cpu_count() - 14)  # 保留一點核心給系統
    print(f"Generating Captions with {num_workers} workers...")

    with multiprocessing.Pool(num_workers) as pool:
        list(tqdm.tqdm(pool.imap_unordered(process_single_caption_folder, folders),
                       total=len(folders),
                       desc="Processing Captions"))


def process_single_copy_folder(folder):
    """處理單個資料夾的複製"""
    DATA_dir = f'{root}/Dataset'
    DataDoP_dir = '../DataDoP/DataDoP'

    old_path = os.path.join(DATA_dir, folder)
    new_path = os.path.join(DataDoP_dir, folder)

    if os.path.exists(new_path):
        return  # Skip if exists

    try:
        shutil.copytree(old_path, new_path)
    except Exception as e:
        print(f"Error copying {folder}: {e}")


def process_single_verify(name):
    """驗證單筆資料完整性"""
    DataDoP_dir = '../DataDoP/DataDoP'
    required_suffixes = [
        '_caption.json', '_rgb.png', '_depth.npy',
        '_intrinsics.txt', '_traj.txt',
        '_transforms_cleaning.json', '_traj_cleaning.png'
    ]

    for suffix in required_suffixes:
        file_path = f"{DataDoP_dir}/{name}{suffix}"
        if not os.path.exists(file_path):
            return f"Invalid: {name} (Missing {suffix})"
    return None


def process_single_cleanup(folder):
    """清理多餘檔案"""
    DataDoP_dir = '../DataDoP/DataDoP'
    folder_path = os.path.join(DataDoP_dir, folder)

    if not os.path.isdir(folder_path):
        return

    valid_suffixes = (
        '_caption.json', '_rgb.png', '_depth.npy',
        '_intrinsics.txt', '_traj.txt',
        '_transforms_cleaning.json', '_traj_cleaning.png'
    )

    for file in os.listdir(folder_path):
        if not file.endswith(valid_suffixes):
            file_path = os.path.join(folder_path, file)
            try:
                os.remove(file_path)
                # print(f"Removed extra file: {file_path}") # 註解掉以減少 I/O
            except OSError as e:
                print(f"Error deleting {file_path}: {e}")


def check_DataDoP():
    DATA_dir = f'{root}/Dataset'
    DataDoP_dir = '../DataDoP/DataDoP'
    os.makedirs(DataDoP_dir, exist_ok=True)

    # 1. Parallel Copy
    folders = sorted(os.listdir(DATA_dir))
    num_workers = max(1, os.cpu_count() - 4)

    print(f"Copying files to DataDoP with {num_workers} workers...")
    with multiprocessing.Pool(num_workers) as pool:
        list(tqdm.tqdm(pool.imap_unordered(process_single_copy_folder, folders),
                       total=len(folders),
                       desc="Copying"))

    # 2. Parallel Validity Check
    valid_list_path = '../DataDoP/DataDoP_valid.txt'
    if os.path.exists(valid_list_path):
        with open(valid_list_path, 'r') as f:
            valid_name_list = [line.strip() for line in f.readlines()]

        print(f"Verifying {len(valid_name_list)} items...")
        with multiprocessing.Pool(num_workers) as pool:
            results = list(tqdm.tqdm(pool.imap_unordered(process_single_verify, valid_name_list),
                                     total=len(valid_name_list),
                                     desc="Verifying"))

        # 顯示錯誤
        for res in results:
            if res:
                print(res)
    else:
        print(f"Warning: {valid_list_path} not found.")

    # 3. Parallel Cleanup
    # 這裡重新 listdir 一次，確保處理的是目前的 DataDoP 目錄
    current_folders = sorted(os.listdir(DataDoP_dir))
    print("Cleaning up extra files...")
    with multiprocessing.Pool(num_workers) as pool:
        list(tqdm.tqdm(pool.imap_unordered(process_single_cleanup, current_folders),
                       total=len(current_folders),
                       desc="Cleanup"))

    print("Done.")


if __name__ == '__main__':
    if len(sys.argv) > 1:
        command = sys.argv[1]
    else:
        command = None
    if command == 'basic':
        build_dataset()
        pose_check()
        pose_clean()
    elif command == 'caption':
        get_captions()
        check_DataDoP()
    else:
        print("Please provide a valid command: 'basic' or 'caption'")
