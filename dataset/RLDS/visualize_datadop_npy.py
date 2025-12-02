# -*- coding: utf-8 -*-
"""
視覺化驗證 DataDoP .npy 格式的 episode 資料（無 GUI 版本）。

這個腳本會讀取一個目錄中所有的 .npy 檔案，並為每一個檔案
生成多張圖片來檢視影像序列和 3D 相機軌跡。

作者：Gemini (Adapted for DataDoP - Headless Version)
日期：2025-12-03

使用方法：
1. 確認已安裝必要的函式庫: matplotlib, scipy, numpy, opencv-python
   pip install matplotlib scipy numpy opencv-python
2. 在終端機中執行，指向一個包含 .npy 檔案的目錄：
   python visualize_datadop_npy.py --npy_dir ../DATA/RLDS/datadop-npy-test/splits/train --output_dir ./visualizations
"""
import argparse
from pathlib import Path

import numpy as np
import matplotlib
matplotlib.use('Agg')  # 使用非互動式後端
import matplotlib.pyplot as plt
from scipy.spatial.transform import Rotation

def apply_action_to_state(state_7d, action_7d):
    """
    將 7D action 應用於 7D state，返回新的 7D state
    
    Args:
        state_7d: [x, y, z, qx, qy, qz, qw]
        action_7d: [dx, dy, dz, d_roll, d_pitch, d_yaw, gripper]
    
    Returns:
        new_state_7d: [x, y, z, qx, qy, qz, qw]
    """
    pos_start, quat_start = state_7d[:3], state_7d[3:7]
    rot_start = Rotation.from_quat(quat_start)
    T_start = np.eye(4)
    T_start[:3, :3] = rot_start.as_matrix()
    T_start[:3, 3] = pos_start

    delta_pos, delta_euler = action_7d[:3], action_7d[3:6]
    delta_rot = Rotation.from_euler('xyz', delta_euler)
    T_delta = np.eye(4)
    T_delta[:3, :3] = delta_rot.as_matrix()
    T_delta[:3, 3] = delta_pos

    T_end = T_start @ T_delta

    pos_end = T_end[:3, 3]
    quat_end = Rotation.from_matrix(T_end[:3, :3]).as_quat()
    
    return np.concatenate([pos_end, quat_end]).astype(np.float32)


def visualize_episode(npy_path, output_dir):
    """
    視覺化單一 .npy episode 檔案並保存為圖片。
    """
    # 載入 episode 資料
    try:
        episode = np.load(npy_path, allow_pickle=True)
        if not isinstance(episode, np.ndarray) or episode.ndim == 0 or len(episode) == 0:
            print(f"  --> 錯誤：檔案 {npy_path.name} 不是一個有效的 episode .npy 檔案，已跳過。")
            return
        print(f"  成功載入 episode，包含 {len(episode)} 個時間步。")
    except Exception as e:
        print(f"  --> 載入 .npy 檔案 {npy_path.name} 時發生錯誤: {e}，已跳過。")
        return

    # 提取視覺化所需的資料
    images = [step['image'] for step in episode]
    states = [step['state'] for step in episode]
    actions = [step['action'] for step in episode]
    instruction = episode[0]['language_instruction']
    
    # 獲取 caption 資訊
    caption_info = ""
    if 'caption_data' in episode[0]:
        caption_data = episode[0]['caption_data']
        movement = caption_data.get('Movement', 'N/A')
        caption_info = f"Movement: {movement}"
    
    # 補上軌跡的最後一個點
    last_state = states[-1]
    last_action = actions[-1]
    final_state = apply_action_to_state(last_state, last_action)
    
    full_states = np.vstack([states, final_state])
    positions = full_states[:, :3]

    # 創建輸出目錄
    episode_output_dir = output_dir / npy_path.stem
    episode_output_dir.mkdir(parents=True, exist_ok=True)

    # === 1. 生成 3D 軌跡總覽圖 ===
    fig = plt.figure(figsize=(12, 10))
    ax = fig.add_subplot(111, projection='3d')
    
    ax.plot(positions[:, 0], positions[:, 1], positions[:, 2], 'b-', linewidth=2, label='Trajectory')
    ax.scatter(positions[0, 0], positions[0, 1], positions[0, 2], c='g', s=100, marker='o', label='Start')
    ax.scatter(positions[-1, 0], positions[-1, 1], positions[-1, 2], c='r', s=100, marker='*', label='End')
    
    # 每隔幾幀標記一個點
    step_size = max(1, len(positions) // 10)
    for i in range(0, len(positions), step_size):
        ax.scatter(positions[i, 0], positions[i, 1], positions[i, 2], c='orange', s=30, alpha=0.6)
    
    ax.set_xlabel('X Axis')
    ax.set_ylabel('Y Axis')
    ax.set_zlabel('Z Axis')
    ax.set_title(f'3D Camera Trajectory\n{npy_path.stem}')
    ax.legend()
    
    # 設定等比例座標軸
    axis_ranges = np.array([
        positions[:, 0].max() - positions[:, 0].min(), 
        positions[:, 1].max() - positions[:, 1].min(), 
        positions[:, 2].max() - positions[:, 2].min()
    ])
    max_range = axis_ranges.max() / 2.0
    if max_range < 1e-6:
        max_range = 1.0
    
    mid_x = (positions[:,0].max()+positions[:,0].min()) * 0.5
    mid_y = (positions[:,1].max()+positions[:,1].min()) * 0.5
    mid_z = (positions[:,2].max()+positions[:,2].min()) * 0.5
    ax.set_xlim(mid_x - max_range, mid_x + max_range)
    ax.set_ylim(mid_y - max_range, mid_y + max_range)
    ax.set_zlim(mid_z - max_range, mid_z + max_range)
    
    ax.view_init(elev=20., azim=-60)
    
    plt.tight_layout()
    plt.savefig(episode_output_dir / 'trajectory_3d.png', dpi=150, bbox_inches='tight')
    plt.close()

    # === 2. 生成關鍵幀圖片網格 ===
    num_keyframes = min(12, len(images))
    indices = np.linspace(0, len(images)-1, num_keyframes, dtype=int)
    
    fig, axes = plt.subplots(3, 4, figsize=(16, 12))
    fig.suptitle(f'{npy_path.stem}\nInstruction: {instruction}\n{caption_info}', fontsize=12)
    
    for idx, ax in enumerate(axes.flat):
        if idx < len(indices):
            frame_idx = indices[idx]
            ax.imshow(images[frame_idx])
            ax.set_title(f'Frame {frame_idx}', fontsize=10)
            ax.axis('off')
            
            # 添加 action 資訊
            action_vals = actions[frame_idx]
            action_text = (
                f"dx={action_vals[0]:.2f}, dy={action_vals[1]:.2f}, dz={action_vals[2]:.2f}\n"
                f"dR={np.rad2deg(action_vals[3]):.1f}°, dP={np.rad2deg(action_vals[4]):.1f}°, dY={np.rad2deg(action_vals[5]):.1f}°"
            )
            ax.text(0.5, -0.1, action_text, transform=ax.transAxes, 
                   ha='center', va='top', fontsize=8, bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.5))
        else:
            ax.axis('off')
    
    plt.tight_layout()
    plt.savefig(episode_output_dir / 'keyframes_grid.png', dpi=150, bbox_inches='tight')
    plt.close()

    # === 3. 生成每個時間步的詳細圖片（前10幀） ===
    num_detailed = min(10, len(images))
    for i in range(num_detailed):
        fig = plt.figure(figsize=(14, 6))
        
        # 左側：影像
        ax_img = fig.add_subplot(1, 2, 1)
        ax_img.imshow(images[i])
        ax_img.set_title(f'Frame {i}/{len(images)-1}')
        ax_img.axis('off')
        
        # 右側：3D 軌跡（標記當前位置）
        ax_traj = fig.add_subplot(1, 2, 2, projection='3d')
        ax_traj.plot(positions[:, 0], positions[:, 1], positions[:, 2], 'b-', alpha=0.3, linewidth=1)
        ax_traj.plot(positions[:i+1, 0], positions[:i+1, 1], positions[:i+1, 2], 'b-', linewidth=2, label='Traveled')
        ax_traj.scatter(positions[i, 0], positions[i, 1], positions[i, 2], c='r', s=100, marker='o', label='Current')
        
        # 繪製相機方向
        state = full_states[i]
        pos, quat = state[:3], state[3:7]
        rot_matrix = Rotation.from_quat(quat).as_matrix()
        
        origin = pos
        x_axis, y_axis, z_axis = rot_matrix[:, 0], rot_matrix[:, 1], rot_matrix[:, 2]
        arrow_length = max_range * 0.1
        
        ax_traj.quiver(origin[0], origin[1], origin[2], x_axis[0], x_axis[1], x_axis[2], 
                      color='r', length=arrow_length, normalize=True, label='X')
        ax_traj.quiver(origin[0], origin[1], origin[2], y_axis[0], y_axis[1], y_axis[2], 
                      color='g', length=arrow_length, normalize=True, label='Y')
        ax_traj.quiver(origin[0], origin[1], origin[2], z_axis[0], z_axis[1], z_axis[2], 
                      color='b', length=arrow_length, normalize=True, label='Z')
        
        ax_traj.set_xlabel('X')
        ax_traj.set_ylabel('Y')
        ax_traj.set_zlabel('Z')
        ax_traj.set_xlim(mid_x - max_range, mid_x + max_range)
        ax_traj.set_ylim(mid_y - max_range, mid_y + max_range)
        ax_traj.set_zlim(mid_z - max_range, mid_z + max_range)
        ax_traj.view_init(elev=20., azim=-60)
        ax_traj.legend(fontsize=8)
        
        # 添加 action 資訊
        action_vals = actions[i]
        action_text = (
            f"Action @ t={i}:\n"
            f"Translation: dx={action_vals[0]:.3f}, dy={action_vals[1]:.3f}, dz={action_vals[2]:.3f}\n"
            f"Rotation: dR={np.rad2deg(action_vals[3]):.2f}°, dP={np.rad2deg(action_vals[4]):.2f}°, dY={np.rad2deg(action_vals[5]):.2f}°\n"
            f"Gripper: {action_vals[6]:.3f}"
        )
        fig.text(0.5, 0.02, action_text, ha='center', va='bottom', fontsize=9, 
                bbox=dict(boxstyle='round', facecolor='lightblue', alpha=0.8))
        
        plt.tight_layout()
        plt.savefig(episode_output_dir / f'frame_{i:03d}.png', dpi=100, bbox_inches='tight')
        plt.close()

    print(f"  ✓ 已保存視覺化圖片到: {episode_output_dir}")
    print(f"    - trajectory_3d.png (3D 軌跡總覽)")
    print(f"    - keyframes_grid.png (關鍵幀網格)")
    print(f"    - frame_XXX.png (前 {num_detailed} 幀的詳細圖)")


def main(args):
    """主執行函式"""
    npy_dir = Path(args.npy_dir)
    output_dir = Path(args.output_dir)
    
    if not npy_dir.is_dir():
        print(f"錯誤：找不到指定的目錄: {npy_dir}")
        return

    npy_files = sorted(list(npy_dir.glob('*.npy')))
    if not npy_files:
        print(f"錯誤：在目錄 {npy_dir} 中找不到任何 .npy 檔案。")
        return

    # 限制處理的檔案數量
    if args.max_files:
        npy_files = npy_files[:args.max_files]

    print(f"在目錄 {npy_dir} 中找到 {len(npy_files)} 個 episode 檔案。")
    print(f"將保存視覺化圖片到: {output_dir}")

    output_dir.mkdir(parents=True, exist_ok=True)

    for i, npy_path in enumerate(npy_files):
        print(f"\n--- 正在處理 [{i+1}/{len(npy_files)}]: {npy_path.name} ---")
        visualize_episode(npy_path, output_dir)

    print(f"\n✓ 所有視覺化完成！圖片已保存到: {output_dir}")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="視覺化 DataDoP .npy episode 檔案並保存為圖片。")
    parser.add_argument('--npy_dir', type=str, required=True, help='包含 .npy 檔案的目錄路徑。')
    parser.add_argument('--output_dir', type=str, default='./visualizations', help='輸出圖片的目錄路徑。')
    parser.add_argument('--max_files', type=int, help='最多處理的檔案數量（用於測試）。')
    
    args = parser.parse_args()
    main(args)
