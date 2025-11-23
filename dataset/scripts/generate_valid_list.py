import os
import glob

# 設定路徑 (與 Dataset_DataDoP.py 保持一致)
root = "./DATA"
monst3r_dir = os.path.join(root, "Monst3r")
output_txt = os.path.join(root, "DataDoP_valid.txt")


def generate_list():
    valid_names = []

    if not os.path.exists(monst3r_dir):
        print(f"Error: Directory {monst3r_dir} does not exist.")
        return

    print(f"Scanning {monst3r_dir} for valid trajectories in 'VideoID/ShotID' format...")

    # 使用 glob 搜尋兩層目錄: Monst3r/<VideoID>/<ShotID>
    # 例如: ./DATA/Monst3r/1_0000/shot_0070
    search_pattern = os.path.join(monst3r_dir, "*", "*")
    candidates = sorted(glob.glob(search_pattern))

    for folder in candidates:
        if os.path.isdir(folder):
            # 檢查是否存在關鍵軌跡檔案
            # 路徑結構: .../<VideoID>/<ShotID>/NULL/pred_traj.txt
            traj_file = os.path.join(folder, "NULL", "pred_traj.txt")
            intrinsics_file = os.path.join(folder, "NULL", "pred_intrinsics.txt")

            if os.path.exists(traj_file) and os.path.exists(intrinsics_file):
                # 取得相對路徑作為名稱，例如 "1_0000/shot_0070"
                # relpath 會把 ./DATA/Monst3r/1_0000/shot_0070 轉成 1_0000/shot_0070
                name = os.path.relpath(folder, monst3r_dir)
                # 統一將 Windows 的反斜線 \ 換成 Linux 的正斜線 / (以防萬一)
                name = name.replace(os.sep, '/')
                valid_names.append(name)

    # 寫入 DataDoP_valid.txt
    with open(output_txt, 'w') as f:
        for name in valid_names:
            f.write(f"{name}\n")

    print(f"Done! Found {len(valid_names)} valid shots.")
    print(f"Saved list to: {output_txt}")


if __name__ == "__main__":
    generate_list()
