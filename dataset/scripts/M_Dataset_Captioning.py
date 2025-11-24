import os
import base64
import random
from openai import OpenAI
import json
import numpy as np
from time import sleep
from tqdm import tqdm
# 新增並行處理需要的庫
from concurrent.futures import ThreadPoolExecutor, as_completed

data_dir = "./DATA"
try:
    from api_config import api_key
except ImportError:
    raise ImportError("Could not import 'config.py'. Please create it in the root directory with OPENAI_API_KEY defined.")

client = OpenAI(api_key=api_key)


def encode_image(image_path):
    with open(image_path, "rb") as image_file:
        return base64.b64encode(image_file.read()).decode('utf-8')


def call_gpt4_v(user_prompt, user_img_path, max_tokens=700):
    # global conversation_history
    base64_image = encode_image(user_img_path)
    conversation_history = [({"role": "user",
                              "content": [
                                  {"type": "text", "text": user_prompt},
                                  {"type": "image_url",
                                   "image_url": {
                                       "url": f"data:image/jpeg;base64,{base64_image}"
                                   }
                                   }
                              ]
                              }
                             )]
    response = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=conversation_history,
        max_tokens=max_tokens,
    )
    return response


def show_content(response):
    print(response.choices[0].message.content)


def save_content(response, file):
    with open(file, 'w') as f:
        f.write(response.choices[0].message.content)


def single_test(foldername, prompt_text, name):
    # 為了避免多線程同時 print 造成混亂，這裡可以選擇註解掉 print，或者保留
    print(f"Processing {name}...")

    image_path = f"{data_dir}/Captions/{name}.png"
    caption_path = f"{foldername}/{name}_caption.txt"
    relationship_path = f"{foldername}/{name}_relationship.txt"

    # 再次檢查存在性 (雖然外面檢查過了，但多線程環境下為了保險保留邏輯)
    if os.path.exists(relationship_path):
        print(f"Skipping {name}...")
        return

    with open(caption_path, 'r') as f:
        caption = f.read().strip()

    prompt_text = prompt_text + "\n\nMovement: " + caption
    for k in range(3):
        try:
            response = call_gpt4_v(prompt_text, image_path)
            para_len = response.choices[0].message.content.split("\n")
            # 注意：assert 在多線程中拋出異常會被捕獲並 print，不會中斷主程序
            assert len(para_len) == 3, f"\nLen Error: {para_len}, {response.choices[0].message.content}\n"
            save_content(response, relationship_path)
            break
        except Exception as e:
            print(f"Error in {name}: {e}")
            continue
    # show_content(response)


if __name__ == "__main__":
    prompt_path = "./scripts/configs/captioning/llm/relationship+image.json"
    prompts = json.load(open(prompt_path, 'r'))
    prompt_text = prompts['context'] + prompts['instruction'] + prompts['constraint'] + prompts['format']
    tag_dir = f"{data_dir}/Tagging/cam_segments"

    # 1. 先收集所有需要執行的任務參數
    tasks = []

    # 這裡只負責收集任務，不做 API 呼叫，速度很快不需要 tqdm
    print("Collecting tasks...")
    for folder in sorted(os.listdir(tag_dir)):
        folder_path = os.path.join(tag_dir, folder)
        if not os.path.isdir(folder_path):  # 簡單的防錯，確保是資料夾
            continue

        files = os.listdir(folder_path)
        txts = [f for f in files if f.lower().endswith('_caption.txt')]

        for txt in sorted(txts):
            name = f"{folder}/{txt.replace('_caption.txt', '')}"
            relationship_path = f"{tag_dir}/{name}_relationship.txt"

            # 只有檔案不存在時才加入任務列表
            if not os.path.exists(relationship_path):
                tasks.append((tag_dir, prompt_text, name))

    print(f"Total tasks to process: {len(tasks)}")

    # 2. 設定並行數量 (Max Workers)
    # 建議從 5 開始測試，避免觸發 OpenAI Rate Limit 429 Error
    MAX_WORKERS = 60

    # 3. 使用 ThreadPoolExecutor 進行並行處理
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        # 提交所有任務
        futures = [executor.submit(single_test, *task) for task in tasks]

        # 使用 tqdm 顯示進度，as_completed 會在任務完成時 yield
        for _ in tqdm(as_completed(futures), total=len(futures), desc="Processing"):
            pass
