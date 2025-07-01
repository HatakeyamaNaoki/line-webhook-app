# handlers/text_handler.py

import os
from handlers.file_handler import get_or_create_folder, save_text_to_drive
from handlers.csv_handler import append_to_xlsx
from handlers.utils import get_now, get_operator_name
from .prompt_templates import TEXT_ORDER_PROMPT
from openai import OpenAI

def analyze_text_with_gpt(text, operator_name, now_str, now_verbose, openai_client, max_retries=3):
    prompt = TEXT_ORDER_PROMPT.format(
        now_verbose=now_verbose,
        operator_name=operator_name,
        now_str=now_str,
        text=text
    )
    for attempt in range(max_retries):
        response = openai_client.chat.completions.create(
            model="gpt-4o",
            messages=[
                {"role": "system", "content": "あなたはテキスト注文をCSV形式に変換するアシスタントです。"},
                {"role": "user", "content": prompt}
            ],
            max_tokens=1000,
            temperature=0.2
        )
        content = response.choices[0].message.content.strip()
        if "申し訳ありません" in content or "直接抽出することはできません" in content:
            continue
        lines = content.splitlines()
        cleaned_lines = [line for line in lines if not line.strip().startswith("この情報") and line.strip() not in ["...", "…"]]
        return "\n".join(cleaned_lines)
    print("構造化テキストが空です。GPT応答なしまたはすべて謝罪文")
    return ""

def process_text_message(event):
    user_id = event['source']['userId']
    headers = {'Authorization': f'Bearer {os.environ.get("LINE_CHANNEL_ACCESS_TOKEN")}'}
    operator_name = get_operator_name(user_id, headers)
    now, now_str, now_verbose = get_now()

    text = event['message']['text']
    file_name = now.strftime('%Y%m%d_%H%M') + '.txt'

    # Google Drive保存先取得
    root_id = get_or_create_folder('受注集計')
    date_id = get_or_create_folder(now.strftime('%Y%m%d'), parent_id=root_id)
    image_folder_id = get_or_create_folder('Line画像保存', parent_id=date_id)
    csv_folder_id = get_or_create_folder('集計結果', parent_id=date_id)

    # テキストをDrive保存
    save_text_to_drive(text, file_name, image_folder_id)

    # GPTで構造化
    openai_client = OpenAI(api_key=os.environ.get("OPENAI_API_KEY"))
    structured_text = analyze_text_with_gpt(
        text, operator_name, now_str, now_verbose, openai_client
    )

    # CSV追記
    append_to_xlsx(structured_text, csv_folder_id)
