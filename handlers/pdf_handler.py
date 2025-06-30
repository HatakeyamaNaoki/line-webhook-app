# handlers/pdf_handler.py

import os
import requests
from pdf2image import convert_from_path
from openai import OpenAI
import base64
from handlers.prompt_templates import IMAGE_ORDER_PROMPT
from handlers.file_handler import get_or_create_folder, save_image_to_drive
from handlers.csv_handler import append_to_csv
from handlers.utils import get_now, get_operator_name

def analyze_pdf_with_gpt(pdf_path, operator_name, now_str, now_verbose, openai_client, max_retries=3):
    # 1. PDF→画像（ページ毎にjpg化）
    images = convert_from_path(pdf_path)
    results = []
    for i, image in enumerate(images):
        img_path = f"/tmp/pdf_page_{i}.jpg"
        image.save(img_path, 'JPEG')
        # 画像ページをbase64化
        with open(img_path, "rb") as image_file:
            image_base64 = base64.b64encode(image_file.read()).decode("utf-8")
        prompt = IMAGE_ORDER_PROMPT.format(
            now_verbose=now_verbose,
            operator_name=operator_name,
            now_str=now_str
        )
        for attempt in range(max_retries):
            response = openai_client.chat.completions.create(
                model="gpt-4o",
                messages=[
                    {"role": "system", "content": "あなたは画像の内容をCSV形式に変換するアシスタントです。"},
                    {"role": "user", "content": [
                        {"type": "text", "text": prompt},
                        {"type": "image_url", "image_url": {"url": f"data:image/jpeg;base64,{image_base64}"}}
                    ]}
                ],
                max_tokens=1000,
                temperature=0.2
            )
            content = response.choices[0].message.content.strip()
            if "申し訳ありません" in content or "直接抽出することはできません" in content:
                continue
            lines = content.splitlines()
            cleaned_lines = [line for line in lines if not line.strip().startswith("この情報") and line.strip() not in ["...", "…"]]
            results.append("\n".join(cleaned_lines))
            break  # 成功したらretryしない
        os.remove(img_path)
    return "\n".join(results)

def process_pdf_message(event):
    """
    LINEイベントからPDFデータを受け取り、Drive保存→GPT解析→CSV追記まで実行
    """
    # 1. ユーザー名
    user_id = event['source']['userId']
    headers = {'Authorization': f'Bearer {os.environ.get("LINE_CHANNEL_ACCESS_TOKEN")}'}
    operator_name = get_operator_name(user_id, headers)
    now, now_str, now_verbose = get_now()

    # 2. PDF取得
    message_id = event['message']['id']
    pdf_url = f'https://api-data.line.me/v2/bot/message/{message_id}/content'
    pdf_data = requests.get(pdf_url, headers=headers).content
    file_name = now.strftime('%Y%m%d_%H%M') + '.pdf'

    # 3. Google Drive保存先取得
    root_id = get_or_create_folder('受注集計')
    date_id = get_or_create_folder(now.strftime('%Y%m%d'), parent_id=root_id)
    pdf_folder_id = get_or_create_folder('PDF保存', parent_id=date_id)
    csv_folder_id = get_or_create_folder('集計結果', parent_id=date_id)

    # 4. PDF保存
    pdf_path = f'/tmp/{file_name}'
    with open(pdf_path, 'wb') as f:
        f.write(pdf_data)
    save_image_to_drive(pdf_data, file_name, pdf_folder_id)  # PDFもバイナリ保存可

    # 5. PDF→テキスト（GPT解析）
    openai_client = OpenAI(api_key=os.environ.get("OPENAI_API_KEY"))
    structured_text = analyze_pdf_with_gpt(
        pdf_path, operator_name, now_str, now_verbose, openai_client
    )
    os.remove(pdf_path)

    # 6. CSV追記
    append_to_csv(structured_text, csv_folder_id)
