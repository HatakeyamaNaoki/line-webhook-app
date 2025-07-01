# handlers/pdf_handler.py
import os
import requests
import base64
from openai import OpenAI
from pdf2image import convert_from_path
from handlers.prompt_templates import IMAGE_ORDER_PROMPT
from handlers.file_handler import get_or_create_folder, save_pdf_to_drive
from handlers.csv_handler import append_to_xlsx
from handlers.utils import get_now, get_operator_name

def analyze_pdf_with_gpt(pdf_path, operator_name, now_str, now_verbose, openai_client, max_retries=3):
    # 1. PDFを画像（JPEG）にページごとに変換
    images = convert_from_path(pdf_path)
    results = []
    for i, image in enumerate(images):
        img_path = f"/tmp/pdf_page_{i}.jpg"
        image.save(img_path, 'JPEG')
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
                    {"role": "system", "content": "あなたは画像の内容をEXCEL形式に変換するアシスタントです。"},
                    {"role": "user", "content": [
                        {"type": "text", "text": prompt},
                        {"type": "image_url", "image_url": {"url": f"data:image/jpeg;base64,{image_base64}"}}
                    ]}
                ],
                max_tokens=1000,
                temperature=0.2
            )
            content = response.choices[0].message.content.strip()
            print("GPT content:", content)
            if "申し訳ありません" in content or "直接抽出することはできません" in content:
                continue
            lines = content.splitlines()
            cleaned_lines = [line for line in lines if not line.strip().startswith("この情報") and line.strip() not in ["...", "…"]]
            results.append("\n".join(cleaned_lines))
            break  # 成功したらretryしない
        os.remove(img_path)
    if not results:
        print("構造化テキストが空です。GPT応答なしまたはすべて謝罪文")
        return ""
    return "\n".join(results)

def process_pdf_message(event):
    user_id = event['source']['userId']
    headers = {'Authorization': f'Bearer {os.environ.get("LINE_CHANNEL_ACCESS_TOKEN")}'}
    operator_name = get_operator_name(user_id, headers)
    now, now_str, now_verbose = get_now()

    # PDF取得
    message_id = event['message']['id']
    pdf_url = f'https://api-data.line.me/v2/bot/message/{message_id}/content'
    pdf_data = requests.get(pdf_url, headers=headers).content
    file_name = now.strftime('%Y%m%d_%H%M') + '.pdf'

    # Google Drive保存先取得
    root_id = get_or_create_folder('受注集計')
    date_id = get_or_create_folder(now.strftime('%Y%m%d'), parent_id=root_id)
    pdf_folder_id = get_or_create_folder('PDF保存', parent_id=date_id)
    csv_folder_id = get_or_create_folder('集計結果', parent_id=date_id)

    # PDF保存
    save_pdf_to_drive(pdf_data, file_name, pdf_folder_id)  # PDFバイナリ保存

    # PDF→テキスト（GPT解析）
    openai_client = OpenAI(api_key=os.environ.get("OPENAI_API_KEY"))
    pdf_path = f'/tmp/{file_name}'
    with open(pdf_path, 'wb') as f:
        f.write(pdf_data)
    structured_text = analyze_pdf_with_gpt(
        pdf_path, operator_name, now_str, now_verbose, openai_client
    )
    os.remove(pdf_path)

    # CSV追記
    openai_client = OpenAI()
    append_to_xlsx(structured_text, csv_folder_id, openai_client)
