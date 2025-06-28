from flask import Flask, request
import requests
import os
from datetime import datetime, timedelta
import base64
import pandas as pd
from openai import OpenAI
import io
import pytz

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload, MediaIoBaseDownload

app = Flask(__name__)
CHANNEL_ACCESS_TOKEN = os.environ.get('LINE_CHANNEL_ACCESS_TOKEN')

openai_client = OpenAI(
    base_url="https://api.openai.com/v1",
    api_key=os.environ["OPENAI_API_KEY"]
)

SCOPES = ['https://www.googleapis.com/auth/drive.file']
SERVICE_ACCOUNT_FILE = '/etc/secrets/credentials.json'

credentials = service_account.Credentials.from_service_account_file(
    SERVICE_ACCOUNT_FILE, scopes=SCOPES
)
drive_service = build('drive', 'v3', credentials=credentials)

CSV_FORMAT_PATH = '集計フォーマット.csv'
CSV_HEADERS = pd.read_csv(CSV_FORMAT_PATH, encoding='utf-8').columns.tolist()

JST = pytz.timezone('Asia/Tokyo')

def get_or_create_folder(folder_name, parent_id=None):
    query = f"name = '{folder_name}' and mimeType = 'application/vnd.google-apps.folder' and trashed = false"
    if parent_id:
        query += f" and '{parent_id}' in parents"
    else:
        query += f" and 'root' in parents"
    response = drive_service.files().list(q=query, fields='files(id, name)').execute()
    files = response.get('files', [])
    if files:
        return files[0]['id']
    file_metadata = {'name': folder_name, 'mimeType': 'application/vnd.google-apps.folder'}
    if parent_id:
        file_metadata['parents'] = [parent_id]
    folder = drive_service.files().create(body=file_metadata, fields='id').execute()
    return folder['id']

def analyze_image_with_gpt(image_path, operator_name, max_retries=3):
    with open(image_path, "rb") as image_file:
        image_base64 = base64.b64encode(image_file.read()).decode("utf-8")

    now = datetime.now(JST)
    now_str = now.strftime("%Y%m%d%H")
    now_verbose = now.strftime("%Y年%m月%d日 %H時")

    prompt = f"""
以下の画像に含まれる注文内容を、CSV形式で構造化してください。
- 出力はカンマ区切り、以下の順番と一致させてください。
- 数量は数値＋単位に分けて記載してください（例：10, 玉）
- 「小さい」「大きめ」などの形容詞は備考欄に記載してください。
- 何か注意点がある際にも備考欄に記載してください。
- ヘッダーは出力せず、データ部分のみ複数行で出力してください。
- 不要な補足文（例：「この情報を参考にしてください」など）は出力しないでください。
- 顧客名と発注者名は画像上部のテキストから会社名と人名を抽出して出力してください。
- "..." のような行や意味のない行は出力しないでください。
- 納品希望日が「明日」「明後日」「3日後」など相対的な表現の場合は、以下の「現在日時（日本時間）」を基準に、「明日＝+1日」「明後日＝+2日」「3日後＝+3日」として正確に日付を加算し、YYYYMMDD形式で出力してください（※月またぎ・年またぎにも対応すること）。
  特に「明後日」は+2日、「3日後」は+3日、「4日後」は+4日というふうに、語に対応する日数を厳密に解釈してください。
- 現在日時（日本時間）: {now_verbose}（JST）
- 社内担当者は常に「{operator_name}」としてください（画像から読み取らない）。
- 読み取りができない場合でも、謝罪や案内文は出力せず、読み取れる範囲でデータのみを返してください。

列順: 顧客,発注者,商品名,数量,単位,納品希望日,納品場所,時間,社内担当者,備考

時間: {now_str}
以下が画像データです：
"""

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
        print("GPT Response Content:\n", content)

        if "申し訳ありません" in content or "直接抽出することはできません" in content:
            continue
        lines = content.splitlines()
        cleaned_lines = [line for line in lines if not line.strip().startswith("この情報") and line.strip() != "..." and line.strip() != "…"]
        return "\n".join(cleaned_lines)

    print("構造化テキストが空です。GPT応答なしまたはすべて謝罪文")
    return ""

def append_to_csv(structured_text, parent_id):
    if not structured_text.strip():
        return

    today = datetime.now(JST).strftime('%Y%m%d')
    filename = f'集計結果_{today}.csv'
    file_path = f'/tmp/{filename}'

    try:
        new_data = pd.read_csv(io.StringIO(structured_text), header=None, names=CSV_HEADERS)
    except Exception as e:
        print("CSV parsing error:", e)
        return

    print("使用された構造化テキスト:\n", structured_text)

    new_data = new_data[~new_data.iloc[:, 0].astype(str).str.contains("…|...|…")]
    now_str = datetime.now(JST).strftime('%Y%m%d%H')
    new_data['時間'] = new_data['時間'].replace("不明", now_str)

    query = f"name = '{filename}' and '{parent_id}' in parents and trashed = false"
    response = drive_service.files().list(q=query, fields='files(id)').execute()
    files = response.get('files', [])

    if files:
        file_id = files[0]['id']
        request = drive_service.files().get_media(fileId=file_id)
        fh = io.BytesIO()
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while not done:
            status, done = downloader.next_chunk()
        fh.seek(0)
        existing = pd.read_csv(fh)
        combined = pd.concat([existing, new_data], ignore_index=True)
        combined.to_csv(file_path, index=False, encoding='utf-8-sig')
        media = MediaFileUpload(file_path, mimetype='text/csv')
        drive_service.files().update(fileId=file_id, media_body=media).execute()
    else:
        new_data.to_csv(file_path, index=False, encoding='utf-8-sig')
        file_metadata = {'name': filename, 'parents': [parent_id]}
        media = MediaFileUpload(file_path, mimetype='text/csv')
        drive_service.files().create(body=file_metadata, media_body=media).execute()

@app.route('/webhook', methods=['POST'])
def webhook():
    data = request.get_json()
    events = data.get('events', [])
    if not events:
        return 'OK', 200

    event = events[0]
    if event.get('message', {}).get('type') == 'image':
        message_id = event['message']['id']
        image_url = f'https://api-data.line.me/v2/bot/message/{message_id}/content'
        headers = {'Authorization': f'Bearer {CHANNEL_ACCESS_TOKEN}'}
        image_data = requests.get(image_url, headers=headers).content

        user_id = event['source']['userId']
        profile_res = requests.get('https://api.line.me/v2/bot/profile/' + user_id, headers=headers)
        operator_name = profile_res.json().get('displayName', '不明')

        timestamp = datetime.now(JST)
        file_name = timestamp.strftime('%Y%m%d_%H%M') + '.jpg'
        file_path = f'/tmp/{file_name}'
        with open(file_path, 'wb') as f:
            f.write(image_data)

        root_id = get_or_create_folder('受注集計')
        date_id = get_or_create_folder(timestamp.strftime('%Y%m%d'), parent_id=root_id)
        image_folder_id = get_or_create_folder('Line画像保存', parent_id=date_id)
        csv_folder_id = get_or_create_folder('集計結果', parent_id=date_id)

        file_metadata = {'name': file_name, 'parents': [image_folder_id]}
        media = MediaFileUpload(file_path, mimetype='image/jpeg')
        drive_service.files().create(body=file_metadata, media_body=media).execute()

        structured_text = analyze_image_with_gpt(file_path, operator_name)
        append_to_csv(structured_text, csv_folder_id)

    return 'OK', 200

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=10000)
