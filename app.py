from flask import Flask, request
import requests
import os
from datetime import datetime
import base64
import pandas as pd
import openai
import io

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload, MediaIoBaseDownload

app = Flask(__name__)
CHANNEL_ACCESS_TOKEN = os.environ.get('LINE_CHANNEL_ACCESS_TOKEN')
openai.api_key = os.environ.get("OPENAI_API_KEY")

SCOPES = ['https://www.googleapis.com/auth/drive.file']
SERVICE_ACCOUNT_FILE = '/etc/secrets/credentials.json'

credentials = service_account.Credentials.from_service_account_file(
    SERVICE_ACCOUNT_FILE, scopes=SCOPES
)
drive_service = build('drive', 'v3', credentials=credentials)

CSV_FORMAT_PATH = '集計フォーマット.csv'
CSV_HEADERS = pd.read_csv(CSV_FORMAT_PATH, encoding='utf-8').columns.tolist()

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

def extract_sender_info(display_name):
    parts = display_name.strip().split()
    if len(parts) >= 2:
        return parts[0], parts[1]
    elif parts:
        return parts[0], ''
    return '', ''

def analyze_image_with_gpt(image_path, sender_name):
    with open(image_path, "rb") as image_file:
        image_base64 = base64.b64encode(image_file.read()).decode("utf-8")
    now = datetime.now()
    now_str = now.strftime("%Y%m%d%H")
    prompt = f"""
次の画像に含まれる内容を、以下のCSVフォーマットで構造化してください。
出力フォーマット（順番通りに、カンマ区切りで）:
{','.join(CSV_HEADERS)}
顧客は「{extract_sender_info(sender_name)[0]}」
発注者は「{extract_sender_info(sender_name)[1]}」
時間は「{now_str}」
以下が画像データです：
"""
    response = openai.chat.completions.create(
        model="gpt-4o",
        messages=[
            {"role": "system", "content": "あなたは画像の内容をCSV形式に構造化するアシスタントです。"},
            {"role": "user", "content": [
                {"type": "text", "text": prompt},
                {"type": "image_url", "image_url": {"url": f"data:image/jpeg;base64,{image_base64}"}}
            ]}
        ],
        max_tokens=1000,
        temperature=0
    )
    return response.choices[0].message.content.strip()

def append_to_csv(structured_text, parent_id):
    today = datetime.now().strftime('%Y%m%d')
    filename = f'集計結果_{today}.csv'
    file_path = f'/tmp/{filename}'
    new_data = pd.read_csv(io.StringIO(structured_text), header=None, names=CSV_HEADERS)

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
        combined.to_csv(file_path, index=False)
        media = MediaFileUpload(file_path, mimetype='text/csv')
        drive_service.files().update(fileId=file_id, media_body=media).execute()
    else:
        new_data.to_csv(file_path, index=False)
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
        user_name = event['source'].get('userId', '不明')
        image_url = f'https://api-data.line.me/v2/bot/message/{message_id}/content'
        headers = {'Authorization': f'Bearer {CHANNEL_ACCESS_TOKEN}'}
        image_data = requests.get(image_url, headers=headers).content

        timestamp = datetime.now().strftime('%Y%m%d_%H%M')
        file_name = f'{timestamp}.jpg'
        file_path = f'/tmp/{file_name}'
        with open(file_path, 'wb') as f:
            f.write(image_data)

        root_id = get_or_create_folder('受注集計')
        date_id = get_or_create_folder(datetime.now().strftime('%Y%m%d'), parent_id=root_id)
        image_folder_id = get_or_create_folder('Line画像保存', parent_id=date_id)
        csv_folder_id = get_or_create_folder('集計結果', parent_id=date_id)

        file_metadata = {'name': file_name, 'parents': [image_folder_id]}
        media = MediaFileUpload(file_path, mimetype='image/jpeg')
        drive_service.files().create(body=file_metadata, media_body=media).execute()

        structured_text = analyze_image_with_gpt(file_path, user_name)
        append_to_csv(structured_text, csv_folder_id)

    return 'OK', 200

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=10000)