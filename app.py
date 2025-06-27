from flask import Flask, request
import requests
import os
from datetime import datetime
import openai
import csv
import io
import pandas as pd

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload

app = Flask(__name__)
CHANNEL_ACCESS_TOKEN = os.environ.get('LINE_CHANNEL_ACCESS_TOKEN')
OPENAI_API_KEY = os.environ.get('OPENAI_API_KEY')
openai.api_key = OPENAI_API_KEY

SCOPES = ['https://www.googleapis.com/auth/drive.file']
SERVICE_ACCOUNT_FILE = '/etc/secrets/credentials.json'
credentials = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)
drive_service = build('drive', 'v3', credentials=credentials)

CSV_FORMAT_FILE = '集計フォーマット.csv'

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
    file_metadata = {
        'name': folder_name,
        'mimeType': 'application/vnd.google-apps.folder'
    }
    if parent_id:
        file_metadata['parents'] = [parent_id]
    folder = drive_service.files().create(body=file_metadata, fields='id').execute()
    return folder['id']

def extract_text_from_image(image_path):
    with open(image_path, "rb") as image_file:
        response = openai.ChatCompletion.create(
            model="gpt-4o",
            messages=[
                {"role": "system", "content": "以下の画像は注文書です。内容をCSV形式に構造化してください。"},
                {"role": "user", "content": [
                    {"type": "image_url", "image_url": {"url": f"data:image/jpeg;base64,{image_file.read().encode('base64').decode()}"}}
                ]}
            ],
            temperature=0.2
        )
        return response.choices[0].message.content

def get_headers_from_csv():
    with open(CSV_FORMAT_FILE, newline='', encoding='utf-8') as f:
        reader = csv.reader(f)
        return next(reader)

def append_to_csv_on_drive(folder_id, filename, row_data, headers):
    query = f"name = '{filename}' and '{folder_id}' in parents and trashed = false"
    response = drive_service.files().list(q=query, fields='files(id, name)').execute()
    files = response.get('files', [])

    if files:
        file_id = files[0]['id']
        existing = drive_service.files().get_media(fileId=file_id).execute()
        df_existing = pd.read_csv(io.BytesIO(existing))
        df_new = pd.DataFrame([row_data], columns=headers)
        df_all = pd.concat([df_existing, df_new], ignore_index=True)
    else:
        df_all = pd.DataFrame([row_data], columns=headers)

    temp_csv = f"/tmp/{filename}"
    df_all.to_csv(temp_csv, index=False, encoding='utf-8-sig')

    media = MediaFileUpload(temp_csv, mimetype='text/csv', resumable=True)
    if files:
        drive_service.files().update(fileId=file_id, media_body=media).execute()
    else:
        file_metadata = {
            'name': filename,
            'parents': [folder_id],
            'mimeType': 'text/csv'
        }
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
        user_id = event['source']['userId']
        timestamp = datetime.now().strftime('%Y%m%d_%H%M')
        image_name = f'{timestamp}.jpg'

        image_url = f'https://api-data.line.me/v2/bot/message/{message_id}/content'
        headers = {'Authorization': f'Bearer {CHANNEL_ACCESS_TOKEN}'}
        image_data = requests.get(image_url, headers=headers).content

        image_path = f'/tmp/{image_name}'
        with open(image_path, 'wb') as f:
            f.write(image_data)

        root_folder_id = get_or_create_folder('受注集計')
        today_str = datetime.now().strftime('%Y%m%d')
        date_folder_id = get_or_create_folder(today_str, parent_id=root_folder_id)
        line_folder_id = get_or_create_folder('Line画像保存', parent_id=date_folder_id)
        result_folder_id = get_or_create_folder('集計結果', parent_id=date_folder_id)

        # 画像アップロード
        file_metadata = {'name': image_name, 'parents': [line_folder_id]}
        media = MediaFileUpload(image_path, mimetype='image/jpeg')
        uploaded = drive_service.files().create(body=file_metadata, media_body=media, fields='id').execute()
        print(f"Uploaded image: {uploaded.get('id')}")

        # ChatGPTで画像解析（仮で空データ構造化）
        # extracted_text = extract_text_from_image(image_path)
        headers = get_headers_from_csv()
        customer = "テスト株式会社"
        orderer = "佐藤"
        time_now = datetime.now().strftime('%Y%m%d%H')
        dummy_data = [customer, orderer, "商品A", 5, "2025-07-01", "東京都港区", time_now]

        append_to_csv_on_drive(result_folder_id, f"集計結果_{today_str}.csv", dummy_data, headers)

    return 'OK', 200

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=10000)
