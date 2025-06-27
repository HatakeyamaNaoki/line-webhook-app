from flask import Flask, request
import requests
import os
from datetime import datetime

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload

app = Flask(__name__)
CHANNEL_ACCESS_TOKEN = os.environ.get('LINE_CHANNEL_ACCESS_TOKEN')

# Google Drive 認証
SCOPES = ['https://www.googleapis.com/auth/drive.file']
SERVICE_ACCOUNT_FILE = '/etc/secrets/credentials.json'  # RenderのSecret Filesパス

credentials = service_account.Credentials.from_service_account_file(
    SERVICE_ACCOUNT_FILE, scopes=SCOPES
)
drive_service = build('drive', 'v3', credentials=credentials)

# 📁 フォルダ取得 or 作成関数
def get_or_create_folder(folder_name, parent_id='root'):
    query = f"name = '{folder_name}' and mimeType = 'application/vnd.google-apps.folder' and '{parent_id}' in parents and trashed = false"
    results = drive_service.files().list(q=query, spaces='drive', fields="files(id, name)").execute()
    items = results.get('files', [])
    
    if items:
        return items[0]['id']
    
    file_metadata = {
        'name': folder_name,
        'mimeType': 'application/vnd.google-apps.folder',
        'parents': [parent_id]
    }
    folder = drive_service.files().create(body=file_metadata, fields='id').execute()
    return folder.get('id')

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

        # 📂 フォルダ構成作成
        root_folder_id = get_or_create_folder("受注集計")
        today_str = datetime.now().strftime("%Y%m%d")
        date_folder_id = get_or_create_folder(today_str, parent_id=root_folder_id)
        images_folder_id = get_or_create_folder("Line画像保存", parent_id=date_folder_id)

        # 🖼️ 一時ファイル保存
        file_path = f'/tmp/{message_id}.jpg'
        with open(file_path, 'wb') as f:
            f.write(image_data)

        # ☁️ Google Driveにアップロード
        file_metadata = {
            'name': f'{message_id}.jpg',
            'parents': [images_folder_id]
        }
        media = MediaFileUpload(file_path, mimetype='image/jpeg')
        uploaded = drive_service.files().create(body=file_metadata, media_body=media, fields='id').execute()

        print(f"Uploaded to Google Drive. File ID: {uploaded.get('id')}")

    return 'OK', 200

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=10000)
