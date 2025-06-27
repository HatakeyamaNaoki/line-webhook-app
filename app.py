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
SERVICE_ACCOUNT_FILE = '/etc/secrets/credentials.json'

credentials = service_account.Credentials.from_service_account_file(
    SERVICE_ACCOUNT_FILE, scopes=SCOPES
)
drive_service = build('drive', 'v3', credentials=credentials)

def get_or_create_folder(service, name, parent_id=None):
    """指定された名前と親フォルダ内にフォルダが存在するか確認し、なければ作成してIDを返す"""
    query = f"mimeType='application/vnd.google-apps.folder' and trashed = false and name='{name}'"
    if parent_id:
        query += f" and '{parent_id}' in parents"
    else:
        query += f" and 'root' in parents"

    results = service.files().list(q=query, fields="files(id, name)").execute()
    items = results.get('files', [])
    if items:
        return items[0]['id']
    else:
        file_metadata = {
            'name': name,
            'mimeType': 'application/vnd.google-apps.folder',
            'parents': [parent_id] if parent_id else ['root']
        }
        folder = service.files().create(body=file_metadata, fields='id').execute()
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

        # 一時ファイルとして保存
        file_path = f'/tmp/{message_id}.jpg'
        with open(file_path, 'wb') as f:
            f.write(image_data)

        # Google Drive フォルダ階層を作成
        root_folder_id = get_or_create_folder(drive_service, "受注集計")
        today_str = datetime.now().strftime('%Y%m%d')
        date_folder_id = get_or_create_folder(drive_service, today_str, parent_id=root_folder_id)
        line_folder_id = get_or_create_folder(drive_service, "Line画像保存", parent_id=date_folder_id)
        _ = get_or_create_folder(drive_service, "集計結果", parent_id=date_folder_id)

        # Google Drive にアップロード
        file_metadata = {
            'name': f'{message_id}.jpg',
            'parents': [line_folder_id]
        }
        media = MediaFileUpload(file_path, mimetype='image/jpeg')
        uploaded = drive_service.files().create(body=file_metadata, media_body=media, fields='id').execute()

        print(f"Uploaded to Google Drive. File ID: {uploaded.get('id')}")

    return 'OK', 200

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=10000)
