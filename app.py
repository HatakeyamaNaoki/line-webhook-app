from flask import Flask, request
import requests
import os
from datetime import datetime

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload

app = Flask(__name__)
CHANNEL_ACCESS_TOKEN = os.environ.get('LINE_CHANNEL_ACCESS_TOKEN')
SERVICE_ACCOUNT_FILE = '/etc/secrets/credentials.json'
USER_EMAIL = 'hatake.hatake.hatake7@gmail.com'  # 👈←あなたのGmail

# Google Drive APIの設定
SCOPES = ['https://www.googleapis.com/auth/drive']
credentials = service_account.Credentials.from_service_account_file(
    SERVICE_ACCOUNT_FILE, scopes=SCOPES)
drive_service = build('drive', 'v3', credentials=credentials)

def get_or_create_folder(folder_name, parent_id=None):
    query = f"name='{folder_name}' and mimeType='application/vnd.google-apps.folder'"
    if parent_id:
        query += f" and '{parent_id}' in parents"
    else:
        query += " and 'root' in parents"

    results = drive_service.files().list(q=query, fields="files(id, name)").execute()
    files = results.get('files', [])
    if files:
        return files[0]['id']

    file_metadata = {
        'name': folder_name,
        'mimeType': 'application/vnd.google-apps.folder',
    }
    if parent_id:
        file_metadata['parents'] = [parent_id]

    folder = drive_service.files().create(body=file_metadata, fields='id').execute()
    return folder.get('id')

def share_file_with_user(file_id, email):
    permission = {
        'type': 'user',
        'role': 'writer',
        'emailAddress': email
    }
    drive_service.permissions().create(
        fileId=file_id,
        body=permission,
        fields='id',
        sendNotificationEmail=False
    ).execute()

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

        file_path = f'/tmp/{message_id}.jpg'
        with open(file_path, 'wb') as f:
            f.write(image_data)

        # フォルダ構成
        root_folder_id = get_or_create_folder('受注集計')
        today_str = datetime.now().strftime('%Y%m%d')
        date_folder_id = get_or_create_folder(today_str, root_folder_id)
        line_folder_id = get_or_create_folder('Line画像保存', date_folder_id)
        get_or_create_folder('集計結果', date_folder_id)

        # アップロード
        file_metadata = {
            'name': f'{message_id}.jpg',
            'parents': [line_folder_id]
        }
        media = MediaFileUpload(file_path, mimetype='image/jpeg')
        uploaded = drive_service.files().create(body=file_metadata, media_body=media, fields='id').execute()

        file_id = uploaded.get('id')
        print(f"Uploaded to Google Drive. File ID: {file_id}")

        # あなたに共有
        share_file_with_user(file_id, USER_EMAIL)

    return 'OK', 200

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=10000)
