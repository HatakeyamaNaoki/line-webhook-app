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

# 指定フォルダがなければ作成し、IDを返す
def get_or_create_folder(folder_name, parent_id=None):
    print(f"フォルダ確認中: {folder_name}")
    query = f"name = '{folder_name}' and mimeType = 'application/vnd.google-apps.folder' and trashed = false"
    if parent_id:
        query += f" and '{parent_id}' in parents"
    else:
        query += f" and 'root' in parents"

    response = drive_service.files().list(q=query, fields='files(id, name)').execute()
    files = response.get('files', [])
    if files:
        folder_id = files[0]['id']
        print(f"既存フォルダ発見: {folder_name} (ID: {folder_id})")
        return folder_id
    else:
        file_metadata = {
            'name': folder_name,
            'mimeType': 'application/vnd.google-apps.folder'
        }
        if parent_id:
            file_metadata['parents'] = [parent_id]

        folder = drive_service.files().create(body=file_metadata, fields='id').execute()
        print(f"新規フォルダ作成: {folder_name} (ID: {folder['id']})")
        return folder['id']

# 指定フォルダをユーザーと共有する
def share_folder_with_user(folder_id, user_email):
    permission = {
        'type': 'user',
        'role': 'writer',
        'emailAddress': user_email
    }
    try:
        drive_service.permissions().create(
            fileId=folder_id,
            body=permission,
            sendNotificationEmail=False
        ).execute()
        print(f"フォルダ '{folder_id}' を {user_email} と共有しました。")
    except Exception as e:
        print(f"共有エラー: {e}")

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

        # フォルダ階層作成
        root_folder_id = get_or_create_folder('受注集計')
        share_folder_with_user(root_folder_id, 'hatake.hatake.hatake7@gmail.com')  # フォルダ共有
        today_str = datetime.now().strftime('%Y%m%d')
        date_folder_id = get_or_create_folder(today_str, parent_id=root_folder_id)
        line_folder_id = get_or_create_folder('Line画像保存', parent_id=date_folder_id)
        get_or_create_folder('集計結果', parent_id=date_folder_id)

        # 画像アップロード
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
