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

# フォルダ取得/作成関数（ログあり）
def get_or_create_folder(service, name, parent_id=None):
    print(f"フォルダ確認中: {name}")
    query = f"name='{name}' and mimeType='application/vnd.google-apps.folder' and trashed=false"
    if parent_id:
        query += f" and '{parent_id}' in parents"
    else:
        query += " and 'root' in parents"

    results = service.files().list(q=query, spaces='drive', fields='files(id, name)').execute()
    folders = results.get('files', [])
    if folders:
        folder_id = folders[0]['id']
        print(f"既存フォルダ発見: {name} (ID: {folder_id})")
        return folder_id

    print(f"フォルダ作成中: {name}")
    metadata = {
        'name': name,
        'mimeType': 'application/vnd.google-apps.folder'
    }
    if parent_id:
        metadata['parents'] = [parent_id]
    try:
        folder = service.files().create(body=metadata, fields='id').execute()
        print(f"フォルダ作成成功: {name} (ID: {folder['id']})")
        return folder['id']
    except Exception as e:
        print(f"フォルダ作成失敗: {name} → {str(e)}")
        raise

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

        # フォルダ構成の作成
        try:
            root_folder = get_or_create_folder(drive_service, '受注集計')
            date_folder_name = datetime.now().strftime('%Y%m%d')
            date_folder = get_or_create_folder(drive_service, date_folder_name, root_folder)
            line_folder = get_or_create_folder(drive_service, 'Line画像保存', date_folder)
            get_or_create_folder(drive_service, '集計結果', date_folder)
        except Exception as e:
            print(f"フォルダ構成エラー: {str(e)}")
            return 'Internal Server Error', 500

        # Google Drive にアップロード
        file_metadata = {
            'name': f'{message_id}.jpg',
            'parents': [line_folder]
        }
        media = MediaFileUpload(file_path, mimetype='image/jpeg')
        uploaded = drive_service.files().create(body=file_metadata, media_body=media, fields='id').execute()

        print(f"Uploaded to Google Drive. File ID: {uploaded.get('id')}")

    return 'OK', 200

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=10000)
