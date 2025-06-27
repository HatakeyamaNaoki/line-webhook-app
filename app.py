from flask import Flask, request
import requests
import os

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload

app = Flask(__name__)
CHANNEL_ACCESS_TOKEN = os.environ.get('LINE_CHANNEL_ACCESS_TOKEN')

# Google Drive 認証
SCOPES = ['https://www.googleapis.com/auth/drive.file']
SERVICE_ACCOUNT_FILE = '/etc/secrets/credentials.json'  # RenderでSecretとして登録したファイルパス

credentials = service_account.Credentials.from_service_account_file(
    SERVICE_ACCOUNT_FILE, scopes=SCOPES
)
drive_service = build('drive', 'v3', credentials=credentials)

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

        # Google Drive にアップロード
        FOLDER_ID = '1pCUixzHNM4OfHGP86rbs1vtXBd8rqXD5'
        file_metadata = {
            'name': f'{message_id}.jpg',
            'parents': [FOLDER_ID]
        }
        media = MediaFileUpload(file_path, mimetype='image/jpeg')
        uploaded = drive_service.files().create(body=file_metadata, media_body=media, fields='id').execute()

        print(f"Uploaded to Google Drive. File ID: {uploaded.get('id')}")

    return 'OK', 200

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=10000)
