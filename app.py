from flask import Flask, request
import requests
import os
from datetime import datetime
import base64
import openai
import csv

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload

app = Flask(__name__)
CHANNEL_ACCESS_TOKEN = os.environ.get('LINE_CHANNEL_ACCESS_TOKEN')
OPENAI_API_KEY = os.environ.get('OPENAI_API_KEY')
openai.api_key = OPENAI_API_KEY

# Google Drive 認証
SCOPES = ['https://www.googleapis.com/auth/drive.file']
SERVICE_ACCOUNT_FILE = '/etc/secrets/credentials.json'
credentials = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)
drive_service = build('drive', 'v3', credentials=credentials)

def get_or_create_folder(folder_name, parent_id=None):
    print(f"フォルダ確認中: {folder_name}")
    query = f"name = '{folder_name}' and mimeType = 'application/vnd.google-apps.folder' and trashed = false"
    query += f" and '{parent_id}' in parents" if parent_id else " and 'root' in parents"
    response = drive_service.files().list(q=query, fields='files(id, name)').execute()
    files = response.get('files', [])
    if files:
        folder_id = files[0]['id']
        print(f"既存フォルダ発見: {folder_name} (ID: {folder_id})")
        return folder_id
    file_metadata = {
        'name': folder_name,
        'mimeType': 'application/vnd.google-apps.folder',
    }
    if parent_id:
        file_metadata['parents'] = [parent_id]
    folder = drive_service.files().create(body=file_metadata, fields='id').execute()
    print(f"新規フォルダ作成: {folder_name} (ID: {folder['id']})")
    return folder['id']

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

# ChatGPT Vision API を使って画像から構造化情報を抽出
def extract_order_info_from_image(image_path, sender_name):
    with open(image_path, "rb") as image_file:
        image_bytes = image_file.read()
    base64_image = base64.b64encode(image_bytes).decode("utf-8")
    image_data_url = f"data:image/jpeg;base64,{base64_image}"

    today_str = datetime.now().strftime('%Y%m%d%H')

    prompt = f"""
以下の画像はLINEの注文メッセージです。以下の形式で情報を抽出して、CSV形式に変換してください。

【CSVのヘッダー】
顧客,発注者,商品名,数量,納品希望日,納品場所,時間

【抽出ルール】
- 顧客: LINEの送信者名の前半（例: "株式会社リソースクリエイション 畠山萌乃" → "株式会社リソースクリエイション"）
- 発注者: LINEの送信者名の後半（同上 → "畠山萌乃"）
- 商品名, 数量, 納品希望日, 納品場所: メッセージ本文から
- 時間: 今の日時を "YYYYMMDDHH" で記載（今回: {today_str}）
- 項目がない場合は空欄でOK
- メッセージが複数に分かれていても漏れなく集計

【出力例】
株式会社〇〇,田中太郎,玉ねぎ,5,2025/07/01,銀座,2025062722

では、画像を解析して、CSV出力だけしてください。
"""

    response = openai.ChatCompletion.create(
        model="gpt-4o",
        messages=[
            {"role": "user", "content": [
                {"type": "text", "text": prompt},
                {"type": "image_url", "image_url": {"url": image_data_url}}
            ]}
        ],
        temperature=0.2
    )
    return response['choices'][0]['message']['content']

@app.route('/webhook', methods=['POST'])
def webhook():
    data = request.get_json()
    events = data.get('events', [])

    if not events:
        return 'OK', 200

    event = events[0]
    if event.get('message', {}).get('type') == 'image':
        message_id = event['message']['id']
        user_name = event.get('source', {}).get('userId', '不明ユーザー')
        now = datetime.now()
        today_str = now.strftime('%Y%m%d')
        time_str = now.strftime('%H%M')
        file_name = f"{today_str}_{time_str}.jpg"
        file_path = f"/tmp/{file_name}"

        # LINE画像を保存
        image_url = f'https://api-data.line.me/v2/bot/message/{message_id}/content'
        headers = {'Authorization': f'Bearer {CHANNEL_ACCESS_TOKEN}'}
        image_data = requests.get(image_url, headers=headers).content
        with open(file_path, 'wb') as f:
            f.write(image_data)

        # フォルダ作成と共有
        root_folder_id = get_or_create_folder('受注集計')
        share_folder_with_user(root_folder_id, 'hatake.hatake.hatake7@gmail.com')
        date_folder_id = get_or_create_folder(today_str, parent_id=root_folder_id)
        line_folder_id = get_or_create_folder('Line画像保存', parent_id=date_folder_id)
        result_folder_id = get_or_create_folder('集計結果', parent_id=date_folder_id)

        # 画像アップロード
        file_metadata = {
            'name': file_name,
            'parents': [line_folder_id]
        }
        media = MediaFileUpload(file_path, mimetype='image/jpeg')
        uploaded = drive_service.files().create(body=file_metadata, media_body=media, fields='id').execute()
        print(f"Uploaded to Google Drive. File ID: {uploaded.get('id')}")

        # ChatGPTで構造化
        csv_text = extract_order_info_from_image(file_path, user_name)

        # CSVファイル名とパス
        csv_filename = f"集計結果_{today_str}.csv"
        csv_local_path = f"/tmp/{csv_filename}"

        # ファイルがすでに存在するか確認
        query = f"name='{csv_filename}' and '{result_folder_id}' in parents and trashed=false"
        existing = drive_service.files().list(q=query, fields="files(id)").execute().get("files", [])
        if existing:
            # 既存CSVを一時保存 → 追記 → 上書き
            file_id = existing[0]["id"]
            request_file = drive_service.files().get_media(fileId=file_id)
            with open(csv_local_path, "wb") as f:
                downloader = MediaFileUpload(csv_local_path)
                downloader = drive_service._http.request(request_file.uri)[1]
                f.write(downloader)
        with open(csv_local_path, "a", encoding="utf-8") as f:
            f.write(csv_text + "\n")

        # アップロードまたは上書き
        media = MediaFileUpload(csv_local_path, mimetype='text/csv')
        if existing:
            drive_service.files().update(fileId=file_id, media_body=media).execute()
        else:
            metadata = {
                'name': csv_filename,
                'parents': [result_folder_id]
            }
            drive_service.files().create(body=metadata, media_body=media, fields='id').execute()
            print(f"CSVファイル作成: {csv_filename}")

    return 'OK', 200

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=10000)
