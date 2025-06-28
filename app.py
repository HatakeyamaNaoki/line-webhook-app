import os
import io
import base64
from flask import Flask, request, abort
from linebot import LineBotApi, WebhookHandler
from linebot.exceptions import InvalidSignatureError
from linebot.models import MessageEvent, ImageMessage
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload
from openai import OpenAI
from datetime import datetime
import pandas as pd

# Flaskアプリの初期化
app = Flask(__name__)

# LINEの設定
LINE_CHANNEL_ACCESS_TOKEN = os.environ.get("LINE_CHANNEL_ACCESS_TOKEN")
LINE_CHANNEL_SECRET = os.environ.get("LINE_CHANNEL_SECRET")
line_bot_api = LineBotApi(LINE_CHANNEL_ACCESS_TOKEN)
handler = WebhookHandler(LINE_CHANNEL_SECRET)

# OpenAI初期化（新クライアント）
client = OpenAI()

# Google Drive認証
SERVICE_ACCOUNT_FILE = "credentials.json"
SCOPES = ["https://www.googleapis.com/auth/drive"]
credentials = service_account.Credentials.from_service_account_file(
    SERVICE_ACCOUNT_FILE, scopes=SCOPES
)
drive_service = build("drive", "v3", credentials=credentials)

# フォルダ取得または作成
def get_or_create_folder(name, parent_id=None):
    query = f"name='{name}' and mimeType='application/vnd.google-apps.folder' and trashed=false"
    if parent_id:
        query += f" and '{parent_id}' in parents"
    response = drive_service.files().list(q=query, fields="files(id, name)").execute()
    files = response.get("files", [])
    if files:
        return files[0]['id']
    file_metadata = {
        'name': name,
        'mimeType': 'application/vnd.google-apps.folder'
    }
    if parent_id:
        file_metadata['parents'] = [parent_id]
    folder = drive_service.files().create(body=file_metadata, fields='id').execute()
    return folder['id']

# LINE Webhookエンドポイント
@app.route("/callback", methods=['POST'])
def callback():
    signature = request.headers['X-Line-Signature']
    body = request.get_data(as_text=True)
    try:
        handler.handle(body, signature)
    except InvalidSignatureError:
        abort(400)
    return 'OK'

# メッセージ受信時の処理
@handler.add(MessageEvent, message=ImageMessage)
def handle_image_message(event):
    message_id = event.message.id
    image_content = line_bot_api.get_message_content(message_id).content
    today = datetime.now().strftime("%Y%m%d")

    # Google Driveのフォルダ構造生成
    root_folder = get_or_create_folder("受注集計")
    date_folder = get_or_create_folder(today, root_folder)
    image_folder = get_or_create_folder("Line画像保存", date_folder)
    result_folder = get_or_create_folder("集計結果", date_folder)

    # ファイル名定義
    image_filename = datetime.now().strftime("%Y%m%d_%H%M%S") + ".jpg"

    # Driveへ画像アップロード
    file_metadata = {
        'name': image_filename,
        'parents': [image_folder],
        'mimeType': 'image/jpeg'
    }
    media = MediaIoBaseUpload(io.BytesIO(image_content), mimetype='image/jpeg')
    image_file = drive_service.files().create(body=file_metadata, media_body=media, fields='id').execute()

    # OpenAI OCR処理
    base64_image = base64.b64encode(image_content).decode("utf-8")
    response = client.chat.completions.create(
        model="gpt-4o",
        messages=[
            {"role": "system", "content": "以下の画像に写っている注文情報をCSV形式で抽出してください。ヘッダーは『顧客名,青果名,数量,納期,場所』としてください。"},
            {"role": "user", "content": [{"type": "image_url", "image_url": {"url": f"data:image/jpeg;base64,{base64_image}"}}]}
        ],
        temperature=0.0,
        max_tokens=1000
    )
    csv_text = response.choices[0].message.content

    # CSVとしてDriveに保存（追記対応）
    csv_filename = f"{today}_集計.csv"
    csv_path = f"/tmp/{csv_filename}"

    with open(csv_path, "a", encoding="utf-8") as f:
        f.write(csv_text.strip() + "\n")

    file_metadata = {
        'name': csv_filename,
        'parents': [result_folder],
        'mimeType': 'text/csv'
    }
    media = MediaIoBaseUpload(io.FileIO(csv_path, "rb"), mimetype='text/csv')
    drive_service.files().create(body=file_metadata, media_body=media, fields='id').execute()

if __name__ == "__main__":
    app.run(debug=True)
