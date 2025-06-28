import os
import base64
import datetime
import io
from flask import Flask, request, abort
from linebot import LineBotApi, WebhookHandler
from linebot.exceptions import InvalidSignatureError
from linebot.models import MessageEvent, ImageMessage
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload
from openai import OpenAI
import pandas as pd

# === 初期設定 ===
app = Flask(__name__)
line_bot_api = LineBotApi(os.environ["LINE_CHANNEL_ACCESS_TOKEN"])
handler = WebhookHandler(os.environ["LINE_CHANNEL_SECRET"])
openai_client = OpenAI(api_key=os.environ["OPENAI_API_KEY"])

# === Google Drive 認証 ===
SCOPES = ['https://www.googleapis.com/auth/drive']
SERVICE_ACCOUNT_FILE = 'credentials.json'
FOLDER_NAME = '受注集計'

creds = service_account.Credentials.from_service_account_file(
    SERVICE_ACCOUNT_FILE, scopes=SCOPES)
drive_service = build('drive', 'v3', credentials=creds)

# === フォルダを探す or 作成 ===
def get_or_create_folder(folder_name, parent_id=None):
    query = f"name='{folder_name}' and mimeType='application/vnd.google-apps.folder'"
    if parent_id:
        query += f" and '{parent_id}' in parents"
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
    file = drive_service.files().create(body=file_metadata, fields='id').execute()
    return file.get('id')

# === 画像アップロード ===
def upload_image_to_drive(image_bytes, filename, folder_id):
    media = MediaIoBaseUpload(io.BytesIO(image_bytes), mimetype='image/jpeg')
    file_metadata = {
        'name': filename,
        'parents': [folder_id],
    }
    drive_service.files().create(
        body=file_metadata,
        media_body=media,
        fields='id'
    ).execute()

# === 画像解析（GPT-4o） ===
def extract_order_info_from_image(image_bytes):
    base64_image = base64.b64encode(image_bytes).decode('utf-8')
    response = openai_client.chat.completions.create(
        model="gpt-4o",
        messages=[
            {
                "role": "system",
                "content": "以下の画像は手書き注文書です。CSV形式で構造化してください。項目は「顧客名, 青果名, 数量, 納期, 場所」でお願いします。"
            },
            {
                "role": "user",
                "content": [
                    {
                        "type": "image_url",
                        "image_url": {"url": f"data:image/jpeg;base64,{base64_image}"}
                    }
                ]
            }
        ],
        temperature=0.2,
        max_tokens=1024
    )
    return response.choices[0].message.content.strip()

# === CSVをDriveに保存（追記） ===
def append_csv_to_drive(csv_text, folder_id, date_str):
    filename = f"{date_str}.csv"
    results = drive_service.files().list(
        q=f"name='{filename}' and '{folder_id}' in parents",
        fields="files(id, name)").execute()
    files = results.get('files', [])
    
    new_df = pd.read_csv(io.StringIO(csv_text))
    
    if files:
        file_id = files[0]['id']
        request = drive_service.files().get_media(fileId=file_id)
        file_data = request.execute()
        existing_df = pd.read_csv(io.BytesIO(file_data))
        combined_df = pd.concat([existing_df, new_df], ignore_index=True)
    else:
        combined_df = new_df

    output = io.BytesIO()
    combined_df.to_csv(output, index=False)
    output.seek(0)

    media = MediaIoBaseUpload(output, mimetype='text/csv')
    file_metadata = {'name': filename, 'parents': [folder_id]}
    
    if files:
        drive_service.files().update(fileId=files[0]['id'], media_body=media).execute()
    else:
        drive_service.files().create(body=file_metadata, media_body=media).execute()

# === LINEのWebhookエンドポイント ===
@app.route("/callback", methods=['POST'])
def callback():
    signature = request.headers['X-Line-Signature']
    body = request.get_data(as_text=True)
    try:
        handler.handle(body, signature)
    except InvalidSignatureError:
        abort(400)
    return 'OK'

# === LINEから画像受信時の処理 ===
@handler.add(MessageEvent, message=ImageMessage)
def handle_image_message(event):
    message_content = line_bot_api.get_message_content(event.message.id)
    image_data = b''.join(chunk for chunk in message_content.iter_content())

    now = datetime.datetime.now()
    date_str = now.strftime("%Y%m%d")
    time_str = now.strftime("%H%M%S")

    base_folder_id = get_or_create_folder(FOLDER_NAME)
    date_folder_id = get_or_create_folder(date_str, parent_id=base_folder_id)
    image_folder_id = get_or_create_folder("Line画像保存", parent_id=date_folder_id)
    csv_folder_id = get_or_create_folder("集計結果", parent_id=date_folder_id)

    image_filename = f"{date_str}_{time_str}.jpg"
    upload_image_to_drive(image_data, image_filename, image_folder_id)

    extracted_csv = extract_order_info_from_image(image_data)
    append_csv_to_drive(extracted_csv, csv_folder_id, date_str)

    line_bot_api.reply_message(
        event.reply_token,
        TextSendMessage(text="画像を受け取りました。内容をDriveに保存しました。")
    )

# === アプリ起動 ===
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
