from handlers.image_handler import process_image_message
from handlers.text_handler import process_text_message
from handlers.pdf_handler import process_pdf_message
from handlers.csv_handler import xlsx_with_summary_update  # サマリ生成
from handlers.file_handler import get_or_create_folder, drive_service
from googleapiclient.http import MediaIoBaseDownload, MediaFileUpload
from config import CSV_FORMAT_PATH

import os
import pytz
import pandas as pd
from datetime import datetime

def handle_webhook(request):
    data = request.get_json()
    events = data.get('events', [])
    if not events:
        return 'OK', 200

    event = events[0]
    message_type = event.get('message', {}).get('type')

    # --- テキストメッセージの場合（まずはサマリ作成指示か判定） ---
    if message_type == 'text':
        user_text = event['message'].get('text', '').strip()
        if user_text == '集計サマリ作成':
            JST = pytz.timezone('Asia/Tokyo')
            today = datetime.now(JST).strftime('%Y%m%d')

            # Driveの「受注集計＞{today}＞集計結果」までのIDを取得
            try:
                root_id = get_or_create_folder('受注集計')
                date_id = get_or_create_folder(today, parent_id=root_id)
                csv_folder_id = get_or_create_folder('集計結果', parent_id=date_id)
            except Exception as e:
                print(f"DriveフォルダID取得エラー: {e}")
                return 'OK', 200

            filename = f'集計結果_{today}.xlsx'
            file_path = f"/tmp/{filename}"

            # Drive内でファイルを検索
            query = f"name = '{filename}' and '{csv_folder_id}' in parents and trashed = false"
            response = drive_service.files().list(q=query, fields='files(id)').execute()
            files = response.get('files', [])
            if not files:
                print("集計ファイルが見つかりません")
                return 'OK', 200

            # ファイルを一時保存
            file_id = files[0]['id']
            try:
                request_dl = drive_service.files().get_media(fileId=file_id)
                with open(file_path, 'wb') as fh:
                    downloader = MediaIoBaseDownload(fh, request_dl)
                    done = False
                    while not done:
                        status, done = downloader.next_chunk()
            except Exception as e:
                print(f"DriveファイルDLエラー: {e}")
                return 'OK', 200

            # サマリ生成
            try:
                df = pd.read_excel(file_path)
                xlsx_with_summary_update(df, file_path)
                print(f"集計サマリ作成のみ実施: {file_path}")

                # ----- ここでDriveへ再アップロード（上書き） -----
                media = MediaFileUpload(file_path, mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
                drive_service.files().update(
                    fileId=file_id,
                    media_body=media
                ).execute()
                print(f"サマリ生成後にDriveへ再アップロード完了: {filename}")

            except Exception as e:
                print(f"サマリ生成またはDriveアップロードエラー: {e}")

            return 'OK', 200

        # 通常テキスト（注文等）は既存ハンドラへ
        process_text_message(event)

    # --- 画像(jpg, png, etc) ---
    elif message_type == 'image':
        process_image_message(event)

    # --- ファイル(PDF含む) ---
    elif message_type == 'file':
        file_name = event['message'].get('fileName', '').lower()
        if file_name.endswith('.pdf'):
            process_pdf_message(event)
        # 他のファイル型は必要に応じてハンドラ追加

    return 'OK', 200
