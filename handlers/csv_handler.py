import pandas as pd
import io
from handlers.file_handler import drive_service
from googleapiclient.http import MediaFileUpload, MediaIoBaseDownload
from config import CSV_FORMAT_PATH
import pytz
from datetime import datetime

CSV_HEADERS = pd.read_csv(CSV_FORMAT_PATH, encoding='utf-8').columns.tolist()
JST = pytz.timezone('Asia/Tokyo')

def append_to_csv(structured_text, parent_id):
    if not structured_text.strip():
        with open("/tmp/failed_structured_text.txt", "w", encoding="utf-8") as f:
            f.write("No structured_text received!\n")
        print("No structured_text received! ログを保存しました。")
        return
    today = datetime.now(JST).strftime('%Y%m%d')
    filename = f'集計結果_{today}.csv'
    file_path = f'/tmp/{filename}'
    lines = structured_text.strip().splitlines()
    valid_lines = []
    invalid_lines = []
    for line in lines:
        if len(line.split(',')) == len(CSV_HEADERS):
            valid_lines.append(line)
        else:
            invalid_lines.append(line)
    if not valid_lines:
        print("structured_text (for debug):\n", structured_text)
        print("⚠ 有効な行がありません。全行ログ保存")
        with open(f"/tmp/failed_structured_{today}.txt", "w", encoding="utf-8") as f:
            f.write(structured_text)
        return
    if invalid_lines:
        with open(f"/tmp/invalid_structured_{today}.txt", "w", encoding="utf-8") as f:
            f.write("\n".join(invalid_lines))
    structured_text_cleaned = "\n".join(valid_lines)
    try:
        new_data = pd.read_csv(io.StringIO(structured_text_cleaned), header=None, names=CSV_HEADERS)
    except Exception as e:
        print("CSV parsing error:", e)
        with open(f"/tmp/csv_parse_error_{today}.txt", "w", encoding="utf-8") as f:
            f.write(structured_text)
        return
    now_str = datetime.now(JST).strftime('%Y%m%d%H')
    new_data['時間'] = now_str
    query = f"name = '{filename}' and '{parent_id}' in parents and trashed = false"
    response = drive_service.files().list(q=query, fields='files(id)').execute()
    files = response.get('files', [])
    if files:
        file_id = files[0]['id']
        request = drive_service.files().get_media(fileId=file_id)
        fh = io.BytesIO()
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while not done:
            status, done = downloader.next_chunk()
        fh.seek(0)
        existing = pd.read_csv(fh)
        combined = pd.concat([existing, new_data], ignore_index=True)
        combined.to_csv(file_path, index=False, encoding='utf-8-sig')
        media = MediaFileUpload(file_path, mimetype='text/csv')
        drive_service.files().update(fileId=file_id, media_body=media).execute()
    else:
        new_data.to_csv(file_path, index=False, encoding='utf-8-sig')
        file_metadata = {'name': filename, 'parents': [parent_id]}
        media = MediaFileUpload(file_path, mimetype='text/csv')
        drive_service.files().create(body=file_metadata, media_body=media).execute()
