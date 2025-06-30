from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from config import SERVICE_ACCOUNT_FILE, SCOPES
from google.oauth2 import service_account
import os
import re
from datetime import datetime
import pytz

credentials = service_account.Credentials.from_service_account_file(
    SERVICE_ACCOUNT_FILE, scopes=SCOPES
)
drive_service = build('drive', 'v3', credentials=credentials)

def get_or_create_folder(folder_name, parent_id=None):
    query = f"name = '{folder_name}' and mimeType = 'application/vnd.google-apps.folder' and trashed = false"
    if parent_id:
        query += f" and '{parent_id}' in parents"
    else:
        query += f" and 'root' in parents"
    response = drive_service.files().list(q=query, fields='files(id, name)').execute()
    files = response.get('files', [])
    if files:
        return files[0]['id']
    file_metadata = {'name': folder_name, 'mimeType': 'application/vnd.google-apps.folder'}
    if parent_id:
        file_metadata['parents'] = [parent_id]
    folder = drive_service.files().create(body=file_metadata, fields='id').execute()
    return folder['id']

def get_next_sequential_filename(folder_id, ext, date_prefix=None):
    # 今日の日付をデフォルトで使う
    if date_prefix is None:
        JST = pytz.timezone('Asia/Tokyo')
        date_prefix = datetime.now(JST).strftime('%Y%m%d')
    pattern = re.compile(rf'^{date_prefix}_(\d{{3}})\.{ext}$')
    response = drive_service.files().list(
        q=f"'{folder_id}' in parents and trashed = false",
        fields='files(name)').execute()
    files = response.get('files', [])
    numbers = []
    for f in files:
        m = pattern.match(f['name'])
        if m:
            numbers.append(int(m.group(1)))
    next_number = max(numbers) + 1 if numbers else 1
    return f"{date_prefix}_{next_number:03d}.{ext}"

def save_image_to_drive(image_data, folder_id):
    file_name = get_next_sequential_filename(folder_id, 'jpg')
    file_path = f'/tmp/{file_name}'
    with open(file_path, 'wb') as f:
        f.write(image_data)
    file_metadata = {'name': file_name, 'parents': [folder_id]}
    media = MediaFileUpload(file_path, mimetype='image/jpeg')
    drive_service.files().create(body=file_metadata, media_body=media).execute()
    os.remove(file_path)
    return file_name

def save_text_to_drive(text, folder_id):
    file_name = get_next_sequential_filename(folder_id, 'txt')
    file_path = f'/tmp/{file_name}'
    with open(file_path, 'w', encoding='utf-8') as f:
        f.write(text)
    file_metadata = {'name': file_name, 'parents': [folder_id]}
    media = MediaFileUpload(file_path, mimetype='text/plain')
    drive_service.files().create(body=file_metadata, media_body=media).execute()
    os.remove(file_path)
    return file_name

def save_pdf_to_drive(pdf_data, folder_id):
    file_name = get_next_sequential_filename(folder_id, 'pdf')
    file_path = f'/tmp/{file_name}'
    with open(file_path, 'wb') as f:
        f.write(pdf_data)
    file_metadata = {'name': file_name, 'parents': [folder_id]}
    media = MediaFileUpload(file_path, mimetype='application/pdf')
    drive_service.files().create(body=file_metadata, media_body=media).execute()
    os.remove(file_path)
    return file_name
