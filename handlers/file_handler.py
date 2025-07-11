from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from config import SERVICE_ACCOUNT_FILE, SCOPES, SHARED_DRIVE_ID
from google.oauth2 import service_account
import os

credentials = service_account.Credentials.from_service_account_file(
    SERVICE_ACCOUNT_FILE, scopes=SCOPES
)
drive_service = build('drive', 'v3', credentials=credentials)

def get_or_create_folder(folder_name, parent_id=SHARED_DRIVE_ID):
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

def get_unique_filename(file_name, folder_id):
    """必ず_3桁連番（_001, _002...）でファイル名を返す"""
    base, ext = os.path.splitext(file_name)
    for i in range(1, 1000):
        new_name = f"{base}_{i:03d}{ext}"
        query = f"name = '{new_name}' and '{folder_id}' in parents and trashed = false"
        res = drive_service.files().list(q=query, fields='files(id)').execute()
        if not res.get('files'):
            return new_name
    raise Exception("Unique filename could not be determined (too many duplicates).")

def save_image_to_drive(image_data, file_name, folder_id):
    unique_name = get_unique_filename(file_name, folder_id)
    file_path = f"/tmp/{unique_name}"
    with open(file_path, 'wb') as f:
        f.write(image_data)
    file_metadata = {'name': unique_name, 'parents': [folder_id]}
    media = MediaFileUpload(file_path, mimetype='image/jpeg')
    drive_service.files().create(body=file_metadata, media_body=media).execute()
    os.remove(file_path)

def save_text_to_drive(text, file_name, folder_id):
    unique_name = get_unique_filename(file_name, folder_id)
    file_path = f"/tmp/{unique_name}"
    with open(file_path, 'w', encoding='utf-8') as f:
        f.write(text)
    file_metadata = {'name': unique_name, 'parents': [folder_id]}
    media = MediaFileUpload(file_path, mimetype='text/plain')
    drive_service.files().create(body=file_metadata, media_body=media).execute()
    os.remove(file_path)

def save_pdf_to_drive(pdf_data, file_name, folder_id):
    unique_name = get_unique_filename(file_name, folder_id)
    file_path = f"/tmp/{unique_name}"
    with open(file_path, 'wb') as f:
        f.write(pdf_data)
    file_metadata = {'name': unique_name, 'parents': [folder_id]}
    media = MediaFileUpload(file_path, mimetype='application/pdf')
    drive_service.files().create(body=file_metadata, media_body=media).execute()
    os.remove(file_path)
