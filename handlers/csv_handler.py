import pandas as pd
import io
from handlers.file_handler import drive_service
from googleapiclient.http import MediaFileUpload, MediaIoBaseDownload
from config import CSV_FORMAT_PATH
import pytz
from datetime import datetime
import unicodedata
import os
from openpyxl import Workbook

CSV_HEADERS = pd.read_csv(CSV_FORMAT_PATH, encoding='utf-8').columns.tolist()
JST = pytz.timezone('Asia/Tokyo')

def normalize_name(text):
    # カタカナ・ひらがな・漢字・大文字小文字の差を吸収
    if pd.isnull(text):
        return ""
    text = unicodedata.normalize("NFKC", str(text))
    return text.lower()

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
        print("DEBUG line (repr):", repr(line))
        line_stripped = line.strip()
        if not line_stripped:
            continue
        cols = [c.strip() for c in line_stripped.split(',')]
        if len(cols) == len(CSV_HEADERS):
            valid_lines.append(",".join(cols))
        else:
            invalid_lines.append(line)

    if not valid_lines:
        print("CSV_HEADERS:", CSV_HEADERS, "len:", len(CSV_HEADERS))
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

    # ここでExcel出力＋サマリー
    try:
        xlsx_path = csv_to_xlsx_with_summary(file_path)
        print(f"Excelファイル作成成功: {xlsx_path}")
        # エクセルもDriveにアップロード
        xlsx_file_metadata = {'name': os.path.basename(xlsx_path), 'parents': [parent_id]}
        xlsx_media = MediaFileUpload(xlsx_path, mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
        drive_service.files().create(body=xlsx_file_metadata, media_body=xlsx_media).execute()
    except Exception as e:
        print("Excelファイル作成エラー:", e)

def csv_to_xlsx_with_summary(csv_path):
    # 1. CSVをDataFrameで読み込み
    df = pd.read_csv(csv_path, dtype=str).fillna("")
    # 2. 数量カラムを数値に
    df['数量'] = pd.to_numeric(df['数量'], errors='coerce').fillna(0)
    # 3. 集計キー（normalizeで吸収）
    df['集計キー'] = (
        df['商品名'].map(normalize_name) + '_' +
        df['サイズ'].str.upper().str.strip() + '_' +
        df['単位'].map(normalize_name) + '_' +
        df['備考'].map(normalize_name)
    )
    # 4. サマリ用グループ化
    summary = (
        df.groupby('集計キー', as_index=False)
        .agg({
            '商品名': 'first',
            'サイズ': lambda x: x.str.upper().str.strip().iloc[0],
            '単位': 'first',
            '備考': 'first',
            '数量': 'sum'
        })
    )
    # 5. サマリ用に空欄列を追加（ヘッダーに合わせて）
    for col in ['顧客', '発注者', '納品希望日', '納品場所', '時間', '社内担当者']:
        summary[col] = ""
    columns = ['顧客', '発注者', '商品名', 'サイズ', '数量', '単位', '納品希望日', '納品場所', '時間', '社内担当者', '備考']
    summary = summary[columns]
    summary = summary.sort_values('商品名')

    # 6. XLSX保存
    xlsx_path = csv_path.replace('.csv', '.xlsx')
    wb = Workbook()
    ws_raw = wb.active
    ws_raw.title = os.path.splitext(os.path.basename(csv_path))[0]
    # RAW: 1シート目
    ws_raw.append(CSV_HEADERS)
    for row in df[CSV_HEADERS].itertuples(index=False, name=None):
        ws_raw.append(row)
    # サマリ: 2シート目
    ws_summary = wb.create_sheet("集計結果サマリ")
    ws_summary.append(columns)
    for row in summary.itertuples(index=False, name=None):
        ws_summary.append(row)
    wb.save(xlsx_path)
    print(f"集計結果サマリシート付きで {xlsx_path} を作成しました")
    return xlsx_path
