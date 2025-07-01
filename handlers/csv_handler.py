import pandas as pd
import io
from handlers.file_handler import drive_service
from googleapiclient.http import MediaFileUpload, MediaIoBaseDownload
from config import CSV_FORMAT_PATH
import pytz
from datetime import datetime
import unicodedata
import os
from openpyxl import Workbook, load_workbook
import jaconv  # ひらがな→カタカナ正規化用

CSV_HEADERS = pd.read_csv(CSV_FORMAT_PATH, encoding='utf-8').columns.tolist()
JST = pytz.timezone('Asia/Tokyo')

# 商品名・単位・備考などカタカナ化
def normalize_item_name(text):
    if pd.isnull(text):
        return ""
    text = str(text).strip()
    # ひらがな→カタカナ
    text = jaconv.hira2kata(text)
    # 半角カナ→全角カナ
    text = jaconv.h2z(text, kana=True, ascii=False, digit=False)
    lower = text.lower()
    mapping = {
        "トマト": ["トマト", "ＴＯＭＡＴＯ", "とまと", "tomato", "ＴＯＭＡＴＯ"],
        "キュウリ": ["キュウリ", "胡瓜", "きゅうり", "ｷｭｳﾘ", "cucumber", "ＣＵＣＵＭＢＥＲ"],
        "ナス": ["ナス", "なす", "茄子", "ｎａｓｕ", "nasu", "ＮＡＳＵ"],
    }
    for katakana, pats in mapping.items():
        for pat in pats:
            if lower == pat.lower():
                return katakana
    return text

def normalize_size(size):
    if pd.isnull(size):
        return ""
    # 全角→半角、小文字→大文字
    return jaconv.z2h(str(size), kana=False, ascii=True, digit=True).upper().strip()

def append_to_xlsx(structured_text, parent_id):
    """ 受け取った注文データを .xlsx で保存/追記しDriveに反映、サマリも作成 """
    if not structured_text.strip():
        with open("/tmp/failed_structured_text.txt", "w", encoding="utf-8") as f:
            f.write("No structured_text received!\n")
        print("No structured_text received! ログを保存しました。")
        return

    today = datetime.now(JST).strftime('%Y%m%d')
    filename = f'集計結果_{today}.xlsx'
    file_path = f'/tmp/{filename}'

    # 注文データをDataFrameに
    lines = structured_text.strip().splitlines()
    valid_lines = []
    for line in lines:
        line_stripped = line.strip()
        if not line_stripped:
            continue
        cols = [c.strip() for c in line_stripped.split(',')]
        if len(cols) == len(CSV_HEADERS):
            valid_lines.append(",".join(cols))

    if not valid_lines:
        print("⚠ 有効な行がありません。全行ログ保存")
        with open(f"/tmp/failed_structured_{today}.txt", "w", encoding="utf-8") as f:
            f.write(structured_text)
        return

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

    # 既存のxlsxファイルがDriveにあれば取得してマージ
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
        existing = pd.read_excel(fh)
        combined = pd.concat([existing, new_data], ignore_index=True)
    else:
        combined = new_data

    # サマリも含めたxlsxで保存＆Drive反映
    xlsx_with_summary_update(combined, file_path)
    try:
        # Driveへ新規 or update
        media = MediaFileUpload(file_path, mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
        if files:
            drive_service.files().update(fileId=file_id, media_body=media).execute()
        else:
            file_metadata = {'name': filename, 'parents': [parent_id]}
            drive_service.files().create(body=file_metadata, media_body=media).execute()
        print(f"Excelファイル作成成功: {file_path}")
    except Exception as e:
        print("Excelファイル作成/アップロードエラー:", e)

def xlsx_with_summary_update(df, xlsx_path):
    """ 1シート目: 生データ, 2シート目: 商品名セット集計サマリ で.xlsx作成 """
    # データ正規化
    df = df.copy()
    df['数量'] = pd.to_numeric(df['数量'], errors='coerce').fillna(0)
    df['商品名正規化'] = df['商品名'].map(normalize_item_name)
    df['サイズ正規化'] = df['サイズ'].map(normalize_size)
    df['単位正規化'] = df['単位'].map(normalize_item_name)
    df['備考正規化'] = df['備考'].map(normalize_item_name)
    df['集計キー'] = (
        df['商品名正規化'] + "_" +
        df['サイズ正規化'] + "_" +
        df['単位正規化'] + "_" +
        df['備考正規化']
    )

    # グループ化してサマリ生成
    summary = (
        df.groupby('集計キー', as_index=False)
        .agg({
            '商品名正規化': 'first',
            'サイズ正規化': 'first',
            '数量': 'sum',
            '単位正規化': 'first',
            '備考正規化': 'first'
        })
    )
    # サマリ用に空欄列を追加（ヘッダーに合わせて）
    for col in ['顧客', '発注者', '納品希望日', '納品場所', '時間', '社内担当者']:
        summary[col] = ""
    columns = ['顧客', '発注者', '商品名正規化', 'サイズ正規化', '数量', '単位正規化', '納品希望日', '納品場所', '時間', '社内担当者', '備考正規化']
    summary = summary[columns]
    summary.columns = ['顧客', '発注者', '商品名', 'サイズ', '数量', '単位', '納品希望日', '納品場所', '時間', '社内担当者', '備考']
    summary = summary.sort_values('商品名')

    # xlsx出力
    wb = Workbook()
    ws_raw = wb.active
    ws_raw.title = os.path.splitext(os.path.basename(xlsx_path))[0]
    ws_raw.append(CSV_HEADERS)
    for row in df[CSV_HEADERS].itertuples(index=False, name=None):
        ws_raw.append(row)
    ws_summary = wb.create_sheet("集計結果サマリ")
    ws_summary.append(list(summary.columns))
    for row in summary.itertuples(index=False, name=None):
        ws_summary.append(row)
    wb.save(xlsx_path)
    print(f"集計結果サマリシート付きで {xlsx_path} を作成しました")
