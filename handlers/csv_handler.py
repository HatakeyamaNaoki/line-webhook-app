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
from openai import OpenAI  # 新しいOpenAIクライアント
from .prompt_templates import normalize_product_name_prompt

CSV_HEADERS = pd.read_csv(CSV_FORMAT_PATH, encoding='utf-8').columns.tolist()
JST = pytz.timezone('Asia/Tokyo')


def normalize_product_name_ai(product_name, openai_client):
    # 生成AIでカタカナ統一
    response = openai_client.chat.completions.create(
        model="gpt-4o",
        messages=[
            {"role": "system", "content": normalize_product_name_prompt},
            {"role": "user", "content": product_name}
        ],
        max_tokens=10,
        temperature=0
    )
    return response.choices[0].message.content.strip()

def normalize_size(size):
    # 半角英数字・大文字化
    if pd.isnull(size):
        return ""
    return jaconv.z2h(str(size), kana=False, ascii=True, digit=True).upper().strip()

def normalize_quantity(quantity):
    # 半角数字のみ
    if pd.isnull(quantity):
        return ""
    return jaconv.z2h(str(quantity), kana=False, ascii=False, digit=True).strip()

def normalize_unit_ai(product_name, unit, quantity, openai_client):
    from prompt_templates import normalize_unit_prompt
    # AIに問い合わせ
    content = f"商品名: {product_name}\n単位: {unit}\n数量: {quantity}"
    response = openai_client.chat.completions.create(
        model="gpt-4o",
        messages=[
            {"role": "system", "content": normalize_unit_prompt},
            {"role": "user", "content": content}
        ],
        max_tokens=10,
        temperature=0
    )
    return response.choices[0].message.content.strip()

def adjust_quantity_and_unit(quantity, unit):
    # g系はkgに変換
    if unit in ["g", "グラム", "ｇ"]:
        try:
            return float(quantity) / 1000, "kg"
        except Exception:
            return quantity, unit  # 変換できなければそのまま
    elif unit in ["kg", "キログラム", "ＫＧ"]:
        return quantity, "kg"
    else:
        return quantity, unit

# 必要に応じてOpenAIクライアントをDI
def normalize_row(row, openai_client):
    # 商品名
    product_name = normalize_product_name_ai(row['商品名'], openai_client)
    # サイズ
    size = normalize_size(row['サイズ'])
    # 数量
    quantity = normalize_quantity(row['数量'])
    # 単位のAI正規化
    norm_unit = normalize_unit_ai(product_name, row['単位'], quantity, openai_client)
    # 数量・単位補正
    adj_quantity, adj_unit = adjust_quantity_and_unit(quantity, norm_unit)
    return {
        "商品名": product_name,
        "サイズ": size,
        "数量": adj_quantity,
        "単位": adj_unit,
        # 他の項目はそのまま
    }

def append_to_xlsx(structured_text, parent_id, openai_client):
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

    # デバッグ: Drive全体で見える同名ファイル一覧
    print("\n【デバッグ】Drive全体で見える同名ファイル一覧:")
    all_results = drive_service.files().list(
        q=f"name='{filename}' and trashed = false",
        fields="files(id, name, parents, owners)",
        pageSize=10
    ).execute()
    all_files = all_results.get('files', [])
    for f in all_files:
        print(f"ファイル名: {f['name']}, ファイルID: {f['id']}, 親: {f.get('parents')}, オーナー: {f['owners'][0]['displayName'] if f.get('owners') else '-'}")

    # 既存のxlsxファイルがDriveにあれば取得してマージ
    query = f"name = '{filename}' and '{parent_id}' in parents and trashed = false"
    response = drive_service.files().list(q=query, fields='files(id, name, parents, owners)').execute()
    files = response.get('files', [])
    print(f"【デバッグ】指定親フォルダ {parent_id} で見つかったファイル数: {len(files)}")
    for f in files:
        print(f"【デバッグ】指定親: ファイル名: {f['name']}, ファイルID: {f['id']}, 親: {f.get('parents')}, オーナー: {f['owners'][0]['displayName'] if f.get('owners') else '?'}")

    # 追加: Drive全体でのヒットも再掲
    all_query = f"name = '{filename}' and trashed = false"
    all_resp = drive_service.files().list(q=all_query, fields='files(id, name, parents, owners)').execute()
    all_files = all_resp.get('files', [])
    print(f"【デバッグ】Drive全体で '{filename}' のファイル数: {len(all_files)}")
    for f in all_files:
        print(f"【デバッグ】全体: ファイル名: {f['name']}, ファイルID: {f['id']}, 親: {f.get('parents')}, オーナー: {f['owners'][0]['displayName'] if f.get('owners') else '?'}")

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
    xlsx_with_summary_update(combined, file_path, openai_client)
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

def xlsx_with_summary_update(df, xlsx_path, openai_client):
    """ 1シート目: 生データ, 2シート目: 商品名セット集計サマリ で.xlsx作成
        ※openai_clientを引数で受け取る前提
    """
    df = df.copy()
    normalized_rows = []

    # 各行を新しい正規化ロジックで変換
    for _, row in df.iterrows():
        # 商品名（AIカタカナ化）
        product_name = normalize_product_name_ai(row['商品名'], openai_client)
        # サイズ（半角英数字）
        size = normalize_size(row['サイズ'])
        # 数量（半角数字に）
        quantity = normalize_quantity(row['数量'])
        # 単位（AI＆ルール正規化）
        norm_unit = normalize_unit_ai(product_name, row['単位'], quantity, openai_client)
        # 数量＆単位補正
        adj_quantity, adj_unit = adjust_quantity_and_unit(quantity, norm_unit)
        # 他項目はそのまま
        normalized_rows.append({
            "顧客": row.get("顧客", ""),
            "発注者": row.get("発注者", ""),
            "商品名": product_name,
            "サイズ": size,
            "数量": adj_quantity,
            "単位": adj_unit,
            "納品希望日": row.get("納品希望日", ""),
            "納品場所": row.get("納品場所", ""),
            "時間": row.get("時間", ""),
            "社内担当者": row.get("社内担当者", ""),
            "備考": row.get("備考", "")  # 備考はそのまま
        })

    df_norm = pd.DataFrame(normalized_rows)

    # 集計キーを商品名・サイズ・単位でまとめる（備考は使わない）
    df_norm['集計キー'] = (
        df_norm['商品名'].astype(str) + "_" +
        df_norm['サイズ'].astype(str) + "_" +
        df_norm['単位'].astype(str)
    )

    # サマリ生成（備考は集計キーやサマリ集約に使わない）
    summary = (
        df_norm.groupby('集計キー', as_index=False)
        .agg({
            '顧客': 'first',
            '発注者': 'first',
            '商品名': 'first',
            'サイズ': 'first',
            '数量': 'sum',
            '単位': 'first',
            '納品希望日': 'first',
            '納品場所': 'first',
            '時間': 'first',
            '社内担当者': 'first',
            '備考': 'first'
        })
    )
    # 並び順調整
    summary = summary[['顧客', '発注者', '商品名', 'サイズ', '数量', '単位', '納品希望日', '納品場所', '時間', '社内担当者', '備考']]
    summary = summary.sort_values('商品名')

    # xlsx出力
    wb = Workbook()
    ws_raw = wb.active
    ws_raw.title = os.path.splitext(os.path.basename(xlsx_path))[0]
    ws_raw.append(list(df_norm.columns.drop('集計キー')))
    for row in df_norm.drop(columns=['集計キー']).itertuples(index=False, name=None):
        ws_raw.append(row)
    ws_summary = wb.create_sheet("集計結果サマリ")
    ws_summary.append(list(summary.columns))
    for row in summary.itertuples(index=False, name=None):
        ws_summary.append(row)
    wb.save(xlsx_path)
    print(f"集計結果サマリシート付きで {xlsx_path} を作成しました")