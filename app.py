from flask import Flask, request
import requests
import os
from datetime import datetime, timedelta
import base64
import pandas as pd
from openai import OpenAI
import io
import pytz

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload, MediaIoBaseDownload

app = Flask(__name__)
CHANNEL_ACCESS_TOKEN = os.environ.get('LINE_CHANNEL_ACCESS_TOKEN')

openai_client = OpenAI(
    base_url="https://api.openai.com/v1",
    api_key=os.environ["OPENAI_API_KEY"]
)

SCOPES = ['https://www.googleapis.com/auth/drive.file']
SERVICE_ACCOUNT_FILE = '/etc/secrets/credentials.json'

credentials = service_account.Credentials.from_service_account_file(
    SERVICE_ACCOUNT_FILE, scopes=SCOPES
)
drive_service = build('drive', 'v3', credentials=credentials)

CSV_FORMAT_PATH = '集計フォーマット.csv'
CSV_HEADERS = pd.read_csv(CSV_FORMAT_PATH, encoding='utf-8').columns.tolist()

JST = pytz.timezone('Asia/Tokyo')

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

def analyze_image_with_gpt(image_path, operator_name, max_retries=3):
    with open(image_path, "rb") as image_file:
        image_base64 = base64.b64encode(image_file.read()).decode("utf-8")

    now = datetime.now(JST)
    now_str = now.strftime("%Y%m%d%H")
    now_verbose = now.strftime("%Y年%m月%d日 %H時")

    prompt = f"""
以下の画像に含まれる注文内容を、CSV形式で構造化してください。
- 出力はカンマ区切り、以下の順番と一致させてください。
- 数量は数値＋単位に分けて記載してください（例：10, 玉）
- 「玉レタス1個」や「青ネギ3束」など、数量の直後に「個」「箱」「束」など単位が記載されている場合は、「数量」には数値のみ、「単位」にはその単位（例：「個」「箱」「束」など）を正確に記載してください。
  - 例：「玉レタス1個」→ 商品名: 玉レタス, 数量: 1, 単位: 個
  - 例：「青ネギ3束」→ 商品名: 青ネギ, 数量: 3, 単位: 束
- 商品名に単位が含まれておらず、数量のあとに単位がついていない場合は「単位」カラムは空白で良いです。
- 「小さい」「大きめ」などの形容詞は備考欄に記載してください。
- 何か注意点がある際にも備考欄に記載してください。
- ヘッダーは出力せず、データ部分のみ複数行で出力してください。
- 不要な補足文（例：「この情報を参考にしてください」など）は出力しないでください。
- 顧客名と発注者名は画像上部のテキストから会社名と人名を抽出して出力してください。
- "..." のような行や意味のない行は出力しないでください。
- 納品希望日が「明日」「明後日」「3日後」など相対的な表現の場合は、以下の「現在日時（日本時間）」を基準に、「明日＝+1日」「明後日＝+2日」「3日後＝+3日」として正確に日付を加算し、YYYYMMDD形式で出力してください（※月またぎ・年またぎにも対応すること）。
  特に「明後日」は+2日、「3日後」は+3日、「4日後」は+4日というふうに、語に対応する日数を厳密に解釈してください。
- 現在日時（日本時間）: {now_verbose}（JST）
- 社内担当者は常に「{operator_name}」としてください（画像から読み取らない）。
- 読み取りができない場合でも、謝罪や案内文は出力せず、読み取れる範囲でデータのみを返してください。
- 時間列には常に「{now_str}」を出力してください。
- FAXや注文書内に住所や納品場所らしきテキストがあれば“納品場所”カラムに正確に抜き出して記載してください。その際に、郵便番号があれば、郵便番号、住所の順に記載してください。
- 注文書（PDFやFAX）では、宛名欄（通常は左上）や本文冒頭には、その注文書の受取先（発注先・自社）が記載されています。※LINEのスクショ画面の場合は、無視してください。
  一方、注文元（顧客）は、右上または「会社情報欄」（住所・電話番号・担当者などがまとまっている欄）に記載されています。※LINEのスクショ画面の場合は、無視してください。

列順: 顧客,発注者,商品名,数量,単位,納品希望日,納品場所,時間,社内担当者,備考

以下が画像データです：
    """

    for attempt in range(max_retries):
        response = openai_client.chat.completions.create(
            model="gpt-4o",
            messages=[
                {"role": "system", "content": "あなたは画像の内容をCSV形式に変換するアシスタントです。"},
                {"role": "user", "content": [
                    {"type": "text", "text": prompt},
                    {"type": "image_url", "image_url": {"url": f"data:image/jpeg;base64,{image_base64}"}}
                ]}
            ],
            max_tokens=1000,
            temperature=0.2
        )
        content = response.choices[0].message.content.strip()
        print("GPT Response Content:\n", content) # デバッグ用
        if "申し訳ありません" in content or "直接抽出することはできません" in content:
            continue
        lines = content.splitlines()
        cleaned_lines = [line for line in lines if not line.strip().startswith("この情報") and line.strip() != "..." and line.strip() != "…"]
        return "\n".join(cleaned_lines)
    print("構造化テキストが空です。GPT応答なしまたはすべて謝罪文")
    return ""

def analyze_text_with_gpt(text, operator_name, max_retries=3):
    now = datetime.now(JST)
    now_str = now.strftime("%Y%m%d%H")
    now_verbose = now.strftime("%Y年%m月%d日 %H時")

    prompt = f"""
以下の注文内容（テキスト）をCSV形式で構造化してください。
- 出力はカンマ区切り、以下の順番と一致させてください。
- 数量は数値＋単位に分けて記載してください（例：10, 玉）
- 「玉レタス1個」や「青ネギ3束」など、数量の直後に「個」「箱」「束」など単位が記載されている場合は、「数量」には数値のみ、「単位」にはその単位（例：「個」「箱」「束」など）を正確に記載してください。
  - 例：「玉レタス1個」→ 商品名: 玉レタス, 数量: 1, 単位: 個
  - 例：「青ネギ3束」→ 商品名: 青ネギ, 数量: 3, 単位: 束
- 商品名に単位が含まれておらず、数量のあとに単位がついていない場合は「単位」カラムは空白で良いです。
- 「小さい」「大きめ」などの形容詞は備考欄に記載してください。
- 何か注意点がある際にも備考欄に記載してください。
- ヘッダーは出力せず、データ部分のみ複数行で出力してください。
- 不要な補足文（例：「この情報を参考にしてください」など）は出力しないでください。
- 顧客名と発注者名は文頭のテキストから会社名と人名を抽出して出力してください。
- "..." のような行や意味のない行は出力しないでください。
- 納品希望日が「明日」「明後日」「3日後」など相対的な表現の場合は、以下の「現在日時（日本時間）」を基準に、「明日＝+1日」「明後日＝+2日」「3日後＝+3日」として正確に日付を加算し、YYYYMMDD形式で出力してください（※月またぎ・年またぎにも対応すること）。
  特に「明後日」は+2日、「3日後」は+3日、「4日後」は+4日というふうに、語に対応する日数を厳密に解釈してください。
- 現在日時（日本時間）: {now_verbose}（JST）
- 社内担当者は常に「{operator_name}」としてください（画像から読み取らない）。
- 読み取りができない場合でも、謝罪や案内文は出力せず、読み取れる範囲でデータのみを返してください。
- 時間列には常に「{now_str}」を出力してください。
- FAXや注文書内に住所や納品場所らしきテキストがあれば“納品場所”カラムに正確に抜き出して記載してください。その際に、郵便番号があれば、郵便番号、住所の順に記載してください。
- 注文書（PDFやFAX）では、宛名欄（通常は左上）や本文冒頭には、その注文書の受取先（発注先・自社）が記載されています。※LINEのスクショ画面の場合は、無視してください。
  一方、注文元（顧客）は、右上または「会社情報欄」（住所・電話番号・担当者などがまとまっている欄）に記載されています。※LINEのスクショ画面の場合は、無視してください。
  
列順: 顧客,発注者,商品名,数量,単位,納品希望日,納品場所,時間,社内担当者,備考

テキスト注文内容:
{text}
    """

    for attempt in range(max_retries):
        response = openai_client.chat.completions.create(
            model="gpt-4o",
            messages=[
                {"role": "system", "content": "あなたはテキスト注文をCSV形式に変換するアシスタントです。"},
                {"role": "user", "content": prompt}
            ],
            max_tokens=1000,
            temperature=0.2
        )
        content = response.choices[0].message.content.strip()
        print("GPT Response Content:\n", content) # デバッグ用
        if "申し訳ありません" in content or "直接抽出することはできません" in content:
            continue
        lines = content.splitlines()
        cleaned_lines = [line for line in lines if not line.strip().startswith("この情報") and line.strip() != "..." and line.strip() != "…"]
        return "\n".join(cleaned_lines)
    print("構造化テキストが空です。GPT応答なしまたはすべて謝罪文")
    return ""

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
        print("⚠ 有効な行がありません。全行ログ保存")
        with open(f"/tmp/failed_structured_{today}.txt", "w", encoding="utf-8") as f:
            f.write(structured_text)
        return
    # invalid_linesも必要に応じて別ファイルで保存
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

@app.route('/webhook', methods=['POST'])
def webhook():
    data = request.get_json()
    events = data.get('events', [])
    if not events:
        return 'OK', 200
    event = events[0]

    user_id = event['source']['userId']
    headers = {'Authorization': f'Bearer {CHANNEL_ACCESS_TOKEN}'}
    profile_res = requests.get('https://api.line.me/v2/bot/profile/' + user_id, headers=headers)
    operator_name = profile_res.json().get('displayName', '不明')
    timestamp = datetime.now(JST)

    if event.get('message', {}).get('type') == 'image':
        message_id = event['message']['id']
        image_url = f'https://api-data.line.me/v2/bot/message/{message_id}/content'
        image_data = requests.get(image_url, headers=headers).content
        file_name = timestamp.strftime('%Y%m%d_%H%M') + '.jpg'
        file_path = f'/tmp/{file_name}'
        with open(file_path, 'wb') as f:
            f.write(image_data)
        root_id = get_or_create_folder('受注集計')
        date_id = get_or_create_folder(timestamp.strftime('%Y%m%d'), parent_id=root_id)
        image_folder_id = get_or_create_folder('Line画像保存', parent_id=date_id)
        csv_folder_id = get_or_create_folder('集計結果', parent_id=date_id)
        file_metadata = {'name': file_name, 'parents': [image_folder_id]}
        media = MediaFileUpload(file_path, mimetype='image/jpeg')
        drive_service.files().create(body=file_metadata, media_body=media).execute()
        structured_text = analyze_image_with_gpt(file_path, operator_name)
        append_to_csv(structured_text, csv_folder_id)

    elif event.get('message', {}).get('type') == 'text':
        text = event['message']['text']
        file_name = timestamp.strftime('%Y%m%d_%H%M') + '.txt'
        root_id = get_or_create_folder('受注集計')
        date_id = get_or_create_folder(timestamp.strftime('%Y%m%d'), parent_id=root_id)
        image_folder_id = get_or_create_folder('Line画像保存', parent_id=date_id)
        csv_folder_id = get_or_create_folder('集計結果', parent_id=date_id)
        file_path = f'/tmp/{file_name}'
        with open(file_path, 'w', encoding='utf-8') as f:
            f.write(text)
        file_metadata = {'name': file_name, 'parents': [image_folder_id]}
        media = MediaFileUpload(file_path, mimetype='text/plain')
        drive_service.files().create(body=file_metadata, media_body=media).execute()
        structured_text = analyze_text_with_gpt(text, operator_name)
        append_to_csv(structured_text, csv_folder_id)

    return 'OK', 200

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=10000)
