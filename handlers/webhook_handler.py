from handlers.image_handler import process_image_message
from handlers.text_handler import process_text_message
from handlers.pdf_handler import process_pdf_message
from handlers.csv_handler import csv_to_xlsx_with_summary  # サマリだけ生成用にimport
from config import CSV_FORMAT_PATH

import os
import pytz
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
            # 最新の集計結果CSVを探して、サマリ生成
            # CSVファイル名は「集計結果_YYYYMMDD.csv」と想定
            JST = pytz.timezone('Asia/Tokyo')
            today = datetime.now(JST).strftime('%Y%m%d')
            csv_path = f"/tmp/集計結果_{today}.csv"
            # ファイルが無い場合はエラー応答
            if not os.path.exists(csv_path):
                # 必要に応じてLINE返信（未実装。LINE bot SDKで返信したい場合は追加）
                print("集計ファイルが見つかりません")

            else:
                # サマリのみ作成
                xlsx_path = csv_to_xlsx_with_summary(csv_path)
                print(f"集計サマリ作成のみ実施: {xlsx_path}")
                # 必要に応じてLINE返信で「サマリファイルをDrive等に保存した」旨伝える
            return 'OK', 200

        # 通常テキスト（注文等）は既存ハンドラへ
        process_text_message(event)

    # --- 画像(jpg, png, etc) ---
    elif message_type == 'image':
        process_image_message(event)

    # --- ファイル(PDF含む) ---
    elif message_type == 'file':
        file_name = event['message'].get('fileName', '').lower()
        # PDFの場合のみPDFハンドラへ
        if file_name.endswith('.pdf'):
            process_pdf_message(event)
        # 他のファイル型は必要に応じてハンドラ追加

    return 'OK', 200
