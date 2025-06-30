from handlers.image_handler import process_image_message
from handlers.text_handler import process_text_message
from handlers.pdf_handler import process_pdf_message

def handle_webhook(request):
    data = request.get_json()
    events = data.get('events', [])
    if not events:
        return 'OK', 200

    event = events[0]
    message_type = event.get('message', {}).get('type')

    # 画像(jpg, png, etc)
    if message_type == 'image':
        process_image_message(event)

    # テキスト
    elif message_type == 'text':
        process_text_message(event)

    # ファイル(PDF含む)
    elif message_type == 'file':
        file_name = event['message'].get('fileName', '').lower()
        # PDFの場合のみPDFハンドラへ
        if file_name.endswith('.pdf'):
            process_pdf_message(event)
        # 必要に応じて: 他のファイル型もハンドラ追加可

    return 'OK', 200
