from handlers.image_handler import process_image_message
from handlers.text_handler import process_text_message

def handle_webhook(request):
    data = request.get_json()
    events = data.get('events', [])
    if not events:
        return 'OK', 200

    event = events[0]
    message_type = event.get('message', {}).get('type')
    if message_type == 'image':
        process_image_message(event)
    elif message_type == 'text':
        process_text_message(event)
    return 'OK', 200
