from flask import Flask, request
from config import CHANNEL_ACCESS_TOKEN
from handlers.webhook_handler import handle_webhook

app = Flask(__name__)

@app.route('/webhook', methods=['POST'])
def webhook():
    return handle_webhook(request)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=10000)
