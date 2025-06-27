from flask import Flask, request
import requests

app = Flask(__name__)
CHANNEL_ACCESS_TOKEN = 'ここにLINEのアクセストークンを貼る'

@app.route('/webhook', methods=['POST'])
def webhook():
    event = request.json['events'][0]
    if event['message']['type'] == 'image':
        message_id = event['message']['id']
        image_url = f'https://api-data.line.me/v2/bot/message/{message_id}/content'
        headers = {'Authorization': f'Bearer {CHANNEL_ACCESS_TOKEN}'}
        image_data = requests.get(image_url, headers=headers).content

        with open(f'{message_id}.jpg', 'wb') as f:
            f.write(image_data)

    return 'OK'

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=10000)
