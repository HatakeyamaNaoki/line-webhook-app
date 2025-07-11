import os

CHANNEL_ACCESS_TOKEN = os.environ.get('LINE_CHANNEL_ACCESS_TOKEN')
OPENAI_API_KEY = os.environ.get('OPENAI_API_KEY')
SERVICE_ACCOUNT_FILE = '/etc/secrets/credentials.json'
SCOPES = ['https://www.googleapis.com/auth/drive.file']
CSV_FORMAT_PATH = '集計フォーマット.csv'
SHARED_DRIVE_ID = "15tyS6xLu203jttUZlxllyuhQbtKHrXjN"