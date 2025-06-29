import pytz
from datetime import datetime
import requests

JST = pytz.timezone('Asia/Tokyo')

def get_now():
    """
    日本時間の今の時刻(datetime), YYYYMMDDHH, YYYY年MM月DD日 HH時 を返す
    """
    now = datetime.now(JST)
    now_str = now.strftime("%Y%m%d%H")
    now_verbose = now.strftime("%Y年%m月%d日 %H時")
    return now, now_str, now_verbose

def get_operator_name(user_id, headers):
    """
    LINEのユーザIDから表示名を取得
    """
    profile_res = requests.get('https://api.line.me/v2/bot/profile/' + user_id, headers=headers)
    return profile_res.json().get('displayName', '不明')

def clean_lines(lines):
    """
    GPT応答の不要な行を除去
    """
    return [line for line in lines if not line.strip().startswith("この情報") and line.strip() not in ["...", "…"]]
