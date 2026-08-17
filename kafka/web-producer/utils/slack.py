import requests
import logging
from datetime import datetime
from config import SLACK_WEBHOOK_URL

def send_slack_alert(title, summary, details, level="danger"):
    if not SLACK_WEBHOOK_URL:
        logging.warning("SLACK_WEBHOOK_URL이 설정되지 않아 알림을 보낼 수 없습니다.")
        return

    color_map = {
        "info": "#36a64f",
        "warning": "#f2c744",
        "danger": "#dc3545"
    }
    
    payload = {
        "attachments": [
            {
                "fallback": f"{title}: {summary}",
                "color": color_map.get(level, "#dc3545"),
                "pretext": f"*{title}*",
                "title": summary,
                "text": f"```{details}```",
                "footer": "실시간 데이터 파이프라인",
                "ts": int(datetime.now().timestamp())
            }
        ]
    }
    
    try:
        response = requests.post(SLACK_WEBHOOK_URL, json=payload)
        if response.status_code != 200:
            logging.error(f"Slack 전송 실패: {response.status_code}, {response.text}")
    except Exception as e:
        logging.error(f"Slack 알림 중 예외 발생: {e}")