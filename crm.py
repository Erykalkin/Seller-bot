import requests
import uuid
import datetime
import json
from decouple import config

# URL формы amoCRM
url = "https://forms.amocrm.ru/queue/add"
form_id = config('CRM_FORM_ID')
hash = config('CRM_HASH')
referer = config('CRM_REFERER')


def send_to_crm(name: str, phone: str, note: str, telegram: str):
    try:
        data = {
            "fields[name_1]": name,
            "fields[581821_1][521181]": phone,
            "fields[note_2]": note,
            "fields[656491_1]": telegram,
            "form_id": form_id,
            "hash": hash,
            "user_origin": json.dumps({
                "datetime": datetime.datetime.now().strftime('%a %b %d %Y %H:%M:%S GMT%z'),
                "timezone": "Europe/Moscow",
                "referer": "https://yyvladelets.amocrm.ru/"
            }),
            "visitor_uid": str(uuid.uuid4()),              
            "form_request_id": str(uuid.uuid4()),          
            "gso_session_uid": str(uuid.uuid4()),          
        }

        headers = {
            "Origin": "https://forms.amocrm.ru",
            "Referer": referer
        }

        response = requests.post(url, data=data, headers=headers)
        return response.status_code == 200
    except Exception as e:
        print(f"CRM Error: {e}")
        return False