import requests
import json
from datetime import datetime, timezone


url = "http://localhost:8082/v3/clusters"

try:
    response = requests.get(url)
    
    print("Информация о кластере")
    
    print(response.json())
except requests.exceptions.RequestException as e:
    print(f"Ошибка отправки сообщения: {e}")
    
url = "http://localhost:8082/v3/clusters/CJ4glkmuQReCciZ8SR8XkQ/topics"

try:
    response = requests.get(url)
    
    print("Список топиков")
    
    print(response.json())
except requests.exceptions.RequestException as e:
    print(f"Ошибка отправки сообщения: {e}")
    
#url = "http://localhost:8082/v3/clusters/CJ4glkmuQReCciZ8SR8XkQ/topics"
#data = {"topic_name": "topic2"}
#json_string = json.dumps(data)
#print(json_string)

#try: 
#    response = requests.post(url, headers={"Content-Type":"application/json"}, data=json_string)
#    
#    print("Создать топик")
#    
#    print(response.json())
#except requests.exceptions.RequestException as e:
#    print(f"Ошибка отправки сообщения: {e}") 

current_time = datetime.now(timezone.utc)

url = "http://localhost:8082/v3/clusters/CJ4glkmuQReCciZ8SR8XkQ/topics/topic2/records"
data = {"partition_id": 1,
        "headers": [{"name": "head1", "value": "SGVhZGVyLTE="}],
        "key": {"type": "BINARY", "data": "Zm9vYmFy"},
        "value": {"type": "JSON", "data": {"foo": "bar1323"}},
        "timestamp": current_time.isoformat(timespec='seconds').replace("+00:00", "Z")}

print(current_time)

json_string = json.dumps(data)
print(json_string)

try:
    response = requests.post(url, headers={"Content-Type":"application/json"}, data=json_string)
    
    print("Отправка сообщения в топик")
    
    print(response.json())
except requests.exceptions.RequestException as e:
    print(f"Ошибка отправки сообщения: {e}")       