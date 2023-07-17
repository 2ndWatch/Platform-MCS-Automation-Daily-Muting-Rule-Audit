import requests
from datetime import datetime, timedelta
import json

# Monday API details
api_token = 'eyJhbGciOiJIUzI1NiJ9.eyJ0aWQiOjI2OTMxOTg5MywiYWFpIjoxMSwidWlkIjozMzcyNzc0NCwiaWFkIjoiMjAyMy0wNy0xN1' \
            'QxOToxMzo0OS4xNjlaIiwicGVyIjoibWU6d3JpdGUiLCJhY3RpZCI6NDE3MTQ5MywicmduIjoidXNlMSJ9.iYC1a-24mBIfGesr' \
            'iT_OVvVC3t2zXVGewv2dvYcf0zc'
endpoint = 'https://api.monday.com/v2'

headers = {
    'Authorization': api_token,
    'Content-Type': 'application/json'
}

query = """
{
  boards (ids: 413857267) {
    items {
      name
      id
      column_values (ids: [text, status, date2, numeric2]) {
        title
        text
      }
    }
  }
}
"""

board_response = requests.get(endpoint, headers=headers, json={'query': query})
# print(board_response.json())
response_json = board_response.json()['data']['boards'][0]['items']
for i in range(20, 33):
    if response_json[i]['column_values'][1]['text'] == 'Event Prep In Progress':
        client_name = response_json[i]['name']
        environment = response_json[i]['column_values'][0]['text']
        start_time = response_json[i]['column_values'][2]['text']
        maint_window = response_json[i]['column_values'][3]['text']
        end_time_dt = datetime.strptime(start_time, '%Y-%m-%d %H:%M') + timedelta(hours=int(maint_window))
        end_time = datetime.strftime(end_time_dt, '%Y-%m-%d %H:%M')
        print(f'{client_name}: {environment} at {start_time} for {maint_window} hours, ending at {end_time}')
