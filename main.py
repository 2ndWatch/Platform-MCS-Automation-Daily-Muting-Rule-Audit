import requests
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
from string import Template

# pull muting rules excel into a dataframe
muting_df = pd.read_excel('Muting Rules.xlsx', usecols=['Client', 'Environment', 'Muting Rule ID', 'NR Account #'])
# print(muting_df.tail())

# Monday API details
monday_api_token = 'eyJhbGciOiJIUzI1NiJ9.eyJ0aWQiOjI2OTMxOTg5MywiYWFpIjoxMSwidWlkIjozMzcyNzc0NCwiaWFkIjoiMjAyMy0wNy0xN1' \
            'QxOToxMzo0OS4xNjlaIiwicGVyIjoibWU6d3JpdGUiLCJhY3RpZCI6NDE3MTQ5MywicmduIjoidXNlMSJ9.iYC1a-24mBIfGesr' \
            'iT_OVvVC3t2zXVGewv2dvYcf0zc'
monday_endpoint = 'https://api.monday.com/v2'
monday_headers = {
    'Authorization': monday_api_token,
    'Content-Type': 'application/json'
}

monday_gql_template = Template("""
{
  boards (ids: $board_id) {
    items {
      name
      id
      column_values (ids: $column_ids) {
        title
        text
      }
    }
  }
}
""")
monday_gql_formatted = monday_gql_template.substitute({'board_id': '413857267',
                                                       'column_ids': '[text, status, date2, numeric2]'})
status_code = 200


def get_muting_rule_info(client, envir, df):
    rule_df = df[(df['Client'] == client) & (df['Environment'] == envir)]
    rule_ids = [int(value) for value in rule_df['Muting Rule ID']]

    nr_account = df.loc[(df['Client'] == client) & (df['Environment'] == envir), 'NR Account #'].iloc[0]

    return rule_ids, nr_account


board_response = requests.get(monday_endpoint, headers=monday_headers, json={'query': monday_gql_formatted})
monday_items = board_response.json()['data']['boards'][0]['items']

for i in range(31, 32):
    if monday_items[i]['column_values'][1]['text'] == 'Event Prep In Progress':
        client_name = monday_items[i]['name']
        environment = monday_items[i]['column_values'][0]['text']
        start_time = monday_items[i]['column_values'][2]['text']
        maint_window = monday_items[i]['column_values'][3]['text']
        end_time_dt = datetime.strptime(start_time, '%Y-%m-%d %H:%M') + timedelta(hours=int(maint_window))
        end_time = datetime.strftime(end_time_dt, '%Y-%m-%dT%H:%M:%S')
        print(f'{client_name}: {environment} at {start_time} for {maint_window} hours, ending at {end_time}')

        # TODO: get correct muting rule ID and account from dataframe using monday response data
        muting_rule_ids, nr_account_num = get_muting_rule_info(client_name, environment, muting_df)
        print(f'   Muting Rule ID: {muting_rule_ids} in NR Account: {int(nr_account_num)}')

        # TODO: mutate correct muting rule with updated date and time
        # NR API details
        nr_api_key = 'NRAK-7DVT82DILPFIAXSZZ6CLPKYB8YU'
        nr_endpoint = 'https://api.newrelic.com/graphql'
        nr_headers = {
            'Content-Type': 'application/json',
            'API-Key': nr_api_key,
        }
        enabled = 'true'
        start_time_split = start_time.split(' ')
        start_time_nr = start_time_split[0] + 'T' + start_time_split[1] + ':00'
        print(start_time_nr)
        print(end_time)
        # time_zone = 'America/Denver'
        nr_gql_template = Template("""
        mutation {
          alertsMutingRuleUpdate(accountId: $account_id, rule: {enabled: $enabled, schedule: {startTime: "$start_time", 
            endTime: "$end_time"}}, id: $rule_id) {
            id
          }
        }
        """)

        for muting_rule_id in muting_rule_ids:
            nr_gql_formatted = nr_gql_template.substitute({'account_id': 3720977,
                                                           'enabled': enabled,
                                                           'start_time': start_time_nr,
                                                           'end_time': end_time,
                                                           'rule_id': '38434772'})
            # nr_gql_formatted = nr_gql_template.substitute({'account_id': nr_account_num,
            #                                                'enabled': enabled,
            #                                                'start_time': start_time,
            #                                                'end_time': end_time,
            #                                                'time_zone': time_zone,
            #                                                'rule_id': muting_rule_id})

            nr_response = requests.post(nr_endpoint, headers=nr_headers, json={'query': nr_gql_formatted})
            print(nr_response.json())

            if nr_response.json()['data']['alertsMutingRuleUpdate']['id'] == muting_rule_id:
                print(f'Muting rule ID {muting_rule_id} was successfully modified.')
