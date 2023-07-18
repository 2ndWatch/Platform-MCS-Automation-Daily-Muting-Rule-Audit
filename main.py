from datetime import datetime, timedelta
import pandas as pd
from string import Template
import requests
import sys
import logging

TESTING = True


def initialize_logger():
    # Initialize the logger
    logger = logging.getLogger('muting')
    logging.basicConfig(level=logging.DEBUG,
                        filename=f'muting_{datetime.now().strftime("%Y-%m-%d_%H%M%S")}.log',
                        filemode='a')
    console = logging.StreamHandler(sys.stdout)
    console.setLevel(logging.INFO)
    logger.addHandler(console)

    return logger


def get_all_rule_data(logger):
    logger.info('Fetching muting rule data...')

    # pull muting rules excel into a dataframe
    muting_df = pd.read_excel('Muting Rules.xlsx', usecols=['Client', 'Environment', 'Muting Rule ID', 'NR Account #'])

    if not muting_df.empty:
        logger.info('   Muting rule IDs loaded successfully.')
        return muting_df
    else:
        logger.warning('   No muting rule data found.')
        sys.exit(1)


def get_patching_events(logger):
    logger.info('Fetching patching events...')

    # Monday API call data
    api_token = 'eyJhbGciOiJIUzI1NiJ9.eyJ0aWQiOjI2OTMxOTg5MywiYWFpIjoxMSwidWlkIjozMzcyNzc0NCwiaWFkIjoiMjAyMy0wNy0xN1' \
                'QxOToxMzo0OS4xNjlaIiwicGVyIjoibWU6d3JpdGUiLCJhY3RpZCI6NDE3MTQ5MywicmduIjoidXNlMSJ9.iYC1a-24mBIfGesr' \
                'iT_OVvVC3t2zXVGewv2dvYcf0zc'
    endpoint = 'https://api.monday.com/v2'
    headers = {
        'Authorization': api_token,
        'Content-Type': 'application/json'
    }
    # Monday board GraphQL query to filter for specific columns
    gql_query = """
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

    # Call the Monday API and transform the response into JSON format
    response = requests.get(endpoint, headers=headers, json={'query': gql_query}).json()

    if 'errors' in response.keys():
        logger.warning(f'There was an error calling the Monday API:\n{response}')
        sys.exit(1)
    else:
        monday_items = response['data']['boards'][0]['items']
        logger.info('   Patching events collected successfully.')
        return monday_items


def get_muting_rule_info(client, envir, df, logger):
    logger.info('Extracting muting rule ID and New Relic account number...')
    try:
        rule_df = df[(df['Client'] == client) & (df['Environment'] == envir)]
        rule_ids = [int(value) for value in rule_df['Muting Rule ID']]
        nr_account = df.loc[(df['Client'] == client) & (df['Environment'] == envir), 'NR Account #'].iloc[0]
        logger.info(f'   Muting Rule ID: {rule_ids} in NR Account: {int(nr_account)}')
        return rule_ids, nr_account
    except KeyError as e:
        logger.warning(f'   There was an error extracting muting rule information:\n{e}')
        sys.exit(1)


def mutate_nr_rules(monday_items, muting_df, logger):
    for i in range(34, 35):
        if monday_items[i]['column_values'][1]['text'] == 'Event Prep In Progress':
            client_name = monday_items[i]['name']
            environment = monday_items[i]['column_values'][0]['text']
            start_time = monday_items[i]['column_values'][2]['text']
            patching_window = monday_items[i]['column_values'][3]['text']
            end_time_dt = datetime.strptime(start_time, '%Y-%m-%d %H:%M') + timedelta(hours=int(patching_window))
            end_time = datetime.strftime(end_time_dt, '%Y-%m-%dT%H:%M:%S')
            print(f'{client_name}: {environment} at {start_time} for {patching_window} hours, ending at {end_time}')

            # get correct muting rule ID and account from dataframe using patching event data
            muting_rule_ids, nr_account_num = get_muting_rule_info(client_name, environment, muting_df, logger)

            # mutate correct muting rule with updated date and time
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
            nr_gql_template = Template("""
            mutation {
              alertsMutingRuleUpdate(accountId: $account_id, rule: {enabled: $enabled, schedule: {startTime: "$start_time", 
                endTime: "$end_time"}}, id: $rule_id) {
                id
              }
            }
            """)

            for muting_rule_id in muting_rule_ids:

                if TESTING:
                    # GraphQL query data to mutate a test muting rule in the Tooling-Test New Relic account
                    nr_gql_formatted = nr_gql_template.substitute({'account_id': 3720977,
                                                                   'enabled': enabled,
                                                                   'start_time': start_time_nr,
                                                                   'end_time': end_time,
                                                                   'rule_id': '38434772'})
                else:
                    # GraphQL query data to mutate the appropriate muting rule
                    nr_gql_formatted = nr_gql_template.substitute({'account_id': nr_account_num,
                                                                   'enabled': enabled,
                                                                   'start_time': start_time,
                                                                   'end_time': end_time,
                                                                   'rule_id': muting_rule_id})

                nr_response = requests.post(nr_endpoint, headers=nr_headers, json={'query': nr_gql_formatted})
                print(nr_response.json())

                if nr_response.json()['data']['alertsMutingRuleUpdate']['id'] == muting_rule_id:
                    print(f'Muting rule ID {muting_rule_id} was successfully modified.')


def handler():
    logger = initialize_logger()
    muting_df = get_all_rule_data(logger)
    monday_items = get_patching_events(logger)
    mutate_nr_rules(monday_items, muting_df, logger)


handler()
