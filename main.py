from datetime import datetime, timedelta
import pandas as pd
from string import Template
import requests
import sys
import logging

TESTING = False


# TODO: API keys as secrets for deployment
# TODO: service account API keys - which ones?
# TODO: muting rule spreadsheet --> S3
# TODO: send log to S3


def initialize_logger():
    # Initialize the logger
    logger = logging.getLogger('muting_change')
    logging.basicConfig(level=logging.DEBUG,
                        filename=f'muting_change_{datetime.now().strftime("%Y-%m-%d_%H%M%S")}.log',
                        filemode='a')
    console = logging.StreamHandler(sys.stdout)
    console.setLevel(logging.INFO)
    logger.addHandler(console)

    return logger


def get_stored_rule_data(logger):
    logger.info('Fetching muting rule info...')

    # Pull muting rules Excel sheet into a dataframe
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
    logger.debug(f'Monday API response:\n{response}')

    if 'errors' in response.keys():
        logger.warning(f'There was an error calling the Monday API:\n{response}')
        sys.exit(1)
    else:
        monday_items = response['data']['boards'][0]['items']
        logger.debug(f'Monday items:\n{monday_items}')
        logger.info('   Patching events collected successfully.')
        return monday_items


def get_muting_rule_info(client, envir, df, logger):
    logger.info('   Extracting muting rule ID and New Relic account number...')

    # Special handling for Lenovo patching
    if client == 'Lenovo':
        if 'Linux' in envir:
            envir = 'Weekly Linux'
        elif 'Windows' in envir:
            envir = 'Weekly Windows'

    try:
        rule_df = df[(df['Client'] == client) & (df['Environment'] == envir)]
        rule_ids = [int(value) for value in rule_df['Muting Rule ID']]
        nr_account = df.loc[(df['Client'] == client) & (df['Environment'] == envir), 'NR Account #'].iloc[0]
        logger.info(f'      Muting Rule ID(s): {rule_ids} in NR Account: {int(nr_account)}')
        return rule_ids, int(nr_account)
    except Exception as e:
        error_type = e.__class__.__name__
        if error_type == 'IndexError':
            logger.warning(f'      {client} does not have muting rule information.')
        elif error_type == 'ValueError':
            logger.warning(f'      {client} {envir} does not have a muting rule in place.')
        else:
            logger.warning(f'      There was an error extracting muting rule information:\n'
                           f'      {e.__class__.__name__}: {e}')
        return None, None


def mutate_nr_rules(monday_items, muting_df, logger):
    logger.info('Processing patching events...')
    rule_ids_not_mutated = []

    # Iterate through patching events and action any events that are still in progress
    for i in range(34, 39):
        event_status = monday_items[i]['column_values'][1]['text']
        client_name = monday_items[i]['name']
        environment = monday_items[i]['column_values'][0]['text']
        start_time = monday_items[i]['column_values'][2]['text']
        patching_window = monday_items[i]['column_values'][3]['text']
        end_time_dt = datetime.strptime(start_time, '%Y-%m-%d %H:%M') + timedelta(hours=float(patching_window))
        end_time = datetime.strftime(end_time_dt, '%Y-%m-%dT%H:%M:%S')

        clients_without_muting = ['2W Infra', 'Gas Station TV', 'Michael Kors', 'NAIC', 'TitleMax']

        if client_name in clients_without_muting:
            logger.info(f'\n   Event {i + 1}: {client_name} does not have muting rules in place; skipping event.')
            continue
        else:
            logger.info(f'\n   Event {i + 1}: {event_status} for {client_name} {environment} at {start_time} '
                        f'for {patching_window} hours, ending at {end_time}.')

            if TESTING:
                # Test data for mutating a rule in 2W-MCS-Tooling-Test NR account
                muting_rule_ids = ['38434772']
                nr_account_num = 3720977
            else:
                # Muting rule ID and account corresponding to patching event data
                muting_rule_ids, nr_account_num = get_muting_rule_info(client_name, environment, muting_df, logger)
                if not muting_rule_ids:
                    continue

            # NR API details
            nr_api_key = 'NRAK-7DVT82DILPFIAXSZZ6CLPKYB8YU'
            nr_endpoint = 'https://api.newrelic.com/graphql'
            nr_headers = {
                'Content-Type': 'application/json',
                'API-Key': nr_api_key,
            }

            # Event statuses: 'Paused/On_Hold', 'To-Do', 'Event Prep In Progress', 'Event Complete', 'All Compliant'
            if event_status == 'Event Prep In Progress':
                # Mutate correct muting rule with correctly-formatted date and time
                # Format start time string
                start_time_split = start_time.split(' ')
                start_time_nr = start_time_split[0] + 'T' + start_time_split[1] + ':00'
                nr_gql_mutate_template = Template("""
                mutation {
                  alertsMutingRuleUpdate(accountId: $account_id, id: $rule_id, rule: {enabled: true, schedule: 
                    {startTime: "$start_time", endTime: "$end_time"}}) {
                    id
                  }
                }
                """)

                for muting_rule_id in muting_rule_ids:
                    # TODO: check muting rule for start, end time
                    # if start time and end time = what's in Monday
                    # continue
                    # else: mutate
                    logger.info(f'   Mutating muting rule {muting_rule_id} for {client_name}...')

                    # GraphQL query data to mutate the appropriate muting rule
                    nr_gql_mutate_formatted = nr_gql_mutate_template.substitute({'account_id': nr_account_num,
                                                                                 'start_time': start_time_nr,
                                                                                 'end_time': end_time,
                                                                                 'rule_id': muting_rule_id})

                    nr_response = requests.post(nr_endpoint,
                                                headers=nr_headers,
                                                json={'query': nr_gql_mutate_formatted}).json()
                    logger.debug(f'New Relic API response:\n{nr_response}')

                    if nr_response['data']['alertsMutingRuleUpdate']['id'] == muting_rule_id:
                        logger.info(f'      Muting rule ID {muting_rule_id} was successfully modified.')
                    else:
                        logger.warning(f'      There was an error mutating the muting role:\n{nr_response}')
                        rule_ids_not_mutated.append(muting_rule_id)
                        continue
            elif event_status == 'Event Complete' or event_status == 'Paused/On-Hold' or \
                    event_status == 'All Compliant':
                logger.info(f'   Checking enabled/disabled muting rule status for this event...')
                nr_gql_query_template = Template("""
                {
                  actor {
                    account(id: $account_id) {
                      alerts {
                        mutingRule(id: $rule_id) {
                          id
                          enabled
                        }
                      }
                    }
                  }
                }
                """)

                for muting_rule_id in muting_rule_ids:
                    nr_gql_query_formatted = nr_gql_query_template.substitute({'account_id': nr_account_num,
                                                                               'rule_id': muting_rule_id})

                    nr_response = requests.post(nr_endpoint,
                                                headers=nr_headers,
                                                json={'query': nr_gql_query_formatted}).json()
                    logger.debug(f'New Relic API response:\n{nr_response}')

                    try:
                        logger.warning(f'      NR error: {muting_rule_id} {nr_response["errors"][0]["message"]}')
                        rule_ids_not_mutated.append(muting_rule_id)
                    except KeyError:
                        if not nr_response['data']['actor']['account']['alerts']['mutingRule']['enabled']:
                            logger.info(f'      Muting rule {muting_rule_id} is already disabled; no action taken.')
                            continue
                        else:
                            nr_gql_disable_template = Template("""
                            mutation {
                                alertsMutingRuleUpdate(accountId: $account_id, id: $rule_id, rule: 
                                  {enabled: false}) {
                                  id
                              }
                            }
                            """)
                            nr_gql_disable_formatted = nr_gql_disable_template.substitute(
                                {'account_id': nr_account_num,
                                 'rule_id': muting_rule_id})

                            nr_response = requests.post(nr_endpoint,
                                                        headers=nr_headers,
                                                        json={'query': nr_gql_disable_formatted}).json()
                            logger.debug(f'New Relic API response:\n{nr_response}')

                            if nr_response['data']['alertsMutingRuleUpdate']['id'] == str(muting_rule_id):
                                logger.info(f'      Muting rule ID {muting_rule_id} was successfully disabled.')
                            else:
                                logger.warning(f'      There was an error disabling the muting role:\n'
                                               f'{nr_response}')
                                rule_ids_not_mutated.append(muting_rule_id)
                                continue

    return rule_ids_not_mutated


def handler(event, context):
    logger = initialize_logger()
    muting_df = get_stored_rule_data(logger)
    monday_items = get_patching_events(logger)
    not_mutated = mutate_nr_rules(monday_items, muting_df, logger)
    logger.warning(f'\nProcessing is complete.\n'
                   f'   The following rule IDs were not mutated due to errors: {not_mutated}')


handler('', '')
