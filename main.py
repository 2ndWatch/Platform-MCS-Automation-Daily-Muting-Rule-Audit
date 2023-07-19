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

    # Special handling for Lenovo patching event names
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


def transform_event_times(start, window, start_delta=None):
    start_time_dt = datetime.strptime(start, '%Y-%m-%d %H:%M')
    end_time_dt = datetime.strptime(start, '%Y-%m-%d %H:%M') + timedelta(hours=float(window))

    if start_delta:
        start_time_dt = start_time_dt + timedelta(hours=float(start_delta))
        end_time_dt = end_time_dt + timedelta(hours=float(start_delta))

    start_time = datetime.strftime(start_time_dt, '%Y-%m-%dT%H:%M:%S')
    end_time = datetime.strftime(end_time_dt, '%Y-%m-%dT%H:%M:%S')
    return start_time, end_time


def check_nr_rules(monday_items, muting_df, logger):
    logger.info('Processing patching events...')
    rule_ids_not_mutated = []
    events_not_processed = []
    nr_response = None
    lenovo_win_mod = False
    lenovo_linux_mod = False

    nbly_patching_windows = {
        38495798: {
            'description': 'Dev/QA ssm_patch_wave1',
            'length': 1,
            'delta': 0
        },
        38495968: {
            'description': 'Dev/QA ssm_patch_wave1.5',
            'length': 3,
            'delta': 1
        },
        38496432: {
            'description': 'Production ssm_patch_wave2',
            'length': 1,
            'delta': 0
        },
        38496605: {
            'description': 'Production ssm_patch_wave2.5',
            'length': 3,
            'delta': 1
        }
    }

    try:
        for i in range(len(monday_items)):
            event_status = monday_items[i]['column_values'][1]['text']
            client_name = monday_items[i]['name']
            environment = monday_items[i]['column_values'][0]['text']
            patching_window = monday_items[i]['column_values'][3]['text']
            start_time = monday_items[i]['column_values'][2]['text']

            start_time_nr, end_time_nr = transform_event_times(start_time, patching_window)

            clients_without_muting = ['2W Infra', 'Gas Station TV', 'Michael Kors', 'NAIC', 'Symetra', 'TitleMax']

            if client_name in clients_without_muting:
                logger.info(f'\n   Event {i + 1}: {client_name} does not have muting rules in place; skipping event.')
                continue
            # If the first instance of an upcoming Lenovo event has already been mutated, skip any other events
            elif (client_name == 'Lenovo' and lenovo_win_mod) or (client_name == 'Lenovo' and lenovo_linux_mod):
                logger.info(f'\n   Event {i + 1}: More recent Lenovo event is scheduled; skipping event.')
                continue
            else:
                logger.info(f'\n   Event {i + 1}: {event_status} for {client_name} {environment} at {start_time_nr} '
                            f'for {patching_window} hours, ending at {end_time_nr}.')

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
                nr_gql_mutate_template = Template("""
                    mutation {
                      alertsMutingRuleUpdate(accountId: $account_id, id: $rule_id, rule: {enabled: $enabled, schedule: 
                        {startTime: "$start_time", endTime: "$end_time"}}) {
                        id
                      }
                    }
                    """)
                nr_gql_query_template = Template("""
                        {
                          actor {
                            account(id: $account_id) {
                              alerts {
                                mutingRule(id: $rule_id) {
                                  id
                                  enabled
                                  schedule {
                                    endTime
                                    startTime
                                  }
                                }
                              }
                            }
                          }
                        }
                    """)
                nr_gql_enable_template = Template("""
                    mutation {
                        alertsMutingRuleUpdate(accountId: $account_id, id: $rule_id, rule: 
                          {enabled: $enabled}) {
                          id
                      }
                    }
                    """)

                if event_status == 'Event Prep In Progress' or event_status == 'To-Do':
                    # Check rule for start and end time and enabled;
                    # if needed, mutate if times are incorrect and enable rule
                    for muting_rule_id in muting_rule_ids:

                        # Special time handling for Neighborly event times
                        if client_name == 'Neighborly':
                            nbly_patching_window = nbly_patching_windows[muting_rule_id]['length']
                            start_delta = nbly_patching_windows[muting_rule_id]['delta']
                            start_time_nr, end_time_nr = transform_event_times(start_time,
                                                                               nbly_patching_window,
                                                                               start_delta=start_delta)

                        nr_gql_query_fmtd = nr_gql_query_template.substitute({'account_id': nr_account_num,
                                                                              'rule_id': muting_rule_id})
                        try:
                            nr_response = requests.post(nr_endpoint,
                                                        headers=nr_headers,
                                                        json={'query': nr_gql_query_fmtd}).json()
                            logger.debug(f'New Relic API response:\n{nr_response}')

                            event_rule = nr_response['data']['actor']['account']['alerts']['mutingRule']
                            event_start = event_rule['schedule']['startTime']
                            event_end = event_rule['schedule']['endTime']
                            enabled = event_rule['enabled']

                            # If rule start time and end time match Monday event data, skip mutation
                            if start_time_nr == event_start[:19] and end_time_nr == event_end[:19] and enabled:
                                logger.info(
                                    f'   Muting rule {muting_rule_id} times match Monday event; no action taken.')
                                continue
                            elif start_time_nr == event_start[:19] and end_time_nr == event_end[:19] and not enabled:
                                logger.info(f'   Muting rule {muting_rule_id} times match Monday event but rule is '
                                            f'disabled; enabling rule...')

                                nr_gql_enable_fmtd = nr_gql_enable_template.substitute(
                                    {'account_id': nr_account_num,
                                     'rule_id': muting_rule_id,
                                     'enabled': 'true'})
                                nr_response = requests.post(nr_endpoint,
                                                            headers=nr_headers,
                                                            json={'query': nr_gql_enable_fmtd}).json()
                                logger.debug(f'New Relic API response:\n{nr_response}')

                                try:
                                    if nr_response['data']['alertsMutingRuleUpdate']['id'] == str(muting_rule_id):
                                        # Sepcial handling for weekly repeating Lenovo patching events
                                        if client_name == 'Lenovo':
                                            if 'Windows' in environment:
                                                lenovo_win_mod = True
                                            elif 'Linux' in environment:
                                                lenovo_linux_mod = True
                                        logger.info(f'      Muting rule ID {muting_rule_id} was successfully enabled.')
                                except KeyError:
                                    logger.warning(f'      There was an error enabling the muting role:\n'
                                                   f'{nr_response}')
                                    rule_ids_not_mutated.append(muting_rule_id)
                                continue
                            else:
                                logger.info(f'   Mutating muting rule {muting_rule_id} for {client_name}...')

                                nr_gql_mutate_fmtd = nr_gql_mutate_template.substitute({'account_id': nr_account_num,
                                                                                        'start_time': start_time_nr,
                                                                                        'end_time': end_time_nr,
                                                                                        'rule_id': muting_rule_id,
                                                                                        'enabled': 'true'})
                                nr_response = requests.post(nr_endpoint,
                                                            headers=nr_headers,
                                                            json={'query': nr_gql_mutate_fmtd}).json()
                                logger.debug(f'New Relic API response:\n{nr_response}')

                                try:
                                    if nr_response['data']['alertsMutingRuleUpdate']['id'] == str(muting_rule_id):
                                        # Sepcial handling for weekly repeating Lenovo patching events
                                        if client_name == 'Lenovo':
                                            if 'Windows' in environment:
                                                lenovo_win_mod = True
                                            elif 'Linux' in environment:
                                                lenovo_linux_mod = True
                                        logger.info(f'      Muting rule ID {muting_rule_id} was successfully modified.')
                                except KeyError:
                                    logger.warning(f'      There was an error mutating the muting role:\n{nr_response}')
                                    rule_ids_not_mutated.append(muting_rule_id)
                                    continue
                        except KeyError:
                            logger.warning(f'      There was an error querying the muting role:\n{nr_response}')
                            rule_ids_not_mutated.append(muting_rule_id)
                            continue
                elif event_status == 'Event Complete' or event_status == 'Paused/On-Hold' or \
                        event_status == 'All Compliant':
                    logger.info(f'   Checking enabled/disabled muting rule status for this event...')

                    for muting_rule_id in muting_rule_ids:
                        # Query rule to check if it is enabled or disabled
                        nr_gql_query_fmtd = nr_gql_query_template.substitute({'account_id': nr_account_num,
                                                                              'rule_id': muting_rule_id})
                        nr_response = requests.post(nr_endpoint,
                                                    headers=nr_headers,
                                                    json={'query': nr_gql_query_fmtd}).json()
                        logger.debug(f'New Relic API response:\n{nr_response}')

                        try:
                            # If the 'errors' key exists in the API response, log the error
                            logger.warning(f'      NR error for {muting_rule_id}: '
                                           f'{nr_response["errors"][0]["message"]}')
                            rule_ids_not_mutated.append(muting_rule_id)
                        except KeyError:
                            # If the 'errors' key does not exist in the API response, disable the rul if necessary
                            if not nr_response['data']['actor']['account']['alerts']['mutingRule']['enabled']:
                                logger.info(f'      Muting rule {muting_rule_id} is already disabled; no action taken.')
                                continue
                            else:
                                nr_gql_disable_fmtd = nr_gql_enable_template.substitute(
                                    {'account_id': nr_account_num,
                                     'rule_id': muting_rule_id,
                                     'enabled': 'false'})
                                nr_response = requests.post(nr_endpoint,
                                                            headers=nr_headers,
                                                            json={'query': nr_gql_disable_fmtd}).json()
                                logger.debug(f'New Relic API response:\n{nr_response}')

                                if nr_response['data']['alertsMutingRuleUpdate']['id'] == str(muting_rule_id):
                                    logger.info(f'      Muting rule ID {muting_rule_id} was successfully disabled.')
                                else:
                                    logger.warning(f'      There was an error disabling the muting role:\n'
                                                   f'{nr_response}')
                                    rule_ids_not_mutated.append(muting_rule_id)
                                    continue
                else:
                    logger.warning(f'   Status "{event_status}" is a mismatch. Skipping event.')
                    events_not_processed.append(f'{client_name} {environment}')
                    continue
        return rule_ids_not_mutated, events_not_processed
    except Exception as e:
        logger.warning(f'\nThere was a general error:\n   {e}')


def handler(event, context):
    logger = initialize_logger()
    muting_df = get_stored_rule_data(logger)
    monday_items = get_patching_events(logger)
    not_mutated, not_processed = check_nr_rules(monday_items, muting_df, logger)
    logger.warning(f'\nProcessing is complete.\n'
                   f'   The following rule IDs were not mutated due to errors: {not_mutated}\n'
                   f'   The following events were not processed due to errors: {not_processed}')


handler('', '')
