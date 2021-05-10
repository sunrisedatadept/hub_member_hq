# Civis container script: https://platform.civisanalytics.com/spa/#/scripts/containers/111507437

# For each hub, this script takes all of the contacts added to HQ sheet since the last time this script ran
# successfully for that hub, and subscribes them to their EveryAction committee. The control table that this script
# references is sunrise.hq_ea_sync_control_table. Upsert errors are logged in sunrise.hq_ea_sync_errors and all other
# errors are logged in Sunrise.hub_hq_errors

#-------------------------------------------------------------------------------
# Import necessary Packages
#-------------------------------------------------------------------------------
import json
import time
from parsons import GoogleSheets, Redshift, Table, VAN
import gspread
from googleapiclient.errors import HttpError
from oauth2client.service_account import ServiceAccountCredentials
import logging
from datetime import timezone, datetime, date
import datetime
import os
import traceback
import re



#-------------------------------------------------------------------------------
# Set up logger
#-------------------------------------------------------------------------------
# Set up logger
logger = logging.getLogger(__name__)
_handler = logging.StreamHandler()
_formatter = logging.Formatter('{levelname} {message}',style='{')
_handler.setFormatter(_formatter)
logger.addHandler(_handler)
logger.setLevel('INFO')



#-------------------------------------------------------------------------------
# Load environment
#-------------------------------------------------------------------------------
#If running on container, load this env
try:
    location = os.environ['CIVIS_RUN_ID']
    # Set environ using civis credentials from container script
    os.environ['REDSHIFT_DB'] = os.environ['REDSHIFT_DATABASE']
    os.environ['REDSHIFT_USERNAME'] = os.environ['REDSHIFT_CREDENTIAL_USERNAME']
    os.environ['REDSHIFT_PASSWORD'] = os.environ['REDSHIFT_CREDENTIAL_PASSWORD']
    os.environ['S3_TEMP_BUCKET'] = 'parsons-tmc'
    # Load google credentials for parsons
    creds = json.loads(os.environ['GOOGLE_JSON_CRED_PASSWORD'])  # Load JSON credentials
    api_keys = json.loads(os.environ['EVERYACTION_KEYS_PASSWORD'])

#If running locally, load this env
except KeyError:
    from dotenv import load_dotenv
    load_dotenv()
    # Load google credentials for parsons
    creds_file = 'service_account.json'  # File path to OAuth2.0 JSON Credentials
    creds = json.load(open(creds_file))  # Load JSON credentials
    api_keys_file = 'api_keys.json'
    api_keys = json.load(open(api_keys_file))



#-------------------------------------------------------------------------------
# Intantiate parsons and gspread classes
#-------------------------------------------------------------------------------
# Load redshift and VAN credentials
rs = Redshift()
# Instantiate parsons google sheets class
parsons_sheets = GoogleSheets(google_keyfile_dict=creds)  # Instantiate parsons GSheets class
# Set up google sheets connection for gspread package
scope = [
    'https://spreadsheets.google.com/feeds'
]
credentials = ServiceAccountCredentials.from_json_keyfile_dict(creds, scope)
gspread_client = gspread.authorize(credentials)



#-------------------------------------------------------------------------------
# Set global variables
#-------------------------------------------------------------------------------
# Connect to scheduled sheet
hubs = parsons_sheets.get_worksheet('1ESXwSfjkDrgCRYrAag_SHiKCMIgcd1U3kz47KLNpGeA', 'scheduled')
# Open errors tables and control table update table
upsert_errors = [['date', 'hub', 'first', 'last', 'email', 'error', 'traceback']]
hq_errors =[['date', 'script', 'hub', 'error', 'traceback', 'other_messages']]
control_table_update = [['hub', 'date_of_ea_sync_success']]



#-------------------------------------------------------------------------------
# Define functions
#-------------------------------------------------------------------------------
def log_error(e, script: str, note:str, error_table: list, hub:dict):
    """

    :param e: the exception
    :param script: a string with the name of the script where the error occurred
    :param note: a brief explanation of where the error occured formatted as a string
    :param error_table: the error table to log the error in
    :param hub: a dictionary with information about the hub from the scheduled sheet
    :return: Appends a row to the hq_errors list of lists, which is logged in Redshift at the end of the script
    """
    response = str(e)
    exception = str(traceback.format_exc())[:999]
    error_table.append([str(date.today()), script, hub['hub_name'], note, response[:999], exception])
    logger.info(f'''{note} for {hub['hub_name']}''')
    logger.info(response)

def get_hq(spreadsheet_id: str):
    """
    Get all records from hub's HQ
    :param spreadsheet_id: spreadsheet ID for the hub's HQ
    :return: Parson's table of all of the records
    """
    # Connect to the hq with gspread
    hq = gspread_client.open_by_key(spreadsheet_id)
    # Connect to the set up worksheet via gspread
    hq_worksheet = hq.worksheet('Hub HQ')
    hq_lists = hq_worksheet.get_all_values()
    hq_table = Table(hq_lists[2:])
    return hq_table

def subscribe_to_ea(new_hq_contacts, van, upsert_errors: list, hub):
    """
    Upsert (i.e. findOrCreate) new HQ contacts into hub's EveryAction committee
    :param new_hq_contacts: parsons table of new contacts
    :param van: parsons object of van class
    :param upsert_errors: list of lists where we're storing errors
    :param hub: dict of hub from scheduled sheet
    :return: None
    """
    new_hq_contacts.convert_column(['Phone'],lambda x: re.sub("[^0-9]", "", x)[-10:])
    new_hq_contacts.convert_column(['Zipcode'], lambda x:re.sub("[^0-9]", "", x)[:5])
    for contact in new_hq_contacts:
        json_dict = {
      'firstName': contact['First Name'],
      "lastName": contact['Last Name'],
      "emails":
            [{"email": contact['Email'],
            "isSubscribed":'true'}],
      "addresses":
            [{"zipOrPostalCode": contact['Zipcode']}],
      "phones":
            [{"phoneNumber":contact['Phone']}]
        }

        #Except (need to figure out what kind of errors I'll get here)
        try:
            van.upsert_person_json(json_dict)
            time.sleep(.5)
        except Exception as e:
            log_error(e, 'everyaction_sync', 'Upsert error', upsert_errors, hub)
            logger.info(json_dict)


def last_successful_syncs():
    """
    Get dates of the last time the HQ > EA sync ran successfully for each hub
    :return: Parson's table of dates of the last time the HQ > EA sync ran successfully for each hub
    """
    sql = f'''
SELECT
	hub
	,MAX(TO_TIMESTAMP(date_of_ea_sync_success,'YYYY-MM-DD HH24:MI:SS'))::TEXT AS date
FROM sunrise.hq_ea_sync_control_table
GROUP BY hub
'''
    date_tbl = rs.query(sql)
    return date_tbl




#-------------------------------------------------------------------------------
# Define main
#-------------------------------------------------------------------------------
def main():
    # get date of most recent successful sync for each hub
    last_successful_sync_tbl = last_successful_syncs()
    # loop through the hubs and take new HQ contacts and post them to hubs committee
    for hub in hubs:
        # connect to hubs EveryAction committee
        van = VAN(api_key = api_keys[hub['hub_name']], db='EveryAction')
        # Get hub's HQ
        hq = get_hq(hub['spreadsheet_id'])
        # Get last time sync succeeded for this hub
        try:
            date_str = last_successful_sync_tbl.select_rows(lambda row: row.hub == hub['hub_name'])
            # Convert string to date time format
            date_last_sync = datetime.datetime.strptime(date_str[0]['date']+':00', "%Y-%m-%d %H:%M:%S%z")
            # Subset HQ rows to only include contacts that synced since last successful run
            new_hq_contacts = hq.select_rows(lambda row: datetime.datetime.strptime(row['Date Joined'][:19] + ' +00:00',
                                                "%m/%d/%Y %H:%M:%S %z") > date_last_sync)
        # For hubs who haven't had a sync yet
        except KeyError as e:
            log_error(e, 'everyaction_sync', 'No record of hub in control table, could be hubs first run', hq_errors, hub)
            # Upsert all contacts in sheet
            new_hq_contacts = hq

        # Upsert new contacts to EA
        subscribe_to_ea(new_hq_contacts, van, upsert_errors,hub)
        # get now
        now = datetime.datetime.now(timezone.utc)
        now_str = datetime.datetime.strftime(now,'%Y-%m-%d %H:%M:%S')
        # add sync date to control table
        control_table_update.append([hub['hub_name'],now_str])

    rs.copy(Table(control_table_update), 'sunrise.hq_ea_sync_control_table', if_exists='append', distkey='hub',
            sortkey='date_of_ea_sync_success', alter_table=True)
    if len(hq_errors) > 1:
        rs.copy(Table(hq_errors), 'sunrise.hub_hq_errors', if_exists='append', distkey='hub',
            sortkey='date', alter_table=True)
        logger.info(f'''{len(hq_errors)-1} errored hubs''')
    else:
        logger.info('Script executed without issue for all hubs')
    if len(upsert_errors) > 1:
        rs.copy(Table(upsert_errors), 'sunrise.hq_ea_sync_errors', if_exists='append', distkey='error',
            sortkey='date', alter_table=True)
        logger.info(f'''{len(hq_errors)-1} errored contacts''')
    else:
        logger.info(f'''All contacts were subscribed to the correct committees without errors''')



#-------------------------------------------------------------------------------
# Run main
#-------------------------------------------------------------------------------
if __name__ == '__main__':
    main()

