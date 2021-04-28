# Civis container script: https://platform.civisanalytics.com/spa/#/scripts/containers/114050467

# This script compiles data from all Hub HQs into one table and copies that table to Redshift, dropping and replacing
# the previous version of the table.

#-------------------------------------------------------------------------------
# Import necessary Packages
#-------------------------------------------------------------------------------
import json
from parsons import GoogleSheets, Redshift, Table
import gspread
from googleapiclient.errors import HttpError
from oauth2client.service_account import ServiceAccountCredentials
import logging
import traceback
import os




#-------------------------------------------------------------------------------
# Set up logger
#-------------------------------------------------------------------------------
logger = logging.getLogger(__name__)
_handler = logging.StreamHandler()
_formatter = logging.Formatter('%(levelname)s %(message)s')
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

#If running locally, load this env
except KeyError:
    from dotenv import load_dotenv
    load_dotenv()
    # Load google credentials for parsons
    creds_file = 'service_account.json'  # File path to OAuth2.0 JSON Credentials
    creds = json.load(open(creds_file))  # Load JSON credentials



#-------------------------------------------------------------------------------
# Instantiate parsons and gspread classes
#-------------------------------------------------------------------------------
# Instantiate redshift class
rs = Redshift()
# Load google credentials for parsons
parsons_sheets = GoogleSheets(google_keyfile_dict=creds)  # Instantiate parsons GSheets class
# Set up google sheets connection for gspread package
scope = [
    'https://spreadsheets.google.com/feeds',
    'https://www.googleapis.com/auth/drive',
]
credentials = ServiceAccountCredentials.from_json_keyfile_dict(creds, scope)
gspread_client = gspread.authorize(credentials)

#-------------------------------------------------------------------------------
# Set global variables used in functions
#-------------------------------------------------------------------------------
# Put HQ columns into a dictionary to make it easy to reference
hq_columns = {
    'date_joined': 4,
    'first_name': 0,
    'last_name': 1,
    'email': 2,
    'phone': 3,
    'total_signups': 5,
    'total_attendances': 6,
    'first_signup': 7,
    'first_attendance': 8,
    'last_signup': 9,
    'last_attendance': 10
}
# Create list of HQ columns
hq_columns_list = ['first_name',
                    'last_name',
                    'email',
                    'phone',
                    'date_joined',
                    'total_signups',
                    'total_attendances',
                    'first_signup',
                    'first_attendance',
                    'days_since_last_signup',
                    'days_since_last_attendance']
# Get scheduled spreadsheet (hub hqs to loop through)
hubs = parsons_sheets.get_worksheet('1ESXwSfjkDrgCRYrAag_SHiKCMIgcd1U3kz47KLNpGeA', 'scheduled')
# Create errors list of lists to populate and push to redshift at the end
hq_errors = [['date', 'script', 'hub', 'error', 'traceback', 'other_messages']]
# Create parsons table to compile hub hqs into
all_hub_members = Table([hq_columns_list])
# insert column for hub
all_hub_members.add_column('hub',index=0)



#-------------------------------------------------------------------------------
# Define Functions
#-------------------------------------------------------------------------------
def connect_to_sheet(hub: dict, sheet: str):
    """
    Connect to HQ worksheet for hub
    :param hub: dictionary for that hub from set up sheet, retrieved by parsons
    :param sheet: a string indicating if you want the hq sheet 'hq' or the settings sheet 'settings'
    :return: A worksheet object of gspread class worksheet
    """
    # connect to spreadsheet with spread
    spreadsheet = gspread_client.open_by_key(hub['spreadsheet_id'])
    # Get Hub HQ as list of lists
    if sheet == 'hq':
        worksheet = spreadsheet.worksheet('Hub HQ')
    elif sheet == 'settings':
        worksheet = spreadsheet.worksheet('HQ Settings')
    return worksheet




#-------------------------------------------------------------------------------
# Define main
#-------------------------------------------------------------------------------
def main():
    for hub in hubs:
        # Connect to HQ sheet
        hq_worksheet = connect_to_sheet(hub, 'hq')
        # Get all values
        hq_all = hq_worksheet.get_all_values()
        # Remove top three rows (instructional and python/sql unfriendly column headers)
        # And insert python/sql friendly column headers
        hq = hq_all[3:]
        hq.insert(0, hq_columns_list)
        # and convert to Parsons table
        hq_table = Table(hq)
        # Add and fill hub column
        hq_table.add_column('hub',hub['hub_name'],0)
        # Stack to all_hub_members
        all_hub_members.stack(hq_table)

    # Copy to redshift and drop existing table
    # Using email as sort and dist key because the AWS help docs say
    #'If you frequently join a table, specify the join column as both the sort key and the distribution key.'
    rs.copy(all_hub_members,'sunrise.hq_hub_members',if_exists='drop',sortkey='email',distkey='email')



#-------------------------------------------------------------------------------
# Run main
#-------------------------------------------------------------------------------
if __name__ == '__main__':
    main()