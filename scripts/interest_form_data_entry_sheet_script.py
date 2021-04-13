# Civis Container Script: https://platform.civisanalytics.com/spa/#/scripts/containers/111826689

# This script takes data from the Data Entry Sheet and the Interest Form sheet, compiles all data for each contacts, and
# then pushes those updates to the hub_hq sheet for any contact that has a match (based on email). It appends any
# contacts that do not match/do not yet exist in HQ. Errors are stored in sunrise.hub_hq_errors


#-------------------------------------------------------------------------------
# Import necessary Packages
#-------------------------------------------------------------------------------
import json
from parsons import GoogleSheets, Redshift, Table
import gspread
from datetime import timezone, timedelta, date
import datetime
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
    'date_joined': 4, 'first_name': 0, 'last_name': 1, 'email': 2, 'phone': 3, 'total_signups': 5,
    'total_attendances': 6, 'first_signup': 7, 'first_attendance': 8, 'last_signup': 9,
    'last_attendance': 10
}
signup_columns = {
    'Timestamp': 0, 'First Name': 1, 'Last Name': 2, 'Email Address': 3, 'Phone Number': 4, 'Zipcode': 5
}

# Get scheduled spreadsheet (hub hqs to loop through)
hubs = parsons_sheets.get_worksheet('1ESXwSfjkDrgCRYrAag_SHiKCMIgcd1U3kz47KLNpGeA', 'scheduled')
# Create errors list of lists to populate and push to redshift at the end
hq_errors = [['date', 'script', 'hub', 'error', 'traceback', 'other_messages']]



#-------------------------------------------------------------------------------
# Define functions
#-------------------------------------------------------------------------------
def connect_to_worksheet(hub: dict, sheet: str):
    """
    Connect to HQ worksheet for hub
    :param hub: dictionary for that hub from set up sheet, retrieved by parsons
    :return: A worksheet object of gspread class worksheet
    """
    # connect to spreadsheet with spread
    spreadsheet = gspread_client.open_by_key(hub['spreadsheet_id'])
    # Get Hub HQ as list of lists
    hq_worksheet = spreadsheet.worksheet(sheet)
    return hq_worksheet


def construct_update_dictionary(worksheet: list):
    """
    Gets all rows from sheet and updates form response/data entry sheet values cell for each record that exists in HQ
    already. Returns a parsons table of records without a match in HQ, which is appended later. The bulk of this
    function takes the data (in columns right of the zipcode column only) from all the rows in the response/data entry
    sheet that correspond to a unique row/record in the HQ sheet, and binds it all together into one single readable
    cell (which is the update sent to the HQ).
    :param: worksheet: a list of lists where each inner list is a spreadsheet row
    :return: a dictionary where each key is a unique email and each item is a list containing timestamp, first, last,
    email, phone, zipcode, and a concatenation of all of the other data from columns to the right of zipcode
    """
    # Open a dictionary, which will be transformed into a deduplicated dictionary of form responses/data entry data and
    # later be subset to only contain records that aren't HQ
    sheet_dict = {}
    # Loop through the form responses/data entry sheet and create a new row/list in the sheet_dict if a row/list doesn't
    # yet exist for the contact, or update the row/list if it already exists in sheet_dict
    for row in worksheet:
        # If the row/list already exists, for each column/list item, append data from the new row to the existing row
        try:
            sheet_dict[row[signup_columns['Email Address']]]
            # only update columns/items past zipcode column/item
            for i in range(len(row) - signup_columns['zipcode'] + 1):
                # jump to column/item right after zipcode column/item
                i = i + signup_columns['zipcode'] + 1
                # Skip if new column value/item has no data
                if len(row[i]) == 0:
                    pass
                # Append new data to existing column/item data
                else:
                    sheet_dict[row[signup_columns['Email Address']]][i] = \
                        sheet_dict[row[signup_columns['Email Address']]][i] + ', ' + row[i]
        # If no row/lists exists, add it to the sheet_dict
        except KeyError:
            sheet_dict[row[signup_columns['Email Address']]] = row
    #        continue

    # Delete the row/list with the column headers
    del sheet_dict['Email Address']

    # Take all of the columns/list items after zipcode and concatenate them together with line breaks and column headers
    # right after the line breaks and right before the data for those columns
    for contact in sheet_dict:
        if sum(1 for i in sheet_dict[contact] if len(i)>0) > 13:
            # If there are too many fields to concatenate into one cell, give them this message
            sheet_dict[contact][signup_columns['Zipcode'] + 1] = 'Too much data to display \n' \
                                                                 'Use ctr + f to find persons data\n' \
                                                                 'in Interest Form or Data Entry sheet'
        else:
            # First stick the column headers right ahead of the data for the first column after zipcdoe (if there's
            # anything there, otherwise skip it)
            if len(sheet_dict[contact][signup_columns['Zipcode'] + 1]) > 0:
                sheet_dict[contact][signup_columns['Zipcode'] + 1] = worksheet[0][signup_columns['Zipcode'] + 1] + ': ' + \
                                                            sheet_dict[contact][signup_columns['Zipcode'] + 1] + '\n'
            else:
                pass
            # Stick all the data from all columns together into one column/list item, separating each column's data with a
            # line break. We're sticking all of the items after zipcode into the item immediately after zipcode, so we
            # really only need to loop through the items starting 2 items after zipcde and append them to the item
            # immediately after zipcode
            for i in range(len(sheet_dict[contact]) - signup_columns['Zipcode'] - 2):
                i = i + signup_columns['Zipcode'] + 2
                # if the concatenated column values are empty, skip
                if len(sheet_dict[contact][i]) == 0:
                    pass
                # Otherwise stick it all together
                else:
                    sheet_dict[contact][signup_columns['Zipcode'] + 1] = \
                        sheet_dict[contact][signup_columns['Zipcode'] + 1] + worksheet[0][i][:20] + ': ' \
                        + sheet_dict[contact][i] + '\n'
            # Remove all columns after concatenation column
        sheet_dict[contact] = sheet_dict[contact][0:signup_columns['Zipcode'] + 2]

    return sheet_dict

def hq_updates(sheet_dict: dict, hq, sheet: str, hq_worksheet, hub: dict):
    """
    Updates concatenated forms response/data entry column in HQ by sending concatenated form response values from
    sheet_dict
    :param sheet_dict: dictionary returned by construct_update_dictionary
    :param hq: list of lists where each list is a row in the HQ
    :param sheet: either 'form responses' sheet or 'data entry sheet'
    :param hq_worksheet: gspread worksheet object for hq worksheet
    :param hub: dictionary for that hub from set up sheet, retrieved by parsons
    :return: parsons table of contacts from sheet_dict that did not have any matches in HQ. This will be appended to HQ
    """
    # Make updates list
    updates = []
    # For each row in the hub_hq, if the contact is in the form data, then update the appropriate fields/items,
    # otherwise, pass
    for hq_row in hq:
        # Update Hub HQ records that have a match in the retrieved form data and remove from the form dictionary
        try:
            # Update each field/list item from the update_items for the match. This will create a whole update list/row
            sheet_dict[hq_row[hq_columns['email']]]
            responses = [sheet_dict[hq_row[hq_columns['email']]][signup_columns['Zipcode'] + 1]]
            updates.append(responses)
            del sheet_dict[hq_row[hq_columns['email']]]
        # When no match is found, create a list/row with empty values/just retain the value on record (which are empty)
        except KeyError:
            updates.append([hq_row[hq_columns['last_attendance'] + 2]])
    # Send the updates to Hub HQ
    #remove column header
    del updates[0]
    # If this is for form responses, put into column M
    if sheet == 'form responses':
        try:
            # Update concatenated form response column
            hq_worksheet.update('M4:M', updates)

            # Convert remainder of sheet_dict rows to lists, which will be converted to a parson's table
            sheet_data_append = [sheet_dict[row][1:signup_columns['Zipcode']] + \
                                 [sheet_dict[row][signup_columns['Timestamp']]] + ['' for x in range(6)] + \
                                 ['HOT LEAD'] + [sheet_dict[row][signup_columns['Zipcode'] + 1]] + [''] + \
                                 [sheet_dict[row][signup_columns['Zipcode']]] \
                                 for row in sheet_dict if sheet == 'form responses']
            sheet_data_append.insert(0, ['first_name', 'last_name', 'email', 'phone', 'date_joined', 'total_signups',
                                         'total_attendances', 'first_signup', 'first_attendance', 'last_signup',
                                         'last_attendance',
                                         'status', 'form_responses', 'data_entry_sheet_data', 'Zipcode'])
        except HttpError as e:
            logger.info(e)
            error = str(e)
            exceptiondata = traceback.format_exc().splitlines()
            exception = exceptiondata[len(exceptiondata)-1]
            hq_errors.append([str(date.today()), 'everayction_sync', hub['hub_name'], error[:999], exception[:999],
                              'if first time run for hub, hub_name will not be in control table'])
            logger.info(f'''Https error while updating {hub['hub_name']} hub's concatenated form response column''')
    # else it's for data entry and put into column N
    elif sheet == 'data entry sheet':
        try:
            # Update existing records
            hq_worksheet.update('N4:N', updates)
            # Take remaining unmatched records from dictionary and put them into a list, which is then converted to a
            # table and appended
            # Fill Timestamp column with today's datetime, which EA sync uses
            now = datetime.datetime.now(timezone.utc)
            now_str = datetime.datetime.strftime(now, '%m/%d/%Y %H:%M:%S')
            sheet_data_append = [sheet_dict[row][1:signup_columns['Zipcode']] + \
                                 [now_str] + ['' for x in range(6)] + ['HOT LEAD'] + [''] + \
                                 [sheet_dict[row][signup_columns['Zipcode'] + 1]] + \
                                 [sheet_dict[row][signup_columns['Zipcode']]] \
                                 for row in sheet_dict]
            sheet_data_append.insert(0, ['first_name', 'last_name', 'email', 'phone', 'date_joined', 'total_signups',
                                         'total_attendances', 'first_signup', 'first_attendance', 'last_signup',
                                         'last_attendance',
                                         'status', 'form_responses', 'data_entry_sheet_data', 'Zipcode'])
        except HttpError as e:
            error = str(e)
            exceptiondata = traceback.format_exc().splitlines()
            exception = exceptiondata[len(exceptiondata)-1]
            hq_errors.append([str(date.today()), 'everayction_sync', hub['hub_name'], error[:999], exception[:999],
                              'if first time run for hub, hub_name will not be in control table'])
            logger.info(f'''Https error while updating {hub['hub_name']} hub's concatenated data entry column''')
    # Convert remainder of sheet_dict rows to lists, which will be converted to a parson's table
    else:
        print('WHYYYY')
    sheet_parsons_append = Table(sheet_data_append)
    return sheet_parsons_append



#-------------------------------------------------------------------------------
# Define main
#-------------------------------------------------------------------------------
def main():
    # Loop through the hubs and update existing HQ contacts with new data, append any new contactsto HQ
    for hub in hubs:
        # Connect to the hub's spreadsheet
        hq_worksheet = connect_to_worksheet(hub, 'Hub HQ')
        # Get Hub HQ table
        hq = hq_worksheet.get_all_values()
        hq = hq[2:]
        # Connect to form responses sheet
        interest_form_responses = connect_to_worksheet(hub, 'Interest Form')
        # Get sign up sheet values
        signup_form_responses = interest_form_responses.get_all_values()
        signup_form_responses = signup_form_responses[1:]
        # Get deduplicated updates dictionary for form responses
        signup_form_dict = construct_update_dictionary(signup_form_responses)
        # Push updates to HQ and get left over unmatched rows back (which we append immediately after)
        signup_form_table = hq_updates(signup_form_dict,hq,'form responses', hq_worksheet, hub)
        # Append left over sign up form rows to HQ
        try:
            parsons_sheets.append_to_sheet(hub['spreadsheet_id'], signup_form_table, 'Hub HQ')
        except ValueError:
            logger.info(f'''No new signup form contacts for {hub['hub_name']}''')
        # Repeat process for data entry sheet
        # Get Hub HQ table, which now has new form respondants
        hq = hq_worksheet.get_all_values()
        hq = hq[2:]
        # Get data entry sheet
        data_entry_sheet = connect_to_worksheet(hub, 'Data Entry')
        # Get the data entry sheet
        data_entry_data = data_entry_sheet.get_all_values()
        data_entry_data = data_entry_data[1:]
        # Get deduplicated updates dictionary for data entry sheet
        data_entry_dict = construct_update_dictionary(data_entry_data)
        # Push updates to HQ and get left over unmatched rows back (which we append after adding date)
        data_entry_table = hq_updates(data_entry_dict,hq,'data entry sheet', hq_worksheet)
        # Append left over data entry sheet rows to HQ
        try:
            parsons_sheets.append_to_sheet(hub['spreadsheet_id'], data_entry_table, 'Hub HQ')
        except ValueError:
            logger.info(f'''No new data entries for {hub['hub_name']}''')
        # Send errors table to redshift
        try:
            rs.copy(Table(hq_errors), 'sunrise.hub_hq_errors', if_exists='append', distkey='hub',
                    sortkey='date', alter_table=True)
            logger.info(f'''{len(hq_errors) - 1} errored hubs''')
        except ValueError:
            logger.info('Script executed without issue for all hubs')



#-------------------------------------------------------------------------------
# Run main
#-------------------------------------------------------------------------------
if __name__ == '__main__':
    main()
