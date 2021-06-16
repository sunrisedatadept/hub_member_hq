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
from datetime import timezone, date
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
#_formatter = logging.Formatter('%(levelname)s %(message)s')
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
    'https://spreadsheets.google.com/feeds'
]
credentials = ServiceAccountCredentials.from_json_keyfile_dict(creds, scope)
gspread_client = gspread.authorize(credentials)



#-------------------------------------------------------------------------------
# Set global variables used in functions
#-------------------------------------------------------------------------------
# Put HQ columns into a dictionary to make it easy to reference
hq_columns = {
                'date_joined': 5,
                'first_name': 0,
                'last_name': 1,
                'email': 2,
                'phone': 3,
                'total_signups': 6,
                'total_attendances': 7,
                'first_signup': 8,
                'first_attendance': 9,
                'days_since_last_signup': 10,
                'days_since_last_attendance': 11,
                'status': 4,
                'zipcode': 14,
                'birthyear': 15,
                'source': 16,
                'interest_form_responses': 12,
                'data_entry_data': 13,
                'date_claimed':17
}

hq_column_letters = {
                'date_joined': 'F',
                'first_name': 'A',
                'last_name': 'B',
                'email': 'C',
                'phone': 'D',
                'total_signups': 'G',
                'total_attendances': 'H',
                'first_signup': 'I',
                'first_attendance': 'J',
                'days_since_last_signup': 'K',
                'days_since_last_attendance': 'L',
                'status': 'E',
                'zipcode': 'O',
                'birthyear': 'P',
                'source': 'Q',
                'interest_form_responses': 'M',
                'data_entry_data': 'N',
                'date_claimed': 'R'
}

unrestricted_column_letters = {
                    'interest_form_responses': 'F',
                    'data_entry_data': 'G'
                    }


signup_columns = {
    'date_joined': 0,
    'first_name': 1,
    'last_name': 2,
    'email': 3,
    'phone': 4,
    'zipcode': 5,
    'birthyear': 6
}

unrestricted_columns = ['first_name',
                        'last_name',
                        'email',
                        'phone',
                        'date_joined',
                        'interest_form_responses',
                        'data_entry_data',
                        'zipcode',
                        'birthyear']

hq_columns_list = ['first_name',
                    'last_name',
                    'email',
                    'phone',
                    'status',
                    'date_joined',
                    'total_signups',
                    'total_attendances',
                    'first_signup',
                    'first_attendance',
                    'days_since_last_signup',
                    'days_since_last_attendance',
                    'interest_form_responses',
                    'data_entry_data',
                    'zipcode',
                    'birthyear',
                    'source']

# Get scheduled spreadsheet (hub hqs to loop through)
hubs = parsons_sheets.get_worksheet('1ESXwSfjkDrgCRYrAag_SHiKCMIgcd1U3kz47KLNpGeA', 'scheduled')
# Create errors list of lists to populate and push to redshift at the end
hq_errors = [['date', 'script', 'hub', 'error', 'traceback', 'other_messages']]



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
    error_table.append([str(date.today()), script, hub['hub_name'], response[:999], exception, note])
    logger.info(f'''{note} for {hub['hub_name']}''')
    logger.info(response)


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
    email, phone, zipcode, birthyear, and a concatenation of all of the other data from columns to the right of birthyear
    """
    # Open a dictionary, which will be transformed into a deduplicated dictionary of form responses/data entry data and
    # later be subset to only contain records that aren't HQ
    sheet_dict = {}
    # Remove the column header from the list of lists
    data = worksheet[1:]
    # Loop through the form responses/data entry sheet and create a new row/list in the sheet_dict if a row/list doesn't
    # yet exist for the contact, or update the row/list if it already exists in sheet_dict
    for row in data:
        email_address = row[signup_columns['email']]
        first_compilable_col = signup_columns['birthyear'] + 1
        num_cols = len(row)
        # If the row/list already exists, for each column/list item, append data from the new row to the existing row
        try:
            sheet_dict[email_address]
            # only update columns/items past birthyear column/item
            i = first_compilable_col
            while i < num_cols:
                # If there's any data in that field and the row has an email, append new data to existing column data
                if row[i] and email_address:
                    sheet_dict[email_address][i] = sheet_dict[email_address][i] + ', ' + row[i]
                i = i+1
        # If no row/lists exists, add it to the sheet_dict
        except KeyError:
            if email_address:
                sheet_dict[email_address] = row



    # Take all of the columns/list items after birthyear and concatenate them together with line breaks and column headers
    # right after the line breaks and right before the data for those columns
    for contact in sheet_dict:
        # check how many fields of data there are for the contact
        num_cols_with_data = sum(1 for i in sheet_dict[contact] if len(i)>0)
        if num_cols_with_data > 13:
            # If there are too many fields to concatenate into one cell, display this message
            sheet_dict[contact][signup_columns['birthyear'] + 1] = ('Too much data to display \n' 
                                                                 'Use ctr + f to find persons data\n' 
                                                                 'in Interest Form or Data Entry sheet')
        else:
            # Stick all the data from all columns together into one column/list item, separating each column's data with a
            # line break. We're sticking all of the items after birthyear into the item immediately after birthyear, so we
            # really only need to loop through the items starting 1 item after zipcde and append them to the concatenated
            # field
            concat_column = signup_columns['birthyear'] + 1
            num_columns = len(sheet_dict[contact])
            i = concat_column
            while i < num_columns:
                # if the concatenated column values are not empty, compile them. First column gets special treatment
                if sheet_dict[contact][i] and i == concat_column:
                    sheet_dict[contact][i] = worksheet[0][i][:20]+': ' + sheet_dict[contact][i] + '\n'
                elif sheet_dict[contact][i]:
                    sheet_dict[contact][concat_column] = (sheet_dict[contact][concat_column] + worksheet[0][i][:20]+': '
                        + sheet_dict[contact][i] + '\n')
                # Go to next columm
                i=i+1

        # Remove all columns after concatenation column
        sheet_dict[contact] = sheet_dict[contact][0:signup_columns['birthyear'] + 2]

    return sheet_dict


def mark_as_claimed(hq_row: list, idx: int, hq_worksheet):
    """
    If a contact originally arrived in Hub HQ from the national database, but then enters the system again via one of 
    the hub's data sources, they need to be marked so that they may be subscribed to the hub's EveryAction committee. 
    This function checks whether a contact originally came to HQ froma the national list but then was 'claimed' by a hub
    then records the date they were claimed in Hub Hq so that they are synced into EveryAction. 
    :param hq_row: the Hub HQ row that corresponds to this contacts
    :param idx: the row number of this hub member in the hub hq table
    :param hq_worksheet: hub's hq worksheet (gspread class)
    :return: None
    """
    source = hq_row[hq_columns['source']]
    date_claimed = hq_row[hq_columns['date_claimed']]
    if source == 'National Email List' and len(date_claimed) == 0:
        now = datetime.datetime.now(timezone.utc)
        now_str = datetime.datetime.strftime(now, '%m/%d/%Y %H:%M:%S')
        cell = hq_column_letters['date_claimed'] + str(idx + 3)
        hq_worksheet.update_acell(cell, now_str)

def hq_updates(sheet_dict: dict, hq, sheet: str, hq_worksheet, unrestricted_sheet, hub: dict):
    """
    Updates concatenated forms response/data entry column in HQ by sending concatenated form response values from
    sheet_dict
    :param sheet_dict: dictionary returned by construct_update_dictionary
    :param hq: list of lists where each list is a row in the HQ
    :param sheet: either 'form responses' sheet or 'data entry sheet'
    :param hq_worksheet: gspread worksheet object for hq worksheet
    :param unrestricted_sheet: gspread worksheet object for hq worksheet
    :param hub: dictionary for that hub from set up sheet, retrieved by parsons
    :return: parsons table of contacts from sheet_dict that did not have any matches in HQ. This will be appended to HQ
    """
    # Make updates list
    updates = []
    # For each row in the hub_hq, if the contact is in the form data, then update the appropriate fields/items,
    # otherwise, pass
    for idx, hq_row in enumerate(hq):
        hq_email = hq_row[hq_columns['email']]
        concat_col_idx = signup_columns['birthyear'] + 1
        # Update Hub HQ records that have a match in the retrieved form data and remove from the form dictionary
        try:
            # Update each field/list item from the update_items for the match. This will create a whole update list/row
            sheet_dict[hq_email]
            responses = [sheet_dict[hq_email][concat_col_idx]]
            updates.append(responses)
            # If contact originated from ntl, added a claimed by hub date
            mark_as_claimed(hq_row, idx, hq_worksheet)
            del sheet_dict[hq_email]
        # When no match is found, create a list/row with empty values
        except KeyError:
            # Need to append what is there if we're going for opt-ins and zip
            updates.append([''])
    # Send the updates to Hub HQ
    #remove column header
    del updates[0]
    # If this is for form responses, put into column M
    if sheet == 'form responses':
        try:
            # Update concatenated form response column in hq sheet
            range1 = hq_column_letters['interest_form_responses'] + '4:' + hq_column_letters['interest_form_responses']
            hq_worksheet.update(range1, updates)
            # Update in unrestricted sheet
            range2 = (unrestricted_column_letters['interest_form_responses'] + '4:' +
                    unrestricted_column_letters['interest_form_responses'])
            unrestricted_sheet.update(range2, updates)
        except HttpError as e:
            log_error(e, 'sheets_sync', 'Error while updating form response column', hq_errors, hub)
    elif sheet == 'data entry sheet':
        try:
            # Update concatenated data entry field for existing records in hq sheet
            range3 = hq_column_letters['data_entry_data'] + '4:' + hq_column_letters['data_entry_data']
            hq_worksheet.update(range3, updates)
            # Update in unrestricted sheet
            range4 = (unrestricted_column_letters['data_entry_data'] + '4:' +
                      unrestricted_column_letters['data_entry_data'])
            unrestricted_sheet.update(range4, updates)
        except HttpError as e:
            log_error(e, 'sheets_sync', 'Error while updating data entry data column', hq_errors, hub)

    # Convert remainder of sheet_dict rows to a parsons table (to append to the bottom of hq)
    #First turn the dict into an array
    sheet_dict_array = [sheet_dict[email] for email in sheet_dict]
    # Create a list for the col headers and insert into the array
    if sheet == 'form responses':
        sheet_dict_cols = list(signup_columns.keys()) + ['interest_form_responses']
    elif sheet == 'data entry sheet':
        sheet_dict_cols = list(signup_columns.keys()) + ['data_entry_data']
    sheet_dict_array.insert(0,sheet_dict_cols)
    #convert to table
    sheet_dict_table = Table(sheet_dict_array)
    # Since there is no timestamp field for the data entry sheet, assign today's date as the date_joined value
    now = datetime.datetime.now(timezone.utc)
    now_str = datetime.datetime.strftime(now, '%m/%d/%Y %H:%M:%S')
    sheet_dict_table.fillna_column('date_joined',now_str)

    # Create empty tbl with HQ columns in the order they appear in the spreadhseet and then fill the table
    # with the new values from sheet dict. Really all we're doing here is ensuring that the new rows
    # we're appending in HQ are in the correct order
    hq_append_tbl = Table([hq_columns_list])
    hq_append_tbl.concat(sheet_dict_table)
    hq_append_tbl.fillna_column('status','HOT LEAD')
    hq_append_tbl.move_column('status',hq_columns['status'])
    if sheet == 'form responses':
        hq_append_tbl.fillna_column('source', 'Interest Form')
    if sheet == 'data entry sheet':
        hq_append_tbl.fillna_column('source', 'Data Entry Sheet')
    hq_append_tbl.move_column('source', hq_columns['source'])

    # Repeat last set of steps to prepare a table to append to the unrestricted sheet
    unrestrict_append_tbl = Table([unrestricted_columns])
    unrestrict_append_tbl.concat(sheet_dict_table)

    return hq_append_tbl, unrestrict_append_tbl



#-------------------------------------------------------------------------------
# Define main
#-------------------------------------------------------------------------------
def main():
    # Loop through the hubs and update existing HQ contacts with new data, append any new contactsto HQ
    for hub in hubs:
        # Connect to the hub's spreadsheet
        hq_worksheet = connect_to_worksheet(hub, 'Hub HQ')
        unrestricted_sheet = connect_to_worksheet(hub, 'Unrestricted')
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
        signup_form_table, unrestict_append1_tbl = hq_updates(signup_form_dict,hq,'form responses', hq_worksheet,
                                                              unrestricted_sheet, hub)
        # Append left over sign up form rows to HQ
        try:
            parsons_sheets.append_to_sheet(hub['spreadsheet_id'], signup_form_table, 'Hub HQ')
            parsons_sheets.append_to_sheet(hub['spreadsheet_id'], unrestict_append1_tbl, 'Unrestricted')
        except ValueError:
            logger.info(f'''No new data entries for {hub['hub_name']}''')
        except Exception as e:
            log_error(e, 'sheets_sync', 'Error adding new Interest Form contacts', hq_errors, hub)
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
        data_entry_table, unrestict_append2_tbl = hq_updates(data_entry_dict,hq,'data entry sheet', hq_worksheet,
                                                             unrestricted_sheet, hub)
        # Append left over data entry sheet rows to HQ
        try:
            parsons_sheets.append_to_sheet(hub['spreadsheet_id'], data_entry_table, 'Hub HQ')
            parsons_sheets.append_to_sheet(hub['spreadsheet_id'], unrestict_append2_tbl, 'Unrestricted')
        except ValueError:
            logger.info(f'''No new data entries for {hub['hub_name']}''')
        except Exception as e:
            log_error(e, 'sheets_sync', 'Error adding new Data Entry Sheet contacts', hq_errors, hub)
        # Send errors table to redshift
    if len(hq_errors) > 1:
        rs.copy(Table(hq_errors), 'sunrise.hub_hq_errors', if_exists='append', distkey='hub',
                sortkey='date', alter_table=True)
        logger.info(f'''{len(hq_errors) - 1} errored hubs''')
    else:
        logger.info('Script executed without issue for all hubs')



#-------------------------------------------------------------------------------
# Run main
#-------------------------------------------------------------------------------
if __name__ == '__main__':
    main()
