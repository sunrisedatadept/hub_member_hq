# Civis container script: https://platform.civisanalytics.com/spa/#/scripts/containers/111401507

# This script gets event attendance data from mobilize for each hub in the 'scheduled' sheet as a table of unique
# contacts with their contact info and event attendance history. It compares those contacts to the contacts in HQ
# Spreadsheet for that hub and updates event attendance history for any contacts that already exist in HQ sheet, and
# appends and new Mobilize contacts that don't have a match in HQ sheet. Match is based on email.
# Errors are logged in sunrise.hub_hq_errors

#-------------------------------------------------------------------------------
# Import necessary Packages
#-------------------------------------------------------------------------------
import json
from parsons import GoogleSheets, Redshift, Table
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import logging
from datetime import timezone, date, timedelta
import datetime
import os
import traceback



#-------------------------------------------------------------------------------
# Load Environment
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
# Set up logger
#-------------------------------------------------------------------------------
##### Set up logger #####
logger = logging.getLogger(__name__)
_handler = logging.StreamHandler()
_formatter = logging.Formatter('%(levelname)s %(message)s')
_handler.setFormatter(_formatter)
logger.addHandler(_handler)
logger.setLevel('INFO')
logger.info('Hey there! I hope youre having a nice day and sorry in advance if I cause you any trouble.')



#-------------------------------------------------------------------------------
# Instantiate classes
#-------------------------------------------------------------------------------
# Set up environment and load credentials
rs = Redshift()  # Redshift
# Load google credentials for parsons
parsons_sheets = GoogleSheets(google_keyfile_dict=creds)
# Set up google sheets connection for gspread package
scope = [
    'https://spreadsheets.google.com/feeds',
    'https://www.googleapis.com/auth/drive',
]
credentials = ServiceAccountCredentials.from_json_keyfile_dict(creds, scope)
gspread_client = gspread.authorize(credentials)



#-------------------------------------------------------------------------------
# Set global variables
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
                'status':4,
                'zipcode':14,
                'interest_form_responses':12,
                'data_entry_data':13
}
# Store as list too
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
                    'zipcode']
# Get 'scheduled' spreadsheet
hubs = parsons_sheets.get_worksheet('1ESXwSfjkDrgCRYrAag_SHiKCMIgcd1U3kz47KLNpGeA', 'scheduled')
# Create errors list of lists to populate and push to redshift at the end
hq_errors = [['date', 'script', 'hub', 'error', 'traceback', 'other_messages']]



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


def get_mobilize_data(hub: dict):
    """
    Get Mobilize event attendance data for hub
    :param hub: dictionary for that hub from set up sheet, retrieved by parsons
    :return: A dictionary of dictionaries where each key is a unique email and each item is a row of info from Mobilize
    """
    # Get Mobilize data -- query returns a table of deduped contacts and their event attendance history
    event_attendance_sql = f'''
with 
-- deals with duplicate
most_recent as 
(
    select
        ppl.created_date,
  		ppl.user_id as person_id, 
        ppl.user__given_name as first_name,
        ppl.user__family_name as last_name,
        ppl.user__email_address as email,
        ppl.user__phone_number as phone_number,
        ppl.status,
        ppl.timeslot_id,
        ppl.event_id,
        ppl.attended,
        ppl.start_date,
  		events.id,
  		events.title,
        row_number() over (partition by ppl.id order by ppl.created_date::date desc) = 1 as is_most_recent
  	from sunrise_mobilize.participations ppl
    left join sunrise_mobilize.events events on ppl.event_id = events.id
    where events.creator__email_address ilike '{hub['hub_email']}'
),


--Get unique signups
signups as
(
    select * from most_recent where is_most_recent = true
)

-- get unique people rows from signups
select 
    max(first_name) as first_name,
    max(last_name) as last_name,
    email,
    max(phone_number) as phone,
    to_char(min(created_date),'MM/DD/YYYY HH24:MI:SS')::text as date_joined,
    count(*) as total_signups,
    sum
    (case 
        when attended = true then 1
        else 0
    end) as total_attendances,
    min(start_date::date)::text as first_signup,
    min
        (case 
            when attended = true then start_date::date
            else null
        end)::text as first_attendance,
    datediff(day,max(start_date)::date,getdate()) as days_since_last_signup,
    datediff(
        day
        ,max(case 
                when attended = true then start_date
                else null
            end)::date
        ,getdate()
        ) as days_since_last_attendance
from signups
group by email
order by min(created_date)
'''
    # Send query to mobilize
    mobilize_data = rs.query(sql=event_attendance_sql)
    if mobilize_data.num_rows == 0:
        return
    # Store mobilize rows in a dictionary where each row's key is an email (used for matching)
    else:
        mobilize_dict = {i['email']: i for i in mobilize_data}
        return mobilize_dict

def assign_status(hq_row: list, mobilize_dict: dict, event_threshold: int, inactivity_threshold: int):
    """
    Assign a member status based on a person's sign up status
    :param hq_row: A list analogue to a row in HQ
    :param mobilize_dict: a dictionary of data from mobilize, where each key is an email address
    :param event_threshold: Num events person must signup for to be considered a member. Comes from settings sheet
    :param inactivity_threshold: Num days of not signing up for events before being considered inactive
    :return: a string with the person's status
    """
    # Assign the email, total_signups, datejoined value for each row to an object for readability

    hq_email = hq_row[hq_columns['email']]
    hq_datejoined = hq_row[hq_columns['date_joined']]
    now = datetime.datetime.now(timezone.utc)
    sevendays = datetime.timedelta(days=7)
    inactivity_threshold_delta = datetime.timedelta(days=inactivity_threshold)
    sixtydays = datetime.timedelta(days=60)
    # Assign status based on event sign up metrics
    # Start by getting date joined from HQ
    date_joined = datetime.datetime.strptime(hq_datejoined[:19] + ' +00:00',
                                             "%m/%d/%Y %H:%M:%S %z")
    time_since_joined = now - date_joined

    if time_since_joined <= sevendays:
        status = 'HOT LEAD'
    elif sevendays < time_since_joined <= sixtydays:
        status = "Prospective/New Member"
    elif mobilize_dict[hq_email]['total_signups'] > event_threshold and \
            mobilize_dict[hq_email]['days_since_last_signup'] < inactivity_threshold:
        status = 'Active Member'
    elif mobilize_dict[hq_email]['total_signups'] > event_threshold and \
            mobilize_dict[hq_email]['days_since_last_signup'] >= inactivity_threshold:
        status = 'Inactive Member'
    elif mobilize_dict[hq_email]['total_signups'] <= event_threshold and time_since_joined > sixtydays:
        status = 'Never got involved'
    else:
        status = 'error (plz contact hub-hq-help@sunrisemovement.org)'
    return status

def mobilize_updates(mobilize_dict: dict, hq: list, hq_worksheet, hq_columns: dict, event_threshold: int,
                     inactivity_threshold: int):
    """
    Each row/list from the HQ is checked for a match in the mobilize data using email. A new list of lists is created
    where each list is a person's event attendance record from mobilize. If there is an email match then the resulting
    list/row for that contact contains event attendance data and the row for that contact is removed  from the mobilize
    data dictionary (which we append to Hub HQ later). If there is no match, then the resulting row/list will have
    four empty values. The rows/lists are ordered within the outer list exactly how the Hub HQ is ordered so that we
    can push the list back to the HQ and have each event attendance record line up with the correct contact in the
    Hub HQ. Finally the updates are pushed to the Hub HQ and the mobilize rows for which there were no matches in
    the HQ are returned as a parson's table
    :param mobilize_dict: dictionary of mobilize data where each key is a unique email
    :param hq: a list of lists, where each inner list is a row from the hub's HQ
    :param hq_columns: dictionary indicating the index of each HQ column in the actual spreadsheet
    :param hq_worksheet: the hq worksheet, which is a gspread class of object
    :param event_threshold: Num events person must signup for to be considered a member. Comes from settings sheet
    :param inactivity_threshold: Num days of not signing up for events before being considered inactive
    :return: A parson's table of mobilize records without matches in the HQ
    """

    # Create a list of the event sign up/attencance summary fields we're going to attenpt to update in the HQ
    update_items = hq_columns_list[hq_columns['total_signups']:hq_columns['interest_form_responses']]
    # Open a list for the updates, which we will fill with lists, one for each contact.
    event_attendance_updates = []
    # For each row in the hub_hq, if the email is in the mobilize data, then update the appropriate fields/items,
    # otherwise,append a list of blank values
    for hq_row in hq:
        # Assign the email, total_signups, datejoined value for each row to an object for readability
        hq_email = hq_row[hq_columns['email']]
        # Update Hub HQ records that have a match in the retrieved mobilize data and remove from the mobilize data
        try:
            # Substitute mobilize value in for each field/list item from the update_items for the match. This will
            # create a whole update list/row
            for i in update_items:
                # If the email address of the hq row exists in the mobilize data, append the correct event attendance
                # value to the list
                hq_row[hq_columns[i]] = mobilize_dict[hq_email][i]

            hq_row[hq_columns['status']] = assign_status(hq_row, mobilize_dict, event_threshold,
                                                         inactivity_threshold)
            # Reduce to fields that need to be updated
            update_row = hq_row[hq_columns['status']:hq_columns['interest_form_responses']]
            # Add to the update list of lists
            event_attendance_updates.append(update_row)
            # Remove contact from mobilize parson's table dictionary, which will be appended to Hub HQ sheet
            del mobilize_dict[hq_email]
        # When no match is found, create a list/row with empty values/just retain the value on record (which are empty)

        except KeyError:
            status = assign_status(hq_row, mobilize_dict, event_threshold, inactivity_threshold)
            event_attendance_updates.append([status] + hq_row[hq_columns['date_joined']:
                                                              hq_columns['interest_form_responses']])
    # Send the updates to Hub HQ
    hq_worksheet.update('E4:L', event_attendance_updates)

    # Now we convert the remaining Mobilize records, for which no matches were found, and reformat them to a parson's
    # table so that we can append them to the google sheet using the parson's google sheet append method. We also add a
    # value of 'Mobilize' for the source column

    # Convert remainder of mobilize dictionary rows to lists, which will be converted to a parsons table
    # create list of lists
    hq_columns_wo_status = hq_columns_list[hq_columns['first_name']:hq_columns['status']] + \
                            hq_columns_list[hq_columns['date_joined']:hq_columns['interest_form_responses']]
    mobilize_data_append = [[mobilize_dict[i][value] for value in hq_columns_wo_status] for i in mobilize_dict]
    # insert column headers
    mobilize_data_append.insert(0,hq_columns_wo_status)
    # convert to parsons table
    mobilize_parsons_append = Table(mobilize_data_append)
    # Add column for status and assign value HOT LEAD since this script is running everyday and only people who just
    # signed up for their first event will be in this append table. The updates section of the script will update their
    # status in the future
    mobilize_parsons_append.add_column('status','HOT LEAD',4)
    return mobilize_parsons_append



#-------------------------------------------------------------------------------
# Define main
#-------------------------------------------------------------------------------
def main():
    # Loop through hubs and update event attendance info for contacts that already exist in HQ and contacts that don't
    for hub in hubs:
        # Connect to the hub's spreadsheet
        hq_worksheet = connect_to_sheet(hub,'hq')
        # Connect to hub's settings sheet
        settings_sheet = connect_to_sheet(hub,'settings')
        # Get Hub HQ table
        hq = hq_worksheet.get_all_values()
        # Remove first 3 rows (column headers and instructions/tips)
        hq = hq[3:]
        # Send for Mobilize Data
        mobilize_dict = get_mobilize_data(hub)
        # if not mobilize data
        if mobilize_dict is None:
            hq_errors.append([str(date.today()), 'mobilize_script', hub['hub_name'],
                              f'''No mobilize events associated with hub email {hub['hub_email']}''', 'NA', 'NA'])
            logger.info(f'''No mobilize events associated with hub {hub['hub_name']} email {hub['hub_email']}''')
            continue
        else:
            # Try to send mobilize event attendance updates to HQ and get the left over mobilize rows for which no
            # matches were found in HQ
            try:
                # Get inactivity threshold that determines when to mark a member as inactive
                inactivity_threshold = int(settings_sheet.get('E4')[0][0])
                # Get event threshold that determines how many events someone has to sign up for before being
                # considered a member
                event_threshold = int(settings_sheet.get('F4')[0][0])
                mobilize_parsons_append = mobilize_updates(mobilize_dict, hq, hq_worksheet,
                                                           hq_columns, event_threshold, inactivity_threshold)
            # Append left over mobilize rows to HQ
                try:
                    parsons_sheets.append_to_sheet(hub['spreadsheet_id'], mobilize_parsons_append, 'Hub HQ')
                except ValueError as e:
                    logger.info(f'''No new mobilize contacts for {hub['hub_name']}''')
                except Exception as e:
                    response = str(e)
                    exception = str(traceback.format_exc())[:999]
                    hq_errors.append([str(date.today()), 'mobilize_script', hub['hub_name'],
                                      'Error applying event sign up updates', response[:999], exception])
                    logger.info(f'''Error appending new mobilize contacts for {hub['hub_name']}''')
                    logger.info(response)
            except Exception as e:
                response = str(e)
                exception = str(traceback.format_exc())[:999]
                hq_errors.append([str(date.today()), 'mobilize_script', hub['hub_name'],
                                  'Error applying event sign up updates', response[:999], exception])
                logger.info(f'''Error updating event history for {hub['hub_name']}''')
                logger.info(response)
    if len(hq_errors) > 1:
        rs.copy(Table(hq_errors), 'sunrise.hub_hq_errors', if_exists='append', distkey='hub',
            sortkey='date', alter_table=True)
        logger.info(f'''{len(hq_errors)-1} errored hubs''')
    else:
        logger.info('Script executed without issue for all hubs')



#-------------------------------------------------------------------------------
# Run main
#-------------------------------------------------------------------------------
if __name__ == '__main__':
    main()
