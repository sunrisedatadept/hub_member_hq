# Civis container script: https://platform.civisanalytics.com/spa/#/scripts/containers/112676833

# This script pushes new contacts from the national database to a hub's HQ. It looks into hub HQ to find the most
# recently created/added contact from the national database, and queries Redshift for any contacts who have had
# addresses created since then that are within the hub's zipcode radius search (zipcode radius search info stored in the
# 'scheduled' tab of the hub HQ Set Up sheet
# [https://docs.google.com/spreadsheets/d/1ESXwSfjkDrgCRYrAag_SHiKCMIgcd1U3kz47KLNpGeA/edit#gid=0]).
# To prevent duplicates in HQ, the script compares the email address of each new contact to the email addresses of
# records in HQ and only appends non-matches.
# Errors are logged in sunrise.hub_hq_errors

#-------------------------------------------------------------------------------
# Import necessary packages
#-------------------------------------------------------------------------------
from pyzipcode import ZipCodeDatabase
from parsons import GoogleSheets, Redshift, Table
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import json
import logging
import traceback
from datetime import date
import os



#-------------------------------------------------------------------------------
# Set up logger
#-------------------------------------------------------------------------------
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

#If running locally, load this env
except KeyError:
    from dotenv import load_dotenv
    load_dotenv()
    # Load google credentials for parsons
    creds_file = 'service_account.json'  # File path to OAuth2.0 JSON Credentials
    creds = json.load(open(creds_file))  # Load JSON credentials



#-------------------------------------------------------------------------------
# Intantiate parsons, gspread, and pyzipcode classes
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

# load zipcode database
zcdb = ZipCodeDatabase()



#-------------------------------------------------------------------------------
# Set global variables
#-------------------------------------------------------------------------------
# Define index of each spreadsheet column
sheet_columns = {
    'vanid': 0,
    'date_joined': 1,
    'first': 2,
    'last': 3,
    'age': 4,
    'email': 5,
    'phone': 6
}

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

# Get scheduled spreadsheet
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
    error_table.append([str(date.today()), script, hub['hub_name'],  response[:999], exception, note])
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
    worksheet = spreadsheet.worksheet(sheet)
    return worksheet


def zipcode_search(hub: dict):
    """
    Search for zipcodes within __ miles of hub's central zipcode
    :param hub: Parson's table row from hubs parson's table retrieved from setup spreadsheet
    :return: A string of zip codes within xyz radius of hub's central zip separated by commas and bounded by parentheses
    """
    try:
        # Search for zip codes within specified radius
        found = [z.zip for z in zcdb.get_zipcodes_around_radius(hub['zipcode'], hub['search_radius'])]
        # Put all zip codes from zip radius into parentheses for the SQL query below
        zip_object = '(' + ','.join(found) + ')'
        return zip_object
    # If something was wrong with the zipcode or the zipcode radius, log an error
    except Exception as e:
        log_error(e, 'new_national_contacts_sync', 'Zipcode search error', hq_errors, hub)

        return

def query_everyaction(zip_object: str, last_date: str, hub: dict):
    """
    Query EveryAction tables to get contacts in hub's zipcode radius created since last contact was synced
    :param zip_object: String returned by zipcode_search()
    :param last_date: The date_joined value of the last row in the table retrieved from the HQ
    :param hub: dictionary for that hub from set up sheet, retrieved by parsons
    :return: A parson's table of contacts in this hub's area created since last contact was synced
    """
    ea_query = f'''
-- we only want contacts with zip codes in our search radius
with zipcodes AS (
	SELECT
  		vanid
  		, zip5 AS zip
  		, datemodified
  		-- we only want most recent address
  		, ROW_NUMBER() OVER (PARTITION BY vanid ORDER BY datemodified DESC) = 1 AS is_most_recent
	FROM sunrise_ea.tsm_tmc_contactsaddresses_sm
	--merge field for zip codes go here
  	WHERE zip in {zip_object}),

-- narrow down to zip code of most recent address
zip AS (
    SELECT
  		vanid
  		, zip
        , datemodified
    FROM zipcodes
    WHERE
  		is_most_recent = TRUE
  		-- Can we actually retrieve the last date in the spreadsheet?
  		AND CONVERT_TIMEZONE('EST','UTC',datemodified) > DATEADD(MINUTE,1,'{last_date}'::datetime)
		-- remove contacts that have been deduped
  		AND vanid NOT IN
			(SELECT dupvanid
            FROM sunrise_ea.tsm_tmc_contactsdeduped_sm)),

-- Get contacts, which we will join to addresses in our zip search
contacts AS (
	SELECT
  		vanid
  		, datecreated as date_joined
  		, firstname AS first
  		, lastname AS last
  		, date_part_year(dob::date) AS birthyear
	FROM sunrise_ea.tsm_tmc_contacts_sm),

-- We also want their emails
emails AS (
	SELECT
  		vanid
  		, email
  		, ROW_NUMBER() OVER (PARTITION BY vanid ORDER BY datecreated) = 1 AS is_most_recent
	FROM sunrise_ea.tsm_tmc_contactsemails_sm),

-- get most recently created emails, but only ones that are subscribed
email AS (
	SELECT
		emails.vanid
  		, sub.email
	FROM emails
  	LEFT JOIN sunrise_ea.tsm_tmc_emailsubscriptions_sm sub ON sub.email = emails.email
	WHERE
		is_most_recent = TRUE
		AND sub.emailsubscriptionstatusid=2
		AND committeeid = 80541),

phones AS (
	SELECT
  		vanid
  		, phone
  		, ROW_NUMBER() OVER (PARTITION BY vanid ORDER BY datecreated) = 1 AS is_most_recent
	FROM sunrise_ea.tsm_tmc_contactsphones_sm),

-- get most recently created emails, but only ones that are opted in
phone AS (
	SELECT
		phones.vanid
  		, opt.phone
	FROM phones
  	LEFT JOIN sunrise_ea.tsm_tmc_phonesoptins_sm opt ON opt.phone = phones.phone
	WHERE
		is_most_recent = TRUE
		AND opt.phoneoptinstatusid IN (1,2)
		AND committeeid = 80541),

final_base as (
    SELECT
        contacts.first as first_name
      , contacts.last as last_name
      , email.email
      , phone.phone
      , TO_CHAR(CONVERT_TIMEZONE('EST','UTC', zip.datemodified), 'MM/DD/YYYY HH24:MI:SS') as date_joined
      , contacts.birthyear
      -- need to dedup by email
      , row_number() over(partition by email.email order by zip.datemodified desc) = 1 as is_most_recent
    FROM zip
    LEFT JOIN contacts on contacts.vanid = zip.vanid
    LEFT JOIN email on email.vanid = zip.vanid
    LEFT JOIN phone on phone.vanid = zip.vanid
    WHERE 
        -- Since hub_hq relies on email for matching, we can only give them contacts with subscribed email addresses
        email.email IS NOT NULL 
        AND date_part_year(current_date) - contacts.birthyear > 17
    ORDER BY date_joined)

select * from final_base where is_most_recent = TRUE
'''

    # Send query to Redshift
    try:
        ntl_contacts = rs.query(ea_query)
        return ntl_contacts
    except Exception as e:
        log_error(e, 'new_national_contacts_sync', 'Issue with redshift query', hq_errors, hub)
        # return table with 0 rows so script continues instead of erroring
        return




#-------------------------------------------------------------------------------
# Define main
#-------------------------------------------------------------------------------
def main():
    # Loop through hubs from spreadsheet and dump contacts from ntl database into respective spreadsheet
    for hub in hubs:
        zip_object = zipcode_search(hub)
        if zip_object:
            # Get date created of last contact dumped into hub hq from national database
            hub_hq = connect_to_worksheet(hub,'Hub HQ')
            hub_hq_rows = hub_hq.get_all_values()
            # Convert header rows and everything below into parsons table (top two rows of sheets are instructions)
            hub_hq_tbl = Table(hub_hq_rows[2:])
            hq_ntl_contacts = hub_hq_tbl.select_rows(lambda row: row.Source == 'National Email List')
            last_one = hq_ntl_contacts.num_rows - 1
            last_date = hq_ntl_contacts[last_one]['Date Joined']

            # This is the query used to get contacts from the national database.
            ntl_contacts = query_everyaction(zip_object, last_date, hub)
            if ntl_contacts is None:
                continue
            else:
                # Get list of emails that are already in hub hq
                hub_hq_emails = [row['Email'] for row in hub_hq_tbl]
                # Get all rows from the list of new national signups that don't exist in hub hq yet
                new_ntl_contacts = ntl_contacts.select_rows(lambda row: row.email not in hub_hq_emails)
                # Add source column and status columns and fill values accordingly
                new_ntl_contacts.add_column('source','National Email List')
                new_ntl_contacts.add_column('status', 'HOT LEAD')
                # Remove is_most_recent column
                new_ntl_contacts.remove_column('is_most_recent')
                # Put new_ntl_contacts into a table with the same column order as Hub HQ
                append_tbl = Table([hq_columns_list])
                append_tbl.concat(new_ntl_contacts)

                # Push new contacts to spreadsheet
                try:
                    parsons_sheets.append_to_sheet(hub['spreadsheet_id'], append_tbl, 'Hub HQ')
                except ValueError:
                    logger.info(f'''No new ntl contacts in {hub['hub_name']} hub\'s area''')
                except Exception as e:
                    log_error(e, 'new_national_contacts_sync', 'Error appending contacts to sheet', hq_errors, hub)
                    continue
        else:
            continue
# Append errors to Redshift errors table
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
