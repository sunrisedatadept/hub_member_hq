#Civis container script: https://platform.civisanalytics.com/spa/#/scripts/containers/112448113

# This script "sets up" Hub HQs by dumping all the contacts from the national EveryAction that are in the hub's area
# (based on a zipcode radius search) into the the 'contacts from national sheet.' It does so by looping through each hub
# in the 'set up' tabl of the Hub HQ Set Up Sheet
# (https://docs.google.com/spreadsheets/d/1ESXwSfjkDrgCRYrAag_SHiKCMIgcd1U3kz47KLNpGeA/edit#gid=0) and for each hub:
# 1) using the zipcode and zipcode radius provided to find all the zipcodes in the radius
# 2) Sending a query to REdshift to get all contacts that live in those zipcodes
# 3) Appending that list of contacts to the 'contacts from national' sheet
# If the script succeeds for a hub, that hub's info is transferred from the 'set up' tab to the 'scheduled' tab.
# If the script fails for a hub, the hub remains in the 'set up' tab and the traceback error is logged in the 'errors'
# tab of the same spreadsheet

#-------------------------------------------------------------------------------
# Import necessary Packages
#-------------------------------------------------------------------------------
from pyzipcode import ZipCodeDatabase
from parsons import GoogleSheets, Redshift, Table
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import json
import logging
import os
import traceback
from datetime import date



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
# Set up logger
#-------------------------------------------------------------------------------
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
#load zipcode database
zcdb = ZipCodeDatabase()
# Instantiate parson's Redshift class
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
# Define functions
#-------------------------------------------------------------------------------
def zipcode_search(hub: dict, errored_hub_list: list):
    """
    Search for zipcodes within __ miles of hub's central zipcode
    :param hub: Parson's table row from hubs parson's table retrieved from setup spreadsheet
    :param errors: List of errors
    :param errored_hub_list: Errorored hubs list (should already be in memory when this function is executed)
    :return: A string of zip codes within xyz radius of hub's central zip separated by commas and bounded by parentheses
    """
    try:
        # Search for zip codes within specified radius
        found = [z.zip for z in zcdb.get_zipcodes_around_radius(hub['zipcode'], hub['search_radius'])]

    # If something was wrong with the zipcode or the zipcode radius, log an error
    except Exception as e:
        response = str(e)
        exceptiondata = traceback.format_exc().splitlines()
        exception = exceptiondata[len(exceptiondata) - 1]
        errors.append([str(date.today()), hub['hub_name'], response, exception])
        # Create errored hub list, then append to list of errored hubs
        errored_hub_list.append(hub['hub_name'])
        logger.info(f'''Error for {hub['hub_name']} hub zip code radius search''')
        logger.info(response)
        return

    # Put all zip codes from zip radius into parentheses for the SQL query below
    zip_object = '('
    for i in found:
        zip_object = zip_object + str(i) + ', '
    zip_object = zip_object[:-2] + ')'
    return zip_object

def query_everyaction(zip_object: str, errors: list, errored_hub_list: list, hub: dict):
    """
    Query EveryAction tables to get contacts in hub's zipcode radius
    :param zip_object: String returned by zipcode_search()
    :param errors: List of errors
    :param errored_hub_list:
    :param hub: dictionary with values for hub from 'scheduled sheet'
    :return: A parson's table of contacts in this hub's area
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
  		, DATEDIFF(YEAR, dob, GETDATE()-1) AS age
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
		AND committeeid = 80541)

SELECT
  zip.vanid
  , contacts.first
  , contacts.last
  , email.email
  , phone.phone
  , TO_CHAR(CONVERT_TIMEZONE('EST','UTC', zip.datemodified), 'YYYY-MM-DD hh24:MI:SS') as date_joined
  , contacts.age
FROM zip
LEFT JOIN contacts on contacts.vanid = zip.vanid
LEFT JOIN email on email.vanid = zip.vanid
LEFT JOIN phone on phone.vanid = zip.vanid
WHERE email.email IS NOT NULL OR phone.phone IS NOT NULL AND contacts.age > 17
ORDER BY date_joined
'''

    # Send query to Redshift
    try:
        ntl_contacts = rs.query(ea_query)
        return ntl_contacts
    except Exception as e:
        response = str(e)
        exceptiondata = traceback.format_exc().splitlines()
        exception = exceptiondata[len(exceptiondata) - 1]
        errors.append([str(date.today()), hub['hub_name'], response, exception])
        # Create errored hub list, then append to list of errored hubs
        errored_hub_list.append(hub['hub_name'])
        logger.info(f'''Error querying national contacts for {hub['hub_name']} hub''')
        logger.info(response)
        return



#-------------------------------------------------------------------------------
# Connect to necessary google sheet worksheets and set global variables
#-------------------------------------------------------------------------------
# Connect to the set up spreadsheet via gspread
hq_set_up_sheet = gspread_client.open_by_key('1ESXwSfjkDrgCRYrAag_SHiKCMIgcd1U3kz47KLNpGeA')
# Connect to the set up worksheet via gspread
setup_worksheet = hq_set_up_sheet.worksheet('set up')
# Connect to the errors worksheet via gspread
errors_worksheet = hq_set_up_sheet.worksheet('errors')

# Get set up spreadsheet and errors spreadsheet
hubs = parsons_sheets.get_worksheet('1ESXwSfjkDrgCRYrAag_SHiKCMIgcd1U3kz47KLNpGeA','set up')
logged_errors = parsons_sheets.get_worksheet('1ESXwSfjkDrgCRYrAag_SHiKCMIgcd1U3kz47KLNpGeA','errors')
# Get row count (used later to wipe sheet after set up is complete and append errors)
num_hubs = hubs.num_rows
num_errors = logged_errors.num_rows
# Define columns of spreadsheet for future use
columns = ['hub_name','hub_email','spreadsheet_id','zipcode','search_radius']
# This list of errors will become parsons table and be pushed to the errors tab in the spreadsheet
errors = []



#-------------------------------------------------------------------------------
# Define main
#-------------------------------------------------------------------------------
def main():
    ##### Loop through hubs from spreadsheet and dump contacts from ntl database into respective spreadsheet #####
    ##### Open lists for error logging #####

    # This list of hubs with errors will become parsons table and be pushed to the errors tab in the spreadsheet
    errored_hub_list = []
    for hub in hubs:
        zip_object = zipcode_search(hub, errored_hub_list)

        # This is the query used to get contacts from the national database. zip_object at bottom of first CTE
        if zip_object is None:
            continue
        else:
            ntl_contacts = query_everyaction(zip_object, errors, errored_hub_list, hub)
        # Send that table of contacts to the hub's spreadsheet
        try:
            parsons_sheets.append_to_sheet(hub['spreadsheet_id'], ntl_contacts, 'Contacts From National')
        except Exception as e:
            response = str(e)
            exceptiondata = traceback.format_exc().splitlines()
            exception = exceptiondata[len(exceptiondata) - 1]
            errors.append([str(date.today()), hub['hub_name'],response, exception])
            # Create errored hub list, then append to list of errored hubs
            errored_hub_list.append(hub['hub_name'])
            logger.info(f'''Error appending national contacts for {hub['hub_name']} hub''')
            logger.info(response)
            continue

    succeeded_hubs = hubs.select_rows(lambda row: row.hub_name not in errored_hub_list)
    errored_hubs = hubs.select_rows(lambda row: row.hub_name in errored_hub_list)



    ##### Modify Set Up Spreadsheet #####
    # This part of the script takes the hubs that we just set up successfully, and moves them to the 'scheduled' sheet,
    # which is what the rest of the scripts are running on. Hubs whose set ups failed will remain in the set up sheet
    # and any error logs will be put into the errors sheet
    # Move successful hubs from set up sheet to 'scheduled' sheet

    if succeeded_hubs.num_rows == 0:
        logger.info(f'''Set up succeeded for {succeeded_hubs.num_rows} hub(s)''')
        pass
    else:
        parsons_sheets.append_to_sheet('1ESXwSfjkDrgCRYrAag_SHiKCMIgcd1U3kz47KLNpGeA', succeeded_hubs, 'scheduled')
        logger.info(f'''Set up succeeded for {succeeded_hubs.num_rows} hub(s)''')
    # Wipe set up sheet -- hubs whose spreadsheet set up failed are added to the blank sheet
    sheet_wipe = [['' for i in columns] for i in range(num_hubs)]
    setup_worksheet.update('A2:E', sheet_wipe)

    # Pup hubs whose spreadsheet set up failed back into the blank sheet
    if errored_hubs.num_rows == 0:
        logger.info(f'''Set up succeeded for {errored_hubs.num_rows} hub(s)''')
        pass
    else:
        parsons_sheets.append_to_sheet('1ESXwSfjkDrgCRYrAag_SHiKCMIgcd1U3kz47KLNpGeA', errored_hubs, 'set up')
        logger.info(f'''Set up failed for {len(errored_hub_list)} hub(s)''')


    # Add errors to errors spreadsheet and log number of errors
    errors_worksheet.update(f'''A{num_errors + 2}:D''', errors)



#-------------------------------------------------------------------------------
# Run main
#-------------------------------------------------------------------------------
if __name__ == '__main__':
    main()