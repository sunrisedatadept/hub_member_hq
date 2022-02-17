#Civis container script: https://platform.civisanalytics.com/spa/#/scripts/containers/112448113

# This script "sets up" Hub HQs by adding all the contacts from the national EveryAction that are in the hub's area
# (based on a zipcode radius search) and all of the contacts who have signed up for any of the hubs mobilize events
# into the the ''Hub HQ' sheet. It does so by looping through each hub in the 'set up' table of the Hub HQ Set Up Sheet
# (https://docs.google.com/spreadsheets/d/1ESXwSfjkDrgCRYrAag_SHiKCMIgcd1U3kz47KLNpGeA/edit#gid=0) and for each hub:
# 1) Using the zipcode and zipcode radius provided to find all the zipcodes in the radius
# 2) Sending a query to Redshift to get all contacts that live in those zipcodes
# 3) Querying Redshift for all of the hub's mobilize contacts
# 4) Compiling the two tables
# 5) Appending that table of all contacts to the hub's 'Hub HQ' sheet
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
_formatter = logging.Formatter('{levelname} {message}',style='{')
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
    'https://www.googleapis.com/auth/drive'
]
credentials = ServiceAccountCredentials.from_json_keyfile_dict(creds, scope)
gspread_client = gspread.authorize(credentials)



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
# Put HQ columns we want in redshift into a list
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



#-------------------------------------------------------------------------------
# Define functions
#-------------------------------------------------------------------------------
def log_error(e, note:str, error_table: list, hub:dict):
    """

    :param e: the exception
    :param note: a brief explanation of where the error occured formatted as a string
    :param error_table: the error table to log the error in
    :param hub: a dictionary with information about the hub from the scheduled sheet
    :return: Appends a row to the hq_errors list of lists, which is logged in Redshift at the end of the script
    """
    response = str(e)
    exception = str(traceback.format_exc())[:999]
    error_table.append([str(date.today()), hub['hub_name'], note, response[:999], exception])
    logger.info(f'''{note} for {hub['hub_name']}''')
    logger.info(response)



def zipcode_search(hub: dict, errored_hub_list: list):
    """
    Search for zipcodes within __ miles of hub's central zipcode
    :param hub: Parson's table row from hubs parson's table retrieved from setup spreadsheet
    :param errored_hub_list: Errorored hubs list (should already be in memory when this function is executed)
    :return: A string of zip codes within xyz radius of hub's central zip separated by commas and bounded by parentheses
    """
    try:
        # Search for zip codes within specified radius
        found = [z.zip for z in zcdb.get_zipcodes_around_radius(hub['zipcode'], hub['search_radius'])]

    # If something was wrong with the zipcode or the zipcode radius, log an error
    except Exception as e:
        log_error(e, 'Zip code radius search error', errors, hub)
        # Create errored hub list, then append to list of errored hubs
        errored_hub_list.append(hub['hub_name'])
        return

    # Put all zip codes from zip radius into parentheses for the SQL query below
    zip_object = '(' + ','.join(found) + ')'
    return zip_object

def query_everyaction(zip_object: str, errors: list, errored_hub_list: list, hub: dict):
    """
    Query EveryAction tables to get contacts in hub's zipcode radius
    :param zip_object: String returned by zipcode_search()
    :param errors: List of errors
    :param errored_hub_list: list where hubs for which there are set up errors are stored
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
      zip.vanid
      , contacts.first as first_name
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
        log_error(e, 'Issue querying redshift', errors, hub)
        # Append to list of errored hubs
        errored_hub_list.append(hub['hub_name'])
        return


def get_mobilize_data(hub: dict, errors: list, errored_hub_list: list):
    """
    Get Mobilize event attendance data for hub
    :param hub: dictionary for that hub from set up sheet, retrieved by parsons
    :param errors: List of errors
    :param errored_hub_list: list where hubs for which there are set up errors are stored
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
    try:
        mobilize_tbl = rs.query(sql=event_attendance_sql)
        return mobilize_tbl
    except Exception as e:
        log_error(e, 'Issue querying redshift', errors, hub)
        # Append to list of errored hubs
        errored_hub_list.append(hub['hub_name'])
        return

def protect_range(hub: dict, sheet: str, range: str):
    """
    Protect a range for new hub hq sheet
    :param hub: dictionary for that hub from set up sheet, retrieved by parsons
    :param sheet: the sheet that containts the to be protected range
    :param range: the cell range to protect
    :return: A worksheet object of gspread class worksheet
    """
    # connect to spreadsheet with spread
    spreadsheet = gspread_client.open_by_key(hub['spreadsheet_id'])
    # Connect to the worksheet
    worksheet = spreadsheet.worksheet(sheet)
    # Protect the range
    worksheet.add_protected_range(range, editor_users_emails=['deptdirectors@sunrisemovement.org'], requesting_user_can_edit=True)



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
        # Query the ntl database for contacts in the hubs area and get contacts from the hub's mobilize
        ntl_contacts = query_everyaction(zip_object, errors, errored_hub_list, hub)
        ntl_contacts.add_column('source','National Email List')
        mobilize_table = get_mobilize_data(hub, errors, errored_hub_list)
        mobilize_table.add_column('source','Mobilize')
        # We're going to combine the two above table but need to ensure there are no overlapping contacts/duplicates
        mobilize_emails = [row['email'] for row in mobilize_table]
        ntl_subset = ntl_contacts.select_rows(lambda row: row.email not in mobilize_emails)
        # Now that any duplicates have been removed, concatenate/combine the tables
        ntl_subset.concat(mobilize_table)
        ntl_subset.remove_column('vanid')
        ntl_subset.remove_column('is_most_recent')

        # Then, create an empty table with hub hq columns in the correct order and concatenate all_contacts to that to
        # Send that table of contacts to the hub's spreadsheet
        hub_hq_append = Table([hq_columns_list])
        hub_hq_append.concat(ntl_subset)


        try:
            parsons_sheets.append_to_sheet(hub['spreadsheet_id'], hub_hq_append, 'Hub HQ')
        except Exception as e:
            log_error(e, 'Error appending new contacts', errors, hub)
            # Append to list of errored hubs
            errored_hub_list.append(hub['hub_name'])
            continue

        # Now protect ranges so that hubs don't mess up the sync editing those ranges
        protect_range(hub, 'Interest Form','A:Y')
        protect_range(hub, 'Data Entry', 'A1:G2')
        protect_range(hub, 'Hub HQ', 'A:R')
        protect_range(hub, 'Analytics Dashboard', 'A:Y')
        protect_range(hub, 'Explainer Docs', 'A:Q')
        protect_range(hub, 'HQ Settings', 'A1:F3')
        protect_range(hub, 'Unrestricted', 'A1:I')


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

    # Put hubs whose spreadsheet set up failed back into the blank sheet
    if errored_hubs.num_rows == 0:
        logger.info(f'''Set up failed for {errored_hubs.num_rows} hub(s)''')
        pass
    else:
        parsons_sheets.append_to_sheet('1ESXwSfjkDrgCRYrAag_SHiKCMIgcd1U3kz47KLNpGeA', errored_hubs, 'set up')
        logger.info(f'''Set up failed for {len(errored_hub_list)} hub(s)''')


    # Add errors to errors spreadsheet and log number of errors
    errors_worksheet.update(f'''A{num_errors + 2}:E''', errors)



#-------------------------------------------------------------------------------
# Run main
#-------------------------------------------------------------------------------
if __name__ == '__main__':
    main()
