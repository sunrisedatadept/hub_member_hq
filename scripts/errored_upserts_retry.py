# This script will help you to upsert all of the Hub HQ contacts that didn't make it into EveryAction due to one error
# or another. This script reads a .csv and upserts the data into the correct hubs' EveryAction committee. Take the
# following steps to retry upserting those contacts
# 1) Download a csv of sunrise.hq_ea_sync_errors that have happened since the last time you ran this retry script
#    SELECT * FROM sunrise.hq_ea_sync_errors WHERE date > '2021-05-10'::DATE but replace 2021-05-10 with the last date
#    the retry script ran
# 2) Open the .csv and make corrections to the data, using the error table as your guide
# 3) Once you have cleaned the data, there are two things you need to do to run this script
#    a) Ensure that you have the necessary credentials and .env file saved in a folder with this script. The includes:
#       i) A file called api_keys.json with all of the API keys to hub EveryAction committees
#       ii) An .env file with Redshift credentials (REDSHIFT_PASSWORD, REDSHIFT_USERNAME, REDSHIFT_HOST, REDSHIFT_DB,
#            REDSHIFT_PORT, S3_TEMP_BUCKET, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY
#    b) Put the filepath to the cleaned .csv in the line below
csv_path = '' # e.g. /Users/cormac/Desktop/csv_for_retry/example.csv

#-------------------------------------------------------------------------------
# Import necessary Packages
#-------------------------------------------------------------------------------
import json
import time
from parsons import Redshift, Table, VAN
import logging
from datetime import date
from dotenv import load_dotenv
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
load_dotenv()
# Load API keys
api_keys_file = 'api_keys.json'
api_keys = json.load(open(api_keys_file))



#-------------------------------------------------------------------------------
# Intantiate parsons Redshift class
#-------------------------------------------------------------------------------
# Load redshift and VAN credentials
rs = Redshift()



#-------------------------------------------------------------------------------
# Set global variables
#-------------------------------------------------------------------------------
# Open errors tables and control table update table
upsert_errors = [['date', 'hub','first','last','email','phone','zip','error']]
contacts = Table.from_csv(csv_path)
hubs_with_errors = {row['hub']:row['hub'] for row in contacts}


#-------------------------------------------------------------------------------
# Define functions
#-------------------------------------------------------------------------------
def subscribe_to_ea(hub_contacts, van, upsert_errors: list, hub):
    """
    Upsert (i.e. findOrCreate) new HQ contacts into hub's EveryAction committee
    :param new_hq_contacts: parsons table of new contacts
    :param van: parsons object of van class
    :param upsert_errors: list of lists where we're storing errors
    :param hub: dict of hub from scheduled sheet
    :return: None
    """
    hub_contacts.convert_column(['phone'],lambda x: re.sub("[^0-9]", "", x)[-10:])
    hub_contacts.convert_column(['zip'], lambda x:re.sub("[^0-9]", "", x)[:5])
    for contact in hub_contacts:
        if contact['phone']:
            json_dict = {
                        'firstName': contact['first'],
                        "lastName": contact['last'],
                        "emails":
                            [{"email": contact['email'],
                            "isSubscribed":'true'}],
                        "addresses":
                            [{"zipOrPostalCode": contact['zip']}],
                        "phones":
                            [{"phoneNumber":contact['phone']}]
                        }
        else:
            json_dict = {
                        'firstName': contact['first'],
                        "lastName": contact['last'],
                        "emails":
                            [{"email": contact['email'],
                            "isSubscribed":'true'}],
                        "addresses":
                            [{"zipOrPostalCode": contact['zip']}]
                        }

        #Except (need to figure out what kind of errors I'll get here)
        try:
            van.upsert_person_json(json_dict)
            time.sleep(.5)
        except Exception as e:
            response = str(e)
            upsert_errors.append([str(date.today()),contact['hub'], contact['first'], contact['last'],
                                 contact['email'],contact['phone'], contact['zip'], response])
            logger.info(f'''Upsert error for {contact['hub']}''')
            logger.info(response)
            logger.info(json_dict)



#-------------------------------------------------------------------------------
# Define main
#-------------------------------------------------------------------------------
def main():
    # loop through the .csv and try and upsert each contact
    for hub in hubs_with_errors:
        # connect to hubs EveryAction committee
        van = VAN(api_key = api_keys[hub], db='EveryAction')
        # narrow contacts list to retry upserts to this hub
        hub_contacts = contacts.select_rows(lambda row: row.hub == hub)
        # Upsert new contacts to EA
        subscribe_to_ea(hub_contacts, van, upsert_errors,hub)

    if len(upsert_errors) > 1:
        rs.copy(Table(upsert_errors), 'sunrise.hq_ea_sync_errors', if_exists='append', distkey='error',
            sortkey='date', alter_table=True)
        logger.info(f'''{len(upsert_errors)-1} errored contacts''')
    else:
        logger.info(f'''All contacts were subscribed to the correct committees without errors''')



#-------------------------------------------------------------------------------
# Run main
#-------------------------------------------------------------------------------
if __name__ == '__main__':
    main()

