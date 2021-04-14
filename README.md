# Summary
Hub HQ is a data system that integrates MobilizeAmerica, EveryAction, and Google Sheets to make the most important member data accessible to Sunrise hubs. The scripts in this repo consolidate data from an Interest Form (a Google Form), MobilizeAmerica events hosted by a hub, and a Data Entry Sheet (Google Sheet) into a Google Sheet without creating duplicates. The Google Sheet where contacts are consolidated is called Hub HQ. Data from Hub HQ is pushed to EveryAction (so hubs can send mass email) and Redshift. Through this system, hubs also gain access to ontacts from the national database who live in the their area; new contacts from the national database are added to a Google Worksheet within the same spreadsheet as Hub HQ on a daily basis. 

# Data Pipelines Diagram

![Hub HQ Diagram](https://github.com/sunrisedatadept/hub_member_hq/blob/code-review/images/HubHQ%20Diagram%20Annotated.jpg)


# The Hub HQ Google Sheet
[You can see what the most basic version of a Hub HQ looks like here!](https://docs.google.com/spreadsheets/d/17a4EJjZkLV6Dazjv1bPk7HCte3QuOY-SmyPHfPyDhyo/edit#gid=390228199)
There are four tabs in each Hub HQ Google Sheet: 
1) Interest Form - This tab is connected to a Google Form, which hubs can use to get new sign ups (via social media or the hub map). The hub is free to add new questions after Zipcode, but cannot move or delete any questions/columns before zipcode. Data from this sheet is transferred into the Hub HQ sheet on a daily basis. Any data stored in columns after Zipcode are displayed in the _Interest Form Responses_ column of the Hub HQ sheet, concatenated with each question/response on a seperate line:

![Concatenated Interest Form Response Field](https://github.com/sunrisedatadept/hub_member_hq/blob/code-review/images/Screen%20Shot%202021-04-13%20at%2011.03.32%20AM.png)

2) Data Entry - A hub can enter data from canvassing or copy and paste data from another Google Sheet into the Data Entry sheet. The data they add is transfered into the Hub HQ sheet on a daily basis. Similarly to Interest Form data, any data in columns to the right of Zipcode are transferred to Hub HQ in a concatenated field. If there are too many lines of data to display, the _Data Entry Sheet Data_ column displays:

![Too Much Data Entry Sheet Data to Display](https://github.com/sunrisedatadept/hub_member_hq/blob/code-review/images/Screen%20Shot%202021-04-13%20at%2011.07.02%20AM.png)

3) Hub HQ - Hub HQ is where a hub's up-to-date members list lives. Hub HQ has a single row/record for each person that has signed up for a MobilizeAmerica event hosted by the hub; filled out the hub's interest form; or had their information entered in the Data Entry Sheet. In addition to the concatenated Data Entry Sheet and Interest Form columns, Hub HQ stores:
     * Contact information
     * Event attendance history
     * A membership "status" summarizing a person's event attendance record
     * Columns to track one-on-ones and other onboarding information
     * Columns to allow the hub to phonebank together and record call results
 
 Hubs can add and edit columns right of column N. The rest of the sheet is protected
 
 4) Contacts from National - This sheet contains contacts from the national database that live near the hub (based on zipcode radius search). There's a column that uses a Vlookup to indicate whether or not that a contact already exists in Hub HQ. There are also columns for phonebanking as a hub and recording call results. 

# setup_script.py
This script "sets up" Hub HQs by dumping all the contacts from the national EveryAction that are in the hub's area (based on a zipcode radius search) into the the 'contacts from national' sheet of the hub's HQ. It does so by looping through each hub in the ['set up' tab of the Hub HQ Set Up Sheet](https://docs.google.com/spreadsheets/d/1ESXwSfjkDrgCRYrAag_SHiKCMIgcd1U3kz47KLNpGeA/edit#gid=0) and for each hub:
1) Uses the zipcode and zipcode radius provided to find all the zipcodes in the radius
2) Sends a query to REdshift to get all contacts that live in those zipcodes
3) Appends that list of contacts to the 'contacts from national' sheet
If the script succeeds for a hub, that hub's info is transferred from the 'set up' tab to the 'scheduled' tab (the 'scheduled' tab is the tab that the rest of the scripts work off of when looping through hubs). If the script fails for a hub, the hub remains in the 'set up' tab and the traceback error is logged in the 'errors' tab of the same spreadsheet.

# new_national_contacts_sync.py
This script pushes new contacts from the national database to a hub's HQ on a daily basis. It looks into hub HQ to find the most recently created/added contact from the national database, and queries Redshift for any contacts who have had addresses created since then that are within the hub's zipcode radius search (zipcode radius search info stored in the ['scheduled' tab of the hub HQ Set Up sheet](https://docs.google.com/spreadsheets/d/1ESXwSfjkDrgCRYrAag_SHiKCMIgcd1U3kz47KLNpGeA/edit#gid=0)). To prevent duplicates in HQ, the script compares the email address of each new contact to the email addresses of records in HQ and only appends non-matches.

# mobilize_sync.py
This script updates event attendance history and the "status" column for existing rows/records in Hub HQ and creates new records for new contacts. For each hub, the script gets a table of unique contacts who have signed up for the hub's events (filtering based on the host email) with contact info and event attendance history. It compares those contacts to the contacts in the hub's HQ using email address. It updates event attendance history and the "status" column for any contacts that match, and appends any new Mobilize contacts that don't match assigning them status of "hot lead."

# sheets_sync.py
This script takes data from the Data Entry Sheet and the Interest Form sheet, compiles all non-contact information data into a single "concatenated" field for each contact, and then pushes those updates to the _Hub HQ_ sheet for any contact that has a match (based on email). It appends any contacts that do not match/do not yet exist in HQ. If the concatenated field in Hub HQ is displaying data from less than 7 columns of data, it looks like this: 
![Concatenated Interest Form Response Field](https://github.com/sunrisedatadept/hub_member_hq/blob/code-review/images/Screen%20Shot%202021-04-13%20at%2011.03.32%20AM.png)
If there is information in more than 7 columns of data for a given contact, then the concatenated field will look like this:
![Too Much Data Entry Sheet Data to Display](https://github.com/sunrisedatadept/hub_member_hq/blob/code-review/images/Screen%20Shot%202021-04-13%20at%2011.07.02%20AM.png)


# everyaction_sync.py
For each hub, this script takes all of the contacts added to HQ sheet since the last time this script ran successfully for that hub, and subscribes them to the hub's EveryAction committee. The control table that stores information about the last successful sync for each hub is sunrise.hq_ea_sync_control_table. Upsert errors are logged in sunrise.hq_ea_sync_errors and all other errors are logged in sunrise.hub_hq_errors 

# Error logging
In order to prevent a single error from derailing the whole system, the scripts have lots of try and except statements built in, where the except statement captures error messages and then lets the script continue. Errors for the set up scrip are stored in the ['errors' tab of the Hub HQ Set Up Sheet](https://docs.google.com/spreadsheets/d/1ESXwSfjkDrgCRYrAag_SHiKCMIgcd1U3kz47KLNpGeA/edit#gid=0). Errors for the rest of the scripts are stored in sunrise.hub_hq_errors and logged in the civis run history. EveryAction upsert errors are logged in the sunrise.hq_ea_sync_errors table. 

# Container scripts
This system runs on Civis container scripts. They are all organized into the Hub HQ project: https://platform.civisanalytics.com/spa/#/projects/146391
