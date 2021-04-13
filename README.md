# Summary
Hub HQ is a data system that integrates MobilizeAmerica, EveryAction, and Google Sheets to make the most important member data accessible to Sunrise hubs. The scripts in this repo consolidate data from an Interest Form (a Google Form), MobilizeAmerica events hosted by a hub, and a Data Entry Sheet (Google Sheet) into a Google Sheet without creating duplicates. The Google Sheet where contacts are consolidated is called Hub HQ. Data from Hub HQ is pushed to EveryAction (so hubs can send mass email) and Redshift. Through this system, hubs also gain access to ontacts from the national database who live in the their area; new contacts from the national database are added to a Google Worksheet within the same spreadsheet as Hub HQ on a daily basis. 

# Data Pipelines Diagram

![Hub HQ Diagram](https://github.com/sunrisedatadept/hub_member_hq/blob/code-review/images/HubHQ%20Diagram%20Annotated.jpg)

# An instance of Hub HQ filled with fake data for you to explore
[You can see what the most basic version of a Hub HQ looks like here!](https://docs.google.com/spreadsheets/d/17a4EJjZkLV6Dazjv1bPk7HCte3QuOY-SmyPHfPyDhyo/edit#gid=390228199)
