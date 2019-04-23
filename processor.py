#!/usr/bin/python
# Description:		UCPC Interface Batch processor
# Version:		3.40 
# Author:		Alex Solokhin, University of California 2018

'''
Release Notes v3.40

1. New function to track transfer counts for any of the scheduled transfers. CEMLI dictionary file format includes expected transfer count per location
2. Transfer file count discrepancy triggers incident creation with specified EOC note
3. CEMLI dictionary buffer is stored in a global variable and is renewed every cycle. Variable used by any function that needs it

'''

import base64
import csv
import os
import MySQLdb
import random
import re
import shutil
import sys
import time
from datetime import date, timedelta
from datetime import datetime
from time import strftime
from sys import stdout

#---------------------------------------- Global Variables ---------------------------------------#
#debug_start_time = strftime('%d-%b-%Y %H:%M:%S')
path = '/data/ucb_ctm_logs/prod/'
db_name = 'ucpc_interface'
mysql_user = 'nmsuser'
p = base64.b64decode('bmltc29mdDEyMw==')
log_message = ''
debug_flag = 0
conf_file = '/home/johnrobot/ucpc_processor/conf/processor.cfg'
eoc_notes_conf = '/home/johnrobot/ucpc_processor/conf/eoc_notes.conf'
log_file = '/home/johnrobot/ucpc_processor/processor.log'
debug_log = '/home/johnrobot/ucpc_processor/debug.log'
logmon = '/home/johnrobot/ucpc_processor/uim_logmon.log'
process_name_dict = '/home/johnrobot/ucpc_processor/conf/process_name_dictionary.csv'
cemli_dict = '/home/johnrobot/ucpc_processor/conf/cemli_dictionary.csv'
oscompstat_dict = '/home/johnrobot/ucpc_processor/conf/oscompstat_dictionary.csv'
project_dict = '/home/johnrobot/ucpc_processor/conf/projectname_dictionary.csv'
schedule_dir = '/home/johnrobot/ucpc_processor/conf/schedules/'
path_ctm = '/data/ucb_ctm_logs/prod/ctm/'
path_goa = '/data/ucb_ctm_logs/prod/goa/'
path_ps = '/data/ucb_ctm_logs/prod/PS/'
process_goa_flag = 0 # Indicates whether to run process_goa_data function or not
#--------------------------------------------- Lists ---------------------------------------------#
debug_list = []
exclude_job_list = ['ucmctmlog', 'ucmctmlogxmit', 'ctmagping', 'ctmucoplogxmit']
exclude_file_list = ['processor.py', 'db_admintool.py', 'processor_dev.py'] # list of files excluded from processing
goa_file_list = [] # Global list to be used and modified in functions later
logmon_message_list = [] # Append data for logmon alarm to this list
logmon_conditions_list = [] # List to add values passed to a logmon line
error_buffer_list = ["ERROR", "Caused by"] # Strings to look for in the error/exception log
encryption_data_list = [] # Encryption entries from GoA log 
decryption_data_list = [] # Decryption entries from GoA log
file_operations_list = [] # upload/download operations entries from GoA log
####--------------------------------------- SQL Queries ---------------------------------------####
#sql_get_filenames = 'SELECT file_name FROM ctm_files;' # Read current list of files from opsmon_db
#sql_insert_filenames = '"""INSERT INTO ctm_files VALUES (%s)""", (dir_file_list_new)' # Insert list of file names into db from dir_file_list_new
#sql_check_files = "SELECT count(1) FROM ctm_files WHERE processed_flag = 'Y' OR processed_flag = 'N';" # SQL Command to check if ctm_files is populated at all
#sql_last_file = "SELECT file_name FROM ctm_files ORDER BY file_id DESC LIMIT 1;" # Retrive the last file added to opsmon_db
#sql_last_processed_file = "SELECT file_name FROM ctm_files WHERE processed_flag='Y' ORDER BY file_id DESC LIMIT 1;"

#-------------------------------------------- Functions ------------------------------------------#
def log_writer(log_message): # Write daemon process log entry
    time_stamp = strftime('%Y-%m-%d %H:%M:%S')
    f = open(log_file, 'a')
    f.write('{0} {1}'.format(time_stamp, log_message))
    f.close()

def debug_writer(debug_list, exit_flag): # Write to debug log if enabled in config file
    time_stamp = strftime('%Y-%m-%d %H:%M:%S')
    f = open(debug_log, 'a')
    for line in debug_list:
	f.write('{0} {1}'.format(time_stamp, line))
    f.close()
    if exit_flag == 1:
	sys.exit(0)

def open_db_connection(): # Connect to ucpc_interface db and create cursor object
    try:
	db_connection = MySQLdb.connect('localhost', mysql_user, p, db_name)
	db_cursor = db_connection.cursor()
    except Exception as e:
	log_message = 'Error in open_db_connection: {0}\n'.format(e)
	log_writer(log_message)
	sys.exit(1)
    return db_connection, db_cursor

def close_db_connection(db_connection, db_cursor): # Disconnect from ucpc_interface db, close cursor object
    try:
	db_cursor.close()
	db_connection.close()
    except Exception as e:
	log_message = 'Error in close_db_connection: {0}\n'.format(e)
	log_writer(log_message)
	sys.exit(1)

def time_stamp_converter_goa(time_string, year_id): # Convert string timestamp format from CTM log into MySQL format
    month_dict = {'01':'Jan', '02':'Feb', '03':'Mar', '04':'Apr', '05':'May', '06':'Jun', \
    '07':'Jul', '08':'Aug', '09':'Sep', '10':'Oct', '11':'Nov', '12':'Dec'}
    MO = time_string[:2]
    DD = time_string[2:4]
    HH = time_string[4:6]
    MM = time_string[6:]
    c_month = month_dict[MO]
    time_stamp = '{0}-{1}-{2} {3}:{4}'.format(year_id, MO, DD, HH, MM)
    return time_stamp, c_month, int(DD) 

def sort_log_dir():
    global debug_list
    file_list_raw = sorted(os.listdir(path))
    file_list = [x for x in file_list_raw if x not in exclude_file_list]
    if not file_list:
	# Write debug entry or pass
	if debug_flag == 1:
	    debug_list = []
	    debug_list.append('sort_log_dir: No log files to move.\n')
	    debug_writer(debug_list)
	else:
	    pass
    else:
	for x in file_list:
	    if 'ctmlogp' in x:
		current_path = path + x
		shutil.move(current_path, (path_ctm + x))
	    elif 'goalogp' in x:
		current_path = path + x
		shutil.move(current_path, (path_goa + x))

    goa_file_list = sorted(os.listdir(path_goa)) # Create goa_file_list for use once in every cycle
    return goa_file_list
    
def delete_files(): # Delete log files of 0 size and Excluded Job IDs
    file_list_raw = os.listdir(path)
    # Use var 'x' as file name in file_list
    file_list = [x for x in file_list_raw if x not in exclude_file_list]
    for x in file_list:
	file_name = path + x
	if (os.stat(file_name).st_size == 0) == True: # Check if zero size
	    os.remove(file_name)
	# Remove files with Exclude Job ID's
	if any(item in x for item in exclude_job_list):
	    try:
		os.remove(file_name)
	    except Exception as e:
		log_message = 'Error in delete_files deleting {0}: {1}\n'.format(file_name, e)
		log_writer(log_message)

def get_job_name(log_line): # Retreive job_name from CTM log line
    split_line = log_line.split('|')
    job_name = split_line[3].strip()
    return job_name

def read_conf(): # Read daemon configuration file and apply settings
    exit_code = 0 # Set default exit_code for error conditions
    critical_jobs_flag = 0
    cemli_chart_flag = 0
    critical_job_list = []
    logmon_conditions_list = []
    cemli_chart_list = [] # List of CEMLI's to display on Executive Summary Chart
    try:
	with open(conf_file, mode='r') as f:
	    f_buffer = f.readlines()
    except Exception as e:
	log_message = 'read_conf: Error reading processor.conf: {0}\n'.format(e)
	log_writer(log_message)
	exit_code = 1
	return logmon_conditions_list, 1, critical_job_list, time_dict, cemli_chart_list, exit_code

    for line in f_buffer:
	# Add critical job_name to dedicated list when flag is on
	if '--- End Critical Jobs ---' in line:
	    critical_jobs_flag = 0
	    continue
	elif 'CEMLI Performance Chart' in line:
	    cemli_chart_flag = 1
	    continue

	if critical_jobs_flag == 1:
	    job_name = line.replace('\n', '')
	    critical_job_list.append(job_name)
	elif cemli_chart_flag == 1:
	    line_list = line.split(',')
	    for i in line_list:
		cemli_chart_list.append(i.strip().replace('\n', ''))
	    cemli_chart_flag = 0
	elif 'ENDED OK' in line:
	    ended_ok_switch = (line.split(': ')[1]).strip()
	    if ended_ok_switch == 'on' or ended_ok_switch == 'ON' or ended_ok_switch == 'On':
		logmon_conditions_list.append('ENDED OK')
	elif 'ENDED NOTOK' in line:
	    ended_notok_switch = (line.split(': ')[1]).strip()
	    if ended_notok_switch == 'on' or ended_notok_switch == 'ON' or ended_notok_switch == 'On':
		logmon_conditions_list.append('ENDED NOTOK')
	elif 'HELD BY USER' in line:
	    held_by_user_switch = (line.split(': ')[1]).strip()
	    if held_by_user_switch == 'on' or held_by_user_switch == 'ON' or held_by_user_switch == 'On':
		logmon_conditions_list.append('HELD BY USER')
	elif 'DELETED BY USER' in line:
	    deleted_by_user_switch = (line.split(': ')[1]).strip()
	    if deleted_by_user_switch == 'on' or deleted_by_user_switch == 'ON' or deleted_by_user_switch == 'On':
		logmon_conditions_list.append('DELETED BY USER')
	elif 'Scan interval' in line:
	    scan_interval = int(line.split(': ')[1])
	elif '--- Critical Jobs ---' in line:
	    critical_jobs_flag = 1
	    continue

    return logmon_conditions_list, scan_interval, critical_job_list, cemli_chart_list, exit_code

def eoc_note_processor(job_name, job_status, ontime_status, critical_job_list): # Configure EOC note for alarm/incident
    # Critical Path Notes
    if job_status == 'ENDED NOTOK' and job_name in critical_job_list:
	eoc_note = 'EOC Note: Critical Path Severity 2. Please, assign to PeopleSoft Support Group.'
    # Informational alert for transfer_status
    elif job_status == 'ENDED OK':
	eoc_note = 'EOC Note: No action needed for event' 
    elif job_status == 'ENDED NOTOK':
	eoc_note = 'EOC Note: Non-Critical Severity 3, Follow Standard Operating Procedure'
    elif 'Late' in ontime_status:
	eoc_note = 'EOC Note: Non-Critical Severity 3, Follow Standard Operating Procedure'
    else:
	eoc_note = 'EOC Note: No action needed for event'

    # Debug 
    if debug_flag == 1:
	exit_flag = 0
	debug_list = []
	debug_list.append('eoc_note_processor: Job Name = {0}, Cemli = {1}, EOC Note = {3}\n'.format(job_name, cemli_id, eoc_note))
	debug_writer(debug_list, exit_flag)

    return eoc_note

def get_ctm_file_list(db_connection, db_cursor): # Return list of unprocessed CTM files with tag 'N' in db
    try:
	db_cursor.execute("SELECT file_name FROM ctm_files WHERE processed_flag = 'N';")
	ctm_file_list = [item[0] for item in db_cursor.fetchall()]
    except Exception as e:
	log_message = 'MySQL: Error in get_ctm_file_list: {0}\n'.format(e)
	log_writer(log_message)
	close_db_connection(db_connection, db_cursor)
	sys.exit(1)
    return ctm_file_list

def logmon_sync(db_connection, db_cursor, critical_job_list): # Syncronize logmon file with current alerts
    debug_flag = 1
    debug_list = []
    #---------- Internal Functions ----------#
    # Logmon formatting as follows for Interface Transfers and General Alerts:
    # Scheduled Transfer Alert: Time_Stamp, Message, EOC_Note
    # General Transfer Alert: Time_Stamp, Message, EOC_Note
    # Critical Path Alert: Time_Stamp, Message, EOC_Note
    def write_logmon(alert_type, alert_list): # Append an alert to logmon file
	alert_text = '{0}: {1} | {2} | {3}\n'.format(alert_type, alert_list[0], alert_list[1].replace('\n', ''), \
	    alert_list[2].replace('\n', ''))
	with open(logmon, 'a') as f:
	    f.write(alert_text)

    def get_general_alerts():
	# Retrieve a tuple with active general alerts
	general_alert_list = []
	query = "SELECT time_stamp, alert_text, eoc_note FROM general_alerts WHERE active = 1 ORDER BY time_stamp ASC"
	try:
	    db_cursor.execute(query)
	    general_alert_list = db_cursor.fetchall()
	except Exception as e:
	    log_message = 'logmon_sync -> get_general_alerts: error getting alert list: {0}\n'.format(e)
	    log_writer(log_message)
	    close_db_connection(db_connection, db_cursor)
	    sys.exit(1)
	return general_alert_list

    def get_scheduled_alerts():
	scheduled_alerts_list = []
	# Retrieve a list with active scheduled transfer alerts
	query = "SELECT project_endtime, cemli_id, cemli_title, location, frequency, status, eoc_note \
	    FROM transfer_alerts WHERE active = 1 AND status LIKE '%Failed%' OR active = 1 AND status LIKE '%Late%' OR \
	    active = 1 AND status LIKE '%Past due%' ORDER BY project_endtime ASC"
	try:
	    db_cursor.execute(query)
	    scheduled_alerts_list = db_cursor.fetchall()
	    # Debug
	    if debug_flag == 1:
		if scheduled_alerts_list:
		    debug_list.append('logmon_sync -> get_scheduled_alerts: at least 1 scheduled alert retrieved\n')
		    debug_writer(debug_list, 0)

 	except Exception as e:
	    log_message = 'logmon_sync -> get_scheduled_alerts: error getting alert list: {0}\n'.format(e)
	    log_writer(log_message)
	    close_db_connection(db_connection, db_cursor)
	    sys.exit(1)
	return scheduled_alerts_list

    def check_logmon_match(line, project_endtime, cemli_id, location, frequency, status): # Check if alarm has already been posted, possibly with new time_stamp and added count
	logmon_match = 0
	if str(project_endtime) in line:
	    logmon_match = 1
	    return logmon_match
	# ---------- Use timedelta to prevent false positive double ID on alarm from previous period ----------
	elif (cemli_id in line) and (location in line) and (frequency in line):
	    # Get timestamp from line and convert it to timedate
	    date1_str = line.split('|')[0].split(': ')[1].strip()
	    date1 = datetime.strptime(date1_str, '%Y-%m-%d %H:%M:%S')
	    time_delta_datetime = project_endtime - date1
	    time_delta_days = time_delta_datetime.days # Number of whole days between dates
	    if time_delta_days > 3:
		logmon_match = 0
	    else:
		logmon_match = 1
	    return logmon_match
	return logmon_match

    def check_logmon_general(line, time_stamp, message): # Verify logmon file to prevent duplicate Incidents
	logmon_match = 0
	if str(time_stamp) in line:
	    logmon_match = 1
	    return logmon_match
	
	# Parse Order ID from Message
	order_id = message.split('Order ID: ')[1].split(', Job Status')[0]
	if order_id in line:
	    logmon_match = 1
	    return logmon_match

	return logmon_match
    #------- End Internal Functions -------#

    # Check if logmon file exists
    try:
	with open(logmon, mode = 'r') as f:
	    logmon_buffer = f.readlines()
	    if logmon_buffer:
		logmon_exists = 1
	    else:
		logmon_exists = 0
    except:
	logmon_exists = 0

    general_alert_list = get_general_alerts()
    scheduled_alert_list = get_scheduled_alerts()

    # Process general_alert_list
    if general_alert_list:
	for alert in general_alert_list:
	    time_stamp = alert[0]
	    message = alert[1]
	    eoc_note = alert[2]
	    job_id = message.split('Job Name: ')[1].split(', Order ID')[0]
	    if (job_id in critical_job_list) and logmon_exists == 0: # Start new logmon file
		write_logmon('General Transfer Alert', alert)
	    else:
		for line in logmon_buffer:
		    logmon_match = check_logmon_general(line, time_stamp, message)
		    if job_id in critical_job_list: # Critical path job_name detected
			logmon_match = logmon_check_general(line)
			if logmon_match == 0:
			    write_logmon('Critical Path Alert', alert)
	
    # Process Scheduled Transfer Alerts
    if scheduled_alert_list:
	for alert in scheduled_alert_list:
	    project_endtime = alert[0]
	    cemli_id = alert[1]
	    cemli_title = alert[2]
	    location = alert[3]
	    frequency = alert[4]
	    status = alert[5]
	    eoc_note = alert[6]

	    # Format message
	    alarm_message = 'CEMLI ID: {0}, CEMLI Title: {1}, Location: {2}, Frequency: {3}, Status: {4}'.format(cemli_id, cemli_title, location, frequency, status)
	    if logmon_exists == 0: # No existing logmon file, write all alarms to log
		write_logmon('Scheduled Transfer Alert', [project_endtime, alarm_message, eoc_note])
	    else:
		for line in logmon_buffer: # Iterate through logmon file to check existing records
		    logmon_match = check_logmon_match(line, project_endtime, cemli_id, location, frequency, status)
		    if logmon_match == 1:
			break
		# Proceed based on logmon_match
		if logmon_match == 0:
		    write_logmon('Scheduled Transfer Alert', [project_endtime, alarm_message, eoc_note])

def get_location(job_name): # Return location based on job_name. 
    location = ''
    location_dictionary = {'ucpzducop1':'UCOP', 'ucpzdrvcmp':'Riverside', 'ucpMf_RVCMPtoUCB':'Berkeley', 'ucpzdlamed':'UCLA', 'ucpzdasla1':'ASUCLA', \
    'ucpMf_ASLA1toUCB':'Berkeley', 'ucpzdmecmp':'Merced', 'ucpzdmecmp-la':'UCLA', 'ucpMf_MCCMPtoUCB':'Berkeley', 'ucpzdmecmp-zar':'n/a (archive only)', \
    'ucpMf_IRCMPtoUCI':'Irvine', 'ucpMf_UCANRtoUCD':'Davis', 'ucpMf_DVCMPtoUCD':'Davis', 'ucpMf_DVMEDtoUCD':'Davis', 'ucpMf_SCCMPtoUCSC':'Santa Cruz', \
    'ucpMf_SBCMPtoUCSB':'Santa Barbara', 'pcsucbucop':'UCOP', 'LAMED_zzz_prod':'UCLA Medical', 'LAMED-UCLA_zzz_prod':'UCLA Medical', \
    'LACMP-UCLA_zzz_prod':'UCLA', 'ucop_to_omcs':'UCOP', 'ucla_to_omcs':'UCLA', 'ASLA1_zzz_prod':'ASUCLA', 'RVCMP_zzz_prod':'UC Riverside', \
    'MECMP-LACMP_zzz_prod':'UC Merced', 'MECMP_zzz_prod':'UC Merced', 'SBCMP_zzz_prod':'UC Santa Barbara', 'SBCMPtoUCSB_zzz_prod':'UC Santa Barbara', \
    'ComCk-LACMP_zzz_prod':'UCLA', 'LACMP_zzz_prod':'UCLA', 'BKCMP_zzz_prod':'Berkeley', 'BKCMPtoUCB_zzz_prod':'Berkeley'}

    # Iterate through dictionary to run match to job_name
    for key, value in location_dictionary.iteritems():
	if key in job_name:
	    location = value
    if not location:
	location = 'NULL'

    # Process debug data
    if debug_flag == 1:
	exit_flag = 0
	debug_list = []
	debug_list.append('get_location: job_name = {0}, matched location = {1}\n'.format(job_name, location))
	debug_writer(debug_list, exit_flag)

    return location

def get_process_name(job_name): # Return process name from CSV file matched to Job Name
    process_name_dict = '/home/johnrobot/ucpc_processor/conf/process_name_dictionary.csv'
    with open(process_name_dict, mode='r') as f:
	reader = csv.reader(f)
	process_name_dict = dict((rows[0], rows[1]) for rows in reader)
    try:
	process_name = process_name_dict[job_name]
    except:
	process_name = 'NULL'

    # Debug
    if debug_flag == 1:
	exit_flag = 0
	debug_list = []
	debug_list.append('get_process_name: job_name = {0}, Fetched process_name = {1}\n'.format(job_name, process_name))
	debug_writer(debug_list, exit_flag)

    return process_name

def get_cemli_data(job_name): # Return CEMLI data matched to Job Name
    with open(cemli_dict, mode='r') as f: # Import data from CEMLI CSV file
	reader = csv.reader(f)
	cemli_list = list(reader)

    for line_list in cemli_list:
	if line_list[0] in job_name:
	    cemli_id_str = line_list[0]
	    cemli_title = line_list[1]
	    schedule_name = line_list[3]
	    dashboard_active = line_list[4]
	    return cemli_id_str, cemli_title, schedule_name, dashboard_active
    cemli_id_str = None
    cemli_title = None
    dashboard_active = 'No'
    schedule_name = 'NULL'

    # Debug
    if debug_flag == 1:
	exit_flag = 0
	debug_list = []
	debug_list.append('get_cemli_data: job_name(arg) = {0}, cemli_id = {1}\n'.format(job_name, cemli_id))
	debug_list.append('get_cemli_data: cemli_title = {0}\n'.format(cemli_title))
	debug_list.append('get_cemli_data: schedule_name = {0}, dashboard_active = {1}\n'.format(schedule_name, dashboard_active))
	debug_writer(debug_list, exit_flag)

    return cemli_id_str, cemli_title, schedule_name, dashboard_active

def get_cemli_dict_buffer(): # Return cemli dictionary file buffer for use in the next cycle
    try:
	with open(cemli_dict, mode='r') as f:
	    f_buffer = f.readlines()
    except Exception as e:
	log_message = 'cemli_dict_buffer: Error opening CEMLI dictionary file {0}: {1}'.format(cemli_dict, e)
	log_writer(log_message)
	sys.exit(1)

    return f_buffer

def get_oscompstat(oscompstat): # Match OSCOMPSTAT INT value to Message in CSV file
    with open(oscompstat_dict, mode='r') as f: # Import data from OSCOMPSTAT CSV file
	reader = csv.reader(f)
	oscompstat_table = dict((rows[0], rows[1]) for rows in reader)
    try:
	oscompstat_message = oscompstat_table[str(oscompstat)]
    except:
	oscompstat_message = 'OSCOMPSTAT Return Code {0} not in dictionary.\n'.format(oscompstat)

    # Debug
    if debug_flag == 1:
	exit_flag = 0
	debug_list = []
	debug_list.append('get_oscompstat: oscompstat<arg> = {0}, oscompstat_message = {1}\n'.format(str(oscompstat), oscompstat_message))
	debug_writer(debug_list, exit_flag)	

    return oscompstat_message

def truncate_datagrid_transfers(db_connection, db_cursor): # Delete all data from datagrid_transfers table (initiated every scan cycle)
    try:
	db_cursor.execute('TRUNCATE TABLE datagrid_transfers')
	db_connection.commit();
    except Exception as e:
	db_connection.rollback()
	log_message = 'truncate_datagrid_transfers -> TRUNCATE error:> {0}\n'.format(e)
	log_writer(log_message)
	close_db_connection(db_connection, db_cursor)
	sys.exit(1)

def calendar_sync(db_connection, db_cursor, cemli_dict_buffer): # Sync calendar table with CSV schedule files, populate updated db data
    #------------------------ Functions -------------------------#
    def truncate_calendar(): # Delete all data from calendar table before re-sync
	try:
	    db_cursor.execute('TRUNCATE TABLE calendar')
	    db_connection.commit()
	except Exception as e:
	    db_connection.rollback()
	    log_message = 'calendar_sync -> truncate_calendar: {0}.\n'.format(e)
	    log_writer(log_message)
	    close_db_connection(db_connection, db_cursor)
	    sys.exit(1)

    def insert_calendar(element_list): # Insert data_list into calendar table
	# element_list = [mysql_datetime, schedule, comment_text]
	params = ['%s' for item in element_list]
	query = 'INSERT INTO calendar(date_start, date_end, title, color, text_color) VALUES({0})'.format(', '.join(params))
	try:
	    db_cursor.execute(query, element_list)
	    db_connection.commit()
	except Exception as e:
	    db_connection.rollback()
	    log_message = 'calendar_sync -> insert_calendar: {0}.\n'.format(e)
	    log_writer(log_message)
	    close_db_connection(db_connection, db_cursor)
	    sys.exit(1)

    def get_cemli_list(schedule_name, cemli_dict_buffer): # Return list of cemli IDs for given schedule_name
	cemli_list = [] # Create container for cemli list to return
	for line in cemli_dict_buffer:
	    line_l = line.split(',')
	    if schedule_name in line_l and (line_l[0] not in cemli_list):
		cemli_list.append(line_l[0])
	return cemli_list
    #----------------------- End Functions ---------------------#

    #--------------------------- Main --------------------------#
    # Create local variables used in function
    schedule_name_dict = {'inbound_nonmed':'Inbound Nonmed', 'inbound_med':'Inbound Med', 'I156_fica_exempt':'I156 FICA Exempt'}
    month_dict = {'Jan':'01', 'Feb':'02', 'Mar':'03', 'Apr':'04', 'May':'05', 'Jun':'06', \
	'Jul':'07', 'Aug':'08', 'Sep':'09', 'Oct':'10', 'Nov':'11', 'Dec':'12'}
    c_year = strftime('%Y') # Current year

    # Truncate table calendar before resyncing data to local dictionaries
    truncate_calendar()

    # Read in schedule files into list of lists
    # MySQL date time format = YYYY-mm-dd HH:MM:SS
    # Each element = [date_start, date_end, title, color, text_color]
    schedule_file_list = sorted(os.listdir(schedule_dir))
    full_cemli_list = []
    for schedule_file in schedule_file_list:
	schedule_name = schedule_file.split('.')[0]
	cemli_list = get_cemli_list(schedule_name, cemli_dict_buffer) # Get list of cemli's for given schedule name
	full_cemli_list.extend(cemli_list)
	schedule = schedule_name_dict[schedule_name] # Text formatted schedule name
	schedule_f = '{0}{1}'.format(schedule_dir, schedule_file)
	with open(schedule_f, mode='r') as f:
	    f_buffer = f.readlines()

	for line in f_buffer:
	    line_list = line.split(',')
	    if 'Month' in line_list:
		continue
	    else:
		frequency = line_list[3]
		time = '{0}:00'.format(line_list[4].replace('\n', ''))
		month = month_dict[line_list[1]]
		day = line_list[2]
		mysql_datetime = '{0}-{1}-{2} {3}'.format(c_year, month, day, time)
		date_start = '{0}-{1}-{2} {3}'.format(c_year, month, day, '00:00:00')
		title = 'Schedule: {0} {1}, Due CEMLI: {2}'.format(schedule, frequency, (', '.join(cemli_list)))
		color = '#C0C0C0'
		text_color = '#000000'
		element_list = [date_start, mysql_datetime, title, color, text_color]
		insert_calendar(element_list)

    #---------- Sync transfer records ----------#
    # Each element = [date_start, date_end, title, color, text_color]
    event_list = []
    for cemli_id in full_cemli_list:
	query = "SELECT project_endtime, location, frequency, ontime_status FROM transfer_data WHERE cemli_id = '{0}'".format(cemli_id)
	try:
	    db_cursor.execute(query)
	    event_list = db_cursor.fetchall()
	except Exception as e:
	    log_message = 'calendar_sync -> error fetching transfer records: {0}\n'.format(e)
	    log_writer(log_message)
	    close_db_connection(db_connection, db_cursor)
	    sys.exit(1)

	# Iterate through transfer record event_list and write data to db
	# event = [project_endtime, location, frequency, ontime_status]
	for event in event_list:
	    # Format date_end. Set 30 minute duration for events that end towards the end of the hour
	    date_start = str(event[0])
	    t_minute = int(date_start.split(' ')[1].split(':')[1])
	    if ((t_minute + 30) > 59):
		# Calculate new hour
		t_hour = int(date_start.split(' ')[1].split(':')[0])
		if t_hour == 23:
		    hour = '00'
		else:
		    hour = str(t_hour + 1)
		day = date_start.split(' ')[0]
		date_end = '{0} {1}:15:00'.format(day, hour)
	    else:
		minute = str(t_minute)
		date_end = date_start.split(':')[0] + ':' + minute + ':00'
	    
	    # Format title
	    location = event[1]
	    frequency = event[2]
	    status = event[3]
	    title = 'Cemli {0}, Location {1}, Frequency {2}, Status: {3}'.format(cemli_id, location, frequency, status)
	    if 'Late' in status:
		color = '#D2691E'
	    else:
		color = '#6B8E23'
	    text_color = '#000000'
	    
	    insert_calendar([date_start, date_end, title, color, text_color])

def deactivate_alerts(db_connection, db_cursor): # Set alerts to inactive if older than 1 week
    query = "UPDATE transfer_alerts SET active = 0 WHERE project_endtime < NOW() - INTERVAL 1 WEEK"
    try:
	db_cursor.execute(query)
	db_connection.commit()
    except Exception as e:
	log_message = 'deactivate_alerts: {0}'.format(e)
	log_writer(log_message)
	close_db_connection(db_connection, db_cursor)
	sys.exit(1)

def datagrid_transfers_sync(db_connection, db_cursor, cemli_dict_buffer): # Build fresh datagrid view with active transfers, re-sync all datagrid data
    debug_flag = 1
    debug_list = []
    #-------------------- Internal Functions -------------------#
    def insert_datagrid_transfers(data_list):
	params = ['%s' for item in data_list]
	query = 'INSERT INTO datagrid_transfers(project_endtime, expected_count, actual_count, frequency, cemli_id, cemli_title, location, schedule_name, pcssc_download_status, \
	    pcssc_decryption_status, ps_upload_status, ontime_status, ps_process_status, messages, goa_error, ps_error) VALUES({0})'.format(', '.join(params))
	try:
	    db_cursor.execute(query, data_list)
	    db_connection.commit()
	except Exception as e:
	    db_connection.rollback()
	    log_message = 'datagrid_transfers_sync -> insert_datagrid_transfers: {0}\n'.format(e)
	    log_writer(log_message)
	    close_db_connection(db_connection, db_cursor)
	    sys.exit(1)
    #----------------------- End Functions ---------------------#

    # Define schedule name dictionary for Grid display
    schedule_dict = {'NULL':'No active schedule assigned', 'inbound_nonmed':'Inbound Nonmed', 'inbound_med':'Inbound Med', 'I156_fica_exempt':'I156 FICA Exempt'}

    # Get active alarm data from the transfer_alerts table
    try:
	db_cursor.execute('SELECT project_endtime, expected_count, actual_count, cemli_id, location, frequency, status FROM transfer_alerts WHERE active = 1 \
	    ORDER BY project_endtime DESC')
	active_alarm_list = db_cursor.fetchall() # Returns list of tuples
    except Exception as e:
	log_message = 'datagrid_transfers_sync: fetch of active alerts failed: {0}\n'.format(e)
	log_writer(log_message)
	close_db_connection(db_connection, db_cursor)
	sys.exit(1)

    # Check for a possible 'no active alarms' scenario (very low probability, but required)

    if active_alarm_list: 
	for alarm in active_alarm_list:
	    # active_alarm_list item = [project_time, expected_count, actual_count, cemli_id, location, frequency, status]
	    project_endtime = alarm[0]
	    expected_count = alarm[1]
	    actual_count = alarm[2]
	    cemli_id = alarm[3]
	    location = alarm[4]
	    frequency = alarm[5]
	    # Get schedule_name, cemli_title for current cemli
	    cemli_title = ''
	    for line in cemli_dict_buffer:
		line_list = line.split(',')
		if (line_list[0] == cemli_id) and (line_list[2] == location):
		    schedule_name = line_list[3]
		    schedule_str = schedule_dict[schedule_name]
		    cemli_title = line_list[1] # CEMLI Title extracted from CEMLI dict line
		    break


	    #----- Debug -----#
	    if debug_flag == 1:
		if not cemli_title:
		    debug_list.append('datagrid_transfers_sync: cannot get cemli title for CEMLI {0}, Location {1}, Frequency {2}\n'.format(cemli_id, location, frequency))
		    debug_writer(debug_list, 1)

	    # Check if it is a Past Due alarm and format accordingly
	    status = alarm[6]
	    if 'Past due' in status:
		# [project_endtime, expected_count, actual_count, frequency, cemli_id, cemli_title, location, schedule_name, pcssc_download_status, \
		    # pcssc_decryption_status, ps_upload_status, ontime_status, ps_process_status, messages, goa_error, ps_error]
		pcssc_download_status = 'NULL'
		pcssc_decryption_status = 'NULL'
		ps_upload_status = 'NULL'
		ontime_status = status
		ps_process_status = 'NULL'
		messages = 1
		goa_error = 0
		ps_error = 0
		datagrid_transfers_list = [project_endtime, expected_count, actual_count, frequency, cemli_id, cemli_title, location, schedule_str, \
		    pcssc_download_status, pcssc_decryption_status, ps_upload_status, ontime_status, ps_process_status, messages, goa_error, ps_error]
		insert_datagrid_transfers(datagrid_transfers_list) # Insert data into db
	    else: # Job not past due and has GoA and PS data in DB
		# Get required grid data from db
		# pcssc_download_status, pcssc_decryption_status, ps_upload_status, ontime_status,
		# ps_process_status, messages, goa_error, ps_error
		pcssc_download_status_query = "SELECT loc_to_pcssc_download_status FROM transfer_data WHERE cemli_id = '{0}' AND location = '{1}' AND \
		    project_endtime = '{2}'".format(cemli_id, location, project_endtime)
		pcssc_decryption_status_query = "SELECT loc_to_pcssc_decryption_status FROM transfer_data WHERE cemli_id = '{0}' AND \
		    location = '{1}' AND project_endtime = '{2}'".format(cemli_id, location, project_endtime)
		ps_upload_status_query = "SELECT pcssc_to_ps_upload_status FROM transfer_data WHERE cemli_id = '{0}' AND \
		    location = '{1}' AND project_endtime = '{2}'".format(cemli_id, location, project_endtime)
		ontime_status_query = "SELECT ontime_status FROM transfer_data WHERE cemli_id = '{0}' AND location = '{1}' AND \
		    project_endtime = '{2}'".format(cemli_id, location, project_endtime)
		ps_process_status_query = "SELECT ps_process_status FROM transfer_data WHERE cemli_id = '{0}' AND location = '{1}' AND \
		    project_endtime = '{2}'".format(cemli_id, location, project_endtime)
		messages_query = "SELECT message_buffer FROM transfer_data WHERE cemli_id = '{0}' AND location = '{1}' AND \
		    project_endtime = '{2}'".format(cemli_id, location, project_endtime)
		goa_error_query = "SELECT goa_error_buffer FROM transfer_data WHERE cemli_id = '{0}' AND location = '{1}' AND \
		    project_endtime = '{2}'".format(cemli_id, location, project_endtime)
		ps_error_query = "SELECT ps_error_buffer FROM transfer_data WHERE cemli_id = '{0}' AND location = '{1}' AND \
		    project_endtime = '{2}'".format(cemli_id, location, project_endtime)
		try:
		    db_cursor.execute(pcssc_download_status_query)  
		    pcssc_download_status = db_cursor.fetchone()[0]
		except Exception as e:
		    log_message = 'datagrid_transfers_sync: pcssc_download_status query error: {0}\n'.format(e)
		    log_writer(log_message)
		    pcssc_download_status = 'Query Error: {0}'.format(e)
		try:
		    db_cursor.execute(pcssc_decryption_status_query)
		    pcssc_decryption_status = db_cursor.fetchone()[0]
		except Exception as e:
		    log_message = 'datagrid_transfers_sync: pcssc_decryption_status query error: {0}\n'.format(e)
		    log_writer(log_message)
		    pcssc_decryption_status = 'Query Error: {0}'.format(e)
		try:
		    db_cursor.execute(ps_upload_status_query)
		    ps_upload_status = db_cursor.fetchone()[0]
		except Exception as e:
		    log_message = 'datagrid_transfers_sync: ps_upload_status query error: {0}\n'.format(e)
		    log_writer(log_message)
		    ps_upload_status = 'Query Error: {0}'.format(e)
		try:
		    db_cursor.execute(ontime_status_query)
		    ontime_status = db_cursor.fetchone()[0]
		except Exception as e:
		    log_message = 'datagrid_transfers_sync: ontime_status query error: {0}\n'.format(e)
		    log_writer(log_message)
		    ontime_status = 'Query Error: {0}'.format(e)
		try:
		    db_cursor.execute(ps_process_status_query)
		    ps_process_status = db_cursor.fetchone()[0]
		except Exception as e:
		    log_message = 'datagrid_transfers_sync: ps_process_status query error: {0}\n'.format(e)
		    log_writer(log_message)
		    ps_process_status = 'Query Error: {0}'.format(e)
		try:
		    db_cursor.execute(messages_query)
		    messages = db_cursor.fetchone()[0]
		except Exception as e:
		    log_message = 'datagrid_transfers_sync: ps_process_status query error: {0}\n'.format(e)
		    log_writer(log_message)
		    messages = 'Query Error: {0}'.format(e)
		try:
		    db_cursor.execute(goa_error_query)
		    goa_error_buffer = db_cursor.fetchone()[0]
		except Exception as e:
		    log_message = 'datagrid_transfers_sync: ps_process_status query error: {0}\n'.format(e)
		    log_writer(log_message)
		    goa_error_buffer = 'Query Error: {0}'.format(e)
		try:
		    db_cursor.execute(ps_error_query)
		    ps_error_buffer = db_cursor.fetchone()[0]
		except Exception as e:
		    log_messagei ='datagrid_transfers_sync: ps_error_buffer query error: {0}\n'.format(e)
		    log_writer(log_message)
		    ps_error_buffer = 'Query Error: {0}'.format(e)

		# Format messages, goa_error, ps_error to be 0 or 1
		if ('No messages' in messages) or ('No errors' in messages) or ('events OK' in messages):
		    messages = 0
		else:
		    messages = 1

		if goa_error_buffer == 'NULL':
		    goa_error = 0
		else:
		    goa_error = 1

		if ps_error_buffer == 'NULL':
		    ps_error = 0
		else:
		    ps_error = 1

		# Format datagrid_transfers single record list
		# [project_endtime, expected_count, actual_count, frequency, cemli_id, cemli_title, location, schedule_name,
		# pcssc_download_status, pcssc_decryption_status, ps_upload_status, 
		# ontime_status, ps_process_status, messages, goa_error, ps_error]
		datagrid_transfers_list = [alarm[0], alarm[1], alarm[2], alarm[5], alarm[3], cemli_title, alarm[4], schedule_str, \
		    pcssc_download_status, pcssc_decryption_status, ps_upload_status, ontime_status, ps_process_status, messages, goa_error, ps_error]

		insert_datagrid_transfers(datagrid_transfers_list)

#--------------------------------------------- UNDER CONSTRUCTION -----------------------------------------------#
def cemli_chart_sync(db_connection, db_cursor, cemli_chart_list): # Update CEMLI performance table for Grid.js summary page
    #---------- Variables ----------#
    debug_flag = 0
    date_list_success = [] # List container for distinct success dates
    date_list_error = [] # List container for distinct dates with error events
    date_list = [] # Consolidated list for all active dates for chart data
    data_list = [] # Master list for chart data - list of lists for final DB write

    #---------- Functions ----------#
    def truncate_cemli_chart():
	try:
	    db_cursor.execute("TRUNCATE TABLE cemli_chart_data")
	    db_connection.commit()
	except Exception as e:
	    db_connection.rollback()
	    log_message = 'cemli_chart_sync -> truncate_cemli_chart: {0}\n'.format(e)
	    log_writer(log_message)
	    close_db_connection(db_connection, db_cursor)
	    sys.exit(1)
	
	# Debug output
	#if debug_flag == 1:
	#    debug_list = []
	#    debug_list.append('cemli_chart_sync -> truncate_cemli_chart: TRUNCATE successfull\n')
	#    debug_writer(debug_list, 0)

    def db_writer(data_list): # Write chart data to db (list of lists)
	# Check if data_list is empty
	if not data_list:
	    if debug_flag == 1:
		debug_list.append('cemli_chart_sync -> db_writer: data_list is empty. Skip db_write\n')
		debug_writer(debug_list, 0)
	    return

	for data_set in data_list:
	    params = ['%s' for x in data_set]
	    sql = "INSERT INTO cemli_chart_data(event_date, location, cemli_list, event_type, dataset) VALUES({0})".format(', '.join(params))
	    try:
		db_cursor.execute(sql, data_set)
	    except Exception as e:
		log_message = 'cemli_chart_sync -> db_writer: Error writing data_set -> {0}\n'.format(e)
		log_writer(log_message)
		close_db_connection(db_connection, db_cursor)
		if debug_flag == 1:
		    debug_list.append('cemli_chart_sync -> db_writer: Error writing set {0} -> {1}\n'.format(data_set, e))
		    debug_writer(debug_list, 0)
		sys.exit(1)

	try:
	    db_connection.commit()
	except Exception as e:
	    db_connection.rollback()
	    log_message = 'cemli_chart_sync -> db_writer: Error running <commit> on data_list -> {0}\n'.format(e)
	    log_writer(log_message)
	    close_db_connection(db_connection, db_cursor)
	    sys.exit(1)	    

	#if debug_flag == 1:
	#    debug_list.append('cemli_chart_sync -> db_writer: commit on data_list successfull\n')
	#    debug_writer(debug_list, 0)

    #---------- Main ----------
    # 1. Truncate cemli_chart_data table
    truncate_cemli_chart()

    # 2. Fetch distinct dates for chart cemli's success and error events (past 30 days). Compile data set for each date at the end of iteration
    for cemli_id in cemli_chart_list:
	# Build date list for events with 'success' and 'error'status
	sql_success = "SELECT DISTINCT DATE(project_endtime) FROM transfer_data WHERE cemli_id = '{0}' AND ontime_status \
	    LIKE '%On time%' AND DATE(project_endtime) >= (CURDATE() - INTERVAL 30 DAY) OR  cemli_id = '{0}' AND ontime_status LIKE '%No active schedule%' AND \
	    DATE(project_endtime) >= (CURDATE() - INTERVAL 30 DAY)".format(cemli_id)
	sql_error = "SELECT DISTINCT DATE(project_endtime) FROM transfer_data WHERE cemli_id = '{0}' AND ontime_status \
	    NOT LIKE '%On time%' AND ontime_status NOT LIKE '%No active schedule%' AND DATE(project_endtime) >= (CURDATE() - INTERVAL 30 DAY) OR \
	    cemli_id = '{0}' AND message_buffer NOT LIKE '%All events OK%'".format(cemli_id)
	try:
	    db_cursor.execute(sql_success)
	    date_list_success = [x[0] for x in db_cursor.fetchall()]
	except Exception as e:
	    log_message = 'cemli_chart_sync -> error fetching success dates: {0}\n'.format(e)
	    log_writer(log_message)
	    close_db_connection(db_connection, db_cursor)
	    sys.exit(1)

	for date in date_list_success:
	    #date_str = date.strftime('%Y-%m-%d')
	    if date not in date_list:
		date_list.append(date)

	# Build date list for events with 'Error' status
	try:
	    db_cursor.execute(sql_error)
	    date_list_error = [x[0] for x in db_cursor.fetchall()]
	except Exception as e:
	    log_message = 'cemli_chart_sync -> error fetching error dates: {0}\n'.format(e)
	    log_writer(log_message)
	    close_db_connection(db_connection, db_cursor)
	    sys.exit(1)

	if date_list_error: # Error list not empty
	    for date in date_list_error:
		#date_str = date.strftime('%Y-%m-%d')
		if date not in date_list:
		    date_list.append(date)

	# Build data set for each Location (success + error)
	for date in sorted(date_list):
	    # Build a list of distinct locations for date
	    query = "SELECT DISTINCT location, ontime_status, message_buffer FROM transfer_data WHERE cemli_id = '{0}' AND DATE(project_endtime) = '{1}%'".format(cemli_id, date)
	    try:
		db_cursor.execute(query)
		location_list = db_cursor.fetchall()
	    except Exception as e:
		log_message = 'cemli_chart_sync -> CEMLI {0}, error fetching distinct locations for date {1}: {2}\n'.format(cemli_id, date, e)
		log_writer(log_message)
		close_db_connection(db_connection, db_cursor)
		sys.exit(1)

	    # Build dataset for each location, identify event_type (success/error)
	    for location in location_list: # Each item is a list [location, ontime_status, message_buffer]
		# Identify event_type
		if ('Past due' in location[1]) or ('Critical' in location[2]):
		    event_type = 'error'
		else:
		    event_type = 'success'

		# configure query to fetch number of events for the chart
		query = "SELECT COUNT(*) FROM transfer_data WHERE DATE(project_endtime) = '{0}' AND location = '{1}'".format(date, location[0])
		try:
		    db_cursor.execute(query)
		    event_count = db_cursor.fetchone()[0]
		except Exception as e:
		    log_message = 'cemli_chart_sync -> CEMLI {0}, Location {1}, error fetching event count: {2}\n'.format(cemli_id, location, e)
		    log_writer(log_message)
		    close_db_connection(db_connection, db_cursor)
		    sys.exit(1)

		# Configure dataset and append it to the master_list ---- Don't forget to convert DATE format for date
		data_list.append([date, location[0], cemli_id, event_type, event_count])

    # Send master data_list to the db_writer function
    db_writer(data_list)
	
	    
def check_schedule(): # Return list of schedules due today
    debug_flag = 1
    # Container for due schedule names for today. Each element contains [schedule_name, frequency, due_year, due_time, due_month, due_day]
    due_schedule_list = [] # Container for due schedule names for today
    # Retrive current day in the format of the schedule file
    c_month = strftime('%b')
    c_day = strftime('%d')
    # Get list of schedule files in schedule_dir
    schedule_file_list = sorted(os.listdir(schedule_dir))
    # Iterate through schedule files and compile list of due transfers
    for schedule_file in schedule_file_list:
	schedule_name = schedule_file.split('.')[0]
	schedule_f = '{0}{1}'.format(schedule_dir, schedule_file)
	with open(schedule_f, mode='r') as f: # Import data from schedule csv file
	    reader = csv.reader(f)
	    schedule_list = list(reader)
	
	# Check if today is a due date in current schedule
	for line in schedule_list:
	    if line[1] == c_month and line[2] == c_day:
		due_schedule_list.append([schedule_name, line[3], line[0], line[4], line[1], line[2]])

    # Debug
    if debug_flag == 1:
	exit_flag = 0
	debug_list = []
	debug_list.append('check_schedule: Due schedules today -> {0}\n'.format(due_schedule_list))
	debug_writer(debug_list, exit_flag)

    return due_schedule_list

def check_late_status(db_connection, db_cursor, due_schedule_list, cemli_dict_buffer): # Check if any transfers failed to complete by due time--------- UNDER CONSTRUCTION -------
    # 'info' var used to store informaiton about schedule (e.g. schedule out of date) 
    #---------- Debug (define list early as it will be needed several times in the function) ----------#
    debug_flag = 1
    if debug_flag == 1:
	debug_list = []

    # List container for due jobs
    # Each list element is a list = [cemli_id, cemli_title, location]
    due_job_list = [] 
    # Format time_stamps for MySQL query, today and yesterday (YYYY-mm-dd)
    today_str = strftime('%Y-%m-%d') 
    yesterday_delta = date.today() - timedelta(1)
    yesterday_str = yesterday_delta.strftime('%Y-%m-%d')

    for line in cemli_dict_buffer: # Iterate through dictionary text buffer line by line
	# Line format example below
	# I181,Regular Time Entry,ASUCLA,inbound_nonmed,Yes
	line_list = line.split(',')
	# Iterate through due_schedule_list's each list schedule = [schedule_name, frequency, due_year, due_time, due_month, due_day]
	for schedule in due_schedule_list:
	    if any(item in line_list for item in schedule): # Match based on schedule name
		# Check year match first (schedule out of date case)
		if schedule[0] != strftime('%Y'): # Year in schedule does not match current year
		    info = 'schedule out of date'
		# Check if it is due_time yet
		c_hour = int(strftime('%H')) # Current hour (24 hour system, zero padded decimal), converted to INT
		c_minute = int(strftime('%M')) # Current minute as a zero padded decimal, ceonverted to INT
		due_hour = int(schedule[3].split(':')[0])
		due_minute = int(schedule[3].split(':')[1])
		due_month = schedule[4] 
		due_day = schedule[5]
		if c_hour > due_hour or (c_hour == due_hour and c_minute > due_minute): # Schedule is due now
		    # Append item format = [cemli_id, cemli_title, location, frequency, due_hour, due_minute, info]
		    due_job_list.append([line_list[0], line_list[1], line_list[2], schedule[1], due_hour, due_minute, info])
		    #---------- Debug ----------#
		    if debug_flag == 1:
			debug_list.append('check_late_status: due_job_list = [{0}, {1}, {2}, {3}, {4}, \
			{5}]\n'.format(line_list[0], line_list[1], line_list[2], schedule[1], due_hour, due_minute))

    # Query transfer_data and chek if jobs in due_job_dict have been ran yesterday or today
    # Format MySQL query for each due_job_list entry
    # due_job_list: list of lists with items = [cemli_id, cemli_title, location, frequency, due_hour, due_minute]
    for item in due_job_list:
	if item[6] == 'schedule out of date':
	    continue
	cemli_str = item[0]
	cemli_title = item[1]
	location_str = item[2]
	due_hour = item[4]
	due_minute = item[5]
	sql_query = "SELECT COUNT(*) FROM transfer_data WHERE cemli_id = '{0}' AND location = '{1}' AND project_endtime LIKE '{2}%' \
	    OR cemli_id = '{0}' AND location = '{1}' AND project_endtime LIKE '{3}%';".format(cemli_str, location_str, today_str, yesterday_str)
	try:
	    db_cursor.execute(sql_query)
	    record_count = int(db_cursor.fetchone()[0])
	except Exception as e:
	    log_message = 'check_late_status: Error fetching number of records for cemli {0}, location {1}: {2}\n'.format(cemli_str, location_str, e) 
	    log_writer(log_message)
	    close_db_connection(db_connection, db_cursor)
	    sys.exit(1)

	# Check number of records, write transfer_alerts if number is 0
	if record_count == 0: # No records by due time, generate an alert
	    # Compile an alert list
	    # alert_list = [cemli_id, cemli_title, location, frequency, status, eoc_note]
	    alert_list = []
	    time_stamp = strftime('%Y-%m-%d %H:%M')
	    active = 1
	    eoc_note = 'UCPath Center standard operating procedure.'
	    frequency = item[3]
	    status = 'Past due. Expected completion by {0} {1} {2}:{3}'.format(due_month, due_day, due_hour, due_minute)
	    alert_list = [active, time_stamp, cemli_str, cemli_title, location_str, frequency, status, eoc_note]
	    insert_transfer_alerts(db_connection, db_cursor, alert_list)
	    if debug_flag == 1:
		debug_list.append('check_late_status: Late Job detected - {0}, location = {1}\n'.format(cemli_str, location_str))
	else:
	    #---------- Debug ----------#
	    if debug_flag == 1:
		debug_list.append('check_late_status: No late jobs detected.\n')
    
    #---------- Debug ----------#
    if debug_flag == 1:
	exit_code = 0
	debug_writer(debug_list, exit_code)    
    # No return values. 

def get_ontime_status(db_connection, db_cursor, cemli_id, location, completion_time, cemli_dict_buffer): # Returns Ontime status and Frequency
    ##### DEBUG ####
    debug_flag = 1
    ################
    month_dict = {'01':'Jan', '02':'Feb', '03':'Mar', '04':'Apr', '05':'May', '06':'Jun', \
	'07':'Jul', '08':'Aug', '09':'Sep', '10':'Oct', '11':'Nov', '12':'Dec'}
    adhoc_list = ['I171', 'I104', 'I248', 'I257']
    biweekly_list = ['I261']
    monthly_list = ['I103', 'I378']
    frequency = 'Ad Hoc'
    schedule_name = 'NULL'
    ontime_status = 'Not detected' # Var container for ontime_status value
    schedule_flag = 0 # default value for active schedule flag (set to 1 if there is a schedule)

    # Convert completion_time to datetime MySQL object
    datetime_obj = datetime.strptime(completion_time, '%Y-%m-%d %H:%M:%S')
    
    def check_count(db_connection, db_cursor, datetime_obj): # Check count of existing transfers
	sql = "SELECT COUNT(*) FROM transfer_data WHERE cemli_id = '{0}' AND location = '{1}' AND project_endtime >= '{2}' - INTERVAL 2 DAY".format(cemli_id, location, datetime_obj)
	try:
	    db_cursor.execute(sql)
	    c_count = db_cursor.fetchone()[0]
	except Exception as e:
	    log_message = 'get_ontime_status -> check_count -> Error fetching event count: {0}\n'.format(e)
	    log_writer(log_message)
	    close_db_connection(db_connection, db_cursor)
	    sys.exit(1)

	return c_count

    def get_frequency(schedule_file, schedule_name, completion_month, completion_day, completion_hour, completion_minute):
	# Set local variables
	frequency = 'Ad Hoc'
	ontime_status = 'Transfer processed 2 or more days outside of due date'

	# Check event count for CEMLI/Location in the past 2 day period
	c_count = check_count(db_connection, db_cursor, datetime_obj)

	with open(schedule_file, mode='r') as f:
	    f_buffer = f.readlines()
	for line in f_buffer:
	    # Line example
	    # 2018,Aug,21,Monthly,15:30
	    if 'Year' in line: # Skip header line
		continue
	    # Convert line into list of strings
	    line_list = line.split(',')

	    # Condition 1: absolute date match
	    if completion_month == line_list[1] and completion_day == line_list[2]: # Absolute date match

		#schedule_year = line_list[0]
		frequency = line_list[3]
		due_hour = int(line_list[4].split(':')[0])
		due_minute = int(line_list[4].split(':')[1])
		
		# Subcondition 1: c_count != 0
		if c_count != 0:
		    ontime_status = 'On time'
		    break

		# Subcondition 2: c_count = 0 and project_endtime is past due
		elif c_count == 0 and (int(completion_hour) > due_hour or (int(completion_hour) == due_hour and int(completion_minute) > due_minute)):
		    ontime_status = 'Late. Expected completion by {0} on {1} {2}.'.format(line_list[4], line_list[1], line_list[2])
		    break

		# Subcondition 3: else -> on time
		else:
		    ontime_status = 'On time'
		    break

	    # Condition 2: project_endtime is 1 to 2 days before the due date/time
	    elif completion_month == line_list[1] and ((int(line_list[2]) - int(completion_day)) == 1) or \
		completion_month == line_list[1] and ((int(line_list[2]) - int(completion_day)) == 2):
		#schedule_year = line_list[0]
		frequency = line_list[3]
		ontime_status = 'On time'
		break

	    # Condition 3: c_count = 0 and project_endtime is 1 to 2 days past due
	    elif c_count == 0 and completion_month == line_list[1] and ((int(line_list[2]) - int(completion_day)) == -1):
		frequency = line_list[3]
		ontime_status = 'Late. Expected completion by {0} on {1} {2}.'.format(line_list[4], line_list[1], line_list[2])
		break

	    # Condition 4: c_count != 0 and project_endtime is 1 to 2 days past due
	    elif c_count != 0 and completion_month == line_list[1] and ((int(line_list[2]) - int(completion_day)) == -1) \
		or c_count != 0 and completion_month == line_list[1] and ((int(line_list[2]) - int(completion_day)) == -2):
		frequency = line_list[3]
		ontime_status = 'On time'
		break

	return frequency, ontime_status

    # Check for fixed frequencies
    if cemli_id in adhoc_list:
	frequency = 'Ad Hoc'
    elif cemli_id in biweekly_list:
	frequency = 'Biweekly'
    elif cemli_id in monthly_list:
	frequency = 'Monthly'
    else:
	for line in cemli_dict_buffer: # Iterate through cemli dict line by line
	    # I156,FICA Exempt Inbound,NULL,I156_fica_exempt,Yes
	    line_list = line.split(',')
	    if (cemli_id in line_list) and (location in line_list) and (line_list[3] != 'NULL'): # line_list[3] = schedule_name
		schedule_flag = 1
		schedule_name = line_list[3]
		break

    # Active schedule detected
    if schedule_flag == 1: # Open schedule_name.csv file and check due date/time
	completion_month = month_dict[completion_time.split(' ')[0].split('-')[1]]
	completion_day = completion_time.split(' ')[0].split('-')[2]
	completion_hour = completion_time.split(' ')[1].split(':')[0]
	completion_minute = completion_time.split(' ')[1].split(':')[1]
	schedule_file = '{0}{1}.csv'.format(schedule_dir, schedule_name)

	#--------- Debug ---------#
	if debug_flag == 1:
	    debug_list = []
	    debug_list.append('get_ontime_status: cemli_id = {0}, location = {1}, completion_time = {2}, \
		schedule_file = {3}.csv\n'.format(cemli_id, location, completion_time, schedule_name))
	    debug_writer(debug_list, 0)

	# Fetch list of transfer frequencies due today
	frequency, ontime_status = get_frequency(schedule_file, schedule_name, completion_month, completion_day, completion_hour, completion_minute)

    else:
	ontime_status = 'No active schedule configured'

    # Debug
    if debug_flag == 1:
	debug_list = []
	debug_list.append('get_ontime_status: cemli<arg> = {0}, schedule_name = {1}, ontime_status = {2}.\n'.format(cemli_id, schedule_name, ontime_status))
	debug_writer(debug_list, 0)

    return ontime_status, frequency

def process_ctm_input_buffer(input_buffer, logmon_conditions_list, exclude_job_list, ctm_file): # Format a dictionary of ctm jobs with valid detected completion codes
    #----------------- Local Variables ---------------------#
    # logmon_conditions_list - list of job status conditions to include from conf file
    filtered_buffer_list = [] # List of filtered lines for analysis
    endedat = [] # List of lines with ENDED AT entries
    endedok = [] # List of lines with ENDED OK entries
    endednotok = [] # List of lines with ENDED NOTOK entries
    job_list = [] # Master list of formated Job data for return

    # Debug
    if debug_flag == 1:
	debug_list = []
    #------------------ End Variables ----------------------#

    #--------------- Internal Functions --------------------#
    def get_oscompstat_int(endedat, order_id): # Get oscompstat code and job year from ENDEDAT list
	job_year = ''
	oscompstat = None
	for line in endedat:
	    if order_id in line:
		try:
		    log_str = line.split('|')[7].strip()
		    split_log = log_str.split(' ')
		    oscompstat = int(split_log[4].replace('.', ''))
		    job_year = split_log[2][:4]
		    break
		except Exception as e:
		    log_message = 'process_ctm_input_buffer -> get_oscompstat_int: error parsing OSCOMPSTAT - {0}. Full line <{1}>. CTM Log: {2}.\n'.format(e, line, ctm_file)
		    log_writer(log_message)
		    sys.exit(1)

	if not job_year:
	    job_year = '0000'
	    # Debug
	    if debug_flag == 1:
		debug_list.append('process_ctm_input_buffer: get_oscompstat_int: Current year not detected, job_year = 0000.\n')
	return job_year, oscompstat

    def format_time(job_year, split_line): # Format timestamp to MySQL datetime format
	# Year is extracted from ENDED AT line in get_oscompstat function
	MO = split_line[1][:2] # month value from ENDED OK line
	DD = split_line[1][2:] # day value from ENDED OK line
	HH = split_line[2][:2] # hour value from ENDED OK line
	MM = split_line[2][2:] # minute value from ENDED OK line
	time_stamp = '{0}-{1}-{2} {3}:{4}'.format(job_year, MO, DD, HH, MM)
	return time_stamp

    def append_job_list(list_name, job_list): # Add new list data to Master Job List
	for line in list_name:
	    split_line = line.split('|')
	    order_id = split_line[4].strip()
	    job_name = split_line[3].strip()
	    if job_name in exclude_job_list: # Check if ctm_job in the excluded list
		continue
	    job_year, oscompstat = get_oscompstat_int(endedat, order_id)
	    job_time = format_time(job_year, split_line)
	    if endedok:
		job_status = 'ENDED OK'
	    elif endednotok:
		job_status = 'ENDED NOTOK'
	    job_list.append([job_time, job_name, order_id, job_status, oscompstat])
	    # Debug
	    if debug_flag == 1:
		debug_list.append('process_ctm_input_buffer: append_job_list: job_time = {0}, job_name = {1}, order_id = {2}, job_status = {3}, \
		oscompstat = {4}\n'.format(job_time, job_name, order_id, job_status, oscompstat))

	return job_list # Temp list
    #------------- End Internal Functions ------------------#

    #----------------- Internal Main -----------------------#
    # Compile lists with ENDEDOK, ENDEDNOTOK, and ENDEDAT lines into separate lists
    for line in input_buffer:
	if 'ENDED AT' in line:
	    endedat.append(re.sub('[ \t]+' , ' ', line.replace('\n', ''))) # Regex removal of extra spaces and tabs --->
	elif 'ENDED OK' in line:
	    endedok.append(re.sub('[ \t]+' , ' ', line.replace('\n', '')))
	elif 'ENDED NOTOK' in line:
	    endednotok.append(re.sub('[ \t]+' , ' ', line.replace('\n', '')))

    # Run append_job_list function to add job data with ENDED OK and ENDED NOTOK conditions
    job_list = append_job_list(endedok, job_list)
    endedok = [] # Reset list after processing to avoid detection
    job_list = append_job_list(endednotok, job_list)
    endednotok = [] # Reset list after processing to avoid further detection

    # Debug
    if debug_flag == 1:
	exit_code = 0
	debug_writer(debug_list, exit_code)

    # job_list = [job_time, job_name, order_id, job_status, oscompstat]
    return job_list # Master list

def update_ctm_job_list(job_list): # Add process name and location to individual ctm job list
    process_name = get_process_name(job_list[1]) # Send job name to translate to process name
    location = get_location(job_list[1])
    oscompstat_message = get_oscompstat(job_list[4]) # oscompstat int as argument to fetch message
    job_list.insert(3, process_name)
    job_list.insert(4, location)
    job_list.insert(7, oscompstat_message)

    # Debug
    if debug_flag == 1:
	debug_list = []
	exit_code = 0
	debug_list.append('update_ctm_job_list: process_name = {0}, location = {1}, oscompstat_message = {2}.\n'.format(process_name, location, oscompstat_message))
	debug_writer(debug_list, exit_code)

    return job_list
	
def sync_ctm_files(db_connection, db_cursor): # Insert ctm file list into ctm_files table
    ctm_file_list = [] # Empty container for final file list
    def fetch_file_list():
	try:
	    db_cursor.execute('SELECT file_name FROM ctm_files')
	    db_file_list = [item[0] for item in db_cursor.fetchall()]
	except Exception as e:
	    log_message = 'sync_ctm_files: SELECT from ctm_files error: {0}\n'.format(e)
	    log_writer(log_message)
	    close_db_connection(db_connection, db_cursor)
	    sys.exit(1)
	return db_file_list

    # Read in the current ctm_path log directory
    ctm_dir_list = sorted(os.listdir(path_ctm))

    # Fetch file list from db
    db_file_list = fetch_file_list()

    # Build a list of files to remove (if any have been moved from directory) and remove from table
    try:
	delete_list = [f for f in db_file_list if f not in ctm_dir_list]
    except:
	delete_list = []
    if delete_list:
	for file_name in delete_list:
	    try:
		query = "DELETE FROM ctm_files WHERE file_name = '{0}'".format(file_name)
		db_cursor.execute(query)
	    except Exception as e:
		log_message = 'sync_ctm_files: Error deleting from table -> {0}'.format(e)
		log_writer(log_message)
		close_db_connection(db_connection, db_cursor)
		sys.exit(1)
	db_connection.commit()

    # Create list of unique file names to be written to ctm_files table in ctm_file_list object
    if not delete_list:
	ctm_files = [x for x in ctm_dir_list if x not in db_file_list]
    else:
	db_file_list = fetch_file_list()
	ctm_files = [x for x in ctm_dir_list if x not in db_file_list]

    # Re-iterate trhrough file list to sort out excluded job names
    for x in ctm_files:
	if not any(item in x for item in exclude_job_list):
	    ctm_file_list.append(x)
    # Write file names to the ctm_files table 
    try:
    	for x in ctm_file_list:
    	    db_cursor.execute('INSERT INTO ctm_files(file_name, processed_flag) VALUES (%s, %s)', (x, 'N'))
    except Exception as e:
    	db_connection.rollback()
    	log_message = 'sync_ctm_files: INSERT INTO ctm_files failed: {0}\n'.format(e)
    	log_writer(log_message)
	db_cursor.close()
	db_connection.close()
    db_connection.commit()

def id_goa_log(job_name, order_id, cemli_id, goa_file_list): # Determine job_type (CTM/GoA), identify correct goa log file
    goa_file = ''
    for file_name in goa_file_list: # using global goa_file_list generated in 'sort_log_dir' function
	if cemli_id and order_id in file_name: # Match GoA file using CEMLI id
	    goa_file = file_name
	elif job_name and order_id in file_name: # Match GoA file using job name (No CEMLI ID)
	    goa_file = file_name
    
    if not goa_file:
	job_type = 'CTM'
	process_goa_flag = 0
    else:
	job_type = 'GOA'
	process_goa_flag = 1
    return goa_file, job_type

def process_ps_data(db_connection, db_cursor): # Process PS Log and insert data into DB
    #### DEBUG ####
    debug_flag = 1
    debug_list = []
    ###############

    #--------------- Functions ---------------#
    def db_writer(data_set):
	params = ['%s' for x in data_set]
	sql = "INSERT INTO ps_files(file_name, processed_flag) VALUES({0})".format(', '.join(params))
	try:
	    db_cursor.execute(sql, data_set)
	except Exception as e:
	    db_connection.rollback()
	    log_message = 'process_ps_data -> db_writer - Error writing data_set: {0}\n'.format(e)
	    log_writer(log_message)
	    close_db_connection(db_connection, db_cursor)
	    sys.exit(1)
	
    def get_timestamp(line): # Return timestamp from the text string
	location = 'Not detected'
	location_dictionary = {'ASLA':'UCLA', 'RVCMP':'UC Riverside', 'LACMP':'UCLA', 'MECMP':'UC Merced', 'SBCMP':'UC Santa Barbara', \
	    'UCOP':'UCOP'}
	date_text = line.split('- ')[1].split('_')[3].split('.')[0]
	date_year = date_text[:4]
	date_month = date_text[4:6]
	date_day = date_text[6:8]
	date_hour = date_text[8:10]
	date_minute = date_text[10:12]
	date_second = date_text[12:14]

	# Check location
	for key, value in location_dictionary.iteritems():
	    if key in line:
		location = location_dictionary[key]

	time_stamp = '{0}-{1}-{2} {3}:{4}:{5}'.format(date_year, date_month, date_day, date_hour, date_minute, date_second)
	return time_stamp, location

    def update_record(time_stamp, location, total_records, success_records, error_records): # Update existing Transfer record with PS data
	# Search for matching record
	sql = "SELECT"

    def scan_f_buffer(file, f_buffer): # Parse PS f_buffer and format metrics for DB write
	global debug_list
	mismatch_score = 0 # Accumulating score to determine valid PS log
	checkpoint = 0 # Additional flag for log validity check
	record_flag = 0
	for i, line in enumerate(f_buffer):
	    if i == 0 and '*******' not in line:
		mismatch_score += 1 
	    elif i == 2 and '*******' not in line:
		mismatch_score += 1
	    elif 'Error Details and Flagged off records' in line:
		checkpoint = 1

	    if 'File Name -' in line: # Begin new record
		# Set flag for active record
		record_flag = 1
		# Parse time_stamp, location from file name
		time_stamp, location = get_timestamp(line)
	    elif 'Total number of records processed' in line:
		total_records = line.split(': ')[1]
	    elif 'Total number of successful records loaded' in line:
		success_records = line.split(': ')[1]
	    elif 'Total number of error records and error messages' in line:
		error_records = line.split(': ')[1]
	    elif 'Error rows/Details' in line: # End of record, send data to update a matching DB record
		if record_flag == 1: # Make sure record flag is active
		    update_record(time_stamp, location, total_records, success_records, error_records)
		    record_flag = 0
		    # Process debug
		    if debug_flag == 1:
			debug_list.append('process_ps_data -> scan_f_buffer - Record sent to DB update. Timestamp = {0}, CEMLI = {1}, Location = {2}\n'.format(time_stamp, ))

	# Check mismatch_score and checkpoint values
	if mismatch_score >=1 and checkpoint == 1:
	    log_message = 'PS log {0} needs to be examined, content sequence out of order\n'.format(file)
	    log_writer(log_message)
	elif mismatch_score == 2:
	    log_message = 'PS log {0} appears to be invalid\n'.format(file)

    #--------------- Main ---------------# 
    file_data_set = [] # lists of file data for ps_files table

    # Fetch a list of indexed PS files from DB
    sql = "SELECT file_name FROM ps_files"
    try:
	db_cursor.execute(sql)
	indexed_file_list = [i[0] for i in db_cursor.fetchall]
    except Exception as e:
	log_message = 'process_ps_data -> index_ps_files - Error fetching list from DB: {0}\n'.format(e)
	log_writer(log_message)
	close_db_connection(db_connection, db_cursor)
	sys.exit(1)

    # Get a list of PS dir listing and deduce a unique list of new files
    ps_file_list = [] # Container for new PS files that have NOT been indexed
    full_ps_file_list = os.listdir(path_ps) # Full directory list
    ps_file_list = [i for i in full_ps_list if i not in indexed_file_list]

    # Iterate through ps_file_list and build a list of file index data - a list of lists
    for file in ps_file_list:
	if 'tar' in file:
	    continue
	file_name = '{0}{1}'.format(path_ps, file)
	with open(file_name, mode='r') as f:
	    f_buffer = f.readlines()
	    # Pass f_buffer to the scanner function
	    scan_f_buffer(file, f_buffer)


    #ps_process_time_stamp = None
    #ps_process_status = 'NULL'
    #ps_error_buffer = 'NULL'
    #return ps_process_time_stamp, ps_process_status, ps_error_buffer


def process_goa_data(goa_log, dashboard_active, cemli_id): # Process GoA log data
    #-------------------- Local Variables --------------------#
    debug_flag = 0
    debug_list = []
    transfer_status = None
    goa_read_buffer = [] # Build buffer with raw goa log file
    error_buffer_list = []
    message_buffer_list = [] # Message collector for scheduled transfer jobs (or other jobs if useful)
    upload_success_flag = 0 # Indicate the state of file upload (0 = did not upload)
    goa_user_exception_list = ['I106', 'I129', 'I158'] # List of cemli's with no dedicated GoA user
    #--------------------- End Variables ---------------------#

    #------------------ Internal Functions -------------------#
    def get_event_time(line): # Return time stamp from log line
	s_line = line.split()
	time_stamp = '{0} {1}'.format(s_line[0], s_line[1])
	return time_stamp

    def get_generic_data(goa_text_buffer): # Collect generic data for job_data table
	goa_data_list = [] # List to append log lines to
	goa_error_list = [] # List or lines with detected erroneous conditions
	include_list = ['COMMAND: get', 'successfully downloaded', 'decrypted successfully', 'COMMAND: put', 'successfully uploaded', \
	    'COMMAND: rm', 'deleted successfully']
	goa_messages = '' # Final formated text container
	goa_error_buffer = '' # Final formated container for error messages
	error_conditions_list = ['Raise Error', 'exception', 'Exception', 'Error', 'ERROR', 'Failed', 'Caused by']
	'''
	Data to collect:
	1. GoA messages
	2. Error messages (if detected)
	'''
	for line in goa_text_buffer:
	    if any(item in line for item in include_list):
		goa_data_list.append(re.sub('[ \t]+' , ' ', line.replace('\n', ''))) # Regex removal of multi spaces and/or tabs
	    elif any(item in line for item in error_conditions_list):
		goa_error_list.append(re.sub('[ \t]+' , ' ', line.replace('\n', '')))
	
	# Join message and error lists into text containters
	if goa_data_list:
	    goa_messages = '|'.join(goa_data_list)
	else:
	    goa_messages = 'NULL'

	if goa_error_list:
	    goa_error_buffer = '|'.join(goa_error_list)
	else:
	    goa_error_buffer = 'NULL'

	# Truncate GoA error/log text if exceeds 8100 characters
	if len(goa_messages) > 8100:
	    goa_messages_formatted = '{0} --> Data truncated to 8100 characters.'.format(goa_messages[:8100])
	else:
	    goa_messages_formatted = goa_messages
	if len(goa_error_buffer) > 5000:
	    goa_error_buffer_formatted = '{0} --> Data truncated to 8100 characters.'.format(goa_error_buffer[:5000])
	else:
	    goa_error_buffer_formatted = goa_error_buffer

	# Debug
	if debug_flag == 1:
	    debug_list.append('process_goa_data -> get_generic_data: goa_log<arg> = {0}\n'.format(goa_log))
	    debug_writer(debug_list, 0)
	return goa_messages_formatted, goa_error_buffer_formatted

    def get_transfer_data(goa_text_buffer): # Collect detailed data fro transfer_data table
	'''
	Data to collect:
	1. GoA user ID
	2. PS user ID
	3. Location to PCSSC download timestamp
	4. Location to PCSSC download status
	5. Location to PCSSC decryption status
	6. PCSSC to PS upload timestamp
	7. PCSSC to PS upload status
	8. PCSSC transfer status
	9. Ontime status for interface transfers running on schedule
	'''
	message_buffer_list = [] # Container for message_buffer with Informational messages
	goa_user_id = 'NULL' # Container for GoA user ID string value
	ps_user_id = 'NULL' # Container for PS user ID string value
	completion_time = None # Container for time of GoA operation completion
	loc_to_pcssc_download_timestamp = None # Container for Location to PCSSC download timestamp
	pcssc_to_ps_upload_timestamp = None # Container for PCSSC to PS upload timestamp
	loc_to_pcssc_download_status = 'NULL' # COntainter for Location to PCSSC download status
	loc_to_pcssc_decryption_status = 'NULL' # Container for Decryption status of downlaoded file
	pcssc_to_ps_upload_status = 'NULL'
	loc_to_pcssc_decryption_status = 'NULL'

	def get_timestamp(line): # Get MySQL formatted timestamp from line
	    split_line = line.split()
	    time_stamp = '{0} {1}'.format(split_line[0], split_line[1])
	    return time_stamp

	# Iterate through the GoA Log text buffer
	decryption_flag = 0
	for line in goa_text_buffer:
	    if 'as user' in line and 'berkeley.edu' in line:
		split_line = line.split('user')
		goa_user_id = split_line[1].replace("'", "").strip()
	    elif 'as user' in line and 'oracleoutsourcing.com' in line:
		split_line = line.split('user')
		ps_user_id = split_line[1].replace("'", "").strip()
	    elif 'as user' in line and 'commutercheckdirect.com' in line:
		split_line = line.split('user')
		goa_user_id = split_line[1].replace("'", "").strip()

	    elif 'successfully downloaded' in line:
		loc_to_pcssc_download_timestamp = get_timestamp(line)
		loc_to_pcssc_download_status = 'Success'
		#---------- Debug ----------#
		if debug_flag == 1:
		    debug_list.append('process_goa_data -> get_transfer_data: loc_to_pcssc_download_status = {0}\n'.format(loc_to_pcssc_download_status))

	    elif 'successfully uploaded' in line:
		pcssc_to_ps_upload_timestamp = get_timestamp(line)
		pcssc_to_ps_upload_status = 'Success'
		#---------- Debug ----------#
		if debug_flag == 1:
		    debug_list.append('process_goa_data -> get_transfer_data: pcssc_to_ps_upload_status = {0}\n'.format(pcssc_to_ps_upload_status))

	    elif 'pgpDecrypt' in line and 'it is disabled' in line:
		loc_to_pcssc_decryption_status = 'Disabled'
		decryption_flag = 1

	    elif 'decrypted successfully' in line:
		loc_to_pcssc_decryption_status = 'Success'
		decryption_flag = 1

	    elif 'End Date and Time:' in line:
		completion_time = get_timestamp(line)
		#---------- Debug ----------#
		if debug_flag == 1:
		    debug_list.append('process_goa_data -> get_transfer_data: project end date/time = {0}\n'.format(completion_time))

	# Check decryption flag
	if decryption_flag == 0:
	    loc_to_pcssc_decryption_status = 'Not Used'

	# Check variable status and compile message_buffer
	if cemli_id in goa_user_exception_list:
	    goa_user_id = 'GoA login not used for {0}'.format(cemli_id)
	elif goa_user_id == 'NULL':
	    message_buffer_list.append('Warning: GoA user ID detection error.')
	if ps_user_id == 'NULL':
	    message_buffer_list.append('Warning: PS user ID detection error.')
	if loc_to_pcssc_download_timestamp == None:
	    message_buffer_list.append('Warning: Location to PCSSC download event detection error.')
	if pcssc_to_ps_upload_timestamp == None:
	    message_buffer_list.append('Warning: PCSSC to PS upload event detection error.')
	if loc_to_pcssc_decryption_status == 'NULL':
	    message_buffer_list.append('Warning: Location to PCSSC download decryption detection error.')
	if completion_time == None:
	    message_buffer_list.append('Warning: GoA completion time detection error.')

	if message_buffer_list:
	    message_buffer = '|'.join(message_buffer_list)
	else:
	    message_buffer = 'Info: No messages'

	# Return set of transfer data
	return [goa_user_id, ps_user_id, loc_to_pcssc_download_timestamp, pcssc_to_ps_upload_timestamp, loc_to_pcssc_download_status, \
	    loc_to_pcssc_decryption_status, pcssc_to_ps_upload_status, completion_time, message_buffer]


    #---------------- End Internal Functions -----------------#
    # Read in the GoA Log text buffer
    goa_log_file = '{0}{1}'.format(path_goa, goa_log)
    with open(goa_log_file, mode='r') as f:
	goa_text_buffer = f.readlines()
 
    # Begin processing GoA Log buffer
    # 1. Get generic GoA data for job_data table
    if len(goa_text_buffer) > 100:
	goa_generic_messages, goa_error_buffer = get_generic_data(goa_text_buffer)

	# 2. Check dashboard_active value for scheduled interface transfers
	if dashboard_active == 'Yes':
	    goa_data_list = get_transfer_data(goa_text_buffer)
	    goa_data_list.append(goa_generic_messages)
	    goa_data_list.append(goa_error_buffer)
	    #---------- Debug ----------#
	    if debug_flag == 1:
		exit_code = 0
		debug_list.append('process_goa_data: transfer_data_list fetched (dash active = Yes).\n')
		debug_writer(debug_list, exit_code)

	    return goa_data_list
	else:
	    #---------- Debug ----------#
	    if debug_flag == 1:
		exit_code = 0
		debug_list.append('process_goa_data: generic GoA data fetched (dash active = No).\n')
		debug_writer(debug_list, exit_code)

	    # Return only generic data back to process_log_data function
	    return [goa_generic_messages, goa_error_buffer]
    else: # Goa log has less than 100 lines
	return [] # Return empty list container to indicate an invalid GoA file
	

def insert_job_data(db_connection, db_cursor, job_data_list): # Insert into job_data table. Accept list as argument
    # job_data_list = [mysql_timestamp, job_type, file_item, goa_log_file, job_name, order_id, \
	# process_name, location, cemli_id_str, cemli_title, job_status, oscompstat, \
	# oscompstat_message, goa_messages, error_buffer]
    # Format query string
    params = ['%s' for item in job_data_list]
    query = 'INSERT into job_data(time_stamp, source, ctm_log_file, goa_log_file, job_name, order_id, process_name, \
	location, cemli_id, cemli_title, job_status, oscompstat, oscompstat_message, goa_messages, error_buffer) VALUES ({0});'.format(', '.join(params))
    try: 
	db_cursor.execute(query, job_data_list)
	db_connection.commit()
    except Exception as e:
	db_connection.rollback()
	log_message = 'insert_job_data: {0}. CTM File: {1}, GoA File = {2}\n'.format(e, job_data_list[2], job_data_list[3])
	stdout.write('Num of params = {0}\n'.format(len(params)))
	log_writer(log_message)
	close_db_connection(db_connection, db_cursor)
	sys.exit(1)

def insert_transfer_data(db_connection, db_cursor, transfer_data_list): # Write data to transfer_data table
    debug_flag = 0
    # Format query string
    params = ['%s' for item in transfer_data_list]
    query = 'INSERT INTO transfer_data(ctm_log_file, goa_log_file, location, job_name, cemli_id, cemli_title, frequency, goa_user_id, ps_user_id, \
	loc_to_pcssc_download_timestamp, loc_to_pcssc_download_status, loc_to_pcssc_decryption_status, \
	pcssc_to_ps_upload_timestamp, pcssc_to_ps_upload_status, project_endtime, ontime_status, ps_process_timestamp, \
	ps_process_status, message_buffer, goa_error_buffer, ps_error_buffer) VALUES ({0});'.format(', '.join(params))

    try:
	db_cursor.execute(query, transfer_data_list)
	db_connection.commit()
	
	#------ Debug ------#
	if debug_flag == 1:
	    debug_list = []
	    debug_list.append('insert_transfer_data: Insert successful for job_name = {0}, location = {1}\n'.format(transfer_data_list[1], transfer_data_list[0]))
	    debug_writer(debug_list, 0)
    except Exception as e:
	db_connection.rollback()
	log_message = 'insert_transfer_data: INSERT INTO transfer_data failed: {0}. GoA file: {1}\n'.format(e, transfer_data_list[1])
	log_writer(log_message)
	close_db_connection(db_connection, db_cursor)
	
	#------ Debug ------#
	if debug_flag == 1:
	    debug_list = []
	    debug_list.append('insert_transfer_data: Insert failed for job_name = {0}, location = {1}\n'.format(transfer_data_list[1], transfer_data_list[0]))
	    debug_writer(debug_list, 1)
	sys.exit(1)

def deactivate_general_alerts(db_connection, db_cursor): # Set alerts to inactive if older than 1 week
    query = "UPDATE general_alerts SET active = 0 WHERE time_stamp < NOW() - INTERVAL 1 WEEK"
    try:
	db_cursor.execute(query)
	db_connection.commit()
    except Exception as e:
	log_message = 'deactivate_general_alerts: {0}\n'.format(e)
	log_writer(log_message)
	close_db_connection(db_connection, db_cursor)
	sys.exit(1)

def insert_general_alerts(db_connection, db_cursor, alert_list, goa_log_file):
    # alert_list = [active, time_stamp, alert_text, eoc_note]
    params = ['%s' for item in alert_list]
    query = 'INSERT INTO general_alerts(active, time_stamp, alert_text, eoc_note) VALUES ({0})'.format(', '.join(params))
    try:
	db_cursor.execute(query, alert_list)
	db_connection.commit()
	#---------- Debug ----------#
	if debug_flag == 1:
	    exit_code = 0
	    debug_list = []
	    debug_list.append('insert_general_alerts: {0}\n'.format(alert_list[2]))
	    debug_writer(debug_list, exit_code)
    except Exception as e:
	db_connection.rollback()
	log_message = 'insert_general_alerts: INSERT INTO general_alerts error: {0}. GOA File = {1}.\n'.format(e, goa_log_file)
	log_writer(log_message)
	close_db_connection(db_connection, db_cursor)
	sys.exit(1)

def insert_transfer_alerts(db_connection, db_cursor, alert_list, cemli_dict_buffer): # Insert new alerts or update existing alerts on File Transfers
    debug_flag = 1
    debug_list = []

    def get_expected_count(cemli_id, location, frequency, cemli_dict_buffer, debug_flag): # Retrieve expected_count value for current transfer from CEMLI Dictionary
	expected_count = 'Not set' # set default value
	for line in cemli_dict_buffer:
	    line_list = line.split(',')
	    if (cemli_id == line_list[0]) and (location == line_list[2]) and (frequency == line_list[5]): # Job is a match
		file_count = line_list[6]
		if file_count == 'NULL':
		    expected_count = 'Not set'
		else:
		    expected_count = file_count
	#----- Debug -----#
	if debug_flag == 1:
	    debug_list.append('insert_transfer_alerts -> get_expected_count: CEMLI = {0}, Location = {1}, Frequency = {2}. Expected Count = {3}\n'.format(cemli_id, \
		location, frequency, expected_count))
	    debug_writer(debug_list, 0)

	return expected_count

    def get_current_count(cemli_id, location, frequency): # Determine actual current count
	#---------- Configure MySQL queries ----------#
	# Query 1 - check if current alarm exists before attempting to retrieve actual_count value
	query_exists = "SELECT COUNT(*) FROM transfer_alerts WHERE active = 1 AND cemli_id = '{0}' \
	    AND location = '{1}' AND frequency = '{2}' AND status NOT LIKE '%Past due%'".format(cemli_id, location, frequency)
	# Query 2 - get actual_count once active alarm is verified
	query_count = "SELECT actual_count FROM transfer_alerts WHERE active = 1 AND cemli_id = '{0}' \
	    AND location = '{1}' AND frequency = '{2}' AND status NOT LIKE '%Past due%'".format(cemli_id, location, frequency)
	# Query 3 - Check if Past Due transfer alarm exists
	query_past_due_exists = "SELECT COUNT(*) FROM transfer_alerts WHERE active = 1 AND cemli_id = '{0}' \
	    AND location = '{1}' AND frequency = '{2}' AND status LIKE '%Past due%'".format(cemli_id, location, frequency)
	# Query 4 - Get actual_count for Past Due alarm if active
	query_past_due_count = "SELECT actual_count FROM transfer_alerts WHERE active = 1 AND cemli_id = '{0}' \
	    AND location = '{1}' AND frequency = '{2}' AND status LIKE '%Past due%'".format(cemli_id, location, frequency)

	# Execute Query 1 - Check if active alarm exists that is not a 'Past Due'
	try:
	    db_cursor.execute(query_exists)
	    exists_count = db_cursor.fetchone()[0]
	except Exception as e:
	    log_message = 'insert_transfer_alerts -> get_current_count: Error in query_exists - {0}\n'.format(e)
	    log_writer(log_message)
	    close_db_connection(db_connection, db_cursor)
	    sys.exit(1)

	# Execute Query 3 - Check if Past Due record exists
	try:
	    db_cursor.execute(query_past_due_exists)
	    past_due_exists = db_cursor.fetchone()[0]
	except Exception as e:
	    log_message = 'insert_transfer_alerts -> get_current_count: Error in past_due_query_exists - {0}\n'.format(e)
	    log_writer(log_message)
	    close_db_connection(db_connection, db_cursor)
	    sys.exit(1)

	# If exists_count not 0, retrieve actual_count value. If <None>, set value to 0
	if exists_count != 0:
	    # Execute Query 2
	    try:
		db_cursor.execute(query_count)
		actual_count = db_cursor.fetchone()[0]
		#------- Debug -------#
		if debug_flag == 1:
		    debug_list.append('insert_transfer_alerts -> get_current_count: actual_count = {0}\n'.format(actual_count))
	    except Exception as e:
		log_message = 'insert_transfer_alerts -> get_current_count: Error fetching actual_count - {0}'.format(e)
		log_writer(log_message)
		close_db_connection(db_connection, db_cursor)
		sys.exit(1)
	else:
	    actual_count = 0	

	# If Past Due count not 0, retrieve past_due_count. If <None>, set value to 0
	if past_due_exists != 0:
	    # Execute Query 4
	    try:
		db_cursor.execute(query_past_due_count)
		past_due_count = db_cursor.fetchone()[0]
	    except Exception as e:
		log_message = 'insert_transfer_alerts -> get_current_count: Error fetching past_due_count - {0}'.format(e)
		log_writer(log_message)
		close_db_connection(db_connection, db_cursor)
		sys.exit(1)
	else:
	    past_due_count = 0
		
	# Write debug data
	debug_writer(debug_list, 0)

	# Return values for actual_count and past_due_count
	return actual_count, past_due_count

    def db_write(alert_parameters): # Write data to db
	params = ['%s' for item in alert_parameters]
	query = 'INSERT INTO transfer_alerts(active, expected_count, actual_count, project_endtime, cemli_id, cemli_title, location, \
	    frequency, status, eoc_note) VALUES ({0});'.format(', '.join(params))
	try:
	    db_cursor.execute(query, alert_parameters)
	    db_connection.commit()
	except Exception as e:
	    db_connection.rollback()
	    log_message = 'insert_transfer_alerts -> db_write: INSERT INTO transfer_alerts failed - {0}.\n'.format(e)
	    log_writer(log_message)
	    close_db_connection(db_connection, db_cursor)
	    sys.exit(1)
	    

    # Unpack alert_list = [active, project_endtime, cemli_str, cemli_title, location_str, frequency, status, eoc_note]
    cemli_id = alert_list[2]
    cemli_title = alert_list[3]
    location = alert_list[4]
    frequency = alert_list[5]
    project_endtime = alert_list[1]
    status = alert_list[6]
    eoc_note = alert_list[7]

    # Get value for File expected_count VAR. The value is stored in cemli_dict file for each CEMLI
    expected_count = get_expected_count(cemli_id, location, frequency, cemli_dict_buffer, debug_flag)

    # Get actual count values including Past Due if exists
    actual_count, past_due_count = get_current_count(cemli_id, location, frequency)

    # Based on count values, determine the record update parameters. Configure alert_parameters list
    #alert_parameters = [active, actual_count, expected_count, project_endtime, cemli_id, cemli_title, location, frequency, status, eoc_note]
    if actual_count == 0 and past_due_count == 0: # No active alert present in DB, perform an INSERT and set actual_count to 1
	# Set actual_count to 1
	actual_count = 1
	# Format alert_parameters list
	alert_parameters = [1, expected_count, actual_count, project_endtime, cemli_id, cemli_title, location, frequency, status, eoc_note]
	# Write alert data to db
	db_write(alert_parameters)

    elif actual_count == 0 and past_due_count != 0: # Record is Past Due. Increase past_due_count by 1 if the current alarm is also Past Due
	if 'Past due' in status: # Alarm is still past due. Update time_stamp and increase count 
	    past_due_count += 1
	    # Get current time for updated timestamp
	    time_stamp = strftime('%Y-%m-%d %H:%M:%S')
	    # Format alert parameters
	    alert_parameters = [1, 'n/a', past_due_count, time_stamp, cemli_id, cemli_title, location, frequency, status, eoc_note]
	    # Write alert data to db
	    db_write(alert_parameters)

	else: # Past due record has been processed. Add a regular current alarm with count = 1
	    actual_count += 1
	    # Format alert parameters
	    alert_parameters = [1, expected_count, actual_count, project_endtime, cemli_id, cemli_title, location, frequency, status, eoc_note]
	    # Write alert data to db
	    db_write(alert_parameters)

    elif (actual_count != 0) and (actual_count != None): # Active alarm already exists. Check current count and increase it by 1
	actual_count += 1

	# Update existing DB record with new count
	query = "UPDATE transfer_alerts SET expected_count = '{0}', actual_count = {1}, project_endtime = '{2}' WHERE cemli_id = '{3}' \
	    AND location = '{4}' AND frequency = '{5}' AND active = 1".format(expected_count, actual_count, project_endtime, cemli_id, location, frequency)
	try:
	    db_cursor.execute(query)
	    db_connection.commit()
	except Exception as e:
	    db_connection.rollback
	    log_message = 'insert_transfer_alerts: error updating existing alarm counter: {0}\n'.format(e)
	    log_writer(log_message)
	    close_db_connection(db_connection, db_cursor)
	    sys.exit(1)

def set_processed_flag(db_connection, db_cursor, active_ctm_file): # Set processed flag to 'Y' for current CTM log file
    try: # Set processed_flag to 'Y' for current_file
	db_cursor.execute('UPDATE ctm_files SET processed_flag = "Y" where file_name = "%s"' % (active_ctm_file))
	db_connection.commit()
    except Exception as e:
	db_connection.rollback()
	close_db_connection(db_connection, db_cursor)
	log_message = 'MySQL: Update processed_flag in ctm_files failed: {0}\n'.format(str(e))
	log_writer(log_message)
	sys.exit(1)

def process_log_data(db_connection, db_cursor, critical_job_list, goa_file_list, cemli_dict_buffer): # Process CTM job data from GoA log file
    #------------------ Variables --------------------#
    debug_flag = 0
    debug_list = []
    oscompstat = None # Set to NULL by default
    job_status_list = ['ENDED OK', 'ENDED NOTOK'] # Active CTM Job status for monitoring
    #--------------- End Variables -------------------#

    # Get list of unprocessed CTM Log files
    # ctm_file_list = complete sorted directory listing of unprocessed CTM Log files
    ctm_file_list = get_ctm_file_list(db_connection, db_cursor)

    #----- Debug -----#
    if debug_flag == 1:
	debug_list.append('process_log_data -> ctm_file_list: ctm_file_list fetched successfully')
	debug_writer(debug_list, 0)

    # Iterate through list of unprocessed files
    for file_item in ctm_file_list:
	# Read all lines from file into input_buffer object
	active_ctm_file = ''.join(file_item) # Store actual file name of current ctm file without full path
	current_file = path_ctm + active_ctm_file
	f = open((current_file), 'r')
	input_buffer = f.readlines()
	f.close()

	# Fetch a formatted list of ctm jobs from the input_buffer (list of lists) for a single CTM Log file
	# ctm_job_list = [job_time, job_name, order_id, job_status, oscompstat]
	ctm_job_list = process_ctm_input_buffer(input_buffer, logmon_conditions_list, exclude_job_list, file_item)

	# Debug
	if debug_flag == 1:
	    exit_code = 0
	    debug_list = []
	    debug_list.append('process_log_data: base ctm_job_list fetched OK.\n')

	# Iterate through the list of Job Data and retreive General and GoA datapoints
	'''
	General data points to retrieve:
	1. Process Name - use process name dictionary in a CSV file
	2. Location - use location dictionary CSV file
	
	GoA data points to retrieve:
	1. CEMLI ID - parse from the GoA log filename
	2. CEMLI Title - use CEMLI dictionary CSV file
	3. GoA messages in consolidated form for job_data table
	4. Error messages for job_data table
	'''
	for job_list in ctm_job_list:
	    # 1. Reset flag for transfer_data table writer
	    # 2. Modify list and insert process_name, location, oscompstat_message
	    # full_job_list = [mysql_job_time, job_name, order_id, process_name, location, job_status, oscompstat, oscompstat_message]
	    full_job_list = update_ctm_job_list(job_list)
	    #-------- Debug -------#
	    if debug_flag == 1:
		exit_code = 0
		debug_list.append('process_log_data: full_job_list updated with process_name = {0}, location = {1}, \
		oscompstat_message = {2}.\n'.format(full_job_list[3], full_job_list[4], full_job_list[7]))

	    # 3. Retreive CEMLI data and ID GoA Log file for job
	    job_name = full_job_list[1]
	    order_id = full_job_list[2]
	    mysql_timestamp = full_job_list[0]
	    process_name = full_job_list[3]
	    location = full_job_list[4]
	    job_status = full_job_list[5]
	    oscompstat = full_job_list[6]
	    oscompstat_message = full_job_list[7]
	    cemli_id_str, cemli_title, schedule_name, dashboard_active = get_cemli_data(job_name)

	    # 4. Identify job_type (CTM/GoA) and identify GoA Log file if applicable
	    goa_log_file, job_type = id_goa_log(job_name, order_id, cemli_id_str, goa_file_list)
	    #-------- Debug -------#
	    if debug_flag == 1:
		debug_list.append('process_log_data: goa_file = {0}, job_type = {1}.\n'.format(goa_log_file, job_type))

	    # General Alarm on job_status = ENDED NOTOK
	    if job_status == 'ENDED NOTOK':
		alert_text = 'Job Name: {0}, Order ID: {1}, Job Status: {2}, Oscompstat: {3}, Oscompstat message: {4}'.format(job_name, order_id, job_status, oscompstat, oscompstat_message)
		eoc_note = eoc_note_processor(job_name, job_status, 'n/a', critical_job_list)
		alert_list = [1, mysql_timestamp, alert_text, eoc_note]
		insert_general_alerts(db_connection, db_cursor, alert_list, goa_log_file)

	    #-------- Debug -------#
	    if debug_flag == 1:
		debug_list.append('process_log_data: CEMLI data retreived, cemli_id = {0}, cemli_title = {1}, \
		schedule_name = {2}.\n'.format(cemli_id_str, cemli_title, schedule_name))

	    # 5. Retreive GoA data for GoA jobs. Get Location and PS data if dashboard_active = Yes. Execute DB write
	    if job_type == 'GOA':
		goa_data_list = process_goa_data(goa_log_file, dashboard_active, cemli_id_str)
		if goa_data_list: # List is not NULL
		    if dashboard_active == 'Yes': # Current Job has to be written to transfer_data table
			# goa_data_list = [goa_user_id(0), ps_user_id(1), loc_to_pcssc_download_timestamp(2), pcssc_to_ps_upload_timestamp(3), loc_to_pcssc_download_status(4), loc_to_pcssc_decryption_status(5), \
			# pcssc_to_ps_upload_status(6), completion_time(7), message_buffer(8), goa_generic_messages(9), goa_error_buffer(10)]
			# Format job_data_list 
			goa_message_buffer = goa_data_list[9]
			goa_error_buffer = goa_data_list[10]
			job_data_list = [mysql_timestamp, job_type, file_item, goa_log_file, job_name, \
			    order_id, process_name, location, cemli_id_str, cemli_title, job_status, oscompstat, \
			    oscompstat_message, goa_message_buffer, goa_error_buffer]
			# Assign temp status to PS Data. process_ps_data will update upon OMCS completion
			ps_process_timestamp = None
			ps_process_status = 'NULL'
			ps_error_buffer = 'NULL'
			# Check ontime status. project_endtime = completion_time
			completion_time = goa_data_list[7] # GoA successful end time
			ontime_status, frequency = get_ontime_status(db_connection, db_cursor, cemli_id_str, location, completion_time, cemli_dict_buffer)
			# Format transfer_data_list
			goa_user_id = goa_data_list[0]
			ps_user_id = goa_data_list[1]
			# transfer_data_list = [active_ctm_file, goa_file, location, job_name, cemli_id, cemli_title, frequency, goa_user_id, ps_user_id, \
			    # loc_to_pcssc_download_timestamp, loc_to_pcssc_download_status, loc_to_pcssc_decryption_status, \
			    # pcssc_to_ps_upload_timestamp, pcssc_to_ps_upload_status, project_endtime, ontime_status, ps_process_timestamp, \
			    # ps_process_status, message_buffer, goa_error_buffer, ps_error_buffer]
			transfer_data_list = [active_ctm_file, goa_log_file, location, job_name, cemli_id_str, cemli_title, frequency, goa_user_id, ps_user_id, \
			    goa_data_list[2], goa_data_list[4], goa_data_list[5], goa_data_list[3], goa_data_list[6], completion_time, ontime_status, \
			    ps_process_timestamp, ps_process_status, goa_data_list[8], goa_error_buffer, ps_error_buffer]

			# Format alarm_list to send to transfer_alerts
			# Identify consolidated status (using 3 success points)
			# goa_data_list[4] = loc_to_pcssc_download_status, goa_data_list[5] = loc_to_pcssc_decryption_status, goa_data_list[6] = pcssc_to_ps_upload_status
			status = 'loc to pcssc download: {0}, pcssc decryption: {1}, pcssc to ps upload: {2}'.format(goa_data_list[4], goa_data_list[5], goa_data_list[6])
			eoc_note = eoc_note_processor(job_name, job_status, ontime_status, critical_job_list)
			alert_list = [1, completion_time, cemli_id_str, cemli_title, location, frequency, status, eoc_note]
			insert_transfer_alerts(db_connection, db_cursor, alert_list, cemli_dict_buffer)

			# Format job_data_list:
			job_data_list = [mysql_timestamp, job_type, file_item, goa_log_file, job_name, \
			    order_id, process_name, location, cemli_id_str, cemli_title, job_status, oscompstat, \
			    oscompstat_message, goa_data_list[9], goa_data_list[10]]

			# Send data to the DB writer functions
			insert_job_data(db_connection, db_cursor, job_data_list)
			insert_transfer_data(db_connection, db_cursor, transfer_data_list)
			#--------- Debug ---------#
			if debug_flag == 1:
			    debug_list.append('process_log_data: ontime_status = {0}, frequency = {1}.\n'.format(ontime_status, frequency))
		
		    
		    else: # Current Job is not a scheduled Interface batch transfer and only has generic data for job_data table
			# goa_data_list = [goa_generic_messages, goa_error_buffer]
			goa_message_buffer = goa_data_list[0]
			goa_error_buffer = goa_data_list[1]
			# Format list with job_data to write to DB
			# job_data_list = [db_connection, db_cursor, mysqltimestamp, source(CTM/GoA), active_ctm_file, active_goa_file, \
			    # job_name, order_id, process_name, location, cemli_id, cemli_title, job_status, oscompstat_value, \
			    # oscompstat_message, goa_message_buffer, goa_error_buffer]
			job_data_list = [mysql_timestamp, job_type, file_item, goa_log_file, job_name, \
			    order_id, process_name, location, cemli_id_str, cemli_title, job_status, oscompstat, \
			    oscompstat_message, goa_message_buffer, goa_error_buffer]

			# Send data to the DB writer function
			insert_job_data(db_connection, db_cursor, job_data_list)
		else: # Invalid GoA file case
		    job_data_list = [mysql_timestamp, job_type, file_item, goa_log_file, job_name, \
			order_id, process_name, location, cemli_id_str, cemli_title, job_status, oscompstat, \
			oscompstat_message, 'Invalid GoA log file', 'Error processing GoA data - check source file']
		    insert_job_data(db_connection, db_cursor, job_data_list)

	# Set processed flag to 'Y' for current CTM flile
	set_processed_flag(db_connection, db_cursor, active_ctm_file)

def main():
    while True:
	# Read main config file
	logmon_conditions_list, scan_interval, critical_job_list, cemli_chart_list, read_conf_exit_code = read_conf()

	# Check error codes from read_conf function
	if read_conf_exit_code == 1:
	    sys.exit(1) # The log is already written in read_conf function, no need to do it here

	# Fetch CEMLI dictionary buffer to use in the cycle
	cemli_dict_buffer = get_cemli_dict_buffer()

	# Open database connection to ucpc_interface database
	db_connection, db_cursor = open_db_connection()
		
	#-------------------- Run Main Sequence --------------------#
	delete_files() # Deletes empty files from log dir	
	goa_file_list = sort_log_dir() # Sort ctm and goa files into their respective directories, get goa_file_list
	sync_ctm_files(db_connection, db_cursor) # Insert unique ctm file names into ctm_files table and remove file that are no longer in the dir
	process_log_data(db_connection, db_cursor, critical_job_list, goa_file_list, cemli_dict_buffer) # Inject data from GoA logs into ucpc_interface db. Attach GoA data within function
	due_schedule_list = check_schedule() # Retreive list of schedules due today
	check_late_status(db_connection, db_cursor, due_schedule_list, cemli_dict_buffer)# Check if there are jobs behind current set schedules
	calendar_sync(db_connection, db_cursor, cemli_dict_buffer) # Truncate and rebuild calendar table based on current state of events
	deactivate_alerts(db_connection, db_cursor) # Deactivate alerts older than 1 week (Set 'active' flag to 0)
	truncate_datagrid_transfers(db_connection, db_cursor) # Truncate datagrid_transfers table before Re-Sync
	datagrid_transfers_sync(db_connection, db_cursor, cemli_dict_buffer) # Rebuild the datagrid_transfers table - the main transfers dashboard view
	deactivate_general_alerts(db_connection, db_cursor) # Deactivate alarms older than 1 week
	cemli_chart_sync(db_connection, db_cursor, cemli_chart_list) # Sync cemli_chart DB table with updated data
	logmon_sync(db_connection, db_cursor, critical_job_list) # Syncronize logmon file with active alerts
	# Close db connection until the next cycle
	close_db_connection(db_connection, db_cursor)
	# Free variables by setting them to <None>
	cemli_dict_buffer = None

	# Sleep for configured number of minutes
	time.sleep(scan_interval * 60) # Comment this line out for testing or debugging
	#sys.exit(0)
	
if __name__ == '__main__':
    main()
