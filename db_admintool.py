#!/usr/bin/python
# Description:	ucpc_interface DB administration tool for UCPC Processor
# Version:	3.00
# Author:	Alex Solokhin @ University of California 2018

'''
Release Notes v.300

--- The database has been renamed from ctmgoa to ucpc_interface
	1. table ctm_job_data renamed to job_data
	2. table scheduled_transfer_status renamed to transfer_data

--- Modified schema in job_data table
	1. Removed run_count from job_data
	2. Removed message_number from job_data
	3. Removed ctm_process from job_data
	4. Removed num_of_failures from job_data
	5. Added encryption_status in job_data
	6. Added scheduled_time in scheduled_transfer_status table

--- Added new table transfer_data to include more granular details for scheduled file transfers
	1. Timestamp and status for location to pcssc download
	2. Decryption status fpr pcssc download
	3. Timestamp and status for pcssc to PeopleSoft (PS) upload
	4. PCSSC ontime factor for jobs that have a scheduled date/time (e.g. I181)
	5. Placeholder VARs for PS Process timestamp and status.
'''

import getpass
import MySQLdb
import sys
from sys import stdout

#----------------------------------------- Global Variables --------------------------------------#
db_name = 'ucpc_interface'
db_user = 'nmsuser'
exit_message = ''
exit_code = 0
mysql_password = getpass.getpass('MySQL password for nmsuser: ')
sql_check_db_exists = "select count(*) from information_schema.SCHEMATA where SCHEMA_NAME = '%s';" % db_name
sql_create_db = 'create database ucpc_interface;'
sql_delete_db = 'drop database ucpc_interface;'

#-------------------------------------------- DB Schema ------------------------------------------#
tables = {}
tables['job_data'] = (
    "CREATE TABLE `job_data` ("
    " `record_number` INT(10) NOT NULL AUTO_INCREMENT,"
    " `time_stamp` DATETIME NOT NULL,"
    " `source` VARCHAR(128) NOT NULL,"
    " `ctm_log_file` VARCHAR(128) NOT NULL,"
    " `goa_log_file` VARCHAR(128) NULL,"
    " `job_name` VARCHAR(128) NOT NULL,"
    " `order_id` VARCHAR(128) NOT NULL,"
    " `process_name` VARCHAR(128) NULL,"
    " `location` VARCHAR(128) NULL,"
    " `cemli_id` VARCHAR(128) NULL,"
    " `cemli_title` VARCHAR(256) NULL,"
    " `job_status` VARCHAR(128) NOT NULL,"
    " `oscompstat` INT(3) NULL,"
    " `oscompstat_message` VARCHAR(256) NULL,"
    " `goa_messages` VARCHAR(8192) NULL,"
    " `error_buffer` VARCHAR(5120) NULL,"
    " PRIMARY KEY (`record_number`)"
    ") ENGINE=InnoDB")

tables['ctm_fIiles'] = (
    " CREATE TABLE `ctm_files` ("
    " `file_id` INT(5) NOT NULL AUTO_INCREMENT,"
    " `file_name` VARCHAR(128) NOT NULL,"
    " `processed_flag` VARCHAR(1) NOT NULL,"
    " PRIMARY KEY (`file_id`)"
    ") ENGINE=InnoDB")

tables['ps_files'] = (
    "CREATE TABLE `ps_files` ("
    " `file_id` INT AUTO_INCREMENT,"
    " `file_name` VARCHAR(128) NOT NULL,"
    " `processed_flag` INT(1) NOT NULL,"
    " PRIMARY KEY (`file_id`)"
    ") ENGINE=INNODB")

tables['transfer_data'] = (
    " create table `transfer_data` ("
    " `record_id` int(7) not null auto_increment,"
    " `ctm_log_file` varchar(128) not null,"
    " `goa_log_file` varchar(128) not null,"
    " `location` varchar(128) null,"
    " `job_name` varchar(128) not null,"
    " `cemli_id` varchar(128) not null,"
    " `cemli_title` varchar(256) null,"
    " `frequency` varchar(128) not null,"
    " `goa_user_id` varchar(128) not null,"
    " `ps_user_id` varchar(128) not null,"
    " `loc_to_pcssc_download_timestamp` datetime null,"
    " `loc_to_pcssc_download_status` varchar(128) not null,"
    " `loc_to_pcssc_decryption_status` varchar(128) not null,"
    " `pcssc_to_ps_upload_timestamp` datetime null,"
    " `pcssc_to_ps_upload_status` varchar(128) not null,"
    " `project_endtime` datetime not null,"
    " `ontime_status` varchar(128) not null,"
    " `ps_process_timestamp` datetime null,"
    " `ps_process_status` varchar(128) null,"
    " `message_buffer` varchar(5120) not null,"
    " `goa_error_buffer` varchar(5120) null,"
    " `ps_error_buffer` varchar(5120) null,"
    " primary key (`record_id`)"
    ") engine=InnoDB")

tables['datagrid_transfers'] = (
    " create table `datagrid_transfers` ("
    " `record_id` INT(6) NOT NULL AUTO_INCREMENT,"
    " `project_endtime` DATETIME NOT NULL,"
    " `expected_count` INT(4) NULL,"
    " `actual_count` INT(4) NULL,"
    " `frequency` VARCHAR(128) NOT NULL,"
    " `cemli_id` VARCHAR(128) NOT NULL,"
    " `cemli_title` VARCHAR(256) NOT NULL,"
    " `location` VARCHAR(128) NOT NULL,"
    " `schedule_name` VARCHAR(128) NOT NULL,"
    " `pcssc_download_status` VARCHAR(128) NOT NULL,"
    " `pcssc_decryption_status` VARCHAR(128) NOT NULL,"
    " `ps_upload_status` VARCHAR(128) NOT NULL," 
    " `ontime_status` VARCHAR(128) NOT NULL,"
    " `ps_process_status` VARCHAR(128) NOT NULL,"
    " `messages` INT(1) NOT NULL,"
    " `goa_error` INT(1) NOT NULL,"
    " `ps_error` INT(1) NOT NULL,"
    " PRIMARY KEY (`record_id`)"
    ") ENGINE=InnoDB")

tables['calendar'] = (
    " CREATE TABLE `calendar` ("
    " `record_id` INT(6) NOT NULL AUTO_INCREMENT,"
    " `date_start` DATETIME NOTNULL,"
    ' `date_end` DATETIME NOT NULL,'
    " `title` VARCHAR(1024) NOT NULL,"
    " PRIMARY KEY (`record_id`)"
    ") ENGINE=InnoDB")

tables['cemli_chart_data'] = (
    " CREATE TABLE `cemli_chart_data` ("
    " `record_id` INT AUTO_INCREMENT,"
    " `event_data` DATE NOT NULL,"
    " `location` VARCHAR(256) NULL,"
    " `cemli_list` VARCHAR(1024) NOT NULL,"
    " `event_type` VARCHAR(128) NOT NULL,"
    " `dataset` VARCHAR(2056) NOT NULL,"
    " PRIMARY KEY (`record_id`)"
    ") ENGINE=InnoDB")

tables['transfer_alerts'] = (
    " CREATE TABLE `transfer_alerts` ("
    " `record_id` INT(6) NOT NULL AUTO_INCREMENT,"
    " `active` INT(1) NOTNULL,"
    " `expected_count` INT(4) NULL,"
    " `actual_count` INT(3) NOT NULL,"
    " `project_endtime` DATETIME NOT NULL,"
    " `cemli_id` VARCHAR(128) NOT NULL,"
    " `cemli_title` VARCHAR(128) NOT NULL,"
    " `location` VARCHAR(128) NOT NULL,"
    " `frequency` VARCHAR(128) NOT NULL,"
    " `status` VARCHAR(1024) NOT NULL,"
    " `eoc_note` VARCHAR(2048) NOT NULL,"
    " PRIMARY KEY (`record_id`)"
    ") ENGINE=InnoDB")

tables['general_alerts'] = (
    " create table `general_alerts` ("
    " `record_id` int(6) not null auto_increment,"
    " `active` int(1) not null,"
    " `time_stamp` datetime not null,"
    " `alert_text` varchar(4096) not null,"
    " `eoc_note` varchar(2048) not null,"
    " primary key (`record_id`)"
    ") engine=InnoDB")

#-------------------------------------------- Functions ------------------------------------------#
def create_db():
    table_create_error_flag = 0
    try:
	stdout.write('Creating ucpc_interface database...\n')
	db_cursor.execute(sql_create_db)
    except Exception as e:
	error_message = 'Error creating ucpc_interface database: {0}\n'.format(e)
	exit_code = 1
    stdout.write('ucpc_interface has been successfully created.\n')
    db_cursor.execute('use ucpc_interface;')
    stdout.write('Creating database tables...\n')
    for name, ddl in tables.iteritems():
	try:
	    stdout.write('Creating table {0}...\n'.format(name))
	    db_cursor.execute(ddl)
	except Exception as e:
	    stdout.write('Error creating table {0}: {1}\n'.format(name, e))
	    table_create_error_flag = 1
    if table_create_error_flag == 0:
	exit_message = 'All ucpc_interface database tables created successfully.\n'
	exit_code = 0
    elif table_create_error_flag == 1:
	exit_message = 'Errors occured while creating ucpc_interface database tables. See /var/log/sqldlog for details.\n'
	exit_code = 1
	return exit_message, exit_code

    return exit_message, exit_code

def delete_db():
    try:
	stdout.write('Deleting ucpc_interface database and all of its contents...\n')
	db_cursor.execute(sql_delete_db)
	exit_message = 'Database ucpc_interface has been successfully deleted.\n'
	exit_code = 0	
    except Exception as e:
	exit_message = 'Error while deleting ucpc_interface: {0}\n'.format(e)
	exit_code = 1
	return exit_message, exit_code

    exit_message = 'Database ucpc_interface successfully deleted.\n'
    db_exists_flag = 0
    exit_code = 0

    return exit_message, exit_code

def delete_db_data():
    ctm_file_name = ''
    ctm_file_name = raw_input('Enter CTM Log file name: ')
    stdout.write('Deleting ucpc_interface db data associated with CTM Log {0}\n'.format(ctm_file_name))
   
    # Delete associated data from ctm_job_data table and change processed_flag in ctm_files table to 'N'
    try:
	db_cursor.execute("USE ucpc_interface")
	db_cursor.execute("DELETE FROM job_data WHERE ctm_log_file = '%s'" % (ctm_file_name))
	db_cursor.execute("DELETE FROM transfer_data WHERE ctm_log_file = '%s'" % (ctm_file_name))
	db_cursor.execute("UPDATE ctm_files SET processed_flag = 'N' WHERE file_name = '%s'" % (ctm_file_name))
	db_connection.commit()
	status_message = 'ucpc_interface data associated with CTM Log file {0} has been successfully deleted.'.format(ctm_file_name)
    except Exception as e:
	status_message = 'Error deleting ucpc_interface data: {0}'.format(e)
	return status_message

    return status_message
    

#--------------------------------------------- Main -----------------------------------------------#
# Connect to the database
try:
    db_connection = MySQLdb.connect('localhost', db_user, mysql_password)
    db_cursor = db_connection.cursor()
except Exception as e:
    stdout.write('Connection to database failed: {0}\n'.format(e))
    sys.exit(1)	
	
# Display list of options
status_message_flag = 0
while True:
    # Check if database exists
    stdout.write('Checking existing configuration...\n')
    db_cursor.execute(sql_check_db_exists)
    db_exists_flag = db_cursor.fetchone()[0]
    
    if status_message_flag == 1:
	status_message_flag = 0
    elif db_exists_flag == 1:
	status_message = 'ucpc_interface DB online.'
    else:
	status_message = 'ucpc_interface DB does not exist.'

    # Print options in terminal
    stdout.write('	#-------------------------------------------------------#\n')
    stdout.write('	System: {0}\n'.format(status_message))
    stdout.write('	#-------------------------------------------------------#\n')
    stdout.write('	1. Create ucpc_interface database and tables            |\n')
    stdout.write('	2. Backup ucpc_interface database	                |\n')
    stdout.write('	3. Restore ucpc_interface database from backup file     |\n')
    stdout.write('	4. Delete ucpc_interface data based on ctm_log_file     |\n')
    stdout.write('	5. Delete ucpc_interface database and schema            |\n')
    stdout.write('	6. Exit utility	                                        |\n')
    stdout.write('	#-------------------------------------------------------#\n')
    menu_choice = raw_input('Enter option: ')
    
    # Process menu input
    if menu_choice == '1':
	if db_exists_flag == 1:
	    exit_message = 'Database ucpc_interface already exists. The program will exit.\n'
	    exit_code = 0
	elif db_exists_flag == 0:
	    exit_message, exit_code = create_db()
	else:
	    exit_message = 'An unknown error occured. Exiting.\n'
	    exit_code = 1
    elif menu_choice == '2':
	exit_message = 'Option not yet available. Exiting. \n'
	exit_code = 0
    elif menu_choice == '3':
	exit_message = 'Option not yet available. Exiting. \n'
	exit_code = 0
    elif menu_choice == '4':
	if db_exists_flag == 1:
	    status_message = delete_db_data()
	    status_message_flag = 1
	elif db_exists_flag == 0:
	    exit_message = 'Cannot proceed. ucpc_interface DB does not exist. Exiting.\n'
	    exit_code = 0
	    break
    elif menu_choice == '5':
	the_choice = raw_input('The database and its contents will be permanently deleted. Are you sure (y/n)?: ')
	if the_choice == 'y':
	    if db_exists_flag == 1:
		exit_message, exit_code = delete_db()
	    elif db_exists_flag == 0:
		exit_message = 'Cannot proceed. Database ucpc_interface does not exist. Exiting.\n'
		exit_code = 0
		break
	elif the_choice == 'n':
		exit_message = 'Erroneous selection made, exiting...\n'
		exit_code = 0
		break
    elif menu_choice == '6':
	exit_message = 'Program terminated.\n'
	exit_code = 0
	break
    else:
	exit_message = 'Invalid option entered. Exiting.\n'
	exit_code = 0
	break

# Close db connection
db_cursor.close()
db_connection.close()
stdout.write(exit_message)
sys.exit(exit_code)
