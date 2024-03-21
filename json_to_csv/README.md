#Data Filter

#Getting Started
##Description

Data Filter is a python program aims to filter IoT data and generate CSV files to be inserted in the database.

##Folders
config - inside is the local.env that contains the DB config for the device map.
converted - this is where the successful filtered gz files go
data_errors - this is where the errored payload go
errored - this is where the errored gz files go
outs - this is where the final CSV output go
temp - this is where the temporary storage of CSV file
to_be_processed - put here the .gz file you waant to process
ymls - contains the key values of every application

#Program Flow
to_be_processed --> temp --> outs --> converted
                         --> data_errors
                                  --> errored

#To Start
1. Put the .gz files you want to process in to_be_processed folder
2. Run threaded.py
3. Get the CSV outputs in outs folder once finished
4. To reprocess payloads in errored data folder, gzip the files you want to process and move it to the to_be_processed folder and proceed to step 2
5. To reprocess gz files in errored folder, just move them to to_be_processed folder and proceed to step 2

Note: If you already added the processed CSV files in outs folder, remove or move them to different folder before processing new gz files. This is to avoid appending the new data to the old ones.  
