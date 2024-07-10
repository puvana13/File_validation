'''
Created on 2024-07-09
Created by Puvana Lakshmi Murugesan

Project: File Validation and Data cleansing framework using Python.

Description: This module is triggered considering the required files to be processed are already copied to the unix/windows path from HDFS/API/DropZone.
The previous module pulls these data and footer files from the source which has create time greater than the last pulled time. The last pull date can be saved in a variable in Airflow.

Dev Notes:
1.Consider parameterizing the file path, file extension and file pattern

Explanation:
1. Searches multiple files for different year present in a given path using wildcard characters.
2. Checks if the count in the file matches with the count present in the counter/footer file.
    If count matches , it is loaded to dataframe for data validation.
    Else the file is not processed. The file names are collected and prompted to the user.
3. Concatenates all the files into a single dataframe for data validation.
4. Below Data cleansing operations are performed.
    1. Converting Net worth to proper float type (Ex: $46.0 billion converted to 46.0)
    2. If Age is not present , defaulting it to 99 (Ex: Age is missing for few families. Defaulted to 99)
    3. If Age is > 99 , collect those bad records in a dataframes (Ex: Age is validated)
    4. If networth is not a practical number, then collect those bad records in a dataframe (Ex: Records which have networth unimaginable number is collected. Here 'greater than 99' for just processing)
    5. Combine all the bad records and save it as a csv file with proper reason for rejection (Ex: Rejected records are stored in a file with a reason)
    6. Eliminate the bad records from the dataframes and save it as a file (Ex: The bad records are removed from the dataframes which is ready for processing)
5. To mark the file as processing completed. The processed files are not picked for next run.
    There are multiple ways. But tried the below one.
    Move the data file to an archival folder after compressing and footer file without compressing
        If only data file is received , the file should not be moved to the archival
        If both data file and footer file are present, but footer is incorrect , then move the footer file to the archival
        If both data file and footer file are present and good for processing , move both the file to archival
    All the files are appended with a timestamp. This is to identify incase we receive multiple files with the same name when there is file count mismatch/footer file missing
6. Purging the files which are greater than 30 days. (Here used 20 minutes for testing)

Future Ideas:
1. Option for re-processing a bad record file. No need of footer for bad records file. -- Yet to be done
2. Have the audit in a database to record the files and number of records loaded.
3. Count mismatch files to be loaded in audit table with reason for not processing.
4. Dataframe having records loaded from bad file / data file.
5. E-mail trigger that file is processed / bad record / unprocessed files.
Audit table:
Audit_number , processed_date , file_name , number of records expected, number of records in file, number of records processed, number of bad records, is_bad_file, is_processed, comments, timestamp

'''

#Importing the required packages

import sys
import pandas as pd
import glob
import shutil as sh
from datetime import datetime
import gzip
import os
import time

######################### Function to compress the input files ####################################
def create_gz_file(input_file_path, output_gz_path):
    # Open the input file in read binary mode
    with open(input_file_path, 'rb') as f_in:
        # Open the output file in write binary mode with gzip compression
        with gzip.open(output_gz_path, 'wb') as f_out:
            # Copy the content of the input file to the output file
            sh.copyfileobj(f_in, f_out)
    print(f"File {input_file_path} has been compressed to {output_gz_path}")

####################### Reading files one by one based on the wildcard using glob ####################
file_path='C:/Users/Arun/Desktop/Puvana/Project/'
file_name='Top_10_richest_person_in_the_world_????.csv'
file_pattern=file_path+file_name
files = glob.glob(file_pattern)

#Creating a dataframe list to append multiple dataframes (files)
dataframes=[]

#Creating a list to save the files have mismatch of footer
footer_mismatch=[]

#Creating a list to save the files which doesnt have a footer
footer_missing=[]

for file in files:
    # Reading the file - Handled quoted characters
    print(f"File Name is {file}")
    df = pd.read_csv(file
                     , doublequote=True
                     , names=['Index', 'Name', 'Net_worth_USD', 'Age', 'Nationality', 'Primary_source_of_wealth']
                     , index_col='Index'
                     , header=0)

    ######################################### Count check file ############################################
    # Reading the footer files from the path
    file_pattern_footer = file[-5::-1][::-1] + '_footer.csv'
    print(f"Footer file name is {file_pattern_footer}")

    if not os.path.exists(file_pattern_footer):
        print(f"File {file_pattern_footer} does not exists.")
        footer_missing.append(file)
        print("File appended to mismatch list")

    else:
        print(f"File {file_pattern_footer} exist.")
        footer_df=pd.read_csv(file_pattern_footer
                          ,sep='|'
                          ,header=None)

        expected_no_of_rec=footer_df.iloc[0][1]
        print(f"Expected number of records {expected_no_of_rec}")
        no_of_rec_recieved=len(df)
        print(f"Contents in file number of records {no_of_rec_recieved}")

        # Moving footer file to archival
        current_timestamp = datetime.now()
        timestamp_str = current_timestamp.strftime("%Y%m%d%H%M%S")
        destination_file_footer = file_path + 'archieve/' + file[file.find('\\') + 1::][-5::-1][::-1] + '_footer_' + timestamp_str + '.csv'
        sh.move(file_pattern_footer, destination_file_footer)

        if expected_no_of_rec == no_of_rec_recieved:
            # Appending the dataframe to the list
            dataframes.append(df)
            print("Dataframes appended to the dataframe list for processing")
            # Get the year from the file name - To know from which file the data has been loaded
            year = file[-5:-9:-1][::-1]
            df['Year'] = year
            # Moving the data file to archival folder
            current_timestamp = datetime.now()
            timestamp_str = current_timestamp.strftime("%Y%m%d%H%M%S")
            gz_destination_file = file_path + 'archieve/' + file[
                                                            file.find('\\') + 1:-4:] + '_' + timestamp_str + '.csv.gz'
            create_gz_file(file, gz_destination_file)
            os.remove(file)

        else:
            footer_mismatch.append(file)
            print("File appended to mismatch list")



########################### Concatenating all the dataframes into a single dataframe ###########################
if len(files) > 1:
    final_df = pd.concat(dataframes)
    print("Multiple file concat done")
elif len(dataframes) == 1:
    final_df=dataframes[0]
    print("Single file only present. Concat not needed")
elif len(dataframes) == 0:
    print("No files present for processing")
    sys.exit(1)

print(f"Length of the final dataframe {len(final_df)}")

############################# Footer mismatch files #####################################
print('Below files are not processed due to footer count mismatch')
for item in footer_mismatch:
    print(item)

print('Below files are not processed as footer file is missing')
for item in footer_missing:
    print(item)

########################################## Data cleansing ##############################################

# 1. Converting billion to numeric field

final_df['Net_worth_USD_in_billion']=final_df['Net_worth_USD'].str.replace("billion","").str.replace("$",'').astype(float)

# 2. First Name and Last Name

final_df['First_Name']=final_df["Name"].str.split(n=1).apply(lambda x:x[0])
final_df['Last_Name']=final_df["Name"].str.split(n=1).apply(lambda x:x[1])
#print(final_df[["Name","First_Name","Last_Name"]])

# 3. Primary source of Wealth - convert as an array

final_df['Primary_source_of_wealth_arr'] = final_df["Primary_source_of_wealth"].str.split(",")
#print(final_df[['Primary_source_of_wealth','Primary_source_of_wealth_arr']])

# 4. Converting Age to int after replacing '_' with '0'

mask=final_df['Age'] == '_'
final_df.loc[mask,'Age']='0'
final_df['Age']=final_df['Age'].astype(int)
final_df['Age_check']=final_df['Age'].astype(str).str.len() #Tried this for future use when there is a case to check the number of digits in any columns ex: account number

#####################################Identify Bad records################################################

#1.Bad records in Age to check if age is in three digits. Stores as a dataframe with reason
bad_age_df=final_df[(final_df["Age"].astype(int) > 99)]
bad_age_df["Reason"]="Age is greater than 99"
#print(bad_df)

#2.Bad records in Net worth to check if Amount is greater than a particular number. Stores as a dataframe with reason
bad_net_worth_df=final_df[(final_df["Net_worth_USD_in_billion"] > 99)]
bad_net_worth_df["Reason"]="Amount is not right"
#print(bad_net_worth_df)

bad_df = pd.concat([bad_age_df, bad_net_worth_df])
print(f"Number of bad records {len(bad_df)} ")

#Save the bad record file
current_timestamp = datetime.now()
timestamp_str = current_timestamp.strftime("%Y%m%d%H%M%S")
bad_file_name='C:/Users/Arun/Desktop/Puvana/Project/bad_files/Top_10_richest_person_in_the_world_'+timestamp_str+'_bad_rcd.csv'
bad_df.to_csv(bad_file_name, index=False)
print("Bad Records saved")

################################## Collect all the filtered correct dataframe #####################################
# To get only the correct age records and store the bad records in a seperate Dataframe
correct_age_df=final_df[~(final_df["Age"].astype(int) > 99)]
print(len(correct_age_df))

# To get only the correct age records and store the bad records in a seperate Dataframe
correct_net_worth_df=correct_age_df[~(correct_age_df["Net_worth_USD_in_billion"] > 99)]
print(len(correct_net_worth_df))

# Loading this file to a database of saving it as a file
# Yet to code

############################## Purging the older files #####################################################

directory = 'C:/Users/Arun/Desktop/Puvana/Project/archieve'

# Get the current time
current_time = time.time()

# Calculate the time 20 minutes ago
time_threshold = current_time - (20 * 60)  # 20 minutes * 60 seconds per minute

# List to store files accessed in the last 20 minutes
delete_files = []

# Iterate through files in the directory
for filename in os.listdir(directory):
    filepath = os.path.join(directory, filename)
    if os.path.isfile(filepath):
        # Get the last access time of the file
        last_access_time = os.path.getatime(filepath)
        # Check if the file was accessed in the last 20 minutes
        if last_access_time <= time_threshold:
            delete_files.append(filename)
            os.remove(filepath)

# Print the list of recently accessed files
print("Files deleted :")
for file in delete_files:
    print(file)













