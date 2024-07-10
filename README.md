'''
Created on 2024-07-09
Created by Puvana Lakshmi Murugesan

Project: File Validation and Data cleansing framework using Python.

Description: This module is triggered considering the required files to be processed are already copied to the unix/windows path from HDFS/API/DropZone.
The previous module pulls these data and footer files from the source which has create time greater than the last pull time.
The last pull date can be saved in a variable in Airflow.
Create bad_files, Archival folders

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
