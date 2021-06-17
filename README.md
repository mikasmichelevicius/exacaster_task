# Exacaster homework task

The repository contains several programs that were used to design the initial data structure for a Data Warehouse and perform the initial database population of given data in usage.csv file.

## EDA.ibynb

The jupyter notebook was used to perform some minimal data analysis, to better understand the data that is used. The following document shows the insights, thought process of the task completion. It discuss some questions that were asked within the task, as well as argues some of the design decisions made during the task execution.

## populate.py

This file contains a Python script to process the given data and execute the initial population to the database. MySQL database was used to to perform this task. The script, if does not exist, creates a database called 'telcoDWH', with three tables in the database - 'usage_data', 'type_distribution' and 'rate_distribution'. Using Pandas, the data is handled, necessary information is extracted and uploaded to the mentioned tables.

If the database was already created, the data is not updated again as the script performs only the initial data population.

## resample.py

Due to the large usage.csv file and limited computational resources, resample.py script was used to downsize the provided data. This script produces usage_downsized.csv file which is randomly generated from usage.csv file and it is later used by populate.py script. 
