# Data Modeling Project for Sparkify
Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. Currently, there is no easy way to query the data to generate the results, since the data reside in a directory of CSV files on user activity on the app. I created an Apache Cassandra database to be queried.

To successfully complete this project, I intend to provide the following deliverables:
1. Read all CSV files from the directory and combine their data into one big list in memory.
2. Write that combined data into a new CSV file, but only keep certain columns (necessary ones).

# Extract, Transform
This stage involves collecting all the necessary CSV files from the directory. First, an understanding of the naming convention would save us time. We can do this by quickly checking the directory manually. Next, print out the columns; this step will provide a bird's eye view of the dataset.

<img width="1074" height="482" alt="data summary 2" src="https://github.com/user-attachments/assets/f59d663d-18fc-41ed-aa9c-b92b50589273" />

The Extract/Transform steps are documented in the ETL file.

# Load (Final Stage)
After extraction and transformation, the new file has the following columns:
1. artist
2. firstName of user
3. gender of user
4. item number in session
5. last name of user
6. length of the song
7. level (paid or free song)
8. location of the user
9. sessionId
10. song title
11. userId

Here is a snippet of the final dataset:
![ETL output](https://github.com/user-attachments/assets/7a2e2005-3c24-4d75-84d8-ed0dcadc0bff)

This data needs to be loaded to an Apache Cassandra database. Below is a quick process on how to do so (code available in the "Populate Cassandra" file):
1. create a cluster
2. create a keyspace
3. Set keyspace
4. Create table for Query #1
Output: <img width="438" height="48" alt="query 1" src="https://github.com/user-attachments/assets/99cc403e-2ede-41d0-8fda-8887eae6c5d1" />

5. Create table for Query #2
Output: <img width="658" height="86" alt="query 2" src="https://github.com/user-attachments/assets/4fbef7fb-4563-483a-8cd9-67a650b34cfc" />

6. Create table for Query #3
Output: <img width="157" height="66" alt="query 3" src="https://github.com/user-attachments/assets/f37e07af-db27-4d41-81fc-a836da2f9660" />
