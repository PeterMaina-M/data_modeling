# creating a Cluster
# Make a connection to a Cassandra instance your local machine (127.0.0.1)
import cassandra
import csv
from cassandra.cluster import Cluster

cluster = Cluster([('127.0.0.1')])

# begin a session
session = cluster.connect()

# Create a Keyspace (music_data)
session.execute("""
    CREATE KEYSPACE IF NOT EXISTS music_data
    WITH REPLICATION = 
    {'class': 'SimpleStrategy', 'replication_factor': 1}"""
               )

# Set KEYSPACE to the keyspace specified above
session.set_keyspace('music_data')

## Query 1:  Create table with the artist, song title and song's length in the music app history that was heard during \
## sessionId = 338, and itemInSession = 4
table = "CREATE TABLE IF NOT EXISTS music_history_by_session_item"
table = table + "(session_id int, item_in_session int, artist text, song_title text, song_length decimal, PRIMARY KEY (session_id, item_in_session))"
session.execute(table)

file = 'event_datafile_new.csv'

with open(file, encoding = 'utf8') as f:
    csvreader = csv.reader(f)
    next(csvreader) # skip header
    for line in csvreader:
        ## Assign the INSERT statements into the `query` variable
        query = "INSERT INTO music_history_by_session_item (session_id, item_in_session, artist, song_title, song_length)"
        query = query + "VALUES (%s, %s, %s, %s, %s)"
        
        ## Assign which column element should be assigned for each column in the INSERT statement.
        session.execute(query, (int(line[8]), int(line[3]), line[0], line[9], float(line[5])))


## Add in the SELECT statement to verify the data was entered into the table
query = "SELECT artist, song_title, song_length FROM music_history_by_session_item WHERE session_id = 338 and item_in_session = 4"
rows = session.execute(query)

## Print output with labels
for row in rows:
    print(row.artist + "  " + str(row.song_title) + "  " + str(row.song_length))

## Query 2: Give me only the following: name of artist, song (sorted by itemInSession) and user (first and last name)\
## for userid = 10, sessionid = 182

table2 = "CREATE TABLE IF NOT EXISTS user_by_session"
table2 = table2 + """
                    (user_id int, session_id int, item_in_session int, first_name text, last_name text, song_title text, artist text,
                    PRIMARY KEY ((user_id, session_id), item_in_session))
                    WITH CLUSTERING ORDER BY (item_in_session DESC);
                    """
session.execute(table2)

file = 'event_datafile_new.csv'

with open(file, encoding = 'utf8') as f:
    csvreader = csv.reader(f)
    next(csvreader) # skip header
    for line in csvreader:
        
        ## Assign the INSERT statements into the `query` variable
        query = "INSERT INTO user_by_session (user_id, session_id, item_in_session, first_name, last_name, song_title, artist)"
        query = query + "VALUES (%s, %s, %s, %s, %s, %s, %s)"
        
        ## Assign which column element should be assigned for each column in the INSERT statement.
        session.execute(query, (int(line[10]), int(line[8]), int(line[3]), line[1], line[4], line[9], line[0]))
        
## Run the SELECT query and print out rows
query = "SELECT artist, song_title, first_name, last_name FROM user_by_session WHERE user_id = 10 and session_id = 182"
rows = session.execute(query)

for row in rows:
    print(row.artist + " | " + str(row.first_name) + " | " + str(row.last_name) + " | " + str(row.song_title))


## Query 3: Give me every user name (first and last) in my music app history who listened to the song 'All Hands Against His Own'

table3 = "CREATE TABLE IF NOT EXISTS users_by_song"
table3 = table3 + """
                    (song_title text, first_name text, last_name text,
                    PRIMARY KEY (song_title, first_name));
                    """
session.execute(table3)

file = 'event_datafile_new.csv'

with open(file, encoding = 'utf8') as f:
    csvreader = csv.reader(f)
    next(csvreader) # skip header
    for line in csvreader:
## TO-DO: Assign the INSERT statements into the `query` variable
        query = "INSERT INTO users_by_song (song_title, first_name, last_name)"
        query = query + "VALUES (%s, %s, %s)"
        ## Assign which column element should be assigned for each column in the INSERT statement.
        session.execute(query, (line[9], line[1], line[4]))
        

query = "SELECT first_name, last_name FROM users_by_song WHERE song_title = 'All Hands Against His Own'"
rows = session.execute(query)

for row in rows:
    print(row.first_name + " | " + str(row.last_name))


## Drop the table before closing out the sessions
session.execute("drop table music_history_by_session_item")
session.execute("drop table user_by_session")
session.execute("drop table users_by_song")

session.shutdown()
cluster.shutdown()
