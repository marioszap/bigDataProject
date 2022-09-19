import csv
from connection import session, cons_level
from cassandra.query import BatchStatement
import time
from datetime import timedelta

print("Query = INS -> [movie_info]")
print("Forming query...")
st = time.time()
with open ('movie.csv', 'r', encoding='utf-8') as csv_file:
    csv_reader = list(csv.reader(csv_file, quoting = csv.QUOTE_ALL, delimiter = ','))

insert_movieInfo = session.prepare("INSERT INTO movie_info (movieid,title,avgrating,genres,tag,year) VALUES (?, ?, ?, ?, ?, ?)")
batch = BatchStatement()

for i in range(len(csv_reader)):
    if i == 0:
        continue
    genres = set(csv_reader[i][2].split('|'))
    try:
        year = int(csv_reader[i][1].rsplit('(', 1)[1][0:4])       
        title = csv_reader[i][1].rsplit('(', 1)[0][0:4].replace("'", "''")#title
    except:
        title = csv_reader[i][1].replace("'", "''")
        year = None
    id = int(csv_reader[i][0])#id
    x = session.execute(f'SELECT AVG(rating) FROM ratings_by_date WHERE movieId = {int(csv_reader[i][0])};')
    for j in x:
        avg = j.system_avg_rating
    x = session.execute(f"SELECT tag FROM tags WHERE movieId = {id} LIMIT 5")
    tags = set()
    for n in x:
        tags.add(n.tag.replace("'", "''"))
    batch.add(insert_movieInfo, (id, title, avg, genres, tags, year))
et = time.time()

print (f'Forming query: Done. Time required: {str(timedelta(seconds = et - st))}')
print('Uploading...')
session.execute(batch)
print(f'Upload: Done. Time required: {str(timedelta(seconds = et - st))}')
