from connection import session
import csv
import time
from datetime import timedelta

dateToQuery = '2015-1-1 00:00'
dateToQuery2 = '2015-1-15 23:59'

x = session.execute(f"SELECT movieId, AVG(rating) FROM ratings_by_date WHERE time_of_rating > '{dateToQuery}' AND time_of_rating < '{dateToQuery2}' GROUP BY movieId ALLOW FILTERING;")
y = []
print('Query = INS --> [movies_by_rating_set_date]')
print('Forming query...')
st = time.time()

for line in x:
    y.append([line.movieid, float("{:.2f}".format(line.system_avg_rating))])

csv_file = open('movie.csv', 'r', encoding='utf-8')
csv_reader = csv.reader(csv_file, delimiter=',')

y_titles = []
for line in csv_reader:
    try:
        y_titles.append([int(line[0]), line[1]])
    except:
        pass

query = "BEGIN BATCH "

for l in range(len(y)):
    for k in range(len(y_titles)):
        if y[l][0] == y_titles[k][0]:
            query += f"""INSERT INTO movie_by_rating_set_date(movieid, title,  rating_set_date) VALUES
                    ({y_titles[k][0]}, $${y_titles[k][1]}$$, {y[l][1]});"""
            continue
query += "APPLY BATCH;"
et = time.time()

print(f'Forming query: Done. Time required: {str(timedelta(seconds = et - st))}')
print('Upload in progress...')
st = time.time()
session.execute(query)
et = time.time()
print(f'Upload: Done. Time required: {str(timedelta(seconds = et - st))}')

