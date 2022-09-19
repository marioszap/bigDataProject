import csv 
from bigDataConnect import session, cons_level
import time
from datetime import timedelta



with open ('movie.csv','r',encoding='utf-8') as csv_file:
    csv_reader = list(csv.reader(csv_file, quoting = csv.QUOTE_ALL, delimiter =','))


    
    st = time.time()
    print('Forming query...')

    counter = 0;
    moviesFinal = []
    moviesInsert = 'BEGIN BATCH '
    for i in range(len(csv_reader)):
        
        if i == 0:
            continue
        
        genres = csv_reader[i][2].split('|')

        try:
            year = int(csv_reader[i][1].rsplit('(', 1)[1][0:4])
            title = csv_reader[i][1].rsplit('(', 1)[0]
        except:
            year = '0' #no year specified in movie's title
            title = csv_reader[i][1]

        avgRating = session.execute(f"SELECT AVG(rating) FROM ratings_by_date WHERE movieId = {csv_reader[i][0]}  ALLOW FILTERING;")
        for k in avgRating:
           avgRating = k.system_avg_rating

        for j in range(len(genres)):
            moviesInsert += f"INSERT INTO mm.movie_by_genre(genre, title, movieId, year, avgRating ) VALUES ($${genres[j]}$$, $${title}$$,{csv_reader[i][0]}, {year}, {avgRating});"
            counter += 1

            if len(moviesInsert.encode('utf-8')) > 655000:
                moviesInsert += 'APPLY BATCH;'
                moviesFinal.append(moviesInsert)
                moviesInsert = 'BEGIN BATCH '



    moviesInsert += 'APPLY BATCH;'
    moviesFinal.append(moviesInsert)
    et = time.time()

#print(moviesInsert)
print(counter)

print (f'Forming query(INSERT INTO mm.movie_by_genre): Done. Time required: {str(timedelta(seconds = et - st))}')
input("Press enter to start uploading")
print('Upload in progress...')

st = time.time()

for i in moviesFinal:
    session.execute(i)

et = time.time()

print(f'Upload: Done. Time required: {str(timedelta(seconds = et - st))}')






#SELECT title, tag, avgRating FROM movie_info WHERE tag CONTAINS 'comedy'   LIMIT 20;

