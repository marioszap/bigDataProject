import csv 
from bigDataConnect import session
import time
from datetime import timedelta


with open ('movie.csv','r',encoding='utf-8') as csv_file:
    csv_reader = list(csv.reader(csv_file, quoting = csv.QUOTE_ALL, delimiter =','))

    st = time.time()
    print('Forming query...')

    counter = 0
    moviesFinal = []
    moviesInsert = 'BEGIN BATCH '
    for i in range(len(csv_reader)):
        
            if i == 0:
                continue

            genres = csv_reader[i][2].split('|')
            genreString = '{'
            for j in range(len(genres)):
                genreString += f"'{genres[j]}',"
            genreString = genreString[:-1]
            genreString += '}'
            

            moviesInsert += f"INSERT INTO mm.movie_by_title(title, movieId, genres) VALUES ($${csv_reader[i][1]}$$,{csv_reader[i][0]},{genreString});"

            counter+= 1

            if len(moviesInsert.encode('utf-8')) > 655000:
                moviesInsert += 'APPLY BATCH;'
                moviesFinal.append(moviesInsert)
                moviesInsert = 'BEGIN BATCH '


    moviesInsert += 'APPLY BATCH;'
    moviesFinal.append(moviesInsert)
    et = time.time()

print(counter)

print (f'Forming query: Done. Time required: {str(timedelta(seconds = et - st))}')
input("Press enter to start uploading")
print('Upload in progress...')

st = time.time()

for i in moviesFinal:
    session.execute(i)

et = time.time()

print(f'Upload: Done. Time required: {str(timedelta(seconds = et - st))}')