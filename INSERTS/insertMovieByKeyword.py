from bigDataConnect import session, cons_level
import csv 
import time
from datetime import timedelta
from cassandra.query import SimpleStatement

mByTitle = "CREATE TABLE movie_by_keyword(keyword text, title text, movieId int, PRIMARY KEY(keyword,title));"
session.execute(mByTitle)



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
            
            titleStrings = csv_reader[i][1].split(' ')
            title = csv_reader[i][1].replace("'","''")
            
            for k in range(len(titleStrings)):

                keyword = titleStrings[k].replace("'","''")
        
                if keyword == '':
                    continue
                elif titleStrings[k][-1] == ')' and titleStrings[k][0] == '(':
                    continue
                #elif titleStrings[k][-1] == ')' and titleStrings[k][0] != '(':
                    #moviesInsert += f"INSERT INTO mm.movie_by_keyword(keyword, title, movieId) VALUES ($${titleStrings[k][0:-1]}$$,$${csv_reader[i][1]}$$,{csv_reader[i][0]});"
                #elif titleStrings[k][-1] != ')' and titleStrings[k][0] == '(':
                    #moviesInsert += f"INSERT INTO mm.movie_by_keyword(keyword, title, movieId) VALUES ($${titleStrings[k][1:]}$$,$${csv_reader[i][1]}$$,{csv_reader[i][0]});"
                else:
                    moviesInsert += f"INSERT INTO mm.movie_by_keyword(keyword, title, movieId) VALUES ('{keyword}','{title}',{csv_reader[i][0]});"
            
                counter+= 1

            if len(moviesInsert.encode('utf-8')) > 655000:
                moviesInsert += 'APPLY BATCH;'
                moviesFinal.append(moviesInsert)
                moviesInsert = 'BEGIN BATCH '


    moviesInsert += 'APPLY BATCH;'
    moviesFinal.append(moviesInsert)
    et = time.time()
#print(moviesFinal)
print(counter)

print (f'Forming query(INSERT INTO mm.movie_by_keyword): Done. Time required: {str(timedelta(seconds = et - st))}')
input("Press enter to start uploading")
print('Upload in progress...')

st = time.time()


for i in moviesFinal:
    i = SimpleStatement(i, consistency_level= cons_level)
    session.execute(i)

et = time.time()

print(f'Upload: Done. Time required: {str(timedelta(seconds = et - st))}')




