from operator import itemgetter
from connection import session, cons_level
import time
from datetime import timedelta
from cassandra.query import SimpleStatement

tempTime = 0
avgs = []

keyword = ['Star','Toy', 'President', 'the', 'and', 'Love', 'Water', 'Machine', 'Game', 'Family']

for i in range(len(keyword)):
    st = time.time()
    query = f"SELECT title FROM movie_by_keyword WHERE keyword = '{keyword[i]}';"
    query = SimpleStatement(query, consistency_level= cons_level)
    returns = session.execute(query)
    et = time.time()
    y = session.execute(f"SELECT COUNT(*) FROM movie_by_keyword WHERE keyword = '{keyword[i]}';" )
    for j in y:
        print( 'Num of rows returned: ', j.count,' Keyword requested: ', keyword[i],' Time consumed: ', et-st)
    tempTime += et-st

print(f'Query: SELECT title FROM movie_by_keyword WHERE keyword = Star; Average Time required: {str(timedelta(seconds = tempTime/len(keyword)))}')

avgs.append(tempTime/len(keyword))

keyword= ['Adventure', 'Horror', 'Comedy', 'Thriller', 'Action', 'Drama', 'Animation', 'Romance', 'Children','Sci-Fi']

for i in range(len(keyword)):
    st = time.time()
    query = f"SELECT title FROM movie_by_genre WHERE genre = '{keyword[i]}';"
    query = SimpleStatement(query, consistency_level= cons_level)
    returns = session.execute(query)
    et = time.time()
    y = session.execute(f"SELECT COUNT(*) FROM movie_by_genre WHERE genre = '{keyword[i]}';" )
    for j in y:
        print( 'Num of rows returned: ', j.count,' genre requested: ', keyword[i],' Time consumed: ', et-st)
    tempTime += et-st

print(f'Query: SELECT title FROM movie_by_genre WHERE genre = [___]; Average Time required: {str(timedelta(seconds = tempTime/len(keyword)))}')

avgs.append(tempTime/len(keyword))


dateSlices = [['2015-1-1 00:00','2015-1-15 23:59'],
        ['2010-2-1 00:00','2013-1-15 23:59'], ['2014-2-1 00:00','2015-1-15 23:59'],
        ['2012-1-1 00:00','2013-1-15 23:59'], ['2015-1-1 00:00','2015-8-15 23:59'], ['2009-2-1 00:00','2010-1-15 23:59'], 
        ['2010-1-1 00:00','2012-1-15 23:59'], ['2015-2-1 00:00','2015-5-15 23:59'], ['2010-2-1 00:00','2011-1-10 23:59'],
        ['2015-2-1 00:00', '2015-3-1 00:00']]
tempTime = 0

for sl in range(len(dateSlices)):
    st = time.time()
    x  = f"SELECT movieId, AVG(rating) FROM ratings_by_date WHERE time_of_rating > '{dateSlices[sl][0]}' AND time_of_rating < '{dateSlices[sl][1]}' GROUP BY movieId ALLOW FILTERING;"
    x = SimpleStatement(x, consistency_level= cons_level)
    x = session.execute(x)
    final = []
    for i in x:
        final.append([i.movieid, i.system_avg_rating])
    final = sorted(final, key=itemgetter(1), reverse=True)
    final = final[:30]
    for i in range(len(final)):
        s = f"SELECT title FROM movie_info2 WHERE movieId = {final[i][0]} ALLOW FILTERING"
        s = SimpleStatement(s, consistency_level= cons_level)
        s = session.execute(s)
        for j in s:
            final[i][0] = j.title
    et = time.time()
    tempTime += et-st
    print(final,"\n\n")
print(f"Query: SELECT title,rating FROM movie_info WHERE time_of_rating > '[]' AND time_of_rating < '[]' ORDER BY rating; Average Time required: {str(timedelta(seconds = tempTime/len(dateSlices)))}")

avgs.append(tempTime/len(dateSlices))

tempTime = 0
movies= []

title = ['Jumanji ','Toy Story ','Grumpier Old Men ','Balto ','Nixon ','Casino ','Powder ','Last Ride, The ','Q ','Miffo ']
for i in range(len(title)):
    st = time.time()
    titleReturn = f"SELECT title, genres, avgRating, tag FROM movie_info2 WHERE title = '{title[i]}' ALLOW FILTERING;"
    titleReturn = SimpleStatement(titleReturn, consistency_level= cons_level)
    titleReturn = session.execute(titleReturn)
    et = time.time()
    tempTime += et-st
    for i in titleReturn:
        print(i.title, i.genres, i.avgrating, i.tag)

print(f"Query: SELECT title, genres, avgRating, tag FROM movie_info2 WHERE title = [___] ALLOW FILTERING; Average Time required: {str(timedelta(seconds = tempTime/len(title)))}")
avgs.append(tempTime/len(title))

tempTime = 0
tags = ['comedy','cute','romantic','teen', 'intimate', 'based on true story', 'western', 'russian', 'magic', 'original']

for i in range(len(tags)):
    st = time.time()
    byTag = f"SELECT title, tag, avgRating FROM movie_info2 WHERE tag CONTAINS '{tags[i]}' LIMIT 20 ALLOW FILTERING;"
    byTag = SimpleStatement(byTag, consistency_level= cons_level)
    byTag = session.execute(byTag)
    et = time.time()
    tempTime += et-st
    for i in byTag:
        print(i.title, i.avgrating, i.tag)

print(f"Query: SELECT title, tag, avgRating FROM movie_info2 WHERE tag CONTAINS [___] LIMIT 20 ALLOW FILTERING; Average Time required: {str(timedelta(seconds = tempTime/len(tags)))}")
avgs.append(tempTime/len(tags))

print(avgs)