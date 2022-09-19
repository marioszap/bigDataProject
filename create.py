from connection import session

mBykeyWord = "CREATE TABLE movie_by_keyword(keyword text, title text, movieId int, PRIMARY KEY(keyword,title));"
mByGenre = "CREATE TABLE movie_by_genre (genre text, title text, movieId int, year int, avgRating float, PRIMARY KEY(genre,year,avgRating)) WITH CLUSTERING ORDER BY (year DESC, avgRating DESC ); "
mInfo = "CREATE TABLE movie_info(movieId int, tag set<text>, title text, avgRating float, genres set<text>, year int, PRIMARY KEY(movieId, avgrating)) WITH CLUSTERING ORDER BY (avgrating DESC)) ;"
mByRatingSetDate = "CREATE TABLE movie_by_rating_set_date(movieId int, rating_set_date float, title text, genres set<text>, year int, PRIMARY KEY(movieId, rating_set_date)) WITH CLUSTERING ORDER BY (rating_set_date DESC);" 
ratingsByDate = " CREATE TABLE ratings_by_date (movieId int, time_of_rating timestamp, rating float, PRIMARY KEY(movieId, time_of_rating)) WITH CLUSTERING ORDER BY (time_of_rating DESC);"
tags = "CREATE TABLE tags (movieId int, movieTitle text, tagId int, tag text, relevance float, PRIMARY KEY(movieId, relevance)) WITH CLUSTERING ORDER BY (relevance DESC);"

session.execute(mBykeyWord)
session. execute(tags)
session.execute(mInfo)
session.execute(mByGenre)
session.execute(mByRatingSetDate)


