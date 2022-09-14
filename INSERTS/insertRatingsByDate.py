import csv
from connection import session
import ray
import time
from datetime import timedelta

def insert(allRatings):
    for line in allRatings:
        session.execute(line)

print("Query = INS -> [ratings_by_date]")
@ray.remote
def getQuarterOfTable(quarter):
    allRatings1 = []
    rating = 'BEGIN BATCH '
    for i in range(len(quarter)):
        rating += f"INSERT INTO mm.ratings_by_date(movieId, time_of_rating, rating) VALUES ({int(quarter[i][1])}, '{quarter[i][3]}', {float(quarter[i][2])});"
        if len(rating.encode('utf-8')) > 645000:
            rating += 'APPLY BATCH;'
            allRatings1.append(rating)
            rating = 'BEGIN BATCH '
    rating = 'APPLY BATCH;'
    return allRatings1

with open ('rating.csv', 'r', encoding='utf-8') as csv_file:
    csv_reader = list(csv.reader(csv_file, quoting = csv.QUOTE_ALL, delimiter = ','))
    #sortedTags = sorted(csv_reader, key = lambda row: int(row[1]), reverse=False)
    ray.init()
    st = time.time()
    print('Forming query...')
    allRatings = []
    allRatings += ray.get(getQuarterOfTable.remote(csv_reader[1:5000066]))
    allRatings += ray.get(getQuarterOfTable.remote(csv_reader[5000067: 10000133]))
    allRatings += ray.get(getQuarterOfTable.remote(csv_reader[10000134: 15000199]))
    allRatings += ray.get(getQuarterOfTable.remote(csv_reader[15000200:]))
    #allRatings += getQuarterOfTable(csv_reader[])
    et = time.time()

print (f'Forming query: Done. Time required: {str(timedelta(seconds = et - st))}')
ray.shutdown()
input("Press enter to start uploading")
print('Upload in progress...')

st = time.time()

insert(allRatings)

et = time.time()

print(f'Upload: Done. Time required: {str(timedelta(seconds = et - st))}')