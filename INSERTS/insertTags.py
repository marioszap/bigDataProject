from connection import session, cons_level
import time
from datetime import timedelta
import pandas as pd
from cassandra.query import BatchStatement
import ray

@ray.remote
def ray_func(l, batch, c):
    allBatches = []
    y = 0
    for i in l:
        if "'" in i[4]:
            i[4].replace("'", "''")
        if "'" in i[3]:
            i[3].replace("'", "''")
        
        batch.add(insert_mWithTag, (i[0], i[2], i[4], i[3], i[1]))
        y += 1
        if y == 5000:
            allBatches.append(batch)
            batch = BatchStatement(consistency_level=c)
            y = 0
    allBatches.append(batch)
    return allBatches

print('Query = INS --> [tag]')
print('Forming query...')
st = time.time()
data1 = pd.read_csv('_genome_scores.csv')
data2 = pd.read_csv('genome_tags.csv')
data3 = pd.read_csv('movie.csv')

output1 = pd.merge(data1, data2, on='tagId', how='left')
output1 = pd.merge(output1, data3, on='movieId', how='left')
output1.drop('genres', axis=1, inplace=True)

mWithTags = output1.values.tolist()

insert_mWithTag = session.prepare("INSERT INTO tags(movieid, relevance, movietitle, tag, tagid) VALUES (?, ?, ?, ?, ?);")
batch1 = batch2 = batch3 = batch4 = BatchStatement(consistency_level=cons_level)

#mWithTags = [movieId | tagId | relevance | tag | title]
allData = []
ray.init()

x1 = ray.put(mWithTags[:len(mWithTags)//4])
x2 = ray.put(mWithTags[len(mWithTags)//4+1:2*len(mWithTags)//4])
x3 = ray.put(mWithTags[2*len(mWithTags)//4+1:3*len(mWithTags)//4])
x4 = ray.put(mWithTags[3*len(mWithTags)//4+1:])

allData += ray.get(ray_func.remote(x1, batch1, cons_level))
allData += ray.get(ray_func.remote(x2, batch2, cons_level))
allData += ray.get(ray_func.remote(x3, batch2, cons_level))
allData += ray.get(ray_func.remote(x4, batch2, cons_level))

ray.shutdown()

et = time.time()
print (f'Forming query: Done. Time required: {str(timedelta(seconds = et - st))}')
print('Upload in progress...')

st = time.time()
for i in allData:
    session.execute(i)
et = time.time()

print(f'Upload: Done. Time required: {str(timedelta(seconds = et - st))}')
