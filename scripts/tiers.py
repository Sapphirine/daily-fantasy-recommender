import os, sys, glob
from pyspark.mllib.clustering import KMeans, KMeansModel
from pyspark import SparkContext

def run():
    # Set up
    sc = SparkContext()
    records = sc.textFile(os.path.realpath(__file__+'/..') + '/data-scraper/data')
    # Build clusters
    kvpairs = records.map(keyAndParse)
    cts = kvpairs.groupByKey().map(lambda (name, statList): (name, len(statList))).collectAsMap()
    kvpairs = kvpairs.reduceByKey(combine)

    # Filter outliers with too few records
    kvpairs = kvpairs.filter(lambda (k,v): cts[k] > 2)
    kvpairs = kvpairs.map(lambda (name, statline): (name, normalize(statline, cts[name])))
    
    numClusters = 20
    clusters = KMeans.train(kvpairs.map(lambda (k,v): v),numClusters,10)
    groupedClusters = kvpairs.groupBy(lambda (k,v): clusters.predict(v)).map(lambda x: (x[0], getNames(list(x[1])))).collect()
    # Rank clusters
    centers = avg(clusters.clusterCenters)
    centers.sort(key=lambda x: x['score'], reverse=True)
    # Save sorted clusters
    save(groupedClusters, centers)


# Takes the string record and converts fantasy points,
# price and stat line to doubles. Results in a pair RDD
def keyAndParse(record):
    splitStr = record.split(',')
    values = []
    for i in xrange(0,len(splitStr)):
        if (i == 7):
            try:
                values.append(float(splitStr[i][1:]))
            except ValueError:
                values.append(float(0.0))
        elif (i == 6 or (i >= 13 and i <= 19)):
            try:
                values.append(float(splitStr[i]))
            except ValueError:
                values.append(float(0.0))
    return ((splitStr[4].strip()+ ' ' + splitStr[3]).replace('\"',''), values)

# Combines records with the same player
def combine(x,y):
    toReturn = []
    for i in xrange(0,len(x)):
        toReturn.append(x[i] + y[i])
    return toReturn

# This function is passed a list of tuples
# and returns just the names
def getNames(l_of_t):
    return [x[0] for x in l_of_t]

# Writes the clusters to a file
def save(toWrite, order):
    output = open((os.path.realpath(__file__+'/../..') + '/clustersGrouped.csv'),'w')
    for i in range(len(order)):
        cluster = order[i]['id']
        # output.write(str(cluster) + ', ')
        output.write(', '.join(toWrite[cluster][1]))
        output.write('\n')
    output.close()

def normalize(l, i):
    return [x/float(i) for x in l]

# Averages the values in an array
def avg(arr):
    newArr = []
    for i in range(len(arr)):
        total = 0
        for j in range(len(arr[i])):
            total += arr[i][j]
        newArr.append({ 'id': i, 'score': total/len(arr) })
    return newArr

if __name__ == '__main__':
    run()
