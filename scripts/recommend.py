from pyspark.mllib.regression import LabeledPoint
from pyspark import SparkContext
import os, sys


# OVERALL PIPELINE
# * parse stat historys from rotoguru
# * parse DraftKings current pricing
# * build point per dollar (ppd) metric keyed by player name
# * sort ppd rdd in decreasing order of ppd
# * add a player to roster with the highest ppd as long as budgetary and positional constraints are satisfied
# * proceed in this greedy fashion ignoring players who fail either constraint
#
# This pipeline makes several DraftKings specific assumptions:
# * The minimum price for a player is $3,000
# * Rosters consist of 8 player positions PG,SG,SF,PF,C,G,F,UTIL
# * Budget/SalaryCap is $50,000

def run(mode):
    sc = SparkContext()
    clusters = open(os.path.realpath(__file__+'/../..') + '/clustersGrouped.csv', 'r')
    if (mode == 'standard'):
        numStuds = 0
    else:
        numStuds = 4

    # RotoGuru stat histories
    records = sc.textFile(os.path.realpath(__file__+'/..') + '/data-scraper/data')
    kvpairs = records.map(keyAndParse)

    # Counts for normalizing
    cts = kvpairs.groupByKey().map(lambda (name, statList): (name, len(statList))).collectAsMap()
    kvpairs = kvpairs.reduceByKey(combine)
    kvpairs = kvpairs.map(lambda (name, statline): (name, normalize(statline, cts[name])))

    # RDD of keyed DraftKings prices
    prices = sc.textFile(os.path.realpath(__file__+'/../../DKSalaries.csv'))
    dkprices_pos = prices.map(getPrice)

    # point per dollar RDD
    ppd = kvpairs.join(dkprices_pos).map(lambda (k,v): (k, getPpd(k,v)))

    studList = getStuds(numStuds, clusters, dkprices_pos)
    for stud in studList:
        ppd = ppd.filter(lambda (k,v): k != stud[0])
    sortedPpd = ppd.sortBy(lambda x: -x[1][0])
    getRoster(sortedPpd.collect(), studList)

def getStuds(numStuds, clusters, price_pos):
    if (numStuds == 0):
        return []
    # tally necessary for numStuds > 3 to check positional validity
    tally = {'PG':0, 'SG':0, 'SF':0, 'PF':0, 'C':0, 'G':0, 'F':0, 'UTIL':0}
    toReturn = []
    rosterCt = 0
    for tier in clusters:
        spl = tier.split(',')

        # iterate over tier
        for i in xrange(0,len(spl)):
            if (rosterCt == numStuds):
                return toReturn
            else:
                name = spl[i].strip()
                tup = price_pos.lookup(name)
                if (len(tup) == 0):
                    continue
                else:
                    tup = tup[0]
                cost = int(tup[0])
                pos = tup[1]
                res = posCheck(tally,pos)
                if (cost <= budget(50000, rosterCt) and res[0] == True):
                    tally[res[1]] = 1
                    toReturn.append((name,(cost, res[1])))
                    rosterCt += 1
    return toReturn

# checks positional constraint validity
def posCheck(tally, pos):
    if (tally[pos] == 0):
        return (True,pos)
    elif (pos != 'C' and tally[pos[1:]] == 0):
        return (True,pos[1:])
    elif (tally['UTIL'] == 0):
        return (True,'UTIL')
    else:
        return (False,'N/A')

def getRoster(players, studList):
    salary = 50000
    tally = {'PG':0, 'SG':0, 'SF':0, 'PF':0, 'C':0, 'G':0, 'F':0, 'UTIL':0}
    roster = []

    for stud in studList:
        roster.append((stud[0],stud[1][1]))
        salary -= int(stud[1][0])
        tally[stud[1][1]] = 1

    for player in players:
        name = player[0]
        pos = player[1][1]
        cost = player[1][2]
        try:
            if (tally[pos] == 0 and cost <= budget(salary, len(roster))):
                roster.append((name,pos))
                tally[pos] = 1
                salary -= cost
            elif (pos != 'C' and tally[pos[1:]] == 0 and cost <= budget(salary, len(roster))):
                roster.append((player[0],pos))
                tally[pos[1:]] = 1
                salary -= cost
            elif (tally['UTIL'] == 0 and cost <= budget(salary, len(roster))):
                roster.append((name,pos))
                tally['UTIL'] = 1
                salary -= cost
            if (len(roster) == 8):
                break
        except Exception, e:
            print 'invalid position: ' + pos + ', ' + player[0]

    prettyPrint(roster)
    print '\nRemaining salary: $' + str(int(salary)) + '/$50000'

def prettyPrint(roster):
    for player in roster:
        print player[0] + ', ' + player[1]

def budget(salary, currRosterSize):
    minPlayerCost = 3000
    maxRosterSize = 8
    return salary - (maxRosterSize-1-currRosterSize)*minPlayerCost

def check(k,flt):
	return True if (flt != 0.0) else False

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
def save(toWrite):
    output = open((os.path.realpath(__file__+'/../..') + "/clustersGrouped.csv"),"w")
    for tier in toWrite:
        output.write(str(tier[0])+':\n')
        for i in xrange(0, len(tier[1])-1):
            output.write(tier[1][i] + ', ')
        output.write(tier[1][len(tier[1])-1] + '\n')
    output.close()

def normalize(l, i):
    return [x/float(i) for x in l]

def getPrice(line):
	spl = line.split(',')

    # returns (name, [price, position])
	return (spl[1].strip('"'), [spl[2], spl[0].strip('"')])

def getPpd(k,v):
	points = list(v[0])[0]
	cost = float(list(v[1])[0])
	position = list(v[1])[1].strip('"')

    # returns (ppd, position, cost)
	return (float(points/cost), position, cost)

if __name__ == '__main__':
    if (sys.argv[1] == 'hybrid'):
        run('hybrid')
    else:
        run('standard')
