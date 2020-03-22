import re
import time
from operator import add
import itertools

def generateCandidates(pre_fre_sets, rounds):
    elements = set()
    for i in pre_fre_sets:
        elements.update(i)
    
    def mapfunc(t):
        for ele in elements:
            if ele not in t:
                yield t.union(frozenset((ele, )))
    cansets = set()
    for pre in pre_fre_sets:
        cansets.update(set(mapfunc(pre)))
    print ('\n\n%d\n' % rounds)
    # print (cansets)
    # cansets = pre_fre_sets.flatMap(mapfunc).distinct()

    def func(s):
        subsets = itertools.combinations(s, rounds - 1)
        # print (list(subsets))
        return all(frozenset(subset) in pre_fre_sets for subset in subsets)
    # return cansets.filter(func)
    res = list(filter(func, cansets))
    print ('res len: ', len(res))
    return res


def verifyCandidates(transaction, candidates):
    # print (transaction)
    for c in candidates:
        if c.issubset(transaction):
            yield (c, 1.0)


def verifyRules(transaction, candidates, curRound):
    for set1 in candidates:
        for i in range(curRound):
            subsets = itertools.combinations(set1, i)
            for l1 in subsets:
                l1 = frozenset(l1)
                if l1.issubset(transaction):
                    r = set1.difference(l1)
                    yield ((l1, r), 1.0)


startTime = time.time()
input = '/FileStore/tables/T40I10D100K.dat'
infileName = '/FileStore/tables/T40I10D100K'
output = '/result.dat'
support = 0.1
k = 4
num = 8
confidence = 0.1

transactions = sc.textFile(input, num).map(lambda x : frozenset(x.split())).cache()
numRecords = transactions.count()
freqThre = numRecords * support
print ("frequency threshold：", freqThre)

oneFreqSet = transactions.flatMap(lambda x : x) \
    .map(lambda x : (x, 1.0)).reduceByKey(add) \
    .filter(lambda x : x[1] >= freqThre) \
    .map(lambda x : (frozenset((x[0], )), x[1] / numRecords))

ft1 = time.time()
print ("generate " + str(oneFreqSet.count()) + " Frequent 1-Item Set waste time " + str(ft1-startTime) + " s.")

preFreSets = [x[0] for x in oneFreqSet.collect()]

round = 2
while round <= k and len(preFreSets) > 0:
    candidates = generateCandidates(preFreSets, round)
    broadcast_candidates = sc.broadcast(candidates)
    curFreqSets = transactions.flatMap(lambda x: verifyCandidates(x, broadcast_candidates.value)).reduceByKey(add).filter(lambda x: x[1] >= freqThre)
    preFreSets = [x[0] for x in curFreqSets.collect()]
    # preFreSets.cache()
    print (len(preFreSets))

    if preFreSets:
        ftk2 = time.time()
        print ("generate "+ str(len(preFreSets)) + " Frequent  " + str(round) + "-Item Set waste time " + str(ftk2-ft1) + " s.")


        
        asst1 = time.time()
        
        freqSetIndex = dict()
        for fs in curFreqSets.collect():
            freqSetIndex[fs[0]] = fs[1]
        
        broadcastCurFreqSet = sc.broadcast(freqSetIndex)
        
        associationRules = transactions \
                .flatMap(lambda x : verifyRules(x, frozenset(broadcastCurFreqSet.value.keys()), round)) \
                .reduceByKey(add) \
                .map(lambda x : ((x[0][0], x[0][1]), broadcastCurFreqSet.value.get(x[0][0].union(x[0][1]), 0) * 1.0 / x[1])) \
                .filter(lambda x : x[1] >= confidence)
        
        asst2 = time.time()
        print ("generate " + str(associationRules.count()) + " association rules with " \
                + str(round) + "-Item Set waste time " +  str(asst2 - asst1) + " s.")
    round += 1

print 'Total time:', time.time() - startTime