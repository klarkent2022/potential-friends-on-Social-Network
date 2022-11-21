import re
import sys
from pyspark import SparkConf, SparkContext

conf = SparkConf()
sc = SparkContext(conf=conf)

lines = sc.textFile(sys.argv[1])

# tslines each elemement: (int, [int,int,...])
ts_lines = lines.map(lambda l: l.split())\
.filter(lambda l: len(l) > 1 )\
.map(lambda l: (l[0], l[1].split(',')))\
.map(lambda l: [int(l[0]) , list(map(int, l[1]))])\

# getting distinct direct friend pairs
def distribute(line):
    output = []
    for i in line[1]:
        output.append((line[0], i))
    return output

friends = ts_lines.flatMap(distribute)\
.map(lambda x: tuple(sorted(x)))\
.distinct()\


def combinations(x):
    array = x[1]
    combinations = []
    for i in range(len(array)-1):
        for j in range(i+1, len(array)):
            combinations.append((array[i], array[j]))

    return combinations

possible_friends = ts_lines.flatMap(combinations)\
.map(lambda pair: tuple(sorted(pair)))\

output = possible_friends.subtract(friends)\
.map(lambda x: (x, 1))\
.reduceByKey(lambda x,y: x+y)\
.sortBy(lambda x: [-x[1], x[0][0], x[0][1]])\
.take(10)


for pair in output:
    if pair[1] > 0:
        print(str(pair[0][0]) +  "\t"  + str(pair[0][1]) +  "\t" + str(pair[1]) )


sc.stop()

