from mrjob.job import MRJob
import re
import time
import math

class parta(MRJob):
    def mapper(self, _, line):
         fields = line.split(",")
         try:
             if (len(fields) == 9):
             #access the fields you want, assuming the format is correct now
                 fields = line.split(',')
                 difficulty = int(fields[3])
                 month = time.strftime("%B",time.gmtime(int(fields[7])))
                 year = time.strftime("%Y",time.gmtime(int(fields[7])))
                 day = time.strftime("%d",time.gmtime(int(fields[7])))
                 if (month == "October" and year == "2017"):
                     yield (day, (difficulty,1))
         except:
             pass
             #no need to do anything, just ignore the line, as it was malformed

    def reducer(self, word, values):
        count = 0
        total = 0
        for item in values:
            count = count + item[1]
            total = total + item[0]
        average = total/count
        hash_to_gigahash = 1000000000
        average = average / hash_to_terrahash
        yield(word, average)

    def combiner(self, word, values):
        count = 0
        total = 0
        for value in values:
            count += 1
            total += value
        yield (word, (total, count))

if __name__ == '__main__':
    parta.run()

#python fork_chain.py -r hadoop --output-dir out --no-cat-output hdfs://andromeda.eecs.qmul.ac.uk/data/ethereum/blocks
#hadoop fs -copyToLocal out
#hadoop fs -rm -r out
