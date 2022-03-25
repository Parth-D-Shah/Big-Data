from mrjob.job import MRJob
import re
import time
import math

class partb(MRJob):
    def mapper(self, _, line):
         fields = line.split(",")
         try:
            if (len(fields) == 7):
            	 if int(fields[3]) != 0:
            	    if str(fields[3]) != "null":
                	join_key = fields[2].strip('"')
	 	value = int(fields[3])
                	yield(join_key,value)
         except:
             pass
             #no need to do anything, just ignore the line, as it was malformed

    def reducer(self, word, values):
        total = sum(values)
        yield(word, total)

    def combiner(self, word, values):
        total = sum(values)
        yield(word, total)

if __name__ == '__main__':
    partb.run()
#python top_ten.py -r hadoop --output-dir out --no-cat-output hdfs://andromeda.eecs.qmul.ac.uk/data/ethereum/transactions
#hadoop fs -copyToLocal out
#hadoop fs -rm -r out
#hadoop fs -ls out
#python top_ten.py input/transactions.csv input/contract.csv > out.txt
