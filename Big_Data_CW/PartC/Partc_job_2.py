from mrjob.job import MRJob
import re
import math

class partb(MRJob):
    def mapper(self,_,line):
        try:
            fields = line.split(",")
            if (len(fields) == 1):
                fields = line.split("\t")
                if (len(fields) == 2):
                    yield(None,(fields[0].replace("\"",""),int(fields[1])))
        except:
            pass

    def reducer(self, word, values):
        sorted_values = sorted(values, reverse = True, key = lambda tup:tup[1])[0:10]
        for value in sorted_values:
            yield(value[0],value[1])

if __name__ == '__main__':
    partb.run()
#python parta.py -r hadoop --output-dir out --no-cat-output hdfs://andromeda.eecs.qmul.ac.uk/data/ethereum/transactions
#hadoop fs -copyToLocal out
#hadoop fs -rm -r out
#hadoop fs -ls out
#python Partc_job_2.py -r hadoop --output-dir out --no-cat-output hdfs://andromeda.eecs.qmul.ac.uk/data/ethereum/blocks
#python Partc_job_2.py -r hadoop --output-dir out --no-cat-output hdfs://andromeda.eecs.qmul.ac.uk/user/ps321/out.txt hdfs://andromeda.eecs.qmul.ac.uk/data/ethereum/blocks
