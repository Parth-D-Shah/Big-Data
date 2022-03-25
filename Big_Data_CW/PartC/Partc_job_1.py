from mrjob.job import MRJob
import re
import math

class partb(MRJob):
    def mapper(self,_,line):
        try:
            fields = line.split(",")
            if len(fields) == 9:
                miner_address = fields[2]
                size = int(fields[4])
                yield (miner_address, size)
        except:
            pass

    def reducer(self, word, values):
        total = sum(values)
        yield(word,total)

if __name__ == '__main__':
    partb.run()
#python parta.py -r hadoop --output-dir out --no-cat-output hdfs://andromeda.eecs.qmul.ac.uk/data/ethereum/transactions
#hadoop fs -copyToLocal out
#hadoop fs -rm -r out
#hadoop fs -ls out
#python Partc_job_1.py -r hadoop --output-dir out --no-cat-output hdfs://andromeda.eecs.qmul.ac.uk/data/ethereum/blocks
