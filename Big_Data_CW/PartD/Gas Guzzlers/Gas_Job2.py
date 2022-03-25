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
                    yield(fields[0].replace("\"",""),('T',int(fields[1])))
            else:
                if (len(fields) == 7):
                    address = int(fields[2])
                    gas = int(fields[4]
                    adg = (address,gas)
                    yield(adg),int(fields[6]))
        except:
            pass

    def reducer(self, word, values):
        sorted_values = sorted(values, reverse = True, key = lambda tup:tup[1])[0:10]
        for value in sorted_values:
            yield (word,value[0],value[1])

if __name__ == '__main__':
    partb.run()
#python parta.py -r hadoop --output-dir out --no-cat-output hdfs://andromeda.eecs.qmul.ac.uk/data/ethereum/transactions
#hadoop fs -copyToLocal out
#hadoop fs -rm -r out
#hadoop fs -ls out
#python top_ten_Job2.py out_Job1.txt input/contract.csv > out.txt
#python top_ten_Job2.py -r hadoop --output-dir out --no-cat-output hdfs://andromeda.eecs.qmul.ac.uk/user/ps321/Combined_job1.txt hdfs://andromeda.eecs.qmul.ac.uk/data/ethereum/contracts
