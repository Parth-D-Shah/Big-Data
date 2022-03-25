from mrjob.job import MRJob
import re
import time
import math


#This line declares the class Lab3, that extends the MRJob format.
class parta(MRJob):
    def mapper(self, _, line):
         fields = line.split(",")
         try:
             if (len(fields) == 7):
             #access the fields you want, assuming the format is correct now
                 time_epoch = int(fields[6])
                 month = time.strftime("%B",time.gmtime(time_epoch))
                 year = time.strftime("%Y",time.gmtime(time_epoch))
                 combined_Month_Year = (month,year)
                 yield(combined_Month_Year,1)
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
    parta.run()
#python parta.py -r hadoop --output-dir out --no-cat-output hdfs://andromeda.eecs.qmul.ac.uk/data/ethereum/transactions
