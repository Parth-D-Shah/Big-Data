from mrjob.job import MRJob
import re
import time
import math

class parta(MRJob):
    def mapper(self, _, line):
         fields = line.split(",")
         try:
             if (len(fields) == 7):
             #access the fields you want, assuming the format is correct now
                 time_epoch = int(fields[6])
                 transaction_value = int(fields[3])
                 month = time.strftime("%B",time.gmtime(time_epoch))
                 year = time.strftime("%Y",time.gmtime(time_epoch))
                 combined_Month_Year = (month,year)
                 yield(combined_Month_Year,transaction_value)
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
#python parta.py -r hadoop --output-dir out --no-cat-output hdfs://andromeda.eecs.qmul.ac.uk/data/ethereum/transactions
