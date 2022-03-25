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
      address = fields[0].replace("\"","")
      total_transaction_value = int(fields[1])
                    yield(address,('T', total_transaction_value))
            else:
                if (len(fields) == 5):
                    address = fields[0] 
                    yield(address,('C'))
        except:
            pass

    def reducer(self, word, values):
        total = 0
        boolean_eval = False
        for value in values:
            if value[0] == "T":
                total = total + value[1]
            if value[0] == "C":
                boolean_eval = True
            if((boolean_eval == True) and (total != 0)):
                yield(word,total)

if __name__ == '__main__':
    partb.run()
#python parta.py -r hadoop --output-dir out --no-cat-output hdfs://andromeda.eecs.qmul.ac.uk/data/ethereum/transactions
#hadoop fs -copyToLocal out
#hadoop fs -rm -r out
#hadoop fs -ls out
#python top_ten_Job2.py out_Job1.txt input/contract.csv > out.txt
#python top_ten_Job2.py -r hadoop --output-dir out --no-cat-output hdfs://andromeda.eecs.qmul.ac.uk/user/ps321/Combined_job1.txt hdfs://andromeda.eecs.qmul.ac.uk/data/ethereum/contracts
