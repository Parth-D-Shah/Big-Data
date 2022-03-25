from pyspark.sql import SparkSession
from pyspark.sql.types import *
from operator import add
import time
import pyspark
import re

def transaction_line_checker(line): #check if transaction is legal
    try:
        fields = line.split(',')
        if len(fields) == 7:
            if int(fields[3]) == 0:
                return False
            if str(fields[3]) == "null":
                return False
            return True
    except:
        return False

def contract_line_checker(line): #check if contract is legal
    try:
        fields = line.split(',')
        if len(fields) == 5:
            return True
        return False
    except:
        return False

def main():
    time_Array = []
    for i in range(4):
        start = time.time()
        part_B_In_Spark()
        end = time.time()
        time_took = end - start
        print('took {} seconds'.format(time_took))
        time_Array.append(time_took)
    for x in range(len(time_Array)):
        print(time_Array[x])

def part_B_In_Spark():
    sc = pyspark.SparkContext()
    transactions = sc.textFile('hdfs://andromeda.eecs.qmul.ac.uk/data/ethereum/transactions').filter(transaction_line_checker)
    contracts = sc.textFile('hdfs://andromeda.eecs.qmul.ac.uk/data/ethereum/contracts').filter(contract_line_checker)

    job1mapper = transactions.map(lambda line: (line.split(',')[2], int(line.split(',')[3])))
    job1reducer = job1mapper.reduceByKey(lambda transaction_part0,transaction_part1: transaction_part0 + transaction_part1) #merge the values of each key using an associative reduce function

    job2 = job1reducer.join(contracts.map(lambda line: (line.split(',')[0], 'C'))) #join on contracts to find smart contracts only

    job3 = job2.takeOrdered(10, key = lambda values: - values[1][0])#calculate top 10 smart contracts

    for fields in job3:
        print(fields[0], int(fields[1][0]))

main()
