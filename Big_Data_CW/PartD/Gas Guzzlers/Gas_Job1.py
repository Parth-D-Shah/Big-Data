from mrjob.job import MRJob
import time

class Gas(MRJob):

    def mapper(self,_,line):
        try:
            if len(fields) == 7:
                fields = line.split(',')
                gas_price = int(fields[5])
                date = time.localtime(int(fields[6]))
                yield((date.tm_mon,date.tm_year),(1,gas_price)
        except:
            pass

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


if __name__=='__main__':
    Gas.run()
