from mrjob.job import MRJob
import re
import datetime

class partd(MRJob):

    def mapper(self, _, line):
        try:
            fields = line.split("\t")
            if len(fields) == 2:
                scam_type = fields[0].strip('""')
                value_of_scam = int(fields.replace("\"",""))
                yield (scam_type, value_of_scam)
        except:
            pass

    def reducer(self, word, values):
        total = sum(values)
        yield(word, total)

    def combiner(self, word, values):
        total = sum(values)
        yield(word, total)

if __name__ == '__main__':
    partd.run()
