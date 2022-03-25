from mrjob.job import MRJob
import re
import datetime

class Partd(MRJob):
    def mapper(self, _, line):
        try:
            fields = line.split(",")
            if len(fields) == 7:
                address = fields[2]
                transaction_value = int(fields[3])
                yield (address, (transaction_value, "Transaction"))
            else:
                for item in fields[3:]: #count from field 3 onwards
                    scam_address = fields[1]
                    yield(item.strip(),(scam_address, "Scam")) #to_address, category
        except:
            pass

    def reducer(self, key, values):
        Total = 0
        Transactions = False
        Scams = False
        Scam_Type = None
        for item in values:
            if item[1] == "Transaction":
                Total = item[0]
                Transactions = True
            elif item[1] == "Scam":
                Scam_Type = item[0]
                Scams = True
        if Transactions == True and Scams == True:
            yield(Scam_Type, Total)

if __name__ == '__main__':
    Partd.run()
