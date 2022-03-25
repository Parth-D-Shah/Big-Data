from mrjob.job import MRJob
import datetime

class partd(MRJob):

    def mapper(self, _, line):
        try:
            fields = line.split(",")
            if len(fields) == 7:
                address = fields[2]
                month = time.strftime("%B",time.gmtime(int(fields[6])))
                year = time.strftime("%Y",time.gmtime(int(fields[6])))
                combined_Month_Year = (month,year)
                value = int(fields[3])
                yield (address, (combined_Month_Year, "Transactions", value))
            else:
                for item in fields[3:]:
                    scam_address = fields[1]
                    yield (item.strip(),(scam_address,"Scams"))
        except:
            pass

    def reducer(self, key, values):
        Transactions = False
        Scams = False
        Scam_Type = None
        array_of_scams = []
        for item in values:
            if item[1] == "Transactions":
                Transactions = True
                array_of_scams.append((item[0], item[2]))
            elif item[1] == "Scams":
                Scam_Type = item[0]
                Scams = True
        if Transactions ==True and Scams == True:
            for item in array_of_scams:
                yield (category, (item[0], item[1]))

if __name__ == '__main__':
    partd.run()
