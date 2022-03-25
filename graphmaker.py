import datetime
import matplotlib.pyplot as plot
import matplotlib.dates as dates

f = open("out.txt", "r")
x = []
y = []
for line in f:
    line = line[1:-1].split("\t")
    x.append(datetime.datetime.strptime(line[0][:-1], "%D %M %Y").date())
    y.append(float(line[1]))

plot.ticklabel_format(style='plain')
plot.scatter(x, y, marker='x')
plot.gca().xaxis.set_major_formatter(dates.DateFormatter("%B %Y"))
plot.xlabel("Date")
plot.ylabel("Gas price (Wei)")
plot.show()
