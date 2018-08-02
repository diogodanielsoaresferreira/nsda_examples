#!/usr/bin/env python
#
#
# find_beacons.py
#
# input:
#       rwsort --field=1,9 | rwcut --no-title --epoch --field=1,9 | <stdin>
# command line:
# find_beacons.py precision tolerance [epoch]
#
# precision: integer expression for bin size (in seconds)
# tolerance: floating point representation for tolerance expressed as
# fraction from median, e.g. 0.05 means anything within (median -
# 0.5*median, median + 0.5*median) is acceptable
# epoch: starting time for bins; if not specified, set to midnight of the first time
# read.

# This is a very simple beacon detection script which works by breaking a traffic
# feed into [precision] length bins.  The distance between bins is then calculated and
# the median value is used as representative of the distance.  If all the distances
# are within tolerance% of the median value, then the traffic is treated as a beacon.

from __future__ import division
import sys

if len(sys.argv) >= 3:
    precision = float(sys.argv[1])
    tolerance = float(sys.argv[2])
else:
    sys.stderr.write("Specify the precision and tolerance\n")

starting_epoch = -1 
if len(sys.argv) >= 4:
    starting_epoch = int(sys.argv[3])


def process_epoch_info(bins):
    a = bins.keys()
    a = sorted(a)

    distances = []

    # We create a table of distances between the bins
    for i in range(0, len(a) -1):
        distances.append(a[i + 1] - a[i])

    distances.sort()

    if distances != []:
        median = distances[len(distances)//2]
    else:
        median = 0

    tolerance_range = (median - tolerance * median, median + tolerance * median)

    # Now we check bins
    count = 0 
    for i in distances:
        if (i >= tolerance_range[0]) and (i <= tolerance_range[1]):
            count+=1

    return count, len(distances)

bins = {}    # Checklist of bins hit during construction; sorted and
             # compared later.  AA be cause it's really a set and I
             # should start using those.
results = {} # Associate array containing the results of the binning
             # analysis, dumped during the final report

# Timestamp of the selected bar
bin_bar = 0

# Last IP that is being evaluated
last_ip = ''

# We start reading in data; for each line I'm building a table of
# beaconing events.  The beaconing events are simply indications that
# traffic 'occurred' at time X.  The size of the traffic, how often it occurred,
# how many flows is irrelevant.  Something happened, or it didnt.  
for i in sys.stdin.readlines():
    ip, time = i.split('|')[0:2]
    ip = ip.strip()
    time = float(time)

    # On the first run, the last ip is the first ip
    if last_ip == '':
        last_ip = ip

    # Everytime the IP changes means that we can process
    # the last ip in the bins
    if ip != last_ip:
        results[last_ip] = process_epoch_info(bins)
        bins = {}
            
    if starting_epoch == -1:
        starting_epoch = time - (time % 86400) # Sets it to midnight of that day


    last_ip = ip
    bin_bar = int((time - starting_epoch) / precision)
    bins[bin_bar] = 1

results[last_ip] = process_epoch_info(bins)


ips = results.keys()
ips = sorted(ips)

for ip in ips:
    if results[ip][1]!=0:
        print(ip+": "+str(results[ip]) + " -> " + str(round(results[ip][0]/results[ip][1], 2)))
    else:
        print(ip+": "+str(results[ip]) + " -> 0.00")
