#!/usr/bin/env python3

import os 
from os.path import expanduser
import re
from dateutil.parser import parse as dateparse

def match_0(line):
    regex_0 = r"^.*/(\w+).*\= ([0-9\-\ :\.]+) .+?\| ([0-9\-\ :\.]+) .*"

    found = re.findall(regex_0, line)

    if not found:
        return
    else:
        found = found[0]

    phyfile = found[0]
    start = dateparse(found[1])
    end = dateparse(found[2])
    diff = end-start
    diff_ms = round(diff.total_seconds() * 1000)

    print('{}\t{}\t{}\t{}'.format(id,procs,phyfile,diff_ms))

dir = expanduser('~') + '/aws-s3/mestrado-dev-phyml-fixed'

for subdir in sorted([d for d in os.listdir(dir) if os.path.isdir(os.path.join(dir, d))]):
    id = subdir
    procs = subdir[:2]

    curfile = os.path.join(dir,subdir,'#_stats.txt')
    f = open(curfile)
    for line in f:
        try:
            match_0(line)
        except:
            print('###       {}       ###'.format(curfile))
        pass

    pass
        
