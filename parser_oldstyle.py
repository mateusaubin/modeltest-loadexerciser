#!/usr/bin/env python3

import os 
from os.path import expanduser
import re
from dateutil.parser import parse as dateparse
import datetime

def to_str(obj):
    if isinstance(obj, (datetime.timedelta)):
        hours, remainder = divmod(obj.total_seconds(), 3600)
        minutes, seconds = divmod(remainder, 60)
        formatted = '{0:02}:{1:02}:{2:02}'.format(int(hours),int(minutes), int(seconds))
        return formatted
    return str(obj)

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
    
    if diff.total_seconds() < 3:
        return

    diff_ms = to_str(diff)

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
        except Exception as exc:
            print('###       {}       ### {}'.format(curfile, str(exc)))
        pass

    pass
        

