#!/usr/bin/env python3

import os 
from os.path import expanduser
import re
import datetime
from dateutil.parser import parse as dateparse
from dateutil import tz
import json


def assertCounts(log_data):
    listsizes_ref = [8, 120, 80, 48, 24, 8]
    models_per_stage = [s['models'] for s in log_data['stages']]

    assert models_per_stage == listsizes_ref[:len(models_per_stage)], "Modelcount doesn't match reference"

def to_timedelta(ms_interval):
    t = datetime.timedelta(
        milliseconds=int(ms_interval)
    )

    if t.microseconds >= 500000:
        t = t + datetime.timedelta(seconds=1)

    return t - datetime.timedelta(microseconds=t.microseconds)


def extract_data(logfilepath):
    result = {
        'stages': [],
        'total_models': 0
    }
    f = open(logfilepath)

    state = 0
    stage = None
    for line in f:
        logfields = line.rstrip().split('- ')

        if state == 0 and 'INFO' in logfields[2]:
            logstr = json.loads(logfields[3])
            result['b-cpus'] = int(logstr['batchclustercpus'])
            result['l-power'] = int(logstr['lambdapower'])
            result['l-timeout'] = int(logstr['lambdatimeout'])
            state = 10

        if state == 10 and 'WARN' in logfields[2]:
            #1 [8]
            found = re.findall(r"#([0-9]+) \[([0-9]+)\]", logfields[3])
            if not found:
                continue
            found = found[0]

            stage = {
                'id': found[0],
                'models': int(found[1]),
                'start': dateparse(logfields[0][1:-2])
            }
            result['total_models'] = result['total_models'] + int(found[1])
            state = 11

        if state == 11 and 'INFO' in logfields[2] and logfields[3] == 'ok!':
            stage['finish'] = dateparse(logfields[0][1:-2])
            stage['duration'] = (stage['finish'] - stage['start'])
            result['stages'].append(stage)

            stage = None
            state = 10

        if state == 10 and 'CRITICAL' in logfields[2]:
            duration = logfields[3].split(' ')[2]
            runtime = to_timedelta(duration)
            result['runtime'] = runtime
            state = 999

    return result


def extract_cloud(logfilepath, result):
    f = open(logfilepath)

    batch_models = 0
    accum = { }
    for line in f:
        logfields = line.rstrip().split(' ', 3)

        if logfields[0] == '/aws/batch/job' and logfields[3].startswith('CRITICAL'):

            logdate_direct = dateparse(logfields[2]).replace(tzinfo=None)
            logdate_tzfix = dateparse(logfields[2]).astimezone(tz.tzlocal()).replace(tzinfo=None)
            
            res_dr = [idx for idx, x in enumerate(result['stages']) if x['start'] <= logdate_direct <= x['finish']]
            res_tz = [idx for idx, x in enumerate(result['stages']) if x['start'] <= logdate_tzfix <= x['finish']]
            
            index = [ res_dr[0] if res_dr else 99, res_tz[0] if res_tz else 99 ]
            add_to = min(index)
            accum[add_to] = accum.get(add_to, 0) + 1
            batch_models = batch_models + 1
    
    for k in [x for x in accum.keys() if x < 99]:
        result['stages'][k]['batch'] = accum[k]

    result['total_batch'] = batch_models
    result['total_batch_missing'] = accum.get(99, 0)


dir = expanduser('~') + '/aws-s3/mestrado-dev-phyml'

for subdir in sorted([d for d in os.listdir(dir) if d != 'inputfiles' and os.path.isdir(os.path.join(dir, d))]):
    try:
        execution_time, input_file = subdir.split('_',1)
        
        logfile = os.path.join(dir,subdir,'log','self.txt')
        watchfile = os.path.join(dir,subdir,'log','cloudwatch.txt')

        log_data = extract_data(logfile)

        assertCounts(log_data)

        extract_cloud(watchfile, log_data)

        result = [
            input_file,
            subdir,
            str(log_data['runtime']),
            str(log_data['l-power']),
            str(log_data['l-timeout']),
            str(log_data['b-cpus']),
            str(log_data['total_batch'] / log_data['total_models']),
            json.dumps(log_data, default=str) # indent=4, sort_keys=True,
        ]
        print('\t'.join(result))
    except AssertionError:
        print('###       {}       ###  assertion'.format(subdir))
    except:
        print('###       {}       ###'.format(subdir))
    pass
