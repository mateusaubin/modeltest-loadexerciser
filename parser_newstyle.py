#!/usr/bin/env python3

import os 
from os.path import expanduser
import re
import datetime
from dateutil.parser import parse as dateparse
from dateutil import tz
import json
import sys


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
    state = 0
    stage = None
    result = {
        'stages': [],
        'total_models': 0
    }

    for line in open(logfilepath):
        logfields = line.rstrip().split('- ')

        if state == 0 and 'INFO' in logfields[2]:
            logstr = json.loads(logfields[3])
            result['scenario'] = logstr.get('runner') or logstr.get('batchrunner') or ' -- default -- '
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

        if (state == 11 and 'INFO'  in logfields[2] and logfields[3] == 'ok!') or \
           (state == 11 and 'ERROR' in logfields[2] and 'Tired of waiting for a response' in logfields[3]):
            stage['finish'] = dateparse(logfields[0][1:-2])
            stage['duration'] = (stage['finish'] - stage['start'])
            result['stages'].append(stage)

            if 'ERROR' in logfields[2]:
                result['runtime'] = datetime.timedelta(seconds=-1)
                state = 999
            else:
                state = 10

            stage = None

        if state == 10 and 'CRITICAL' in logfields[2]:
            duration = logfields[3].split(' ')[2]
            runtime = to_timedelta(duration)
            result['runtime'] = runtime
            state = 999

        if state == 999:
            break

    assert state == 999, "Exiting 'extract_data' on non-final state"

    return result


def extract_cloud(logfilepath, result):
    batch_models = 0
    accum = { }
    compute = { }

    for line in open(logfilepath):
        logfields = line.rstrip().split(' ', 3)

        if 'REPORT' in logfields[3]:
            regex_pattern = r"REPORT RequestId: [a-z0-9-]{36}\sDuration: ([0-9\.]+) ms"
            if logfields[0] == '/aws/batch/job':
                # CRITICAL | REPORT RequestId: abb650b3-9226-432b-9db8-db2a3a201e55 Duration: 33442 ms
                regex_pattern = r"CRITICAL \| " + regex_pattern
                pass

            elif logfields[0].startswith('/aws/lambda'):
                # REPORT RequestId: 9a1fec8d-f62b-11e8-91de-25dc3ddf5870	Duration: 97.68 ms	Billed Duration: 100 ms
                regex_pattern = regex_pattern + r"\sBilled Duration: ([0-9\.]+) ms"
                pass

            matches = re.search(regex_pattern, logfields[3])
            if matches and matches.groups():
                groups = matches.groups()

                compute['raw'] = compute.get('raw', 0) + float(groups[0])
                compute['billed'] = compute.get('billed', 0) + (float(groups[1]) if len(groups) > 1 else 0)
                compute[logfields[0]] = compute.get(logfields[0], 0) + float(groups[0])

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

    # convert compute units from ms to timedelta
    for k in compute.keys():
        compute[k] = datetime.timedelta(milliseconds=compute[k])

    result['total_batch'] = batch_models
    result['total_batch_missing'] = accum.get(99, 0)
    result['total_compute'] = compute.get('raw', 0)
    result['compute'] = compute


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
            log_data['scenario'],
            input_file,
            subdir,
            str(log_data['runtime']),
            str(log_data['l-power']),
            str(log_data['l-timeout']),
            str(log_data['b-cpus']),
            str(log_data['total_batch'] / log_data['total_models']),
            str(log_data['total_compute']),
            json.dumps(log_data, default=str) # indent=4, sort_keys=True,
        ]
        print('\t'.join(result))
    except AssertionError:
        print('###       {}       ###  assertion'.format(subdir))
    except:
        print('###       {}       ###'.format(subdir))
    pass
