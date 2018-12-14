#!/usr/bin/env python3

import os 
from os.path import expanduser
import re
import datetime
from dateutil.parser import parse as dateparse
from dateutil import tz
import json
import sys
import math


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

        if state == 0 and 'INFO' in logfields[2] and 'root' in logfields[1]:
            logstr = json.loads(logfields[3])
            result['scenario'] = logstr.get('runner') or logstr.get('batchrunner') or ' -- default -- '
            result['b-cpus'] = int(logstr['batchclustercpus'])
            result['l-power'] = int(logstr['lambdapower'])
            result['l-timeout'] = int(logstr['lambdatimeout'])
            state = 10

        if state == 10 and 'WARN' in logfields[2] and 'root' in logfields[1]:
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

        if  'root' in logfields[1] and \
            (state == 11 and 'INFO'  in logfields[2] and logfields[3] == 'ok!') or \
            (state == 11 and 'ERROR' in logfields[2] and 'Tired of waiting for a response' in logfields[3])\
        :
            stage['finish'] = dateparse(logfields[0][1:-2])
            stage['duration'] = (stage['finish'] - stage['start'])
            result['stages'].append(stage)

            if 'ERROR' in logfields[2]:
                result['runtime'] = datetime.timedelta(days=-1)
                state = 999
            else:
                state = 10

            stage = None

        if state == 10 and 'CRITICAL' in logfields[2] and 'root' in logfields[1]:
            duration = logfields[3].split(' ')[2]
            runtime = to_timedelta(duration)
            result['runtime'] = runtime
            state = 999

        if state == 999:
            break

    assert state == 999, "Exiting 'extract_data' on non-final state"

    return result


def FitLogTimestampToStage(log_timestamp, stages):

    # timestamp do log = sempre UTC
    # stage{start/finish} pode set BR 

    try:
        # ALL DATES ARE UTC AND WE ARE HAPPY
        logdate_direct = dateparse(log_timestamp).replace(tzinfo=None)
        res_dr = [idx for idx, x in enumerate(stages) if x['start'] <= logdate_direct <= x['finish']]
        
        if res_dr:
            return res_dr[0]

        # MATCHING DATE NOT FOUND, MAYBE WAS EXECUTING LOCALLY AND HAD TZ MISMATCH
        logdate_tzfix = dateparse(log_timestamp).astimezone(tz.tzlocal()).replace(tzinfo=None)
        res_tz = [idx for idx, x in enumerate(stages) if x['start'] <= logdate_tzfix <= x['finish']]
        
        if res_tz:
            return res_tz[0]

    except:
        pass

    # ALL HOPE IS LOST
    return None


def DetectSource(log_source):
    ACCEPTED = ['modeltest', 'batch', 'forwarder']

    try:
        indexofdash = log_source.rfind('-')
        source = log_source[indexofdash+1:] if indexofdash else log_source

        for proposed in ACCEPTED:
            if proposed in source:
                return proposed
    except:
        pass
        
    return None


def extract_cloud(logfilepath, result):
    f_size = os.path.getsize(logfilepath)
    if f_size < 1024:
        result['strange'] = 'Cloudwatch logs missing'
        return

    ERROR_STAGE = 99

    batch_models = 0
    compute = { }

    jobs_in_stage = { ERROR_STAGE: { } }
    lambda_runners = { ERROR_STAGE: { } }
    lambda_failers = { ERROR_STAGE: { } }
    for stageNum in range(len(result['stages'])):
        jobs_in_stage[stageNum] = { }
        lambda_runners[stageNum] = { }
        lambda_failers[stageNum] = { }

    for line in open(logfilepath):
        logfields = line.rstrip().split(' ', 3)

        stage_index = FitLogTimestampToStage(logfields[2], result['stages'])
        stage_index = ERROR_STAGE if stage_index == None else stage_index
        source = DetectSource(logfields[0])

        if 'REPORT' in logfields[3]:
            regex_pattern = r"REPORT RequestId: [a-z0-9-]{36}\sDuration: ([0-9\.]+) ms"
            if logfields[0] == '/aws/batch/job':
                # CRITICAL | REPORT RequestId: abb650b3-9226-432b-9db8-db2a3a201e55 Duration: 33442 ms
                regex_pattern = r"CRITICAL \| " + regex_pattern
                batch_models += 1
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

            jobs_in_stage[stage_index][source] = jobs_in_stage.get(stage_index, { }).get(source, 0) + 1


        if logfields[0].startswith('/aws/lambda'):
            # lambda container id
            lcid = logfields[1].index(']')+1
            lcid = logfields[1][lcid:]

            if logfields[3].startswith('START'):
                lcid += '|' + logfields[3].split(' ')[2]
                lambda_runners[stage_index].setdefault(source, set()).add(lcid)

            elif 'Task timed out after' in logfields[3]:
                lcid += '|' + logfields[3].split(' ',2)[1]
                lambda_failers[stage_index].setdefault(source, set()).add(lcid)
    
    
    for stageNum in [x for x in jobs_in_stage.keys() if x < ERROR_STAGE]:

        for kind in jobs_in_stage[stageNum].keys():
            result['stages'][stageNum][kind] = jobs_in_stage[stageNum][kind]
            
            # if some lambdas failed we have to count differently due to retries (mostly due to time-outs)
            if  kind in lambda_failers[stageNum].keys():
                lambda_success = lambda_runners[stageNum][kind] - lambda_failers[stageNum][kind]
                result['stages'][stageNum][kind] = len(lambda_success)

        check = result['stages'][stageNum].get('batch',0) + result['stages'][stageNum].get('modeltest',0)
        exp = result['stages'][stageNum]['models']
        #assert abs(check - exp) <= math.ceil(exp * 0.1) , "Models don't match [Exp: {} | Act: {} | Stage: {}]".format(exp, check, stageNum)
        if abs(check - exp) >= math.ceil(exp * 0.1) and result['runtime'].total_seconds() > 0:
            result['strange'] = "Models don't match\tAct: {} | Exp: {} | Stage: {}".format(check, exp, stageNum)
    

    # distinct lambda execution units
    lambda_executors = len(
        { runid[:runid.rfind('|')] for stage 
            in lambda_runners for runid 
                in lambda_runners[stage].get('modeltest', []) }
    )

    # convert compute units from ms to timedelta
    compute = {kind: datetime.timedelta(milliseconds=compute[kind]) for kind in compute}

    # jobs which didn't fit in any stage
    orphaned_jobs = sum([ jobs_in_stage[ERROR_STAGE][kind] for kind in jobs_in_stage.get(ERROR_STAGE, []) ])


    result['l-cpus'] = lambda_executors
    result['total_batch'] = batch_models
    result['total_orphans'] = orphaned_jobs 
    result['total_compute'] = compute.get('raw', 0)
    result['compute'] = compute

    if not 'strange' in result:
        if orphaned_jobs > 0:
            result['strange'] = 'Orphaned jobs\t{}'.format(orphaned_jobs)
        if batch_models > result['total_models']:
            result['strange'] = 'More batch than Models\t{}/{}'.format(batch_models, result['total_models'])
    
    pass


def to_str(obj):
    if isinstance(obj, (datetime.timedelta)):
        hours, remainder = divmod(obj.total_seconds(), 3600)
        minutes, seconds = divmod(remainder, 60)
        days, hours = divmod(hours, 24)

        baseformat = '{1:02}:{2:02}:{3:02}'
        if (days != 0):
            baseformat = '{0:}.' + baseformat
        
        formatted = baseformat.format(int(days),int(hours),int(minutes), int(seconds))
        return formatted
    return str(obj)


## -------------------------------- ##


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
            to_str(log_data['runtime']),
            str(log_data.get('l-cpus', 0)),
            str(log_data['l-power']),
            str(log_data['l-timeout']),
            str(log_data['b-cpus']),
            str(log_data['total_batch'] / log_data['total_models'] if 'total_batch' in log_data.keys() else 0.0),
            to_str(log_data['total_compute'] if 'total_compute' in log_data.keys() else 0),
            log_data.get('strange', '')
            #json.dumps(log_data, default=to_str) # indent=4, sort_keys=True,
        ]
        print('\t'.join(result))
    except AssertionError as ase:
        print('###       {}       ###  assertion: {}'.format(subdir, str(ase)))
    except Exception as ex:
        print('###       {}       ### {}'.format(subdir, str(ex)))
    pass
