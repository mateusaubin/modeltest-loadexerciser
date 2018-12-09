#!/usr/bin/env python3
import os
import io
import sys
import math
import time
import boto3
import json
import string
import logging
import concurrent.futures
import backoff
from datetime import datetime
from datetime import timedelta

logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] - %(name)s - %(levelname)-8s - %(message)s',
    handlers=[
        logging.FileHandler("log/self.txt", mode='w'),
        logging.StreamHandler()
    ]
)
OTHERS_LEVEL = logging.WARNING

logging.getLogger('nose').setLevel(OTHERS_LEVEL)
logging.getLogger('boto3').setLevel(OTHERS_LEVEL)
logging.getLogger('urllib3').setLevel(OTHERS_LEVEL)
logging.getLogger('botocore').setLevel(OTHERS_LEVEL)
logging.getLogger('s3transfer').setLevel(OTHERS_LEVEL)
logging.getLogger('asyncio').setLevel(OTHERS_LEVEL)
logging.getLogger('backoff').setLevel(OTHERS_LEVEL)


def dynamo_create():
    logging.warn('creating dynamo table: {}'.format(MSG_SUBJECT))

    global dynamo_table
    
    dynamo_table = dynamo_res.create_table(
        TableName=MSG_SUBJECT,
        AttributeDefinitions=[
            {
                'AttributeName': 'Model',
                'AttributeType': 'S'
            },
        ],
        KeySchema=[
            {
                'AttributeName': 'Model',
                'KeyType': 'HASH'
            },
        ],
        ProvisionedThroughput={
            'ReadCapacityUnits':  2,
            'WriteCapacityUnits': 2
        }
    )
    dynamo_table.wait_until_exists()
    logging.info('ok!')


def dynamo_clear():
    global dynamo_table

    if dynamo_table == None:
        logging.warn('dynamodb disposal not needed')
        return

    table_entries = dynamo_countentries()

    logging.warn('disposing dynamodb resources')
    dynamo_table.delete()

    if table_entries > 0:
        logging.error("Table still had {} itens!".format(table_entries))
    
    # dynamo_table.wait_until_not_exists()
    dynamo_table = None

    logging.info('ok!')


def dynamo_countentries():
    scan_response = dynamo_table.scan(
        Select='COUNT',
        ConsistentRead=True
    )
    assert scan_response['ResponseMetadata']['HTTPStatusCode'] == 200

    return scan_response['Count']


def dispatch_parallel(index, sns, dynamo):
    logging.debug('received: #{} | sns=[{}] | dynamo=[{}]'.format(index,len(sns),len(dynamo)))

    dyn_cli = boto3.client('dynamodb')
    b_cli = boto3.client('batch')

    for i in range(0, len(sns)):
        try:
            dynamo_response = dyn_cli.put_item(
                TableName=dynamo_table.name,
                Item={
                    'Model': {
                        'S': dynamo[i]
                    }
                }
            )
            assert dynamo_response['ResponseMetadata']['HTTPStatusCode'] == 200

            payload = json.loads(json.loads(sns[i])['default'])
            payload['jmodeltestrunid'] = MSG_SUBJECT
            payload['sourcerequestid'] = dynamo[i].replace('+','-')
            b_response = b_cli.submit_job(
                jobName       = payload['sourcerequestid'],
                jobDefinition = BATCH_JOBDEF,
                jobQueue      = BATCH_QUEUE,
                parameters    = payload
            )
            assert b_response['ResponseMetadata']['HTTPStatusCode'] == 200
        except:
            logging.exception("dispatch_parallel")
    
    pass


def dispatch(stage_data):
    global retry_lastcount

    assert len(stage_data['sns']) == len(stage_data['dynamo']), "Dynamo and SNS lists must be of the same length"

    maxdop = os.cpu_count() * 2
    maxlen = len(stage_data['sns'])
    size = min(maxdop,maxlen)
    retry_lastcount = maxlen
    
    dynamo_range = list(stage_data['dynamo'][i::size] for i in range(size))
    sns_range    = list(stage_data['sns'][i::size]    for i in range(size))

    with concurrent.futures.ThreadPoolExecutor(max_workers=size) as executor:
        futures = []
        for i in range(0, size):
            promise = executor.submit(dispatch_parallel, i, sns_range[i], dynamo_range[i])
            futures.append(promise)

        concurrent.futures.wait(futures)


    b_cli = boto3.client('batch')
    b_response = b_cli.describe_compute_environments(computeEnvironments=[BATCH_COMPUTE])
    assert b_response['ResponseMetadata']['HTTPStatusCode'] == 200, "Bad response from Batch.Describe_ComputeEnvironments"

    envdata = b_response['computeEnvironments'][0]
    desired = envdata['computeResources']['desiredvCpus']
    maximum = envdata['computeResources']['maxvCpus']

    runnableToCpuRatio=(3/4)
    gross_new_cpu = (maxlen) * runnableToCpuRatio
    net_new_cpu = math.ceil(gross_new_cpu / 2.0) * 2    # rounded to nearest even number
    new_cpus = min(maximum, net_new_cpu)

    if (desired < new_cpus):
        logging.warn("Triggering update to '{}' CPUs in ComputeEnvironment".format(new_cpus))
        b_response = b_cli.update_compute_environment(
            computeEnvironment=BATCH_COMPUTE,
            computeResources={
                'desiredvCpus': new_cpus
            }
        )
        assert b_response['ResponseMetadata']['HTTPStatusCode'] == 200
    pass

retry_lastcount = 0
retry_firstattempt = None
#@backoff.on_predicate(backoff.constant, interval=5, jitter=None)
@backoff.on_predicate(backoff.fibo, max_value=5, jitter=None)
def collect():
    global retry_lastcount
    global retry_firstattempt

    table_entries = dynamo_countentries()
    if table_entries != retry_lastcount:
        logging.info("Worklist Update: {} ({})".format(table_entries, table_entries - retry_lastcount))
        retry_lastcount = table_entries
        retry_firstattempt = None

    if KILL_ON_TIMEOUT:
        if retry_firstattempt == None:
            retry_firstattempt = datetime.now()
        else:
            diff = datetime.now() - retry_firstattempt
            retry_maxwait = timedelta(seconds=int(STACK_OUTPUTS['lambdatimeout'])*5)
            if diff > retry_maxwait:
                logging.error("Tired of waiting for a response... aborting! ({})".format(str(diff)))
                raise EnvironmentError("Tired of waiting")

    return table_entries == 0


def handle_stage(stage_num, stage_data):

    logging.warn('dispatching requests of stage #{} [{}]'.format(stage_num, len(stage_data['sns'])))
    dispatch(stage_data)

    logging.warn('collecting results of stage #{}'.format(stage_num))
    collect()

    stage_data['dynamo'].clear()
    stage_data['sns'].clear()

    logging.info('ok!')


def sendmessages(phy_file):

    with io.open('traces/{}_cmds.log'.format(TRACE_FILE), mode='r', buffering=1048576, encoding='UTF-8', newline=None) as cmds:
        stage_data = { 
            'sns': [], 
            'dynamo': [] 
        }
        stage_count = 1

        for line in cmds:
            if '/tmp/jmodeltest' in line:
                raise Exception('bad trace file!')
            
            line = line.rstrip().replace('/path/data.phy', phy_file)

            if len(line) == 0 or line.isspace():
                continue

            if line == "$$ STAGE COMPLETE $$":
                handle_stage(stage_count, stage_data)
                stage_count += 1
                continue

            path, *args = line.rstrip().lstrip('/').split(' -', maxsplit=2)[1:]
            message = {
                "path": "{}://{}".format(STACK_OUTPUTS['inputbucket'], path[2:]), 
                "cmd": '-' + args[0]
            }
            msg_obj = {'default': json.dumps(message)}
            snsmessage = json.dumps(msg_obj)
            modelname = args[0].split('--run_id')[1].split()[0]

            logging.debug(snsmessage)
            
            stage_data['dynamo'].append(modelname)
            stage_data['sns'].append(snsmessage)

        # one last execution if there's anything left in the buffers
        if len(stage_data['sns']) > 0:
            handle_stage(stage_count, stage_data)
    pass


def reset_logs():
    shell_discover = "aws logs describe-log-groups --output text | awk '{ print $4 }'"
    delete_pattern = "aws logs delete-log-group --log-group-name {0} && aws logs create-log-group --log-group-name {0}"

    # sleep to let logs flush so we don't miss recent events
    time.sleep(1*60)

    f = os.popen(shell_discover)
    logs = f.readlines()

    upload_logs(logs)

    cmds = list(map(lambda logname: delete_pattern.format(logname.rstrip()), logs))
    cmds_exec = ' && '.join(cmds)

    os.system(cmds_exec)
    pass


def upload_logs(logs):
    dirname = 'log'
    filename = 'cloudwatch.txt'
    output_pattern = "mkdir {} ; ({}) | sort -u -k 3 > {}/{}"
    awslogs_pattern = "awslogs get {} --timestamp --start='2w' --no-color"

    cmds = list(map(lambda logname: awslogs_pattern.format(logname.rstrip()), logs))
    inner_cmd = ' ; '.join(cmds)

    exec_cmd = output_pattern.format(dirname, inner_cmd, dirname, filename)
    os.system(exec_cmd)

    s3_cli = boto3.client('s3')
    for list_filename in os.listdir('{}/'.format(dirname)):
        dst_file = '{}/{}/{}'.format(MSG_SUBJECT, dirname, list_filename)
        list_filename = '{}/{}'.format(dirname,list_filename)

        s3_cli.upload_file(
            list_filename,
            STACK_OUTPUTS['inputbucket'],
            dst_file
        )
    pass


def s3_upload():
    src_file = 'traces/{}.phy'.format(TRACE_FILE)
    dst_file = 'inputfiles/{}.phy'.format(MSG_SUBJECT)

    logging.warn('uploading to S3 {}://{}'.format(STACK_OUTPUTS['inputbucket'], dst_file))

    s3_cli = boto3.client('s3')
    s3_cli.upload_file(
        src_file,
        STACK_OUTPUTS['inputbucket'],
        dst_file
    )

    logging.info('ok!')

    return dst_file


def argv(index, default):
    return (sys.argv[index:index+1]+[default])[0]


def get_variables():
    cfn_cli = boto3.client('cloudformation')

    response = cfn_cli.describe_stacks(
        StackName='mestrado-dev'
    )
    assert response['ResponseMetadata']['HTTPStatusCode'] == 200

    STACK_OUTPUTS = { i['OutputKey'] : i['OutputValue'] for i in response['Stacks'][0]['Outputs']}
    logging.info(json.dumps(STACK_OUTPUTS))

    return STACK_OUTPUTS


# --- VAI COMEÇAR A BAIXARIA --- # 


logging.error("BORA FICAR MONSTRO!")

RUNDATE = datetime.now().isoformat()[0:19].replace(' ', '').replace(':', '-')
TRACE_FILE = argv(1, "aP6")
KILL_ON_TIMEOUT = False
BATCH_COMPUTE = argv(3, "")
BATCH_QUEUE = argv(4, "")
BATCH_JOBDEF = argv(5, "")

#S3_BUCKET_INPUT = 'mestrado-dev-phyml'
MSG_SUBJECT = "{}_{}".format(
    RUNDATE,
    TRACE_FILE
)
#MSG_SUBJECT = "VMware-aP6"

#sns_cli = boto3.client('sns')
dynamo_res = boto3.resource('dynamodb')
dynamo_table = None

from timeit import default_timer as timer

try:
    STACK_OUTPUTS = get_variables()
    dynamo_create()
    phy_file = s3_upload()

    start = timer()

    # hotness
    sendmessages(phy_file)

    duration = (timer() - start)
    duration = int(duration * 1000)

    logging.critical("REPORT Duration: {} ms".format(duration))

finally:
    reset_logs()
    dynamo_clear()

logging.error('-- DONE -- ' + MSG_SUBJECT)
