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
import threading
import concurrent.futures
import backoff
from timeit import default_timer as timer
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


s3_cli = boto3.client('s3')
sns_cli = boto3.client('sns')
dyn_cli = boto3.client('dynamodb')
batch_cli = boto3.client('batch')
dynamo_res = boto3.resource('dynamodb')
dynamo_table = None


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
            'ReadCapacityUnits':  3,
            'WriteCapacityUnits': 3
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

            if BENCH_EXECUTION_MODE == 0 or BENCH_EXECUTION_MODE == 1:

                sns_response = sns_cli.publish(
                    TopicArn=STACK_OUTPUTS['inputtopic'],
                    Subject=MSG_SUBJECT,
                    Message=sns[i],
                    MessageStructure='json'
                )
                assert sns_response['ResponseMetadata']['HTTPStatusCode'] == 200

            elif BENCH_EXECUTION_MODE == 2:

                payload = json.loads(json.loads(sns[i])['default'])
                payload['jmodeltestrunid'] = MSG_SUBJECT
                payload['sourcerequestid'] = dynamo[i].replace('+','-')
                b_response = batch_cli.submit_job(
                    jobName       = payload['sourcerequestid'],
                    jobDefinition = STACK_OUTPUTS['batchjobdefinition'],
                    jobQueue      = STACK_OUTPUTS['batchjobqueue'],
                    parameters    = payload
                )
                assert b_response['ResponseMetadata']['HTTPStatusCode'] == 200

            else:
                raise Exception
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


    if BENCH_EXECUTION_MODE == 2:
        b_response = batch_cli.describe_compute_environments(computeEnvironments=[STACK_OUTPUTS['batchcomputeenv']])
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
            b_response = batch_cli.update_compute_environment(
                computeEnvironment=STACK_OUTPUTS['batchcomputeenv'],
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

    if BENCH_EXECUTION_MODE == 1:
        if retry_firstattempt == None:
            retry_firstattempt = datetime.now()
        else:
            diff = datetime.now() - retry_firstattempt
            if diff > RETRY_MAXWAIT:
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


def delete_logs():
    delete_pattern = "aws logs delete-log-group --log-group-name {0} && aws logs create-log-group --log-group-name {0}"

    logging.warn('removing cloudwatch logs')

    logs = discover_logs()
    cmds = list(map(lambda logname: delete_pattern.format(logname.rstrip()), logs))
    cmds_exec = ' && '.join(cmds)

    os.system(cmds_exec)

    logging.info("ok!")
    pass


def discover_logs():
    shell_discover = "aws logs describe-log-groups --output text | awk '{ print $4 }'"

    f = os.popen(shell_discover)
    logs = f.readlines()
    return logs


def save_logs():
    logging.warn('uploading cloudwatch logs')

    logs = discover_logs()
    upload_logs(logs)

    logging.info("ok!")
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

    for list_filename in os.listdir('{}/'.format(dirname)):
        dst_file = '{}/{}/{}'.format(MSG_SUBJECT, dirname, list_filename)
        list_filename = '{}/{}'.format(dirname,list_filename)

        s3_cli.upload_file(
            list_filename,
            STACK_OUTPUTS['inputbucket'],
            dst_file,
            ExtraArgs={
                'ContentType': "text/plain"
            }
        )
    pass


@backoff.on_predicate(backoff.constant, interval=60)
def cooldown(dt_first, scm):
    logging.info("COOLDOWN: started with {}".format(dt_first))

    if BENCH_EXECUTION_MODE != 1:
        try:
            b_response = batch_cli.describe_compute_environments(computeEnvironments=[STACK_OUTPUTS['batchcomputeenv']])
            assert b_response['ResponseMetadata']['HTTPStatusCode'] == 200, "Bad response from Batch.Describe_ComputeEnvironments"

            envdata = b_response['computeEnvironments'][0]
            minimum = envdata['computeResources']['minvCpus']
            desired = envdata['computeResources']['desiredvCpus']
            
            #logging.info("COOLDOWN: {} vCPUs Running".format(desired))
            if desired != minimum:
                return False
        except:
            logging.error("Error obtaining Batch CPUs")
            pass


    diff_s = dt_first + RETRY_MAXWAIT - datetime.now()
    diff_s = diff_s.total_seconds()

    # no point in collecting scaling data anymore
    logging.info("COOLDOWN: Joining SCM thread")
    scm['Event'].set()
    scm['Thread'].join(timeout=diff_s)

    if (datetime.now() - dt_first) < RETRY_MAXWAIT and not BENCH_EXECUTION_MODE == 2:
        logging.info("COOLDOWN: waiting until {}".format(dt_first + RETRY_MAXWAIT))
        time.sleep(diff_s)

    logging.info('ok!')
    return True


def s3_upload():
    src_file = 'traces/{}.phy'.format(TRACE_FILE)
    dst_file = 'inputfiles/{}.phy'.format(MSG_SUBJECT)

    logging.warn('uploading to S3 {}://{}'.format(STACK_OUTPUTS['inputbucket'], dst_file))

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


def create_scaling_monitor(compute_env, log_interval=60):

    t_event = threading.Event()
    backtask = threading.Thread(
        target=log_scaling_behavior,
        args=(
            t_event,
            compute_env[compute_env.rfind('/')+1:],
            log_interval
        ),
        daemon=True
    )
    backtask.start()

    return { 'Thread': backtask, 'Event': t_event}


def log_scaling_behavior(t_event, compute_env, log_interval):
    log = logging.getLogger('SCM')
    log.warn('start')

    scm_batch_cli = boto3.client('batch')
    compute_env = scm_batch_cli.describe_compute_environments(
        computeEnvironments=[compute_env]
    )
    tags_to_look_for = compute_env['computeEnvironments'][0]['computeResources']['tags']
    ec2_filters = {"tag:{}".format(x): tags_to_look_for[x] for x in tags_to_look_for}

    filters = []
    for t in ec2_filters:
        obj = {
            'Name': t,
            'Values': [
                ec2_filters[t]
            ]
        }
        filters.append(obj)
    log.info('ok!')

    scm_ec2_cli = boto3.client('ec2')
    while not t_event.wait(log_interval):
        try:
            instances = scm_ec2_cli.describe_instances(Filters=filters).get('Reservations', [])  
            cpus = sum([y['CpuOptions']['CoreCount'] * y['CpuOptions']['ThreadsPerCore'] for x in instances for y in x['Instances'] if y['State']['Name'] != 'terminated'])
            log.info('{} vCPUs Running'.format(cpus))
        except Exception as ex:
            log.exception(ex)
    
    log.info('stop')


# --- VAI COMEÇAR A BAIXARIA --- # 

if __name__ == '__main__':
    logging.error("BORA FICAR MONSTRO!")

    TRACE_FILE = argv(1, "aP6")

    # 0 - MIXED
    # 1 - LAMBDA ONLY
    # 2 - BATCH ONLY
    BENCH_EXECUTION_MODE = int(argv(2, 0))


    RUNDATE = datetime.now().isoformat()[0:19].replace(' ', '').replace(':', '-')
    MSG_SUBJECT = "{}_{}".format(
        RUNDATE,
        TRACE_FILE
    )


    STACK_OUTPUTS = get_variables()
    RETRY_MAXWAIT = timedelta(seconds=int(STACK_OUTPUTS['lambdatimeout']) * 10)


    # INITIALIZATION
    dynamo_create()
    phy_file = s3_upload()
    delete_logs()
    scaling_monitor = create_scaling_monitor(STACK_OUTPUTS['batchcomputeenv'])


    # HOTNESS STARTS HERE
    try:
        start = timer()

        sendmessages(phy_file)

        duration = (timer() - start)
        duration = int(duration * 1000)

        logging.critical("REPORT Duration: {} ms".format(duration))

    except Exception as caught:
        logging.exception(caught)


    # WRAP UP
    try:
        dt_now = retry_firstattempt or datetime.now()

        cooldown(dt_now, scaling_monitor)
        dynamo_clear()

    except Exception as e_wrap:
        logging.exception(e_wrap)

    finally:
        save_logs()


    logging.error('-- DONE -- ' + MSG_SUBJECT)
