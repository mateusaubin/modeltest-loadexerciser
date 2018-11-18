#!/usr/bin/env python3
import os
import io
import sys
import boto3
import json
import string
import logging
import concurrent.futures
import backoff
from datetime import datetime

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

    logging.warn('disposing dynamodb resources')

    table_entries = dynamo_countentries()

    dynamo_table.delete()
    dynamo_table = None

    if table_entries > 0:
        logging.error("Table still had {} itens!".format(table_entries))

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

    sns_cli = boto3.client('sns')
    dyn_cli = boto3.client('dynamodb')

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

            sns_response = sns_cli.publish(
                TopicArn=STACK_OUTPUTS['inputtopic'],
                Subject=MSG_SUBJECT,
                Message=sns[i],
                MessageStructure='json'
            )
            assert sns_response['ResponseMetadata']['HTTPStatusCode'] == 200
        except:
            logging.error(sys.exc_info()[0])
    
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
    pass

retry_lastcount = 0
#@backoff.on_predicate(backoff.constant, interval=5, jitter=None)
@backoff.on_predicate(backoff.fibo, max_value=5, jitter=None)
def collect():
    global retry_lastcount

    table_entries = dynamo_countentries()
    if table_entries != retry_lastcount:
        logging.info("Worklist Update: {} ({})".format(table_entries, table_entries - retry_lastcount))
        retry_lastcount = table_entries

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


def test_dynamo():
    data = ["GTR", "SYM+I", "GTR+I", "SYM",
            "GTR+G", "SYM+I+G", "SYM+G", "GTR+I+G"]

    dynamo_cli = boto3.client('dynamodb')

    with dynamo_table.batch_writer() as batch:
        for model in data:
            batch.put_item(Item={
                'Model': model
            })
        pass

    for model in data:
        del_response = dynamo_cli.delete_item(
            TableName=MSG_SUBJECT,
            Key={
                'Model': {
                    'S': model
                }
            }
        )
        assert(del_response['ResponseMetadata']['HTTPStatusCode'] == 200)


def reset_logs():
    shell_pattern = "aws logs delete-log-group --log-group-name {0} && aws logs create-log-group --log-group-name {0}"
    shell_discover = "aws logs describe-log-groups --output text | awk '{ print $4 }'"

    f = os.popen(shell_discover)
    logs = f.readlines()

    cmds = list(map(lambda logname: shell_pattern.format(logname.rstrip()), logs))
    cmds_exec = ' && '.join(cmds)

    os.system(cmds_exec)
    pass


def upload_logs():
    dirname = 'log'
    filename = 'cloudwatch.txt'

    shell_cmd = "mkdir {} ; (awslogs get /aws/lambda/mestrado-dev-modeltest --no-color --timestamp --start='2w' ; awslogs get /aws/lambda/mestrado-dev-forwarder --no-color --timestamp --start='2w' ; awslogs get /aws/batch/job --no-color --timestamp --start='2w' ; ) | sort -u -k 3 > {}/{}"
    os.system(shell_cmd.format(dirname,dirname,filename))

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

RUNDATE = datetime.now().isoformat()[0:16].replace(' ', '').replace(':', '-')
TRACE_FILE = argv(1, "aP6")

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
    phy_file = s3_upload()
    dynamo_create()

    start = timer()

    # hotness
    sendmessages(phy_file)

    duration = (timer() - start)
    duration = int(duration * 1000)

    logging.critical("REPORT Duration: {} ms".format(duration))

    upload_logs()
    reset_logs()

finally:
    dynamo_clear()

logging.error('-- DONE -- ' + MSG_SUBJECT)
