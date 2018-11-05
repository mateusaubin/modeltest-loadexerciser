import io
import boto3
import json
import string
import logging
import concurrent.futures
import backoff
from datetime import datetime

logging.basicConfig(
    level=logging.DEBUG,
    format='[%(asctime)s] - %(name)s - %(levelname)-8s - %(message)s'
)
OTHERS_LEVEL = logging.WARNING

logging.getLogger('nose').setLevel(OTHERS_LEVEL)
logging.getLogger('boto3').setLevel(OTHERS_LEVEL)
logging.getLogger('urllib3').setLevel(OTHERS_LEVEL)
logging.getLogger('botocore').setLevel(OTHERS_LEVEL)
logging.getLogger('s3transfer').setLevel(OTHERS_LEVEL)
logging.getLogger('asyncio').setLevel(OTHERS_LEVEL)

logging.warn("BORA FICAR MONSTRO!")

s3bucket = 'mestrado-dev-phyml-input'
runid = "LOCALTEST_" + (datetime.now().isoformat()[0:19].replace(' ', '').replace(':', '-'))
#runid = "VMware-aP6"

sns_cli = boto3.client('sns')
dynamo_res = boto3.resource('dynamodb')
topics = sns_cli.list_topics()
topicarn = topics['Topics'][0]['TopicArn']
dynamo_table = None

def dynamo_create():
    global dynamo_table
    dynamo_table = dynamo_res.create_table(
        TableName=runid,
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
            'ReadCapacityUnits': 2,
            'WriteCapacityUnits': 2
        }
    )
    dynamo_table.wait_until_exists()


def dynamo_clear():
    global dynamo_table

    if dynamo_table == None:
        return
    
    table_entries = dynamo_countentries()

    dynamo_table.delete()
    dynamo_table = None

    if table_entries > 0:
        raise Exception("Table still had itens!")


def dynamo_countentries():
    scan_response = dynamo_table.scan(
        Select='COUNT',
        ConsistentRead=True
    )
    assert(scan_response['ResponseMetadata']['HTTPStatusCode'] == 200)
    return scan_response['Count']


@backoff.on_predicate(backoff.expo, max_value=300)
def collect():
    table_entries = dynamo_countentries()
    return table_entries == 0


def sendmessages(i):

    with io.open('cmdlines.log',mode='r',buffering=1048576,encoding='UTF-8',newline=None) as cmds:
        dynamo_writer = dynamo_table.batch_writer()
        dynamo_writer.__enter__()
        for line in cmds:
            line = line.rstrip()

            if len(line) == 0 or line.isspace():
                continue

            if line == "$$ STAGE COMPLETE $$":
                unused_anything = 0
                dynamo_writer.__exit__(unused_anything, unused_anything, unused_anything)

                collect()

                dynamo_writer = dynamo_table.batch_writer()
                dynamo_writer.__enter__()
                continue

            path, *args = line.rstrip('\n').split(' -',maxsplit=2)[1:]
            message = { "path": "{}:/{}".format(s3bucket, path[2:]), "cmd": '-' + args[0] }
            msg_obj = {'default': json.dumps(message)}
            snsmessage = json.dumps(msg_obj)

            logging.debug(snsmessage)

            modelname = args[0].split('--run_id')[1].split()[0]
            dynamo_writer.put_item(Item={
                'Model': modelname
            })

            # response = sns_cli.publish(
            #     TopicArn=topicarn,
            #     Subject=runid,
            #     Message=snsmessage,
            #     MessageStructure='json'
            # )
            # assert(response['ResponseMetadata']['HTTPStatusCode'] == 200)
            # print(response)
    pass


def test_dynamo():
    data = ["GTR", "SYM+I", "GTR+I", "SYM", "GTR+G", "SYM+I+G", "SYM+G", "GTR+I+G"]

    dynamo_cli = boto3.client('dynamodb')

    with dynamo_table.batch_writer() as batch:
        for model in data:
            batch.put_item(Item={
                'Model': model
            })
        pass
    
    for model in data:
        del_response = dynamo_cli.delete_item(
            TableName=runid,
            Key={
                'Model': {
                    'S': model
                }
            }
        )
        assert(del_response['ResponseMetadata']['HTTPStatusCode'] == 200)


#with concurrent.futures.ThreadPoolExecutor() as executor:
#    for i in range(0, 5):
#        executor.submit(sendmessages, i)
#    print('submitted = ' + runid)

try:
    dynamo_create()
    sendmessages(0)
    #test_dynamo()

finally:
    dynamo_clear()

logging.warn('-- DONE -- ' + runid)