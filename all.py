import boto3
import json
import string
import concurrent.futures
from datetime import datetime


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
    
    scan_response = dynamo_table.scan(
        Select='COUNT',
        ConsistentRead=True
    )
    assert(scan_response['ResponseMetadata']['HTTPStatusCode'] == 200)

    dynamo_table.delete()
    dynamo_table = None

    if scan_response['Count'] > 0:
        raise Exception("Table still had itens!")


def collect():
    pass


def sendmessages(i):

    with open('cmdlines.log','r',1048576,'UTF-8') as cmds:
        for line in cmds:

            if (len(line) < 1):
                continue

            if line == "$$ STAGE COMPLETE $$":
                collect()
                continue

            path, *args = line.rstrip('\n').split(' -',maxsplit=2)[1:]
            message = { "path": "{}:/{}".format(s3bucket, path[2:]), "cmd": '-'+args[0] }
            msg_obj = {'default': json.dumps(message)}
            snsmessage = json.dumps(msg_obj)

            # print(snsmessage)

            response = sns_cli.publish(
                TopicArn=topicarn,
                Subject=runid,
                Message=snsmessage,
                MessageStructure='json'
            )
            assert(response['ResponseMetadata']['HTTPStatusCode'] == 200)
            # print(response)
    print(i)


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
    #sendmessages(0)
    test_dynamo()

finally:
    dynamo_clear()

print('-- DONE -- ' + runid)