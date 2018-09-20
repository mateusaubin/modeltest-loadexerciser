import boto3
import json
import string
import concurrent.futures
from datetime import datetime


s3bucket = 'mestrado-dev-phyml-input'
runid = datetime.now().isoformat()[0:19]

cli = boto3.client('sns')
topics = cli.list_topics()
topicarn = topics['Topics'][0]['TopicArn']

def sendmessages(i):
    with open('cmdlines.log','r',1048576,'UTF-8') as cmds:
        for line in cmds:
            path, *args = line.rstrip('\n').split(' -',maxsplit=2)[1:]
            message = { "path": "{}:/{}".format(s3bucket, path[2:]), "cmd": '-'+args[0] }
            msg_obj = {'default': json.dumps(message)}
            snsmessage = json.dumps(msg_obj)

            # print(snsmessage)

            response = cli.publish(
                TopicArn=topicarn,
                Subject=runid,
                Message=snsmessage,
                MessageStructure='json'
            )
            assert(response['ResponseMetadata']['HTTPStatusCode'] == 200)
            # print(response)
    print(i)


with concurrent.futures.ThreadPoolExecutor() as executor:
    for i in range(0, 5):
        executor.submit(sendmessages, i)
    print('submitted = ' + runid)

#sendmessages()
print('-- DONE -- ' + runid)