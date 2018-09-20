import boto3
import json
import string


s3bucket = 'mestrado-dev-phyml-input'

cli = boto3.client('sns')
topics = cli.list_topics()
topicarn = topics['Topics'][0]['TopicArn']

with open('cmdlines.log','r',True,'UTF-8') as cmds:
    for line in cmds:
        path, *args = line.rstrip('\n').split(' -',maxsplit=2)[1:]
        message = { "path": "{}:/{}".format(s3bucket, path[2:]), "cmd": '-'+args[0] }
        msg_obj = {'default': json.dumps(message)}
        snsmessage = json.dumps(msg_obj)

        # print(snsmessage)

        response = cli.publish(
            TopicArn=topicarn,
            Subject='run1',
            Message=snsmessage,
            MessageStructure='json'
        )
        # print(response)

print('-- DONE --')