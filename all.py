import boto3
import json

cli = boto3.client('sns')
topics = cli.list_topics()
topicarn = topics['Topics'][0]['TopicArn']

message = { "path":"mestrado-dev-phyml-input://small.aP6.phy", "--run_id":"testrun", "--help":None }


response = cli.publish(
    TopicArn=topicarn, 
    Message=json.dumps({'default': json.dumps(message)}),
    MessageStructure='json'
)
print(response)
