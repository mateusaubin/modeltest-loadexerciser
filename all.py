import boto3
import json

cli = boto3.client('sns')
topics = cli.list_topics()
topicarn = topics['Topics'][0]['TopicArn']

with open('small.json','r',True,'UTF-8') as fin:
    message = fin.read()

snsmessage = json.dumps({'default': message})

response = cli.publish(
    TopicArn=topicarn, 
    Message=snsmessage,
    MessageStructure='json'
)
print(response)
