import json
import time
import boto3
REQUEST_QUEUE_URL='https://sqs.us-east-1.amazonaws.com/953341995177/requestQueue'
RESPONSE_QUEUE_URL='https://sqs.us-east-1.amazonaws.com/953341995177/responseQueue'
AWS_ACCESS_KEY_ID = 'AKIA53544NCU67C3P65Z'
# os.environ.get('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = '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'
AWS_REGION_NAME = 'us-east-1'
results = []
# print(f"le: {le}")
sqs = boto3.client('sqs', 
                aws_access_key_id=AWS_ACCESS_KEY_ID, 
                aws_secret_access_key=AWS_SECRET_ACCESS_KEY, 
                region_name=AWS_REGION_NAME) 
while True:
    messages = sqs.receive_message(QueueUrl=RESPONSE_QUEUE_URL,MaxNumberOfMessages=10)
    print(messages)
    if 'Messages' in messages:
        message=messages['Messages']
        for m in message:
            message_body = json.loads(m['Body'])
            print(m)

            image_name = message_body['image_name']
            result = message_body['result']
            results.append((image_name, result))
            # delete the message from the queue
            sqs.delete_message(QueueUrl=RESPONSE_QUEUE_URL,ReceiptHandle=m['ReceiptHandle'])
            # m=sqs.receive_message(QueueUrl=RESPONSE_QUEUE_URL,MaxNumberOfMessages=10, MessageAttributeNames=['All'])
            # print(m)
            print("******** Deleting from Response Queue ********")
    else:
        print(" Waiting for reamning results")
        time.sleep(5)
# print(" ********** Produced all Results *************")