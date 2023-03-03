import base64
import json
import os
import subprocess
import boto3
from my_logger import logger

logger.info(" Logging info for app tier")

REQUEST_QUEUE_URL = os.environ.get('REQUEST_QUEUE_URL')
RESPONSE_QUEUE_URL = os.environ.get('RESPONSE_QUEUE_URL')
AWS_ACCESS_KEY_ID = os.environ.get('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = os.environ.get('AWS_SECRET_ACCESS_KEY')
AWS_REGION_NAME = os.environ.get('us-east-1')
INPUT_BUCKET_NAME = 'cloudbustersinputbucket'
OUTPUT_BUCKET_NAME = 'cloudbustersoutputbucket'
print(AWS_ACCESS_KEY_ID,AWS_SECRET_ACCESS_KEY,AWS_REGION_NAME)
sqs = boto3.client('sqs', 
                   aws_access_key_id=AWS_ACCESS_KEY_ID, 
                   aws_secret_access_key=AWS_SECRET_ACCESS_KEY, 
                   region_name=AWS_REGION_NAME)
s3 = boto3.client('s3', 
                   aws_access_key_id=AWS_ACCESS_KEY_ID, 
                   aws_secret_access_key=AWS_SECRET_ACCESS_KEY, 
                   region_name=AWS_REGION_NAME)

# Define function to upload input image to input bucket
def upload_input_image(input_image_key, input_image_path):
    s3.upload_file(input_image_path, INPUT_BUCKET_NAME, 'input/' + input_image_key)

# Define function to upload output result to output bucket
def upload_output_result(output_result_key, output_result_value):
    s3.put_object(Body=output_result_value, Bucket=OUTPUT_BUCKET_NAME, Key='output/' + output_result_key)


def process_image(m):
    # print(m)
    msg=json.loads(m['Body'])
    # print(msg)
    logger.info("processing image started")
    image_name=msg['image_name']
    user_id=msg['user_id']
    decoded_image=base64.b64decode(msg['image'])
    tempfile='temp_img.jpeg'
    with open(tempfile,'wb') as f:
        f.write(decoded_image) 
    filepath= os.path.abspath(tempfile)
    result=subprocess.check_output(['python3','image_classification.py',filepath])
    result=result.decode('utf-8').strip().split(',')[-1]
    logger.info("{}".format(result))
    # logger.info("Processed the image")
    upload_input_image(image_name, filepath)
    res= "("+str(image_name)+", "+ str(result) + ")"
    upload_output_result(str(image_name.split('.')[0])+'.txt', res)
    print('Images uploaded to buckets')
    os.remove(filepath)
    sqs.delete_message(QueueUrl=REQUEST_QUEUE_URL,ReceiptHandle=m['ReceiptHandle'])
    print("***** Deleted from Request Queue **********")
    message_body={
            'result': result,
            'image_name': image_name,
            'user_id':user_id
        }
    return message_body

while True:
    messages=sqs.receive_message(QueueUrl=REQUEST_QUEUE_URL,MaxNumberOfMessages=1, MessageAttributeNames=['All']) 
    if 'Messages' in messages:
        for m in messages['Messages']:
            result=process_image(m)
            sqs.send_message(QueueUrl=RESPONSE_QUEUE_URL,MessageBody=json.dumps(result))
            print("******  Message Sent to response queue ******")
    # else:        
    #     time.sleep(2)
# for i,result in enumerate(results)
