import json
import os
import boto3
from flask import Flask, render_template, request
import base64
import uuid
import threading
import time
from dotenv import load_dotenv
load_dotenv()
from my_logger import logger
logger.info('Logging info')

app = Flask(__name__)

# Set up SQS client
sqs = boto3.client('sqs', 
                   aws_access_key_id=os.environ.get('AWS_ACCESS_KEY_ID'), 
                   aws_secret_access_key=os.environ.get('AWS_SECRET_ACCESS_KEY'), 
                   region_name=os.environ.get('AWS_REGION_NAME'))     
# Set up SQS queue URL
request_queue_url = os.environ.get('REQUEST_QUEUE_URL')
response_queue_url= os.environ.get('RESPONSE_QUEUE_URL')

# Dictionary to store request ID and result
request_results = {}

# Function to handle each request
def handle_request(user_id, image_name, image_data):
    global request_results
    encoded_image_data = base64.b64encode(image_data).decode('utf-8')
    message_body = {
        'user_id': user_id,
        'image_name': image_name,
        'image': encoded_image_data
    }
    sqs.send_message(QueueUrl=request_queue_url, MessageBody=json.dumps(message_body))

    # Wait for result
    print(" USer id to search:",user_id)
    while True:
        # time.sleep(5)
        # print("Searching for the key in dict", request_results)

        if str(user_id) in request_results:
            print("Found Result")
            result = request_results.pop(user_id)
            res=result['result']
            print(f"Classification result for {image_name}: {result}")
            return result
        time.sleep(5)


@app.route('/')
def index():
    return render_template('upload.html')

@app.route('/upload', methods=['POST'])
def upload_images():
    print(request.files)
    file = request.files['myfile']
    user_id = str(uuid.uuid4())
    image_name = file.filename
    image_data = file.read()

    # Create thread to handle request
    # thread = threading.Thread(target=handle_request, args=(user_id, image_name, image_data))
    # thread.start()
    res= handle_request(user_id, image_name, image_data)
    return res

# SQS message handler
# def handle_message(message):
#     # Parse message body
#     message_body = json.loads(message['Body'])
#     user_id = message_body['user_id']
#     image_name = message_body['image_name']
#     result = message_body['result']
#     result_dict = {
#         'user_id': user_id,
#         'image_name': image_name,
#         'result': result
#     }
#     request_results[user_id] = result_dict

#     # Delete message from SQS queue
#     print("While deleting",message['ReceiptHandle'])
#     sqs.delete_message(QueueUrl=request_queue_url, ReceiptHandle=message['ReceiptHandle'])

# Start polling SQS queue for messages
def start_polling():
    print(" Polling started")
    while True:
        response = sqs.receive_message(QueueUrl=os.environ.get('RESPONSE_QUEUE_URL'), MaxNumberOfMessages=1
            ,VisibilityTimeout=0,
            WaitTimeSeconds=0)
        if 'Messages' in response:
            print(response)
            for message in response['Messages']:
                # handle_message(message)
                message_body = json.loads(message['Body'])
                user_id = message_body['user_id']
                image_name = message_body['image_name']
                result = message_body['result']
                result_dict = {
                    'user_id': user_id,
                    'image_name': image_name,
                    'result': result
                }
                global request_results
                request_results[str(user_id)] = result_dict
                print(request_results)

                # Delete message from SQS queue
                sqs.delete_message(QueueUrl=response_queue_url, ReceiptHandle=message['ReceiptHandle'])
        
                print("******** Deleting from Response Queue ********")
        # time.sleep(2)

if __name__ == '__main__':
    # Start polling thread
    polling_thread = threading.Thread(target=start_polling)
    polling_thread.start()
    # Start Flask server
    app.run(debug=True)