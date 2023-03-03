import time
import uuid
from flask import Flask, jsonify, render_template, request
import boto3
import base64
import json
import threading
import os
from my_logger import logger
logger.info(" Server Started")

app = Flask(__name__)
lock = threading.Lock()
results = {}
stored_results=[]
def receive_and_process_message():
    global lock, results, stored_results
    sqs = boto3.client('sqs', 
                   aws_access_key_id=os.environ.get('AWS_ACCESS_KEY_ID'), 
                   aws_secret_access_key=os.environ.get('AWS_SECRET_ACCESS_KEY'), 
                   region_name= 'us-east-1')
    
    request_queue_url = os.environ.get('REQUEST_QUEUE_URL')
    response_queue_url = os.environ.get('RESPONSE_QUEUE_URL')

    # Continuously receive and process messages from request queue
    while True:
        response = sqs.receive_message(
            QueueUrl=response_queue_url,
            MaxNumberOfMessages=1,
            VisibilityTimeout=0,
            WaitTimeSeconds=0
        )

        if 'Messages' in response:
            message = json.loads(response['Messages'][0]['Body'])
            with lock:
                results[message['user_id']] = message
                print(f"Classification result for {message['image_name']}: {message['result']}")
                stored_results.append({message['image_name']: message['result']})
                logger.info("Classification result for {}:{}".format(message['image_name'],message['result']))
            

            # Delete message from request queue
            receipt_handle = response['Messages'][0]['ReceiptHandle']
            sqs.delete_message(
                QueueUrl=response_queue_url,
                ReceiptHandle=receipt_handle
            )
        time.sleep(2)

@app.route('/')
def index():
    return render_template('upload.html')

@app.route('/results')
def get_result():
    return render_template('results.html',results=stored_results)

@app.route('/upload', methods=['POST'])
def upload_images():
    global lock
    sqs = boto3.client('sqs', 
                   aws_access_key_id=os.environ.get('AWS_ACCESS_KEY_ID'), 
                   aws_secret_access_key=os.environ.get('AWS_SECRET_ACCESS_KEY'), 
                   region_name='us-east-1')
    request_queue_url = os.environ.get('REQUEST_QUEUE_URL')

    # Encode image and send message to request queue
    image_data = request.files['myfile']
    message = {
        'user_id': str(uuid.uuid4()),
        'image': base64.b64encode(image_data.read()).decode('utf-8'),
        'image_name': image_data.filename
    }
    sqs.send_message(
        QueueUrl=request_queue_url,
        MessageBody=json.dumps(message)
    )
    return jsonify({'id': message['user_id']})

if __name__ == '__main__':
    t = threading.Thread(target=receive_and_process_message)
    t.daemon = True
    t.start()
    app.run(debug=True)
