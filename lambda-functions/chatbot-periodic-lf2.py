import json
import boto3

SQS_QUEUE_NAME = "chatbot-sqs-1"
DYNAMO_DB_TABLE = "yelp-restaurants"

def poll_sqs_messages(queue_name):
    # Poll the SQS messages
    sqs_client = boto3.client('sqs')
    sqs_queue_url = sqs_client.get_queue_url(QueueName=queue_name)['QueueUrl']
    print("SQS name: %s - url: %s" % (queue_name, sqs_queue_url))
    try:
        response = sqs_client.receive_message(
            QueueUrl=sqs_queue_url,
            MaxNumberOfMessages=10,
            MessageAttributeNames=['All'],
            VisibilityTimeout=30
        )
        print("SQS Response: %s" % response)

        messages = response['Messages'] if 'Messages' in response else []
        for message in messages:
            receipt_handle = message['ReceiptHandle']
            # Delete received message from queue
            response = sqs_client.delete_message(
                QueueUrl=sqs_queue_url,
                ReceiptHandle=receipt_handle
            )
            print("SQS Delete Response: %s" % response)

        print("SQS deleted %s msgs" % len(messages))
    except Exception as e:
        print("Error: %s" % str(e))

def read_db(table_name):
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(table_name)
    response = table.get_item(Key={"RestaurantID": "1"})
    print(response)

def lambda_handler(event, context):
    poll_sqs_messages(SQS_QUEUE_NAME)
    # read_db(DYNAMO_DB_TABLE)
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
