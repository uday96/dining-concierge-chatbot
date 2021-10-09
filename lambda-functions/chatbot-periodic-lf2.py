import json
import boto3
import urllib3
from decimal import Decimal

SQS_QUEUE_NAME = "chatbot-sqs-1"
DYNAMO_DB_TABLE = "yelp-restaurants"
# Elasticsearch config
ES_CONFIG = {
    "url": "https://search-yelp-restaurants-es-coi3ozmqyi2g5vr624aing4dja.us-east-1.es.amazonaws.com",
    "index": "restaurants",
    "master-username": "chatbot-es-root",
    "master-password": "CCBD-es-123456"
}
MAX_NUM_SUGGESTIONS = 1


def poll_sqs_messages():
    """
    Poll the messages from AWS SQS queue
    """
    # Poll the SQS messages
    sqs_client = boto3.client('sqs')
    sqs_queue_url = sqs_client.get_queue_url(QueueName=SQS_QUEUE_NAME)['QueueUrl']
    print("SQS name: %s - url: %s" % (SQS_QUEUE_NAME, sqs_queue_url))
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
        parsed = []
        for msg in messages:
            body = json.loads(msg['Body'])
            parsed.append({
                "phone": body['phone'],
                "cuisine": body['cuisine']
            })
        print("SQS parsed msgs: %s" % parsed)
        return parsed
    except Exception as e:
        print("Error: %s" % str(e))
        return []


def read_db(restaurant_ids):
    """
    Read data from Dynamo DB
    """
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(DYNAMO_DB_TABLE)
    for restaurant_id in restaurant_ids:
        response = table.get_item(Key={'restaurant-id': str(restaurant_id)})
        response = response['Item'] if 'Item' in response else {}
        print("DB entry for ID '%s': %s" % (restaurant_id, response))


def load_yelp_data():
    """
    Load the JSON data scraped from Yelp
    """
    with open("yelp-data.json", "r") as f:
        data = json.load(f)
        data = json.loads(json.dumps(data), parse_float=Decimal)
        return data


def write_db():
    """
    Write the Yelp data to Dynamo DB
    """
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(DYNAMO_DB_TABLE)
    table_data = load_yelp_data()
    try:
        with table.batch_writer() as writer:
            for item in table_data:
                writer.put_item(Item=item)
        print("Loaded data into table %s" % table.name)
    except Exception as e:
        print("Couldn't load data into table %s" % table.name)
        print("Error: %s" % str(e))


def search_es(cuisine):
    """
    Make an Elasticsearch query
    """
    http = urllib3.PoolManager()
    url = "%s/%s/_search?size=%s" % (ES_CONFIG["url"], ES_CONFIG["index"], MAX_NUM_SUGGESTIONS)
    headers = urllib3.make_headers(basic_auth='%s:%s' % (ES_CONFIG["master-username"], ES_CONFIG["master-password"]))
    headers.update({
        'Content-Type': 'application/json',
        "Accept": "application/json"
    })
    payload = {
        "query": {
            "function_score": {
                "query": {
                    "match_phrase": {
                        "restaurant-cuisine": "%s" % cuisine
                    }
                },
                "random_score": {}
            }
        }
    }
    response = http.request('GET', url, headers=headers, body=json.dumps(payload))
    status = response.status
    data = json.loads(response.data)
    print("ES Response: [%s] %s" % (status, data))
    return data


def parse_es_data_for_ids(data):
    """
    Parse the Elasticsearch response
    """
    restaurant_ids = []
    if "hits" not in data:
        print("Error: 'hits' not found in ES search data")
        return restaurant_ids

    for hit in data['hits']['hits']:
        restaurant_id = hit['_source']['restaurant-id']
        restaurant_ids.append(restaurant_id)

    return restaurant_ids


def get_restaurant_suggestions(cuisine):
    """
    Get restaurant suggestion given a cuisine
    """
    es_data = search_es(cuisine)
    restaurant_ids = parse_es_data_for_ids(es_data)
    print("Restaurant IDs for %s cuisine: %s" % (cuisine, restaurant_ids))
    read_db(restaurant_ids)


# def push_sns_msg():
#     client = boto3.client('sns')
#     response = client.publish(
#         # TopicArn='arn:aws:sns:us-east-1:814789024927:yelp-restaurants-suggestions',
#         TopicArn='arn:aws:sns:us-east-1:814789024927:yelp-restaurants-suggestions:5454350d-5eeb-4b95-8db4-76c09fdf8a57',
#         Message='Hello from Lambda',
#         Subject='AWS SNS Test'
#     )
#     print("SNS Response: %s" % response)

def lambda_handler(event, context):
    """
    Entry point for lambda invocation
    """
    messages = poll_sqs_messages()
    for msg in messages:
        get_restaurant_suggestions(msg["cuisine"])
    # push_sns_msg()
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
