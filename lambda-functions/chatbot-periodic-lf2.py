import json
import boto3
import urllib3
import time
from decimal import Decimal

SQS_QUEUE_NAME = "chatbot-sqs-1"
DYNAMO_DB_TABLE = "yelp-restaurants"
DYNAMO_DB_HISTORY_TABLE = "yelp-suggestions-history"
# Elasticsearch config
ES_CONFIG = {
    "url": "https://search-yelp-restaurants-es-coi3ozmqyi2g5vr624aing4dja.us-east-1.es.amazonaws.com",
    "index": "restaurants",
    "master-username": "chatbot-es-root",
    "master-password": "CCBD-es-123456"
}
TWILIO_CONFIG = {
    "url": "https://api.twilio.com/2010-04-01/Accounts/AC61dbae656b3ec9bd25bf39632edba3a9/Messages.json",
    "sid": "AC61dbae656b3ec9bd25bf39632edba3a9",
    "pwd": "95e911c1b4ec74c6049aaa2913a9f789",
    "from": "+13202333218"
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
            # parsed.append({
            #     "phone": body['phone'],
            #     "cuisine": body['cuisine']
            # })
            parsed.append(body)
        print("SQS parsed msgs: %s" % parsed)
        return parsed
    except Exception as e:
        print("Error: %s" % str(e))
        return []


def format_sms_msg(sqs_resp, db_resps):
    msg = "Hello! Here are my %s restaurant suggestions for %s people, for %s at %s: " % (
        sqs_resp['cuisine'], sqs_resp['capacity'], sqs_resp['date'], sqs_resp['time'])
    suggestions_msg = ""
    for ind, resp in enumerate(db_resps):
        suggestion = "%d. %s at %s. It has a rating of %s. " % (
            ind + 1, resp['restaurant-name'], ', '.join(resp['restaurant-location']['display_address']),
            resp['restaurant-rating'])
        if resp['restaurant-display_phone']:
            suggestion += "You can contact them at %s. " % resp['restaurant-display_phone']
        suggestions_msg += suggestion

    msg += suggestions_msg
    msg += "Enjoy your meal!"
    print("Final suggestion: %s" % msg)
    return msg


def format_history_msg(sqs_resp, db_resps):
    msg = "Your previous search results for %s restaurants for %s people, for %s at %s: " % (
        sqs_resp['cuisine'], sqs_resp['capacity'], sqs_resp['date'], sqs_resp['time'])
    suggestions_msg = ""
    for ind, resp in enumerate(db_resps):
        suggestion = "%d. %s at %s. It has a rating of %s. " % (
            ind + 1, resp['restaurant-name'], ', '.join(resp['restaurant-location']['display_address']),
            resp['restaurant-rating'])
        if resp['restaurant-display_phone']:
            suggestion += "You can contact them at %s. " % resp['restaurant-display_phone']
        suggestions_msg += suggestion

    msg += suggestions_msg
    print("History msg: %s" % msg)
    return msg


def read_db(restaurant_ids):
    """
    Read data from Dynamo DB
    """
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(DYNAMO_DB_TABLE)
    db_resps = []
    for restaurant_id in restaurant_ids:
        response = table.get_item(Key={'restaurant-id': str(restaurant_id)})
        response = response['Item'] if 'Item' in response else {}
        print("DB entry for ID '%s': %s" % (restaurant_id, response))
        db_resps.append(response)

    return db_resps


def load_yelp_data():
    """
    Load the JSON data scraped from Yelp
    """
    with open("yelp-data.json", "r") as f:
        data = json.load(f)
        data = json.loads(json.dumps(data), parse_float=Decimal)
        return data


def load_yelp_to_db():
    """
    Write the Yelp data to Dynamo DB
    """
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(DYNAMO_DB_TABLE)
    table_data = load_yelp_data()
    try:
        with table.batch_writer() as writer:
            for item in table_data:
                item["inserted-at"] = "%.3f" % time.time()
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


def send_twilio_sms(number, msg):
    print("Twilio Request: %s - %s" % (number, msg))
    http = urllib3.PoolManager()
    url = TWILIO_CONFIG['url']
    headers = urllib3.make_headers(basic_auth='%s:%s' % (TWILIO_CONFIG["sid"], TWILIO_CONFIG["pwd"]))
    headers.update({
        'Content-Type': 'application/x-www-form-urlencoded',
    })
    payload = "Body=%s&To=%s&From=%s" % (msg, number, TWILIO_CONFIG["from"])
    response = http.request('POST', url, headers=headers, body=payload)
    status = response.status
    data = json.loads(response.data)
    print("Twilio Response: [%s] %s" % (status, data))


def save_history_to_db(phone, msg):
    """
    Write the Yelp data to Dynamo DB
    """
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(DYNAMO_DB_HISTORY_TABLE)
    try:
        with table.batch_writer() as writer:
            item = {
                "phone": phone,
                "history": msg
            }
            writer.put_item(Item=item)
        print("Loaded data into table %s" % table.name)
    except Exception as e:
        print("Couldn't load data into table %s" % table.name)
        print("Error: %s" % str(e))


def get_restaurant_suggestions(sqs_msg):
    """
    Get restaurant suggestion given a cuisine
    """
    cuisine = sqs_msg["cuisine"]
    es_data = search_es(cuisine)
    restaurant_ids = parse_es_data_for_ids(es_data)
    print("Restaurant IDs for %s cuisine: %s" % (cuisine, restaurant_ids))
    db_resps = read_db(restaurant_ids)
    sms_payload = format_sms_msg(sqs_msg, db_resps)
    phone = sqs_msg['phone']
    if len(phone) == 10:
        phone = "+1%s" % phone
    send_twilio_sms(phone, sms_payload)
    history_msg = format_history_msg(sqs_msg, db_resps)
    save_history_to_db(phone, history_msg)


def lambda_handler(event, context):
    """
    Entry point for lambda invocation
    """
    messages = poll_sqs_messages()
    for msg in messages:
        get_restaurant_suggestions(msg)
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }