import json
import boto3

SQS_QUEUE_NAME = "chatbot-sqs-1"

def get_slots(intent_request):
    return intent_request['sessionState']['intent']['slots']

def get_slot(intent_request, slotName):
    slots = get_slots(intent_request)
    if slots is not None and slotName in slots and slots[slotName] is not None:
        return slots[slotName]['value']['interpretedValue']

def get_session_attributes(intent_request):
    sessionState = intent_request['sessionState']
    if 'sessionAttributes' in sessionState:
        return sessionState['sessionAttributes']

    return {}

def close(intent_request, session_attributes, fulfillment_state, message):
    intent_request['sessionState']['intent']['state'] = fulfillment_state
    return {
        'sessionState': {
            'sessionAttributes': session_attributes,
            'dialogAction': {
                'type': 'Close'
            },
            'intent': intent_request['sessionState']['intent']
        },
        'messages': [message],
        'sessionId': intent_request['sessionId'],
        'requestAttributes': intent_request['requestAttributes'] if 'requestAttributes' in intent_request else None
    }

def dining_suggestions(intent_request):
    session_attributes = get_session_attributes(intent_request)
    phone = get_slot(intent_request, "phone")
    text = "Thank you. I will send the suggestions to %s soon!" % phone
    message = {
        'contentType': 'PlainText',
        'content': text
    }
    fulfillment_state = "Fulfilled"
    return close(intent_request, session_attributes, fulfillment_state, message)

def get_sqs_payload(intent_request):
    slots = get_slots(intent_request)
    keys = slots.keys()
    payload = {}
    for key in keys:
        payload[key] = get_slot(intent_request, key)
    return payload

def send_sqs_message(queue_name, msg_body):
    # Send the SQS message
    sqs_client = boto3.client('sqs')
    sqs_queue_url = sqs_client.get_queue_url(QueueName=queue_name)['QueueUrl']
    print("SQS name: %s - url: %s - msg: %s" % (queue_name, sqs_queue_url, msg_body))
    try:
        msg = sqs_client.send_message(QueueUrl=sqs_queue_url,
                                      MessageBody=json.dumps(msg_body))
        print("SQS Response: %s" % msg)
    except Exception as e:
        print("Error: %s" % str(e))

def lambda_handler(event, context):
    print("Lex Request: %s" % event)
    lex_response = dining_suggestions(event)
    print("Lex Response: %s" % lex_response)
    slots = get_sqs_payload(event)
    print("Slots: %s" % slots)
    send_sqs_message(SQS_QUEUE_NAME, slots)
    return lex_response