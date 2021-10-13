import boto3
import json

# Lex bot config
lex_config = {
    "botId": 'XH88XRXIXE',
    "botAliasId": 'TSTALIASID',
    "localeId": 'en_US',
    "sessionId": 'test_session'
}
DYNAMO_DB_HISTORY_TABLE = "yelp-suggestions-history"


def prepare_error_response():
    response = {
        'statusCode': 500,
        'messages': []
    }
    return response


def prepare_response_from_lex(data):
    messages = []
    for msg in data['messages']:
        messages.append({
            'text': msg['content']
        })

    response = {
        'statusCode': 200,
        'messages': messages
    }
    return response

def get_session_attributes(intent_request):
    sessionState = intent_request['sessionState']
    if 'sessionAttributes' in sessionState:
        return sessionState['sessionAttributes']

    return {}

def get_slots(intent_request):
    """
    Get the slots from Lex intent request
    """
    return intent_request['sessionState']['intent']['slots']


def get_slot(intent_request, slotName):
    """
    Get a specific slot value from Lex intent request
    """
    slots = get_slots(intent_request)
    if slots is not None and slotName in slots and slots[slotName] is not None:
        val = slots[slotName]['value']
        if 'interpretedValue' in val:
            return val['interpretedValue']

def lex_handler(msg):
    # LexV2 client uses 'lexv2-runtime'
    client = boto3.client('lexv2-runtime')

    # Submit the text
    print('Lex request - config: %s, text: %s' % (lex_config, msg))
    response = client.recognize_text(text=msg, **lex_config)
    print('Lex response: %s' % response)
    return response

def read_db(phone):
    """
    Read data from Dynamo DB
    """
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(DYNAMO_DB_HISTORY_TABLE)
    db_resps = []
    response = table.get_item(Key={'phone':  str(phone)})
    response = response['Item'] if 'Item' in response else ""
    print("DB entry for phone '%s': %s" % (phone, response))
    return response["history"]

def lambda_handler(event, context):
    print("Request body: %s" % event)
    try:
        msg = event["messages"][0]["text"]
        lex_response = lex_handler(msg)
        session_attributes = get_session_attributes(lex_response)
        print("Session attributes: %s" % session_attributes)
        history = None
        if len(session_attributes)==2 and "phone" in session_attributes and "cuisine" in session_attributes:
            phone = get_slot(lex_response, "phone")
            if phone:
                if len(phone) == 10:
                    phone = "+1%s" % phone
                history = read_db(phone)
        response = prepare_response_from_lex(lex_response)
        if history:
            response['messages'].insert(0, {
                "text": history
            })
    except Exception as e:
        print("Error: %s" % str(e))
        response = prepare_error_response()

    print("Response: %s" % response)
    return response
