import boto3
import json

lex_config = {
    "botId": 'XH88XRXIXE',
    "botAliasId": 'TSTALIASID',
    "localeId": 'en_US',
    "sessionId": 'test_session'
}

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

def lex_handler(msg):
    # LexV2 client uses 'lexv2-runtime'
    client = boto3.client('lexv2-runtime')

    # Submit the text
    print('Lex request - config: %s, text: %s' % (lex_config, msg))
    response = client.recognize_text(text=msg, **lex_config)
    print('Lex response: %s' % response)
    return response

def lambda_handler(event, context):
    print("Request body: %s" % event)
    try:
        msg = event["messages"][0]["text"]
        lex_messages = lex_handler(msg)
        response = prepare_response_from_lex(lex_messages)
    except Exception as e:
        print("Error: %s" % str(e))
        response = prepare_error_response()

    print("Response: %s" % response)
    return response
