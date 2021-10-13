import json
import boto3
import dateutil.parser
import datetime
import os
import time

SQS_QUEUE_NAME = "chatbot-sqs-1"
# The order in which the slots must be elicited
SLOT_VALIDATION_ORDER = ["phone", "cuisine", "location", "capacity", "date", "time"]


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


def get_session_attributes(intent_request):
    sessionState = intent_request['sessionState']
    if 'sessionAttributes' in sessionState:
        return sessionState['sessionAttributes']

    return {}


def elicit_intent(intent_request, session_attributes, message):
    return {
        'sessionState': {
            'dialogAction': {
                'type': 'ElicitIntent'
            },
            'sessionAttributes': session_attributes
        },
        'messages': [message] if message != None else None,
        'requestAttributes': intent_request['requestAttributes'] if 'requestAttributes' in intent_request else None
    }


def elicit_slot(intent_request, slot_to_slicit, session_attributes, message=None):
    """
    Lex response to elicit a slot
    """
    return {
        'sessionState': {
            'sessionAttributes': session_attributes,
            'dialogAction': {
                'type': 'ElicitSlot',
                'slotToElicit': slot_to_slicit
            },
            'intent': intent_request['sessionState']['intent']
        },
        'messages': [message] if message != None else None,
        'sessionId': intent_request['sessionId'],
        'requestAttributes': intent_request['requestAttributes'] if 'requestAttributes' in intent_request else None
    }


def confirm(intent_request, session_attributes, message=None):
    """
    Lex response to ask for confirmation
    """
    return {
        'sessionState': {
            'sessionAttributes': session_attributes,
            'dialogAction': {
                'type': 'ConfirmIntent'
            },
            'intent': intent_request['sessionState']['intent']
        },
        'messages': [message] if message != None else None,
        'sessionId': intent_request['sessionId'],
        'requestAttributes': intent_request['requestAttributes'] if 'requestAttributes' in intent_request else None
    }


def close(intent_request, session_attributes, fulfillment_state, message=None):
    """
    Lex response to close the intent
    """
    intent_request['sessionState']['intent']['state'] = fulfillment_state
    return {
        'sessionState': {
            'sessionAttributes': session_attributes,
            'dialogAction': {
                'type': 'Close'
            },
            'intent': intent_request['sessionState']['intent']
        },
        'messages': [message] if message != None else None,
        'sessionId': intent_request['sessionId'],
        'requestAttributes': intent_request['requestAttributes'] if 'requestAttributes' in intent_request else None
    }


def validate_phone(intent_request, slot, slot_val):
    """
    Validate the phone slot
    """
    message = {
        'contentType': 'PlainText',
        'content': "Sorry, I did not understand that. Please enter your phone number again."
    }
    if not slot_val or len(slot_val) not in (10, 12):
        return False, message
    if slot_val[0] == "+":
        if not (slot_val[:2] == '+1' and len(slot_val[2:]) == 10) or slot_val[2] == '0':
            return False, message
    elif len(slot_val) != 10 or slot_val[0] == '0':
        return False, message
    return True, None


def validate_cuisine(intent_request, slot, slot_val):
    """
    Validate the cuisine slot
    """
    message = {
        'contentType': 'PlainText',
        'content': "Sorry, I did not understand that. Which cuisine are you interested in?"
    }
    if slot_val is None:
        return False, message
    return True, None


def validate_location(intent_request, slot, slot_val):
    """
    Validate the location slot
    """
    message = {
        'contentType': 'PlainText',
        'content': "Sorry, I did not understand that. Which area would you like to dine in?"
    }
    if slot_val is None:
        return False, message
    return True, None


def validate_date(intent_request, slot, slot_val):
    """
    Validate the date slot
    """
    message = {
        'contentType': 'PlainText',
        'content': "Sorry, I did not understand that. Please enter the date again."
    }
    try:
        req_date = dateutil.parser.parse(slot_val).date()
        now = datetime.datetime.now().date()
        print("Requested date: %s - Current date: %s" % (req_date, now))
        if req_date < now:
            message['content'] = 'Date cannot be in the past. Please enter the date again.'
            intent_request['sessionState']['intent']['slots'][slot] = None
            return False, message
    except TypeError as e:
        intent_request['sessionState']['intent']['slots'][slot] = None
        return False, message
    return True, None


def validate_time(intent_request, slot, slot_val):
    """
    Validate the time slot
    """
    message = {
        'contentType': 'PlainText',
        'content': "Sorry, I did not understand that. Please enter the time again."
    }
    try:
        req_date = get_slot(intent_request, "date")
        req_time = datetime.datetime.strptime("%s %s" % (req_date, slot_val), "%Y-%m-%d %H:%M")
        now = datetime.datetime.now()
        print("Requested time: %s - Current time: %s" % (req_time, now))
        if req_time < now:
            message['content'] = 'Time cannot be in the past. Please enter the time again.'
            intent_request['sessionState']['intent']['slots'][slot] = None
            return False, message
    except Exception as e:
        intent_request['sessionState']['intent']['slots'][slot] = None
        return False, message
    return True, None


def validate_capacity(intent_request, slot, slot_val):
    """
    Validate the capacity slot
    """
    message = {
        'contentType': 'PlainText',
        'content': "Sorry, I did not understand that. How many people?"
    }
    try:
        num = int(slot_val)
        print("Requested capacity: %s" % (num))
        if num <= 0:
            message['content'] = 'Number of people must be positive. Please enter the number of people again.'
            intent_request['sessionState']['intent']['slots'][slot] = None
            return False, message
    except Exception as e:
        intent_request['sessionState']['intent']['slots'][slot] = None
        return False, message
    return True, None


# Map between slot name and slot validation function
VALIDATE_SLOT_FUNC_MAP = {
    "phone": validate_phone,
    "date": validate_date,
    "time": validate_time,
    "capacity": validate_capacity,
    "location": validate_location,
    "cuisine": validate_cuisine
}


def validate(intent_request):
    """
    Validate the intent slot values
    """
    session_attributes = get_session_attributes(intent_request)
    for slot in SLOT_VALIDATION_ORDER:
        slot_val = get_slot(intent_request, slot)
        slot_already_tried = slot in session_attributes
        session_attributes[slot] = True
        if slot_val is None and not slot_already_tried:
            return elicit_slot(intent_request, slot, session_attributes)

        is_valid, message = VALIDATE_SLOT_FUNC_MAP[slot](intent_request, slot, slot_val)
        if not is_valid:
            return elicit_slot(intent_request, slot, session_attributes, message)

    return confirm(intent_request, session_attributes)


def fulfilled(intent_request, success=True):
    """
    Fulfill the intent
    """
    session_attributes = get_session_attributes(intent_request)
    for slot in SLOT_VALIDATION_ORDER:
        session_attributes.pop(slot)

    if success:
        fulfillment_state = "Fulfilled"
        slots = get_sqs_payload(intent_request)
        print("Final slots: %s" % slots)
        send_sqs_message(SQS_QUEUE_NAME, slots)
    else:
        fulfillment_state = "Failed"

    return close(intent_request, session_attributes, fulfillment_state)


def dining_suggestions(intent_request):
    """
    Handle the DiningSuggestions intent
    """
    if intent_request['sessionState']['intent']['name'] != "DiningSuggestionsIntent":
        # For other intents, clear the session attributes
        return close(intent_request, session_attributes={}, fulfillment_state="Fulfilled")

    if intent_request['invocationSource'] == 'FulfillmentCodeHook':
        return fulfilled(intent_request)
    if intent_request['sessionState']['intent']['confirmationState'] == 'Confirmed':
        return fulfilled(intent_request)
    if intent_request['sessionState']['intent']['confirmationState'] == 'Denied':
        return fulfilled(intent_request, success=False)
    return validate(intent_request)


def get_sqs_payload(intent_request):
    """
    Get the payload to be pushed to AWS SQS
    """
    slots = get_slots(intent_request)
    keys = slots.keys()
    payload = {}
    for key in keys:
        payload[key] = get_slot(intent_request, key)
    return payload


def send_sqs_message(queue_name, msg_body):
    """
    Push the information to AWS SQS queue
    """
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
    """
    Entry point for lambda invocation.
    """
    # By default, treat the user request as coming from the America/New_York time zone.
    os.environ['TZ'] = 'America/New_York'
    time.tzset()

    print("Lex Request: %s" % event)
    lex_response = dining_suggestions(event)
    print("Lex Response: %s" % lex_response)
    return lex_response
