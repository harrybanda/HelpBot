import json
import boto3
import logging
import string
from boto3.dynamodb.conditions import Key, Attr

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

client = boto3.resource('dynamodb')
table = client.Table('HTServiceProviderTable')

ht_hotline = "1-888-373-7888 ( TTY: 711)"

# --- Load validation data ---

with open('services.json') as service_json:
    service_data = json.load(service_json)
        
with open('us_states.json') as state_json:
    state_data = json.load(state_json)

# --- Helpers that build all of the responses ---

def elicit_slot(session_attributes, intent_name, slots, slot_to_elicit, message):
    return {
        'sessionAttributes': session_attributes,
        'dialogAction': {
            'type': 'ElicitSlot',
            'intentName': intent_name,
            'slots': slots,
            'slotToElicit': slot_to_elicit,
            'message': message
        }
    }

def close(session_attributes, fulfillment_state, message, cards):
    response = {
        'sessionAttributes': session_attributes,
        "dialogAction": {
            "type": "Close",
            "fulfillmentState": fulfillment_state,
            "message": message,
            "responseCard": cards
        }
    }
    return response
    
def close_no_data(session_attributes, fulfillment_state, message):
    response = {
        'sessionAttributes': session_attributes,
        "dialogAction": {
            "type": "Close",
            "fulfillmentState": fulfillment_state,
            "message": message
        }
    }
    return response
        
def delegate(session_attributes, slots):
    return {
        'sessionAttributes': session_attributes,
        'dialogAction': {
            'type': 'Delegate',
            'slots': slots
        }
    }
    
# --- Helper Functions ---
    
def scan_database(service, service_two, service_three, gender, age, location):
    
    # set age and gender to match value in database
    if (age.lower() == 'adult'):
        age = 'Adult'
    else:
        age = 'Minor (Under 18)'
    
    if (gender.lower() == 'male'):
        gender = 'Male'
    else:
        gender = 'Female'
    
    # set location to match value in database
    if any(state['abbreviation'] == location.lower() for state in state_data):
        location = location.upper()
    elif any(state['name'] == location.lower() for state in state_data):
        match = next(d for d in state_data if d['name'] == location.lower())
        location = match['abbreviation'].upper()
    
    # set services to match value in database    
    if any(s['id'] == service for s in service_data):
        match = next(d for d in service_data if d['id'] == service)
        service = match['name'].lower()
        
    if any(s['id'] == service_two for s in service_data):
        match = next(d for d in service_data if d['id'] == service_two)
        service_two = match['name'].lower()
        
    if any(s['id'] == service_three for s in service_data):
        match = next(d for d in service_data if d['id'] == service_three)
        service_three = match['name'].lower()
    
    # scan database    
    if service_two is None and service_three is None:
        response = table.scan(
        FilterExpression = Attr('populationsDetail').contains('Sex Trafficking') &
        Attr('servicesDetail').contains(format_services(service)) &
        Attr('populationsDetail').contains(gender) &
        Attr('populationsDetail').contains(age) &
        Attr('state').eq(location))
    elif service_three is None:
        response = table.scan(
        FilterExpression = Attr('populationsDetail').contains('Sex Trafficking') &
        Attr('servicesDetail').contains(format_services(service)) &
        Attr('servicesDetail').contains(format_services(service_two)) &
        Attr('populationsDetail').contains(gender) &
        Attr('populationsDetail').contains(age) &
        Attr('state').eq(location))
    else:
        response = table.scan(
        FilterExpression = Attr('populationsDetail').contains('Sex Trafficking') &
        Attr('servicesDetail').contains(format_services(service)) &
        Attr('servicesDetail').contains(format_services(service_two)) &
        Attr('servicesDetail').contains(format_services(service_three)) &
        Attr('populationsDetail').contains(gender) &
        Attr('populationsDetail').contains(age) &
        Attr('state').eq(location))
        
    items = response['Items']
    #print (json.dumps(items, indent=4, sort_keys=True))
    return items
    
def add_card_details(service, service_two, service_three, gender, age, location):
    items = scan_database(service, service_two, service_three, gender, age, location)
    details = None
    if items is not None:
        details = []
        for i in range(min(10, len(items))):
            name = items[i]['name']
            hotline = check_item('hotline',items[i])
            phone = check_item('phone',items[i])
            email = check_item('email',items[i])
            website = check_item('website',items[i])
            
            if hotline is not None:
                contact = 'Hotline: ' + hotline
            elif hotline is None and phone is not None:
                contact = 'Phone: ' + phone
            elif hotline is None and phone is None and email is not None:
                contact = 'Email: ' + email
            else:
                contact = "No contact info"
            
            name = (name[:70] + '...') if len(name) > 70 else name

            details.append({
                'title': name, 
                'subTitle': contact,
                "attachmentLinkUrl": website
            })
            
    #print (json.dumps(details, indent=4, sort_keys=True))
    return details
    
def check_item(key, items):
    if key in items:
        val = items[key]
    else:
        val = None
    return val
            
def build_validation_result(is_valid, violated_slot, message_content):
    if message_content is None:
        return {
            "isValid": is_valid,
            "violatedSlot": violated_slot,
        }

    return {
        'isValid': is_valid,
        'violatedSlot': violated_slot,
        'message': {'contentType': 'PlainText', 'content': message_content}
    }

def build_response_card(service_info):
    generic_attachments = None
    if service_info is not None:
        generic_attachments = []
        for i in range(len(service_info)):
            generic_attachments.append(service_info[i])

    return {
        'contentType': 'application/vnd.amazonaws.card.generic',
        'version': 1,
        'genericAttachments': generic_attachments
    }
    
def validate_find_service(gender, age, location, service, service_two, service_three):
    gender_types = ['male', 'female']
    age_types = ['minor', 'adult']
    
    if gender is not None and gender.lower() not in gender_types:
        return build_validation_result(False,'Gender','Sorry I could not recognise {}, you can enter male or female.'.format(gender))
    
    if age is not None and age.lower() not in age_types:
        return build_validation_result(False,'Age','Sorry I could not recognise {}, please select one of the options below.'.format(age))
        
    if location is not None and not any((d['name'] == location.lower()) or (d['abbreviation'] == location.lower()) for d in state_data):
        return build_validation_result(False,'Location', 'Sorry I could not recognise {}, please enter the U.S state you are located in (e.g. California or CA)'.format(location))
             
    if service is not None and not any((s['name'] == service.lower()) or (s['id'] == service) for s in service_data):
        return build_validation_result(False,'Service',"Sorry I could not understand {}, enter 'Help' to see a list of services I can find.".format(service))         
        
    if service_two is not None and not any((s['name'] == service_two.lower()) or (s['id'] == service_two) for s in service_data):
        return build_validation_result(False,'Service_two',"Sorry I could not understand {}, enter 'Help' to see a list of services I can find.".format(service_two))
                                       
    if service_three is not None and not any((s['name'] == service_three.lower()) or (s['id'] == service_three) for s in service_data):
        return build_validation_result(False,'Service_three',"Sorry I could not understand {}, enter 'Help' to see a list of services I can find.".format(service_three))
     
    return build_validation_result(True, None, None)
    
def format_services(s):
    s = s.lower()
    
    if (s == 'education/job training'):
        s = 'Education/Job Training'
    else:
        s = string.capwords(s)
    return s
    
# --- Functions that control the bot's behavior ---

def find_service(intent_request):
    service = intent_request['currentIntent']['slots']['Service']
    service_two = intent_request['currentIntent']['slots']['Service_two']
    service_three = intent_request['currentIntent']['slots']['Service_three']
    gender = intent_request['currentIntent']['slots']['Gender']
    age = intent_request['currentIntent']['slots']['Age']
    location = intent_request['currentIntent']['slots']['Location']
    
    source = intent_request['invocationSource']
    
    if source == 'DialogCodeHook':
        # Perform basic validation on the supplied input slots.
        # Use the elicitSlot dialog action to re-prompt for the first violation detected.
        slots = intent_request['currentIntent']['slots']
        validation_result = validate_find_service(gender, age, location, service, service_two, service_three)
        
        if not validation_result['isValid']:
            slots[validation_result['violatedSlot']] = None
            return elicit_slot(intent_request['sessionAttributes'],
                               intent_request['currentIntent']['name'],
                               slots,
                               validation_result['violatedSlot'],
                               validation_result['message'])
                               
        return delegate(intent_request['sessionAttributes'], intent_request['currentIntent']['slots'])
        
    if source == 'FulfillmentCodeHook':
        card_details = add_card_details(service, service_two, service_three, gender, age, location)

        if any(state['abbreviation'] == location.lower() for state in state_data):
            match = next(d for d in state_data if d['abbreviation'] == location.lower())
            location = match['name'].lower()
        elif any(state['name'] == location.lower() for state in state_data):
            location = location.lower()
            
        if not card_details:
            return close_no_data(
                intent_request['sessionAttributes'], 
                'Fulfilled', {
                    'contentType': 'PlainText', 
                    'content': "I was not able to find any support services in {}, you call the human trafficking hotline for help. Call {}".format(location, ht_hotline)})
        else:
            return close(
                intent_request['sessionAttributes'], 
                'Fulfilled', {
                    'contentType': 'PlainText', 
                    'content': "Here are some service providers in {}.".format(location)
                },
                build_response_card(card_details))
                
# --- Intents ---

def dispatch(intent_request):
    logger.debug('dispatch userId={}, intentName={}'.format(intent_request['userId'], intent_request['currentIntent']['name']))
    intent_name = intent_request['currentIntent']['name']

    if intent_name == 'FindService':
        logger.debug(intent_name + ' was called')
        return find_service(intent_request)
  
    raise Exception('Intent with name ' + intent_name + ' not supported')

# --- Main handler ---

def handler(event, context):
    logger.debug('event.bot.name={}'.format(event['bot']['name']))
    return dispatch(event)