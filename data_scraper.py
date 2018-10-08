import json
import boto3
from botocore.vendored import requests

client = boto3.resource('dynamodb')
table = client.Table('HTServiceProviderTable')

URL = 'https://ursaminor.polarisproject.org/serviceproviders/data/providers.geojson'
HEADERS = {'User-Agent': "A function that obtains service provider details for victims and survivors of human trafficking"}

data=[]

## get only the domain name
def strip_url(url):
    url = url.replace("http://","")
    url = url.replace("https://","")
    url = url.replace("www.", "")
    url = url.replace("www://","")
    url = url.replace("http//:","")
    url = url.replace(",",".")
    return url
    
## Set item to null if not present
def check_none(item, key, feature):
    if key in feature["properties"]:
        if key == 'website':
            item[key] = 'http://' + strip_url(feature["properties"][key])
        else:
            item[key] = feature["properties"][key]
    else:
        item[key] = None

## Get only the data needed from referral directory        
def organize_data():
        r = requests.get(URL, headers=HEADERS)
        j = r.json()
        
        for feature in j["accountsWithGeo"]["features"]:
            if 'servicesDetail' in feature["properties"] and 'populationsDetail' in feature["properties"]:
                item = {
                    'id':feature["properties"]["id"],
                    'name':feature["properties"]["name"],
                    'servicesDetail':feature["properties"]["servicesDetail"],
                    'populationsDetail':feature["properties"]["populationsDetail"]
                }
                
                check_none(item, 'hotline', feature)
                check_none(item, 'phone', feature)
                check_none(item, 'website', feature)
                check_none(item, 'email', feature)
                check_none(item, 'state', feature)
                check_none(item, 'city', feature)
                
                data.append(item)
                    
## Store in DynamoDB 
def ddbWriter(data):
    with table.batch_writer() as batch:
        item_counter = 0
        while item_counter < len(data):
            item = data[item_counter]
            
            print (json.dumps(item, indent=4, sort_keys=True))
       
            batch.put_item(
                Item = item
            )
         
            item_counter += 1
            
## Lambda handler    
def handler(event, context):
    organize_data()
    ddbWriter(data)
    print(len(data))
    return("Success!")