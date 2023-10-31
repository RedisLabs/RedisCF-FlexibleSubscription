import json
import cfnresponse
import time
import json
import requests
import os
from os import environ

accept = "application/json"

def lambda_handler(event, context):
    #Prints the event retrieved from the Handler function through Step Functions
    print (json.dumps(event))
    global base_url
    global x_api_key
    global x_api_secret_key 
    base_url = event["base_url"]
    x_api_key =  RetrieveSecret("redis/x_api_key")["x_api_key"]
    x_api_secret_key =  RetrieveSecret("redis/x_api_secret_key")["x_api_secret_key"]
    #Calls the GetStatus function in a loop created by the state machine until the status is 'active'
    subscription_id = event["responseBody"]["Data"]["SubscriptionId"]
    sub_status = GetSubscriptionStatus(subscription_id)
    
    #return the event which is a status to Step Functions to use it further to call the CFResponse lambda
    print (sub_status)
    event["sub_status"] = sub_status
    return event
    
def GetSubscriptionStatus (subscription_id):
    url = base_url + "/v1/subscriptions/" + subscription_id
    
    # A GET request to the API
    response = requests.get(url, headers={"accept":accept, "x-api-key":x_api_key, "x-api-secret-key":x_api_secret_key})
    response = response.json()
    sub_status = response["status"]
    print ("Subscription status is: " + sub_status)
    return sub_status
    
def RetrieveSecret(secret_name):
    headers = {"X-Aws-Parameters-Secrets-Token": os.environ.get('AWS_SESSION_TOKEN')}

    secrets_extension_endpoint = "http://localhost:2773/secretsmanager/get?secretId=" + str(secret_name)
    r = requests.get(secrets_extension_endpoint, headers=headers)
    print (r)
    secret = json.loads(r.text)["SecretString"] # load the Secrets Manager response into a Python dictionary, access the secret
    secret = json.loads(secret)

    return secret
