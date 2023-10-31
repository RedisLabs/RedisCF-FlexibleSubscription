import json
import cfnresponse
import requests

#This lambda only send back to CloudFormation the response received from the previous function as displayed on the Step Functions state machine flow
def lambda_handler(event, context):
    print (event)
    responseURL = event["responseURL"]
    responseBody = event["responseBody"]
    GetResponse(responseURL, responseBody)

def GetResponse(responseURL, responseBody): 
    responseBody = json.dumps(responseBody)
    req = requests.put(responseURL, data = responseBody)
    print ('RESPONSE BODY:n' + responseBody)
