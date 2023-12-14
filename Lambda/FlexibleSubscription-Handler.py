import boto3
import cfnresponse
import time
import json
import requests
import os
from os import environ

#Global variables used in composing the URL as in CAPI
accept = "application/json"
content_type = "application/json"

#runtime_region will take the value of the current lambda region
runtime_region = os.environ['AWS_REGION']

#initialization of a stepfunctions client to interact with Step Functions service
stepfunctions = boto3.client("stepfunctions")

def lambda_handler (event, context):
    
    #The event that is sent from CloudFormation. Displayed in CloudWatch logs.
    print (event)
    
    #aws_account_id will take the value of the user's account id
    aws_account_id = context.invoked_function_arn.split(":")[4]
    
    #Due to CloudFormation, the cast from String to Boolean needs to be done from Lambda
    if event['ResourceProperties']["dryRun"] == "true":
        event['ResourceProperties']["dryRun"] = True
    elif event['ResourceProperties']["dryRun"] == "false":
        event['ResourceProperties']["dryRun"] = False
        
    if event['ResourceProperties']["multipleAvailabilityZones"] == "true":
        event['ResourceProperties']["multipleAvailabilityZones"] = True
    elif event['ResourceProperties']["multipleAvailabilityZones"] == "false":
        event['ResourceProperties']["multipleAvailabilityZones"] = False
        
    if event['ResourceProperties']["supportOSSClusterApi"] == "true":
        event['ResourceProperties']["supportOSSClusterApi"] = True
    elif event['ResourceProperties']["supportOSSClusterApi"] == "false":
        event['ResourceProperties']["supportOSSClusterApi"] = False
        
    if event['ResourceProperties']["replication"] == "true":
        event['ResourceProperties']["replication"] = True
    elif event['ResourceProperties']["replication"] == "false":
        event['ResourceProperties']["replication"] = False
    
    #Creating the callEvent dictionary that will be identical with a Swagger API call
    networking = {}
    networking["deploymentCIDR"] = event['ResourceProperties']["deploymentCIDR"]
    if "vpcId" in event['ResourceProperties']:
        networking["vpcId"] = event['ResourceProperties']["vpcId"]
    
    regionsList = []
    regionsDict = {}
    regionsDict["region"] = event['ResourceProperties']["region"]
    if "multipleAvailabilityZones" in event['ResourceProperties']:
        regionsDict["multipleAvailabilityZones"] = event['ResourceProperties']["multipleAvailabilityZones"]
    if "preferredAvailabilityZones" in event['ResourceProperties']:
        regionsDict["preferredAvailabilityZones"] = event['ResourceProperties']["preferredAvailabilityZones"]
    regionsDict["networking"] = networking
    regionsList.append(regionsDict)
    
    cloudProvidersList = []
    cloudProvidersDict = {}
    if "provider" in event['ResourceProperties']:
        cloudProvidersDict["provider"] = event['ResourceProperties']["provider"]
    if "cloudAccountId" in event['ResourceProperties']:
        cloudProvidersDict["cloudAccountId"] = int(event['ResourceProperties']["cloudAccountId"])
    cloudProvidersDict["regions"] = regionsList
    cloudProvidersList.append(cloudProvidersDict)
    
    throughputMeasurement = {}
    if "by" in event['ResourceProperties']:
        throughputMeasurement["by"] = event['ResourceProperties']["by"]
    if "value" in event['ResourceProperties']:
        throughputMeasurement["value"] = int(event['ResourceProperties']["value"])
    
    if "moduleName" in event['ResourceProperties']:
        modulesList = []
        event['ResourceProperties']["moduleName"] = event['ResourceProperties']["moduleName"].replace(" ", "")
        modules = event['ResourceProperties']["moduleName"].split(",")
        modulesList = [{'name': module} for module in modules]
    print (modulesList)
    
    databasesList = []
    databasesDict = {}
    databasesDict["name"] = event['ResourceProperties']["dbname"]
    if "protocol" in event['ResourceProperties']:
        databasesDict["protocol"] = event['ResourceProperties']["protocol"]
    databasesDict["memoryLimitInGb"] = int(event['ResourceProperties']["memoryLimitInGb"])
    if "supportOSSClusterApi" in event['ResourceProperties']:
        databasesDict["supportOSSClusterApi"] = event['ResourceProperties']["supportOSSClusterApi"]
    if "dataPersistence" in event['ResourceProperties']:
        databasesDict["dataPersistence"] = event['ResourceProperties']["dataPersistence"]
    if "replication" in event['ResourceProperties']:
        databasesDict["replication"] = event['ResourceProperties']["replication"]
    if "by" in event['ResourceProperties']:
        databasesDict["throughputMeasurement"] = throughputMeasurement
    if "moduleName" in event['ResourceProperties']:
        databasesDict["modules"] = modulesList
    if "parameters" in event['ResourceProperties']: 
        databasesDict["parameters"] = event['ResourceProperties']["parameters"]
    if "quantity" in event['ResourceProperties']:
        databasesDict["quantity"] = int(event['ResourceProperties']["quantity"])
    if "averageItemSizeInBytes" in event['ResourceProperties']:
        databasesDict["averageItemSizeInBytes"] = int(event['ResourceProperties']["averageItemSizeInBytes"])
    if "respVersion" in event['ResourceProperties']:
        databasesDict["respVersion"] = event['ResourceProperties']["respVersion"]
    databasesList.append(databasesDict)
    
    callEvent = {}
    if "subName" in event['ResourceProperties']:
        callEvent["name"] = event['ResourceProperties']["subName"]
    if "dryRun" in event['ResourceProperties']:
        callEvent["dryRun"] = event['ResourceProperties']["dryRun"]
    callEvent["deploymentType"] = 'single-region'
    if "paymentMethod" in event['ResourceProperties']:
        callEvent["paymentMethod"] = event['ResourceProperties']["paymentMethod"]
    if "paymentMethodId" in event['ResourceProperties']:
        callEvent["paymentMethodId"] = int(event['ResourceProperties']["paymentMethodId"])
    if "memoryStorage" in event['ResourceProperties']:
        callEvent["memoryStorage"] = event['ResourceProperties']["memoryStorage"]
    callEvent["cloudProviders"] = cloudProvidersList
    callEvent["databases"] = databasesList
    if "redisVersion" in event['ResourceProperties']:
        callEvent["redisVersion"] = event['ResourceProperties']["redisVersion"]

    print ("callEvent that is used as the actual API Call is bellow:")
    print (callEvent)
    
    #Additional global variables used in methods for URL composing or as credentials to login.
    global stack_name
    global base_url
    global x_api_key
    global x_api_secret_key 
    base_url = event['ResourceProperties']['baseURL']
    x_api_key =  RetrieveSecret("redis/x_api_key")["x_api_key"]
    x_api_secret_key =  RetrieveSecret("redis/x_api_secret_key")["x_api_secret_key"]
    stack_name = str(event['StackId'].split("/")[1])
    
    #Creating the CloudFormation response block. Presuming the status as SUCCESS. If an error occurs, the status is changed to FAILED.
    responseData = {}
    responseStatus = 'SUCCESS'
    responseURL = event['ResponseURL']
    responseBody = {'Status': responseStatus,
                    'PhysicalResourceId': context.log_stream_name,
                    'StackId': event['StackId'],
                    'RequestId': event['RequestId'],
                    'LogicalResourceId': event['LogicalResourceId']
                    }
    
    #If the action of CloudFormation is Create stack
    if event['RequestType'] == "Create":
        #The API Call the creates the Subscription
        try:
            responseValue = PostSubscription(callEvent)
            print (responseValue) 
    
            if "processing-error" in str(responseValue):           
                sub_error = GetSubscriptionError (responseValue['links'][0]['href'])
                responseStatus = 'FAILED'
                reason = str(sub_error)
                if responseStatus == 'FAILED':
                    responseBody.update({"Status":responseStatus})
                    if "Reason" in str(responseBody):
                        responseBody.update({"Reason":reason})
                    else:
                        responseBody["Reason"] = reason
                    GetResponse(responseURL, responseBody)

            #Retrieving Subscription ID, Subscription Description and DefaultDB ID to populate Outputs tab of the stack
            sub_id, sub_description = GetSubscriptionId (responseValue['links'][0]['href'])
            default_db_id = GetDatabaseId(sub_id)
            print ("New sub id is: " + str(sub_id))
            print ("Description for Subscription with id " + str(sub_id) + " is: " + str(sub_description))
            print("Default Database ID is: " + str(default_db_id))
                    
            responseData.update({"SubscriptionId":str(sub_id), "DefaultDatabaseId":str(default_db_id), "SubscriptionDescription":str(sub_description), "PostCall":str(callEvent)})
            responseBody.update({"Data":responseData})
            
            #Initializing input for Step Functions then triggering the state machine
            SFinput = {}
            SFinput["responseBody"] = responseBody
            SFinput["responseURL"] = responseURL
            SFinput["base_url"] = event['ResourceProperties']['baseURL']
            response = stepfunctions.start_execution(
                stateMachineArn = f'arn:aws:states:{runtime_region}:{aws_account_id}:stateMachine:FlexibleSubscription-StateMachine-{runtime_region}-{stack_name}',
                name = f'FlexibleSubscription-StateMachine-{runtime_region}-{stack_name}',
                input = json.dumps(SFinput)
                )
            print ("Output sent to Step Functions is the following:")
            print (json.dumps(SFinput))
                        
        except:
            #If any error is encounter in the "try" block, then a function will catch the error and throw it back to CloudFormation as a failure reason.
            sub_error = GetSubscriptionError (responseValue['links'][0]['href'])
            responseStatus = 'FAILED'
            reason = str(sub_error)
            if responseStatus == 'FAILED':
                responseBody.update({"Status":responseStatus})
                if "Reason" in str(responseBody):
                    responseBody.update({"Reason":reason})
                else:
                    responseBody["Reason"] = reason
                GetResponse(responseURL, responseBody)
                    
        # except:
        #         #This except block is triggered only for wrong base_url or wrong credentials.
        #         responseStatus = 'FAILED'
        #         reason = 'Please check if the base_url or the credentials set in Secrets Manager are wrong.'
        #         if responseStatus == 'FAILED':
        #             responseBody.update({"Status":responseStatus})
        #             if "Reason" in str(responseBody):
        #                 responseBody.update({"Reason":reason})
        #             else:
        #                 responseBody["Reason"] = reason
        #             GetResponse(responseURL, responseBody)

    #If the action of CloudFormation is Update stack
    if event['RequestType'] == "Update":
        #Retrieve parameters from Outputs tab of the stack and appending the dictionary with the PhysicalResourceId which is a required parameter for Update actions
        cf_sub_id, cf_event, cf_db_id, cf_sub_description = CurrentOutputs()
        PhysicalResourceId = event['PhysicalResourceId']
        responseBody.update({"PhysicalResourceId":PhysicalResourceId})
        sub_status = GetSubscriptionStatus(cf_sub_id)
        
        #Checking if the subscription is created/creating/deleting and taking actions based on that
        if str(sub_status) == "active":
            responseValue = PutSubscription(cf_sub_id, callEvent)
            print ("This is the event key after PUT call:")
            print (callEvent)
            #json.loads function limitation to properly convert boolean and '\' character
            cf_event = cf_event.replace("\'", "\"")
            cf_event = cf_event.replace("False", "false")
            cf_event = cf_event.replace("True", "true")
            cf_event = json.loads(cf_event)
            
            cf_event.update(callEvent)
            print (cf_event)
            #Updating Outputs with the new callEvent
            responseData.update({"SubscriptionId":str(cf_sub_id), "DefaultDatabaseId":str(cf_db_id), "SubscriptionDescription":str(cf_sub_description), "PostCall":str(cf_event)})
            print (responseData)
            responseBody.update({"Data":responseData})
            
            GetResponse(responseURL, responseBody)
          
        #If the subscription is still in pending, the update stack action will fail    
        elif str(sub_status) == "pending":
            responseValue = PutSubscription(cf_sub_id, callEvent)
            print ("this is response value for update in pending")
            print (responseValue)
            sub_error = GetSubscriptionError (responseValue['links'][0]['href'])
            responseStatus = 'FAILED'
            reason = str(sub_error)
            if responseStatus == 'FAILED':
                responseBody.update({"Status":responseStatus})
                if "Reason" in str(responseBody):
                    responseBody.update({"Reason":reason})
                else:
                    responseBody["Reason"] = reason
                GetResponse(responseURL, responseBody)
        
        #If the subscription is deleting, the update stack action will fail, obviously         
        elif str(sub_status) == "deleting":
            responseValue = PutSubscription(cf_sub_id, callEvent)
            sub_error = GetSubscriptionError (responseValue['links'][0]['href'])
            responseStatus = 'FAILED'
            reason = str(sub_error)
            if responseStatus == 'FAILED':
                responseBody.update({"Status":responseStatus})
                if "Reason" in str(responseBody):
                    responseBody.update({"Reason":reason})
                else:
                    responseBody["Reason"] = reason
                GetResponse(responseURL, responseBody)
    
    #If the action of CloudFormation is Delete stack    
    if event['RequestType'] == "Delete":
        #If the parameters cannot be retrieved, this means the stack was already deleted
        try:
            cf_sub_id, cf_event, cf_db_id, cf_sub_description = CurrentOutputs()
        except:
            responseStatus = 'SUCCESS'
            responseBody.update({"Status":responseStatus})
            GetResponse(responseURL, responseBody)
        all_subs = GetSubscription()
        print (all_subs)
        #Checking the number of databases assigned and if the default database is between them. If there are no databases assigned -> Failed. If there are more then 1 database -> Failed because another stack might
        #be impacted. If only one database is assigned and is not the default one -> Failed. If only one database is assigned AND is the default one -> Success.
        databases = GetAllDatabases(cf_sub_id)
        if str(cf_sub_id) in str(all_subs):
            if len(databases["subscription"][0]["databases"]) == 1:
                if str(databases["subscription"][0]["databases"][0]["databaseId"]) == str(cf_db_id):
                    try:
                        responseValue = DeleteSubscription(cf_sub_id, cf_db_id)
                        responseData.update({"SubscriptionId":str(cf_sub_id), "DefaultDatabaseId":str(cf_db_id), "SubscriptionDescription":str(cf_sub_description), "PostCall":str(cf_event)})
                        print (responseData)
                        responseBody.update({"Data":responseData})
                        GetResponse(responseURL, responseBody)
                    except:
                        responseStatus = 'FAILED'
                        reason = "Unable to delete subscription"
                        if responseStatus == 'FAILED':
                            responseBody.update({"Status":responseStatus})
                            if "Reason" in str(responseBody):
                                responseBody.update({"Reason":reason})
                            else:
                                responseBody["Reason"] = reason
                            GetResponse(responseURL, responseBody)
                else:
                    responseStatus = 'FAILED'
                    reason = "The only database assigned to subscription " + str(cf_sub_id) + " is not the default one."
                    if responseStatus == 'FAILED':
                        responseBody.update({"Status":responseStatus})
                        if "Reason" in str(responseBody):
                            responseBody.update({"Reason":reason})
                        else:
                            responseBody["Reason"] = reason
                        GetResponse(responseURL, responseBody)
            elif len(databases["subscription"][0]["databases"]) > 1:
                responseStatus = 'FAILED'
                reason = "Subscription " + str(cf_sub_id) + " has more than one database assigned. Please delete the other databases."
                if responseStatus == 'FAILED':
                    responseBody.update({"Status":responseStatus})
                    if "Reason" in str(responseBody):
                        responseBody.update({"Reason":reason})
                    else:
                        responseBody["Reason"] = reason
                    GetResponse(responseURL, responseBody)
            else:
                responseStatus = 'FAILED'
                reason = "Subscription " + str(cf_sub_id) + " has no databases assigned."
                if responseStatus == 'FAILED':
                    responseBody.update({"Status":responseStatus})
                    if "Reason" in str(responseBody):
                        responseBody.update({"Reason":reason})
                    else:
                        responseBody["Reason"] = reason
                    GetResponse(responseURL, responseBody)
        else:
            print("Subscription does not exists")
            GetResponse(responseURL, responseBody)

#This function retrieves x_api_key and x_api_secret_key from Secrets Manager service and returns them in the function as variables            
def RetrieveSecret(secret_name):
    headers = {"X-Aws-Parameters-Secrets-Token": os.environ.get('AWS_SESSION_TOKEN')}

    secrets_extension_endpoint = "http://localhost:2773/secretsmanager/get?secretId=" + str(secret_name)
    r = requests.get(secrets_extension_endpoint, headers=headers)
    secret = json.loads(r.text)["SecretString"]
    secret = json.loads(secret)

    return secret

#This function retrieves the parameters from Outputs tab of the stack to be used later
def CurrentOutputs():
    cloudformation = boto3.client('cloudformation')
    cf_response = cloudformation.describe_stacks(StackName=stack_name)
    for output in cf_response["Stacks"][0]["Outputs"]:
        if "SubscriptionId" in str(output): 
            cf_sub_id = output["OutputValue"]

        if "PostCall" in str(output): 
            cf_event = output["OutputValue"]

        if "DefaultDatabaseId" in str(output): 
            cf_db_id = output["OutputValue"]

        if "SubscriptionDescription" in str(output): 
            cf_sub_description = output["OutputValue"]
            
    print ("cf_sub_id is: " + str(cf_sub_id))
    print ("cf_event is: " + str(cf_event))
    print ("cf_db_id is: " + str(cf_db_id))
    print ("cf_sub_description is: " + str(cf_sub_description))
    return cf_sub_id, cf_event, cf_db_id, cf_sub_description
    
#Creating Flexible Subscription also requires creating a new Database within    
def PostSubscription (event):
    url = base_url + "/v1/subscriptions/" #will be changed based on new base_url
    
    response = requests.post(url, headers={"accept":accept, "x-api-key":x_api_key, "x-api-secret-key":x_api_secret_key, "Content-Type":content_type}, json = event)
    response_json = response.json()
    return response_json
    Logs(response_json)

def GetSubscription (subscription_id = ""):
    # If subscription_id string is empty, GET verb will print all Flexible Subscriptions
    url = base_url + "/v1/subscriptions/" + subscription_id
    
    response = requests.get(url, headers={"accept":accept, "x-api-key":x_api_key, "x-api-secret-key":x_api_secret_key})
    response_json = response.json()
    print ("This is the response after POST call: " + str(response_json))

    time.sleep(5)
    response = requests.get(response_json['links'][0]['href'], headers={"accept":accept, "x-api-key":x_api_key, "x-api-secret-key":x_api_secret_key})
    response_json = response.json()
    print ("This is the response 5 seconds after POST call: " + str(response_json))

    return response_json
    Logs(response_json)

#Check the subscription's status if it's active/pending/deleting
def GetSubscriptionStatus (subscription_id):
    url = base_url + "/v1/subscriptions/" + subscription_id
    
    response = requests.get(url, headers={"accept":accept, "x-api-key":x_api_key, "x-api-secret-key":x_api_secret_key})
    response = response.json()
    sub_status = response["status"]
    print ("Subscription status is: " + sub_status)
    return sub_status

#Returns subscription ID and it's description    
def GetSubscriptionId (url):
    response = requests.get(url, headers={"accept":accept, "x-api-key":x_api_key, "x-api-secret-key":x_api_secret_key})
    response = response.json()
    print (str(response))
    
    while "resourceId" not in str(response):
        time.sleep(1)
        response = requests.get(url, headers={"accept":accept, "x-api-key":x_api_key, "x-api-secret-key":x_api_secret_key})
        response = response.json()
    print (str(response))

    sub_id = response["response"]["resourceId"]
    sub_description = response["description"]
    return sub_id, sub_description

#Returns the error upon a wrong API call
def GetSubscriptionError (url):
    response = requests.get(url, headers={"accept":accept, "x-api-key":x_api_key, "x-api-secret-key":x_api_secret_key})
    response = response.json()

    while "processing-error" not in str(response):
        time.sleep(1)
        response = requests.get(url, headers={"accept":accept, "x-api-key":x_api_key, "x-api-secret-key":x_api_secret_key})
        response = response.json()

    sub_error_description = response["response"]["error"]["description"]
    return sub_error_description

#Returns the default Database ID    
def GetDatabaseId (subscription_id, offset = 0, limit = 100):
    url = base_url + "/v1/subscriptions/" + str(subscription_id) + "/databases?offset=" + str(offset) + "&limit=" + str(limit)
    
    response = requests.get(url, headers={"accept":accept, "x-api-key":x_api_key, "x-api-secret-key":x_api_secret_key})
    response_json = response.json()
    default_db_id = response_json["subscription"][0]["databases"][0]["databaseId"]
    return default_db_id
    Logs(response_json)

#Returns all databases assigned to the subscription    
def GetAllDatabases (subscription_id, offset = 0, limit = 100):
    url = base_url + "/v1/subscriptions/" + str(subscription_id) + "/databases?offset=" + str(offset) + "&limit=" + str(limit)
    
    response = requests.get(url, headers={"accept":accept, "x-api-key":x_api_key, "x-api-secret-key":x_api_secret_key})
    response_json = response.json()
    return response_json
    Logs(response_json)

#Makes the Update call to the subscription    
def PutSubscription (subscription_id, event):
    url = base_url + "/v1/subscriptions/" + subscription_id
    print (event)
    
    update_dict = {}
    for key in list(event):
    	if key == "name":
    	    update_dict['name'] = event[key]
    	elif key == "paymentMethodId":
    	    update_dict['paymentMethodId'] = event[key]
    
    response = requests.put(url, headers={"accept":accept, "x-api-key":x_api_key, "x-api-secret-key":x_api_secret_key, "Content-Type":content_type}, json = update_dict)
    print ("PutSubscription response is:")
    print(response)
    response_json = response.json()
    return response_json
    Logs(response_json)

#Deleting Subscription requires deleting the Database underneath it first    
def DeleteSubscription (subscription_id, database_id):
    db_url   = base_url + "/v1/subscriptions/" + subscription_id + "/databases/" + database_id
    subs_url = base_url + "/v1/subscriptions/" + subscription_id
    
    response_db   = requests.delete(db_url, headers={"accept":accept, "x-api-key":x_api_key, "x-api-secret-key":x_api_secret_key})
    response_subs = requests.delete(subs_url, headers={"accept":accept, "x-api-key":x_api_key, "x-api-secret-key":x_api_secret_key})
    Logs(response_db.json())
    Logs(response_subs.json())

#Send response back to CloudFormation    
def GetResponse(responseURL, responseBody): 
    responseBody = json.dumps(responseBody)
    req = requests.put(responseURL, data = responseBody)
    print ('RESPONSE BODY:n' + responseBody)

#Checks if there is an error in the description
def Logs(response_json):
    error_url = response_json['links'][0]['href']
    error_message = requests.get(error_url, headers={"accept":accept, "x-api-key":x_api_key, "x-api-secret-key":x_api_secret_key})
    error_message_json = error_message.json()
    if 'description' in error_message_json:
        while response_json['description'] == error_message_json['description']:
            error_message = requests.get(error_url, headers={"accept":accept, "x-api-key":x_api_key, "x-api-secret-key":x_api_secret_key})
            error_message_json = error_message.json()
        print(error_message_json)
    else:
        print ("No errors")
