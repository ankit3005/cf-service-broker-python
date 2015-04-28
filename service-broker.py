import bottle
import requests
import json
import os
import boto
from boto.dynamodb2.table import Table
from boto.dynamodb2.types import NUMBER
from boto.dynamodb2.fields import (HashKey, RangeKey,
                                   AllIndex, KeysOnlyIndex, IncludeIndex,
                                   GlobalAllIndex, GlobalKeysOnlyIndex,
                                   GlobalIncludeIndex)

# constant representing the API version supported
# keys off HEADER X-Broker-Api-Version from Cloud Controller
X_BROKER_API_VERSION = 2.4
X_BROKER_API_VERSION_NAME = 'X-Broker-Api-Version'

# UPDATE THIS FOR YOUR ECHO SERVICE DEPLOYMENT
# service_base = "localhost"  # echo-service.stackato.danielwatrous.com

# service endpoint templates
# service_instance = "http://"+service_base+"/echo/{{instance_id}}"
# service_binding = "http://"+service_base+"/echo/{{instance_id}}/{{binding_id}}"
# service_dashboard = "http://"+service_base+"/echo/dashboard/{{instance_id}}"

# plans
# --------------
# big_plan = {
#           "id": "big_0001",
#           "name": "large",
#           "description": "A large dedicated service with a big storage quota, lots of RAM, and many connections",
#           "free": False
#         }
# 
# small_plan = {
#           "id": "small_0001",
#           "name": "small",
#           "description": "A small shared service with a small storage quota and few connections"
#         }
dynamo_plans = []

dynamo_service_dashboard = "https://{{region}}.console.aws.amazon.com/dynamodb/home?region={{region}}#"

regions_list = [
       ("us-east-1", "US East (N. Virginia)"),
       ("us-west-2", "US West (Oregon)"),
       ("us-west-1", "US West (N. California)"),
       ("eu-west-1", "EU (Ireland)"),         
       ("eu-central-1", "EU (Frankfurt)"),
       ("ap-southeast-1","Asia Pacific (Singapore)"),
       ("ap-southeast-2", "Asia Pacific (Sydney)"),
       ("ap-northeast-1", "Asia Pacific (Tokyo)"),
       ("sa-east-1", "South America (Sao Paulo)")
        ]


# services
# --------------
# big_plan = {
# echo_service = {'id': 'echo_service', 'name': 'Echo Service', 'description': 'Echo back the value received', 'bindable': True, 'plans': [big_plan]}
# invert_service = {'id': 'invert_service', 'name': 'Invert Service', 'description': 'Invert the value received', 'bindable': True, 'plans': [small_plan]}
dynamodb_service = {'id': 'dynamodb_service', 'name': 'DynamoDB', 'description': 'AWS DynamoDB instances', 'bindable': True, 'plans': dynamo_plans}


# mapping between service instance_id  and provison_details
broker_map = {}
binding_map = {}


@bottle.error(401)
@bottle.error(409)
def error(error):
    bottle.response.content_type = 'application/json'
    return '{"error": "%s"}' % error.body

def authenticate(username, password):
    if (username == 'demouser' and password == 'demopassword') :
        return True
    return False

@bottle.route('/v2/catalog', method='GET')
@bottle.auth_basic(authenticate)
def catalog():
    """
    Return the catalog of services handled
    by this broker

    GET /v2/catalog:

    HEADER:
        X-Broker-Api-Version: <version>

    return:
        JSON document with details about the
        services offered through this broker
    """
    api_version = bottle.request.headers.get('X-Broker-Api-Version')
    if not api_version or float(api_version) < X_BROKER_API_VERSION:
        #bottle.abort(409, "Missing or incompatible %s. Expecting version %0.1f or later" % (X_BROKER_API_VERSION_NAME, X_BROKER_API_VERSION))
        print("INFO: Missing or incompatible %s. Expecting version %0.1f or later" % (X_BROKER_API_VERSION_NAME, X_BROKER_API_VERSION))
    return {"services": [dynamodb_service]}


@bottle.route('/v2/service_instances/<instance_id>', method='PUT')
@bottle.auth_basic(authenticate)
def provision(instance_id):
    """
    Provision an instance of this service
    for the given org and space

    PUT /v2/service_instances/<instance_id>:
        <instance_id> is provided by the Cloud
          Controller and will be used for future
          requests to bind, unbind and deprovision

    BODY:
        {
          "service_id":        "<service-guid>",
          "plan_id":           "<plan-guid>",
          "organization_guid": "<org-guid>",
          "space_guid":        "<space-guid>"
        }

    return:
        JSON document with details about the
        services offered through this broker
    """

    if bottle.request.content_type != 'application/json':
        bottle.abort(415, 'Unsupported Content-Type: expecting application/json')
    # get the JSON document in the BODY
    provision_details = bottle.request.json

    print("Provision details:")
    print("-----------------------")
    print(provision_details)

    if instance_id in broker_map:
        # already provisioned earlier
        print(" already provisioned earlier")
        bottle.response.status = 409
        return {}

    region = provision_details["plan_id"][:-5]
    count = 0
    for item in regions_list:
        if item[0] != region:
            count = count + 1
    if count == 9:
        # region in request is not valid
        print("Invalid region specified")
        bottle.response.status = 409
        return {"description": "Invalid region specified"}

    try:
        connection = boto.dynamodb2.connect_to_region(region, aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)
        broker_map[instance_id] = provision_details
    except Exception as e: 
        print("aws_access_key_id: " + aws_access_key_id)
        print("aws_secret_access_key: " + aws_secret_access_key)
        print("region " + region)
        print("Exception in provision: " + e)
        bottle.response.status = 409
        return {}
    else:
        bottle.response.status = 201
        return {"dashboard_url": bottle.template(dynamo_service_dashboard, region=region)}

@bottle.route('/v2/service_instances/<instance_id>', method='DELETE')
@bottle.auth_basic(authenticate)
def deprovision(instance_id):
    """
    Deprovision an existing instance of this service

    DELETE /v2/service_instances/<instance_id>:
        <instance_id> is the Cloud Controller provided
          value used to provision the instance

    return:
        As of API 2.3, an empty JSON document
        is expected
    """

    # deprovision service
    if instance_id not in broker_map:
        # already deleted
        bottle.response.status = 410
        return {}
    else:
        # remove instance_id to provision_details mapping
        del broker_map[instance_id]
        # send response
        bottle.response.status = 200

@bottle.route('/v2/service_instances/<instance_id>/service_bindings/<binding_id>', method='PUT')
@bottle.auth_basic(authenticate)
def bind(instance_id, binding_id):
    """
    Bind an existing instance with the
    for the given org and space

    PUT /v2/service_instances/<instance_id>/service_bindings/<binding_id>:
        <instance_id> is the Cloud Controller provided
          value used to provision the instance
        <binding_id> is provided by the Cloud Controller
          and will be used for future unbind requests

    BODY:
        {
          "plan_id":           "<plan-guid>",
          "service_id":        "<service-guid>",
          "app_guid":          "<app-guid>"
        }

    return:
        JSON document with credentails and access details
        for the service based on this binding
        http://docs.cloudfoundry.org/services/binding-credentials.html
    """

    """
    Response body will be of form -
    {
      "credentials": {
            "aws_access_key_id": <aws_access_key_id>,
            "aws_secret_access_key": <aws_secret_access_key>,
            "region" : <region>
      }
    }
    """
    if bottle.request.content_type != 'application/json':
        bottle.abort(415, 'Unsupported Content-Type: expecting application/json')
    # get the JSON document in the BODY
    binding_details = bottle.request.json

    print("Binding details:")
    print("-----------------------")
    print(binding_details)

    req_plan_id = binding_details["plan_id"]
    app_guid = binding_details["app_guid"]
    plan_id = binding_details["plan_id"]

    if binding_id in binding_map:
        if binding_map[binding_id] == req_plan_id + app_guid:
            #already bound this service plan to this app 
            bottle.response.status = 200
            return {}
        else:
            #bound some different instance with this binding_id already
            bottle.response.status = 409
            return {}

    # add an entry in binding_map
    binding_map[binding_id] = req_plan_id + app_guid
   
    # populate the credentials dict to return
    credentials = {
            "aws_access_key_id": aws_access_key_id,
            "aws_secret_access_key": aws_secret_access_key,
            "region" : plan_id[:-5]
            }

    # return credentials with code 201
    bottle.response.status = 201
    return {"credentials": credentials}

@bottle.route('/v2/service_instances/<instance_id>/service_bindings/<binding_id>', method='DELETE')
@bottle.auth_basic(authenticate)
def unbind(instance_id, binding_id):
    """
    Unbind an existing instance associated
    with the binding_id provided

    DELETE /v2/service_instances/<instance_id>/service_bindings/<binding_id>:
        <instance_id> is the Cloud Controller provided
          value used to provision the instance
        <binding_id> is the Cloud Controller provided
          value used to bind the instance

    return:
        As of API 2.3, an empty JSON document
        is expected
    """

    # check for case of no existing service
    if binding_id not in binding_map:
        # already deleted
        bottle.response.status = 410
    else:
        del binding_map[binding_id]
        bottle.response.status = 200

    # send response
    return {}

if __name__ == '__main__':
    for region in regions_list:
        dynamo_plans.append(
            {
              "id": region[0] + "_0001",
              "name": region[0],
              "description": "AWS Region: " + region[1],

            })

    aws_access_key_id = os.getenv('AWS_ACCESS_KEY_ID')
    aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY')

    port = int(os.getenv('VCAP_APP_PORT', '8080'))
    #appInfo = os.getenv('VCAP_APPLICATION') # contains application info
    host = os.getenv('VCAP_APP_HOST', '0.0.0.0') # contains host name
    bottle.run(host=host, port=port, debug=True, reloader=False)
