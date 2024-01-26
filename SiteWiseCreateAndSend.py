import boto3
import time
import datetime
import uuid

# Use AWS credentials from your console or put them instead of None below
region_name = 'us-east-1'
aws_access_key_id = None
aws_secret_access_key = None

model_name = "TestModelName"

# AWS SiteWise client
client = boto3.client('iotsitewise', region_name=region_name,
                      aws_access_key_id=aws_access_key_id,
                      aws_secret_access_key=aws_secret_access_key)

def create_model_properties():
    return [
        create_measurement("X"),
        create_transform("2X", "x * 2", {"x": "X"}),
        create_metric("avgX", "avg(x)", {"x": "X"}),
        create_metric("lastX", "x", {"x": "X"}),
        create_metric("lastX_10m", "x", {"x": "X"}, "10m"),
        create_metric("latestX", "latest(x)", {"x": "X"}), 
        create_metric("TrueX", "eq(x, true)", {"x": "X"}),
        create_metric("XTrueDuration", "statetime(x)", {"x": "X"}), # duration in seconds when x > 0
    ]

def generate_10_minutes_data_points(start_time):
    dt = floor_time_to_10_minutes(start_time)
    return [
        double_value(1, dt + datetime.timedelta(minutes=0, seconds = 1)),
        double_value(0, dt + datetime.timedelta(minutes=1, seconds = 1)),
        double_value(2, dt + datetime.timedelta(minutes=2, seconds = 1)),
        # minute 3 and 4 are empty
        double_value(0, dt + datetime.timedelta(minutes=5, seconds = 0)),
        double_value(1, dt + datetime.timedelta(minutes=5, seconds = 20)),
        double_value(0, dt + datetime.timedelta(minutes=5, seconds = 40)),       
        double_value(1, dt + datetime.timedelta(minutes=6, seconds = 0)),
        double_value(2, dt + datetime.timedelta(minutes=7, seconds = 1)),
        double_value(3, dt + datetime.timedelta(minutes=8, seconds = 1)),
        # minute 9 is empty
    ]

def double_value(value, dt, quality = "GOOD"):
    epoch_seconds = int(dt.timestamp())
    nanoseconds = dt.microsecond * 1000
    return {'value':{'doubleValue': value},
            'timestamp': {
                'timeInSeconds': epoch_seconds, 
                'offsetInNanos': nanoseconds
                },
            'quality': quality
            }

def split_data(data, page_size):
    return [data[i:i+page_size] for i in range(0, len(data), page_size)]

def floor_time_to_10_minutes(dt):
    minute = (dt.minute // 10) * 10
    rounded_dt = dt.replace(minute=0, second=0, microsecond=0) + datetime.timedelta(minutes=minute)
    return rounded_dt

def batch_put_data(data, asset_id, property_id):
    for page in split_data(data, 10):
        batch_put_request = create_batch_put_data(page, asset_id, property_id)
        responce = client.batch_put_asset_property_value(**batch_put_request)
        result = "Success" if not responce['errorEntries'] else responce['errorEntries']
        print(f"{len(data)} point put with {result}")
        time.sleep(1)
    

def create_batch_put_data(datapoints, asset_id, property_id):
    return {"entries": [{
        'assetId': asset_id,
        'propertyId': property_id,
        'entryId': str(uuid.uuid4()),
        'propertyValues': datapoints
    }]}

def getModelProperties(model_id):
    model_responce = client.describe_asset_model(assetModelId=model_id)
    properties = {}
    for p in model_responce['assetModelProperties']:
        pType = next(iter(p['type']))
        properties[p['name']] = {'id': p['id'], 'type:':pType}
    return properties

def create_property(name, dataType='DOUBLE', unit='a.u.'):
    return {
        'name': name,
        'dataType': dataType,
        'unit': unit
        }

def create_variables(variables):
    propertyVariables = []
    for key,value in variables.items():
        propertyVariables.append({'name':key, 'value': {'propertyId':value}})
    return propertyVariables

def create_measurement(name):
    property = create_property(name)
    property['type'] = {'measurement': {}}
    return property

def create_transform(name, expression, variables):
    property = create_property(name)
    property['type'] = {'transform': {
        'expression': expression,
        'variables': create_variables(variables)
        }}
    return property

def create_metric(name, expression, variables, window="1m"):
    property = create_property(name)
    property['type'] = {'metric': {
        'expression': expression,
        'variables': create_variables(variables),
        'window': {'tumbling':{'interval':window}}
        }}
    return property

def get_model_id(name):
    response = client.list_asset_models()
    for model in response['assetModelSummaries']:
        if model['name'] == model_name:
            return model['id']
    return None

def create_model(name):
    kwargs = {'assetModelName': name}
    kwargs['assetModelProperties'] = create_model_properties()
    model_responce = client.create_asset_model(**kwargs)

    if 'assetModelId' not in model_responce:
        print("Failed to create model:", model_responce)
        exit(1)

    #wait for model active
    model_id=model_responce['assetModelId']
    while True:
        model_responce = client.describe_asset_model(assetModelId=model_id)
        status = model_responce['assetModelStatus']['state']
        print('Model: ', model_id, ', Status: ', status)
        if status == 'ACTIVE':
            break
        time.sleep(3)
    return model_id

def create_asset(asset_name, model_id):
    asset_responce = client.create_asset(assetName=asset_name, assetModelId=model_id)
    asset_id = asset_responce['assetId']

    # wait for all asset ready
    while True:
        asset_responce = client.describe_asset(assetId=asset_id)
        status = asset_responce['assetStatus']['state']
        print("Asset:", asset_id, ", Status: ", status)
        if status == 'ACTIVE':
            break  
        time.sleep(3)
    return asset_id

# Function to delete the model with assets
def delete_model(model_id):
    try:
        model_responce = client.describe_asset_model(assetModelId=model_id)
    except:
        print("Model does not exists.")
        return
    # List assets associated with the model
    assets_response = client.list_assets(assetModelId=model_id)
    asset_ids = [asset['id'] for asset in assets_response['assetSummaries']]
    for asset_id in asset_ids:
        print("Deleting assert Id:", asset_id)
        client.delete_asset(assetId=asset_id)

    # wait for all asset deleted
    for asset_id in asset_ids:
        while True:
            try:
                assert_responce = client.describe_asset(assetId=asset_id)
                status = assert_responce['assetStatus']['state']
                print("Asset:", asset_id, ", Status: ", status)
                time.sleep(3)
            except:
                print('Asset: ', asset_id, ', Status: Deleted.')
                break

    client.delete_asset_model(assetModelId=model_id)
    while True:
        try:
            model_responce = client.describe_asset_model(assetModelId=model_id)
            status = model_responce['assetModelStatus']['state']
            print('Model: ', model_id, ', Status: ', status)
            time.sleep(3)
        except:
            break
    print(f"Model with ID {model_id} deleted successfully.")

def get_values(asset_id, property_id, start, end):
    values = []
    responce = client.get_asset_property_value_history(assetId=asset_id, propertyId=property_id,
                                            startDate=int(start.timestamp()), endDate=int(end.timestamp()), 
                                            maxResults=1000)
    for tqv in responce['assetPropertyValueHistory']:
        values.append({
            "ts": datetime.datetime.fromtimestamp(tqv['timestamp']['timeInSeconds'] + 1e-9*tqv['timestamp']['offsetInNanos']),
            "val": tqv['value']['doubleValue']})
    return values
        


if __name__ == "__main__":
    model_id = get_model_id(model_name)
    if(model_id): delete_model(model_id)

    print("Creating new model", model_name)
    model_id = create_model(model_name)
    asset_id = create_asset("TestAsset", model_id)

    props = getModelProperties(model_id)
    print(f"Model {model_id} has following properties:")
    for name, data in props.items():
        print(f"\t{name}\t{data}")

    time.sleep(1)
    print("Puttign data to SiteWise")
    now = datetime.datetime.now()
    data = generate_10_minutes_data_points(now)

    batch_put_data(data, asset_id, props['X']['id'])

    time.sleep(1)
    end = now + datetime.timedelta(minutes=10)
    start = now - datetime.timedelta(minutes=10)
    print("Wait for 10 mins and dump computed results periodically")

    while end > datetime.datetime.now():
        time.sleep(10)
        for name,property in props.items():
            id = property['id']
            print(f"Property name:{name} id:{id}")
            
            vals = get_values(asset_id, id, start, end)
            if not vals: print("\tempty")
            for tv in vals:
                print(f"\t{tv['ts']}\t{tv['val']}")
    



    
    

