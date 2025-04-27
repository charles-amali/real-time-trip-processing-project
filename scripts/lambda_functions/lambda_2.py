import json
import boto3
import os
import logging
from datetime import datetime

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

dynamodb = boto3.client('dynamodb')
stepfunctions = boto3.client('stepfunctions')

TABLE_NAME = os.environ.get('DYNAMODB_TABLE_NAME')
STATE_MACHINE_ARN = os.environ['STEP_FUNCTION_ARN']

if not TABLE_NAME:
    logger.error("Error: DYNAMODB_TABLE_NAME not set.")
    exit(1)

def trigger_step_function(trip_id):
    response = stepfunctions.start_execution(
        stateMachineArn=STATE_MACHINE_ARN,
        input=json.dumps({"trip_id": trip_id})
    )
    logger.info("Step Function triggered:", response)

def lambda_handler(event, context):
    logger.info(f"Received DynamoDB Stream event: {json.dumps(event)}")
    
    for record in event.get('Records', []):
        if record['eventName'] != 'INSERT':
            continue  
        
        dynamodb_record = record['dynamodb']
        
        # Extract trip_id from the Keys
        pk = dynamodb_record.get('Keys', {}).get('PK', {}).get('S')
        if not pk:
            logger.info("Skipping record with no PK")
            continue

        trip_id = pk
        logger.info(f"Processing trip_id: {trip_id}")
        
        # Query DynamoDB for all events with this trip_id
        try:
            response = dynamodb.query(
                TableName=TABLE_NAME,
                KeyConditionExpression="PK = :trip_id",
                ExpressionAttributeValues={":trip_id": {"S": trip_id}}
            )
        except Exception as e:
            logger.error(f"Error querying DynamoDB: {e}")
            continue  

        items = response.get("Items", [])
        logger.info(f"Found {len(items)} records for trip_id={trip_id}")

        trip_start = None
        trip_end = None

        for item in items:
            sk = item.get('SK', {}).get('S', '')
            if sk.startswith("RAW#trip_start#"):
                trip_start = item
            elif sk.startswith("RAW#trip_end#"):
                trip_end = item

        if not trip_start or not trip_end:
            logger.info(f"Incomplete trip for trip_id={trip_id}, skipping...")
            continue

        # Both trip_start and trip_end found ➔ create completed item
        completed_item = {
            'PK': {'S': trip_id},
            'SK': {'S': f"COMPLETED#{trip_start.get('pickup_datetime', {}).get('S', '')}"},
            'trip_id': {'S': trip_id},
            'status': {'S': 'completed'},
            'pickup_datetime': trip_start.get('pickup_datetime'),
            'dropoff_datetime': trip_end.get('dropoff_datetime'),
            'fare': trip_end.get('fare_amount'),
            'distance': trip_end.get('trip_distance'),
            'processing_timestamp_lambda2': {'S': datetime.utcnow().isoformat()}
        }

        pickup_loc = trip_start.get('pickup_location')
        if pickup_loc:
            completed_item['pickup_location'] = pickup_loc

        dropoff_loc = trip_end.get('dropoff_location')
        if dropoff_loc:
            completed_item['dropoff_location'] = dropoff_loc

        try:
            dynamodb.put_item(TableName=TABLE_NAME, Item=completed_item)
            logger.info(f"Completed trip saved for trip_id={trip_id}")
            trigger_step_function(trip_id) 
        except Exception as e:
            logger.error(f"Error saving completed trip: {e}")
            continue  
