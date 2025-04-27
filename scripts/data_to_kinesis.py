import boto3
import pandas as pd
import os
from datetime import datetime
import json
import time
import uuid

def load_and_sort_data(start_path, end_path):
    # Load and sort trip start data
    trip_start_files = [f for f in os.listdir(start_path) if f.endswith('.csv')]
    trip_start_data = []
    
    for file in trip_start_files:
        df = pd.read_csv(os.path.join(start_path, file))
        df['data_type'] = 'start'  # Mark as start data
        df['pickup_datetime'] = pd.to_datetime(df['pickup_datetime'])  # Convert timestamp to datetime
        trip_start_data.append(df)
    
    trip_start_combined = pd.concat(trip_start_data, ignore_index=True)
    trip_start_sorted = trip_start_combined.sort_values(['pickup_datetime'], ascending=True)
    
    # Load and sort trip end data
    trip_end_files = [f for f in os.listdir(end_path) if f.endswith('.csv')]
    trip_end_data = []
    
    for file in trip_end_files:
        df = pd.read_csv(os.path.join(end_path, file))
        df['data_type'] = 'end'  # Mark as end data
        df['dropoff_datetime'] = pd.to_datetime(df['dropoff_datetime'])  # Convert timestamp to datetime
        trip_end_data.append(df)
    
    trip_end_combined = pd.concat(trip_end_data, ignore_index=True)
    trip_end_sorted = trip_end_combined.sort_values(['dropoff_datetime'], ascending=True)
    
    return trip_start_sorted, trip_end_sorted

def send_data_to_kinesis(data, stream_name, region='eu-west-1', delay=1):
    """
    Sends individual records to Kinesis with proper separation between start and end events.
    """
    kinesis_client = boto3.client('kinesis', region_name=region)
    
    for _, row in data.iterrows():
        try:
            if row['data_type'] == 'start':
                # Send trip start record
                start_record = {
                    'trip_id': str(row['trip_id']),  # Ensure trip_id is a string
                    'pickup_datetime': str(row['pickup_datetime']),
                    'data_type': 'trip_start',
                    'pickup_location_id': int(row['pickup_location_id']) if 'pickup_location_id' in row else None,
                    'dropoff_location_id': int(row['dropoff_location_id']) if 'dropoff_location_id' in row else None,
                    'vendor_id': row['vendor_id'] if 'vendor_id' in row else None,
                    'estimated_dropoff_datetime': str(row['estimated_dropoff_datetime']) if 'estimated_dropoff_datetime' in row else None,
                    'estimated_fare_amount': float(row['estimated_fare_amount']) if 'estimated_fare_amount' in row else None
                }
                
                # Add a unique sequence ID to prevent exact duplicates
                record_id = str(uuid.uuid4())
                start_record['record_id'] = record_id
                
                response = kinesis_client.put_record(
                    StreamName=stream_name,
                    Data=json.dumps(start_record),
                    PartitionKey=str(row['trip_id'])
                )
                print(f"Sent trip start record for trip_id: {row['trip_id']}, seq: {response['SequenceNumber']}")
                
            elif row['data_type'] == 'end':
                # Send trip end record
                end_record = {
                    'trip_id': str(row['trip_id']),  # Ensure trip_id is a string
                    'dropoff_datetime': str(row['dropoff_datetime']),
                    'data_type': 'trip_end',
                    'rate_code': row['rate_code'] if 'rate_code' in row else None,
                    'payment_type': row['payment_type'] if 'payment_type' in row else None,
                    'fare_amount': float(row['fare_amount']) if 'fare_amount' in row else None,
                    'trip_distance': float(row['trip_distance']) if 'trip_distance' in row else None,
                    'tip_amount': float(row['tip_amount']) if 'tip_amount' in row else None,
                    'trip_type': row['trip_type'] if 'trip_type' in row else None,
                    'passenger_count': int(row['passenger_count']) if 'passenger_count' in row else None
                }
                
                # Add a unique sequence ID to prevent exact duplicates
                record_id = str(uuid.uuid4())
                end_record['record_id'] = record_id
                
                response = kinesis_client.put_record(
                    StreamName=stream_name,
                    Data=json.dumps(end_record),
                    PartitionKey=str(row['trip_id'])
                )
                print(f"Sent trip end record for trip_id: {row['trip_id']}, seq: {response['SequenceNumber']}")
            
        except Exception as e:
            print(f"Error sending record to Kinesis for trip_id {row['trip_id']}: {e}")
        
        # Short delay to control the rate of sending
        time.sleep(delay)

def main():
    # Configure these paths according to your data location
    start_data_path = '../bolt_ride_project/data/Trip_Start'
    end_data_path = '../bolt_ride_project/data/Trip_End'
    stream_name = 'Bolt-Trip'
    
    try:
        # Load and sort trip start and trip end data separately
        trip_start_data, trip_end_data = load_and_sort_data(start_data_path, end_data_path)
        
        print(f"Loaded {len(trip_start_data)} trip start records and {len(trip_end_data)} trip end records")
    
        # Only use if you specifically need interleaved start/end events
        
        # Combine and sort all data by timestamp
        trip_start_data['event_time'] = trip_start_data['pickup_datetime']
        trip_end_data['event_time'] = trip_end_data['dropoff_datetime']
        all_data = pd.concat([trip_start_data, trip_end_data])
        all_data_sorted = all_data.sort_values('event_time', ascending=True)
        
        print("Sending mixed event data in time order...")
        send_data_to_kinesis(all_data_sorted, stream_name)
        
        
        print("Data loading complete!")
        
    except Exception as e:
        print(f"Error processing data: {e}")

if __name__ == "__main__":
    main()