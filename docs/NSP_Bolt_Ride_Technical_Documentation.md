# NSP Bolt Ride - Technical Documentation
## Real-Time Trip Processing System

### Table of Contents
1. [System Overview](#1-system-overview)
2. [Architecture Design](#2-architecture-design)
3. [Data Models](#3-data-models)
4. [Component Implementation](#4-component-implementation)
5. [Data Processing Flow](#5-data-processing-flow)
6. [Analytics & KPIs](#6-analytics--kpis)
7. [Deployment Guide](#7-deployment-guide)
8. [Monitoring & Maintenance](#8-monitoring--maintenance)

---

### 1. System Overview

#### 1.1 Purpose
NSP Bolt Ride's real-time processing system handles trip events for operational analytics, enabling:
- Real-time trip state tracking
- Event-driven processing
- Automated KPI generation
- Data-driven decision making

#### 1.2 Core Features
- Event streaming pipeline
- Stateful trip processing
- Daily KPI aggregation
- Scalable data storage
- Event-driven architecture

---

### 2. Architecture Design

#### 2.1 System Components
```plaintext
[Trip Events] → [Kinesis] → [Lambda 1] → [DynamoDB] → [Lambda 2] → [Step Functions] → [Glue] → [S3]
```

#### 2.2 AWS Services Utilization
1. **Amazon Kinesis**
   - Purpose: Real-time event ingestion
   - Configuration: Single stream with multiple shards
   - Location: `scripts/data_to_kinesis.py`

2. **AWS Lambda**
   - First Function: Event processing (`lambda_1.py`)
   - Second Function: Analytics trigger (`lambda_2.py`)
   - Location: `scripts/lambda_functions/`

3. **Amazon DynamoDB**
   - Purpose: Trip state management
   - Schema: Defined in implementation section
   
4. **AWS Glue**
   - Purpose: Data aggregation
   - Job Implementation: `scripts/glue.py`

5. **Amazon S3**
   - Purpose: Analytics storage
   - Structure: Partitioned by date

---

### 3. Data Models

#### 3.1 Event Schemas

##### Trip Start Event
```json
{
    "trip_id": "string",
    "data_type": "trip_start",
    "pickup_datetime": "ISO8601",
    "pickup_location_id": "integer",
    "dropoff_location_id": "integer",
    "vendor_id": "string",
    "estimated_dropoff_datetime": "ISO8601",
    "estimated_fare_amount": "float"
}
```

##### Trip End Event
```json
{
    "trip_id": "string",
    "data_type": "trip_end",
    "dropoff_datetime": "ISO8601",
    "actual_fare_amount": "float"
}
```

#### 3.2 DynamoDB Schema
```json
{
    "trip_id": "string (Primary Key)",
    "status": "string (STARTED|COMPLETED)",
    "pickup_datetime": "string",
    "dropoff_datetime": "string",
    "fare_amount": "number",
    "vendor_id": "string",
    "locations": {
        "pickup": "integer",
        "dropoff": "integer"
    }
}
```

---

### 4. Component Implementation

#### 4.1 Data Ingestion (`data_to_kinesis.py`)
```python
# Key functionality from data_to_kinesis.py
def send_data_to_kinesis(data, stream_name):
    # Sends events to Kinesis with:
    # - Unique record IDs
    # - Error handling
    # - Rate limiting
```

#### 4.2 Event Processing (`lambda_1.py`)
- Processes Kinesis events
- Updates DynamoDB
- Handles state transitions

#### 4.3 Analytics Trigger (`lambda_2.py`)
- Monitors DynamoDB streams
- Triggers Step Functions for completed trips

#### 4.4 Data Aggregation (`glue.py`)
```python
# Key KPI calculation logic
def calculate_kpis(df):
    return df.groupBy('trip_date').agg(
        F.sum('fare_amount').alias('total_fare'),
        F.count('trip_id').alias('count_trips'),
        F.avg('fare_amount').alias('average_fare'),
        F.max('fare_amount').alias('max_fare'),
        F.min('fare_amount').alias('min_fare')
    )
```

---

### 5. Data Processing Flow

#### 5.1 Event Processing Pipeline
1. **Event Ingestion**
   - Events sent to Kinesis
   - Ordered by partition key (trip_id)

2. **State Management**
   - Lambda processes events
   - Updates DynamoDB
   - Maintains trip state

3. **Analytics Processing**
   - Completed trips trigger analytics
   - Glue job aggregates data
   - Results written to S3

#### 5.2 Error Handling
- Retry mechanisms
- Error logging
- State validation

---

### 6. Analytics & KPIs

#### 6.1 Daily Metrics
- Total fare collection
- Trip count
- Average fare per trip
- Maximum fare
- Minimum fare

#### 6.2 Output Format
```json
{
    "date": "2024-01-01",
    "metrics": {
        "total_fare": 274393.88,
        "count_trips": 4999,
        "average_fare": 54.89,
        "max_fare": 90.16,
        "min_fare": 23.19
    }
}
```

---

### 7. Deployment Guide

#### 7.1 Prerequisites
- AWS Account
- Python 3.8+
- AWS CLI
- Required libraries:
  - boto3
  - pandas
  - aws-glue-libs

#### 7.2 Setup Steps
```bash
# 1. Environment Setup
python -m venv venv
source venv/Scripts/activate
pip install -r requirements.txt

# 2. AWS Configuration
aws configure

# 3. Start Pipeline
python scripts/data_to_kinesis.py
```

---

### 8. Monitoring & Maintenance

#### 8.1 Key Metrics
- Event processing latency
- DynamoDB throughput
- Lambda execution metrics
- Glue job performance

#### 8.2 Troubleshooting
1. **Event Processing Issues**
   - Check CloudWatch logs
   - Verify event format
   - Monitor DynamoDB capacity

2. **Analytics Issues**
   - Review Glue job logs
   - Validate input data
   - Check S3 permissions

---

### Appendix

#### A. Project Structure
```
project_root/
├── scripts/
│   ├── data_to_kinesis.py
│   ├── glue.py
│   └── lambda_functions/
│       ├── lambda_1.py
│       └── lambda_2.py
├── data/
│   ├── Trip_Start/
│   └── Trip_End/
└── notebook/
    └── bolt_ride_eda.ipynb
```

#### B. Configuration Parameters
- Kinesis stream name
- DynamoDB table name
- S3 bucket paths
- Lambda function settings

#### C. Performance Optimization
- Batch processing
- Partition strategies
- Caching mechanisms
- Resource scaling