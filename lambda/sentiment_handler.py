import json
import boto3 # type: ignore
import os

# Initialize the S3 client
s3 = boto3.client('s3')

# Get the name of the curated bucket from environment variables set in CloudFormation
CURATED_BUCKET_NAME = os.environ.get('CURATED_BUCKET_NAME', 'socialmediapipeline-curated-data-000000000000')

def handler(event, context):
    """
    Handles SQS messages triggered by S3 file uploads.
    """
    print("Received event:", json.dumps(event))

    # The SQS message body contains the S3 event notification
    for record in event['Records']:
        # SQS message body needs to be loaded as JSON
        message_body = json.loads(record['body'])
        
        # Check if the message is a valid S3 event
        if 'Records' in message_body:
            s3_event_record = message_body['Records'][0]
            
            source_bucket = s3_event_record['s3']['bucket']['name']
            source_key = s3_event_record['s3']['object']['key']

            print(f"Processing file {source_key} from bucket {source_bucket}")

            try:
                # 1. READ the raw data file from S3
                response = s3.get_object(Bucket=source_bucket, Key=source_key)
                raw_data = response['Body'].read().decode('utf-8')

                # 2. TRANSFORM the data (simple uppercase for demonstration)
                processed_data = raw_data.upper()

                # 3. WRITE the processed data to the curated bucket
                curated_key = f"processed/{source_key}"
                s3.put_object(
                    Bucket=CURATED_BUCKET_NAME,
                    Key=curated_key,
                    Body=processed_data.encode('utf-8')
                )
                
                print(f"Successfully processed and saved to {CURATED_BUCKET_NAME}/{curated_key}")

            except Exception as e:
                print(f"Error processing {source_key}: {e}")
                # Raise the exception to trigger SQS retry mechanism
                raise e

    return {
        'statusCode': 200,
        'body': json.dumps('Processing complete')
    }