import json
import boto3
from botocore.exceptions import ClientError

# Initialize S3 client
s3_client = boto3.client('s3')

# Configuration
IMAGE_BUCKET = 'your-image-items-bucket'  # Where images are stored
DATA_BUCKET = 'your-data-items-bucket'  # Where JSON data is stored
URL_EXPIRATION = 3600  # Presigned URL expires in 1 hour


def lambda_handler(event, context):
    """
    Returns list of work items with:
    - Image presigned URLs from ImageItems bucket
    - Extracted metadata from DataItems bucket
    """

    try:
        print("🔍 Fetching work items...")

        # List all JSON files from DataItems bucket
        response = s3_client.list_objects_v2(
            Bucket=DATA_BUCKET
        )

        if 'Contents' not in response:
            print("📭 No items found")
            return create_response(200, [])

        work_items = []

        # Process each JSON file
        for obj in response['Contents']:
            json_key = obj['Key']

            # Only process JSON files
            if not json_key.endswith('_json.txt'):
                continue

            try:
                # Read the JSON data from DataItems bucket
                json_obj = s3_client.get_object(
                    Bucket=DATA_BUCKET,
                    Key=json_key
                )
                json_content = json.loads(json_obj['Body'].read().decode('utf-8'))

                # Get the original image filename
                source_file = json_content.get('source_file', '')

                # Extract just the filename (remove any path)
                image_filename = source_file.split('/')[-1] if source_file else json_key.replace('_json.txt', '.jpg')

                # Generate presigned URL for image from ImageItems bucket
                try:
                    image_url = s3_client.generate_presigned_url(
                        'get_object',
                        Params={
                            'Bucket': IMAGE_BUCKET,
                            'Key': image_filename
                        },
                        ExpiresIn=URL_EXPIRATION
                    )
                except ClientError as e:
                    print(f"⚠️ Could not generate URL for {image_filename}: {e}")
                    # Try with original source_file path
                    image_url = s3_client.generate_presigned_url(
                        'get_object',
                        Params={
                            'Bucket': IMAGE_BUCKET,
                            'Key': source_file
                        },
                        ExpiresIn=URL_EXPIRATION
                    )

                # Build work item response
                work_item = {
                    'key': image_filename,
                    'lastModified': obj['LastModified'].isoformat(),
                    'url': image_url,
                    'metadata': json_content.get('data', {})  # The extracted key-values
                }

                work_items.append(work_item)
                print(f"✅ Added: {image_filename} ({len(json_content.get('data', {}))} fields)")

            except Exception as e:
                print(f"⚠️ Error processing {json_key}: {e}")
                continue

        print(f"📊 Returning {len(work_items)} work items")

        return create_response(200, work_items)

    except Exception as e:
        print(f"❌ Error: {e}")
        return create_response(500, {
            'error': 'Failed to retrieve work items',
            'message': str(e)
        })


def create_response(status_code, body):
    """Create HTTP response with CORS headers"""
    return {
        'statusCode': status_code,
        'headers': {
            'Content-Type': 'application/json',
            'Access-Control-Allow-Origin': '*',  # Update with your domain in production
            'Access-Control-Allow-Headers': 'Content-Type',
            'Access-Control-Allow-Methods': 'GET,OPTIONS'
        },
        'body': json.dumps(body, default=str)
    }