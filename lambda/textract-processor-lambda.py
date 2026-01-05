import json
import boto3
import urllib.parse
from datetime import datetime

# Initialize clients (Lambda execution role provides permissions automatically)
textract_client = boto3.client('textract')
s3_client = boto3.client('s3')

# Configuration
TARGET_BUCKET = '<your-data-bucket-name here>  # ⚠️ UPDATE THIS - Where JSON is saved


def lambda_handler(event, context):
    """
    Triggered automatically when image uploaded to S3
    Processes with Textract and saves JSON to DataItems bucket
    """

    try:
        # Get file info from S3 event
        bucket = event['Records'][0]['s3']['bucket']['name']
        key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'])

        print(f"📥 Processing: {key} from {bucket}")

        # Validate file type
        valid_extensions = ['.jpg', '.jpeg', '.png', '.pdf', '.tiff', '.tif']
        if not any(key.lower().endswith(ext) for ext in valid_extensions):
            print(f"⚠️ Skipping non-image file: {key}")
            return {'statusCode': 200, 'body': 'Not an image file'}

        # Call AWS Textract (file already in S3, no need to read bytes!)
        print("🤖 Calling Textract...")
        response = textract_client.analyze_document(
            Document={
                'S3Object': {
                    'Bucket': bucket,
                    'Name': key
                }
            },
            FeatureTypes=['FORMS', 'TABLES']
        )

        # Extract key-value pairs
        print("📊 Extracting key-value pairs...")
        key_values = extract_key_value_pairs(response)

        # Create output JSON
        output = {
            "status": "success",
            "source_file": key,
            "source_bucket": bucket,
            "data": key_values,
            "metadata": {
                "processed_at": datetime.utcnow().isoformat(),
                "pages_analyzed": response['DocumentMetadata']['Pages'],
                "total_fields": len(key_values)
            }
        }

        # Create JSON filename (same as image but with _json.txt)
        filename = key.split('/')[-1].rsplit('.', 1)[0]
        json_key = f"{filename}_json.txt"

        # Save JSON to DataItems bucket (separate bucket)
        print(f"💾 Saving to {TARGET_BUCKET}/{json_key}")
        s3_client.put_object(
            Bucket=TARGET_BUCKET,
            Key=json_key,
            Body=json.dumps(output, indent=2),
            ContentType='application/json',
            Metadata={
                'source-image': key,
                'source-bucket': bucket
            }
        )

        print(f"✅ Successfully processed: {key}")
        print(f"📄 Extracted {len(key_values)} fields")

        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Success',
                'input_file': key,
                'output_file': json_key,
                'fields_extracted': len(key_values)
            })
        }

    except Exception as e:
        print(f"❌ Error: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})
        }


# =================================================================
# HELPER FUNCTIONS FOR TEXTRACT PARSING
# =================================================================

def get_kv_map(blocks):
    """Creates map of KEY and VALUE blocks"""
    key_map = {}
    value_map = {}
    block_map = {}

    for block in blocks:
        block_id = block['Id']
        block_map[block_id] = block

        if block['BlockType'] == 'KEY_VALUE_SET':
            if 'KEY' in block.get('EntityTypes', []):
                key_map[block_id] = block
            else:
                value_map[block_id] = block

    return key_map, value_map, block_map


def get_text(block, block_map):
    """Extracts text from block by following relationships"""
    text = ''
    if 'Relationships' in block:
        for relationship in block['Relationships']:
            if relationship['Type'] == 'CHILD':
                for child_id in relationship['Ids']:
                    child = block_map.get(child_id)
                    if child and child['BlockType'] == 'WORD':
                        text += child.get('Text', '') + ' '
    return text.strip()


def find_value_block(key_block, value_map):
    """Finds the VALUE block associated with a KEY block"""
    if 'Relationships' in key_block:
        for relationship in key_block['Relationships']:
            if relationship['Type'] == 'VALUE':
                for value_id in relationship['Ids']:
                    return value_map.get(value_id)
    return None


def extract_key_value_pairs(response):
    """Extract all key-value pairs from Textract response"""
    blocks = response['Blocks']
    key_map, value_map, block_map = get_kv_map(blocks)

    kvs = {}

    for key_block_id, key_block in key_map.items():
        # Get key text
        key_text = get_text(key_block, block_map)

        # Find corresponding value
        value_block = find_value_block(key_block, value_map)
        value_text = get_text(value_block, block_map) if value_block else ''

        # Clean up
        key_text = key_text.strip().rstrip(':')
        value_text = value_text.strip()

        # Store
        if key_text:
            kvs[key_text] = value_text

    return kvs