import json
import boto3
import base64
import uuid
from datetime import datetime

# Initialize S3 client (uses Lambda's execution role - no credentials needed!)
s3_client = boto3.client('s3')

# Configuration
TARGET_BUCKET = '<upadte your image bucket name>  # ⚠️ UPDATE THIS
MAX_FILE_SIZE = 5 * 1024 * 1024  # 5MB limit
ALLOWED_TYPES = ['image/jpeg', 'image/png', 'image/jpg', 'application/pdf']


def lambda_handler(event, context):
    """
    Handles file upload from browser
    API Gateway sends file as base64 in request body
    """

    try:
        print("📥 Upload request received")

        # Parse request body
        body = json.loads(event.get('body', '{}'))

        # Get file data
        file_content = body.get('file')  # Base64 encoded file
        file_name = body.get('fileName', 'unknown.jpg')
        file_type = body.get('fileType', 'application/octet-stream')

        # Validate file exists
        if not file_content:
            return create_response(400, {
                'error': 'No file provided',
                'message': 'Please select a file to upload'
            })

        # Validate file type
        if file_type not in ALLOWED_TYPES:
            return create_response(400, {
                'error': 'Invalid file type',
                'message': f'Only JPG, PNG, and PDF files are allowed',
                'receivedType': file_type
            })

        # Decode base64 file
        try:
            # Remove data URL prefix if present (data:image/jpeg;base64,...)
            if ',' in file_content:
                file_content = file_content.split(',')[1]

            file_bytes = base64.b64decode(file_content)
        except Exception as e:
            return create_response(400, {
                'error': 'Invalid file encoding',
                'message': 'File must be base64 encoded'
            })

        # Validate file size
        file_size = len(file_bytes)
        if file_size > MAX_FILE_SIZE:
            return create_response(400, {
                'error': 'File too large',
                'message': f'File must be less than 5MB (received {file_size} bytes)'
            })

        if file_size == 0:
            return create_response(400, {
                'error': 'Empty file',
                'message': 'File is empty'
            })

        # Generate unique filename to avoid collisions
        timestamp = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
        unique_id = str(uuid.uuid4())[:8]
        safe_filename = file_name.replace(' ', '_')  # Remove spaces
        s3_key = f"{timestamp}_{unique_id}_{safe_filename}"

        print(f"📤 Uploading to S3: {s3_key}")
        print(f"   Size: {file_size} bytes")
        print(f"   Type: {file_type}")

        # Upload to S3
        s3_client.put_object(
            Bucket=TARGET_BUCKET,
            Key=s3_key,
            Body=file_bytes,
            ContentType=file_type,
            Metadata={
                'original-filename': file_name,
                'upload-timestamp': timestamp,
                'file-size': str(file_size)
            }
        )

        print(f"✅ Upload successful: {s3_key}")

        # Return success response
        return create_response(200, {
            'message': 'File uploaded successfully',
            'fileName': s3_key,
            'originalName': file_name,
            'size': file_size,
            'bucket': TARGET_BUCKET,
            'status': 'Processing will begin automatically'
        })

    except Exception as e:
        print(f"❌ Error: {str(e)}")
        return create_response(500, {
            'error': 'Upload failed',
            'message': str(e)
        })


def create_response(status_code, body):
    """
    Create HTTP response with CORS headers
    CORS allows your website to call this API from browser
    """
    return {
        'statusCode': status_code,
        'headers': {
            'Content-Type': 'application/json',
            'Access-Control-Allow-Origin': '*',  # ⚠️ Change to your website URL in production
            'Access-Control-Allow-Headers': 'Content-Type',
            'Access-Control-Allow-Methods': 'POST,OPTIONS'
        },
        'body': json.dumps(body)
    }