import boto3
import time
from dotenv import find_dotenv, load_dotenv
import os

# Load environment variables
load_dotenv(find_dotenv())


def process_textract_document(s3_object_path):
    """
    This function uploads a document to an S3 bucket, processes it using Amazon Textract,
    and retrieves text and checkbox data by page from the analyzed document.

    Args:
        s3_object_path (str): Local path to the document to be uploaded and processed.

    Returns:
        dict: A dictionary containing extracted data by page, including text and checkboxes.
    """
    extracted_text = ''
    # Retrieve AWS credentials from environment variables
    access_key = os.environ.get('aws_access_key')
    secret_access_key = os.environ.get('aws_secret_access_key')
    s3_bucket = os.environ.get('bucket_name')
    # Initialize S3 client and Textract client
    s3_client = boto3.client('s3', region_name='ap-south-1', aws_access_key_id=access_key,
                             aws_secret_access_key=secret_access_key)
    textract_client = boto3.client('textract', region_name='ap-south-1', aws_access_key_id=access_key,
                                   aws_secret_access_key=secret_access_key)

    # Upload the document to S3
    s3_object_name = os.path.basename(s3_object_path)
    s3_client.upload_file(s3_object_path, s3_bucket, s3_object_name)

    # Start Textract job
    response = textract_client.start_document_analysis(
        DocumentLocation={
            'S3Object': {
                'Bucket': s3_bucket,
                'Name': s3_object_name
            }
        },
        FeatureTypes=["FORMS", "TABLES"]
    )
    job_id = response['JobId']
    print(f"Started Textract job with ID: {job_id}")

    # Wait for the Textract job to complete
    status = 'IN_PROGRESS'
    while status == "IN_PROGRESS":
        time.sleep(5)
        response = textract_client.get_document_analysis(JobId=job_id)
        status = response['JobStatus']
        print(f"Job status: {status}")

    # Retrieve job results
    result = []
    next_token = None
    while True:
        result.extend(response['Blocks'])
        if 'NextToken' in response:
            next_token = response['NextToken']
            response = textract_client.get_document_analysis(JobId=job_id, NextToken=next_token)
        else:
            break

    # Extract data by page
    pages_data = {}
    blocks_dict = {block['Id']: block for block in result}

    # Organize blocks by page
    for block in result:
        if block['BlockType'] == 'PAGE':
            page_number = block['Page']
            pages_data[page_number] = {
                'text': '',
                'checkboxes': []
            }

    # Extract text and checkboxes by page
    for block in result:
        if block['BlockType'] == 'LINE':
            page_number = block['Page']
            pages_data[page_number]['text'] += block['Text'] + ' '

        if block['BlockType'] == 'KEY_VALUE_SET' and 'KEY' in block['EntityTypes']:
            key = None
            value = None
            page_number = block['Page']
            for rel in block.get('Relationships', []):
                if rel['Type'] == 'CHILD':
                    key = ' '.join(
                        [blocks_dict[child_id]['Text'] for child_id in rel['Ids'] if 'Text' in blocks_dict[child_id]])

            for rel in block.get('Relationships', []):
                if rel['Type'] == 'VALUE':
                    value_block = blocks_dict[rel['Ids'][0]]
                    for val_rel in value_block.get('Relationships', []):
                        if val_rel['Type'] == 'CHILD':
                            for child_id in val_rel['Ids']:
                                child_block = blocks_dict[child_id]
                                if child_block['BlockType'] == 'SELECTION_ELEMENT':
                                    status = "Selected" if child_block[
                                                               'SelectionStatus'] == 'SELECTED' else "Not Selected"
                                    value = status

            if key and value:
                pages_data[page_number]['checkboxes'].append({'header': key, 'status': value})
    for page_number, data in pages_data.items():
        extracted_text += f"Page number: {page_number}" + '\n' + f"Plain Data:\n{data['text']}" + '\n \n' + "Check Box Data:" + '\n' + '\n'.join([f"Header: {checkbox['header']}, Checkbox Status: {checkbox['status']}" for checkbox in data['checkboxes']])+ '\n' + '-------------------------------------------------------'+'\n'
    return extracted_text


def extract_text_from_pdf(pdf_file_path):
    # Upload the PDF file to S3
    access_key = os.environ.get('aws_access_key')
    secret_access_key = os.environ.get('aws_secret_access_key')
    bucket_name = os.environ.get('bucket_name')
    object_name = pdf_file_path
    s3_client = boto3.client('s3', region_name='ap-south-1', aws_access_key_id=access_key, aws_secret_access_key=secret_access_key)

    s3_client.upload_file(pdf_file_path, bucket_name, object_name)

    # Initialize the Textract client
    textract_client = boto3.client('textract', region_name='ap-south-1', aws_access_key_id=access_key, aws_secret_access_key=secret_access_key)

    # Start the Textract analysis
    response = textract_client.start_document_text_detection(
        DocumentLocation={
            'S3Object': {
                'Bucket': bucket_name,
                'Name': object_name
            }
        }
    )

    # Get the JobId from the response
    job_id = response['JobId']

    # Poll the Textract job status
    while True:
        result = textract_client.get_document_text_detection(JobId=job_id)
        status = result['JobStatus']

        if status in ['SUCCEEDED', 'FAILED']:
            break

        print(f"Job status: {status}")
        time.sleep(5)  # Adjust the polling interval as needed

    # Check if the job was successful
    if status == 'SUCCEEDED':
        # Extract and return the text
        extracted_text = {}
        next_token = None
        while True:
            if next_token:
                result = textract_client.get_document_text_detection(JobId=job_id, NextToken=next_token)
            else:
                result = textract_client.get_document_text_detection(JobId=job_id)

            for item in result['Blocks']:
                if item['BlockType'] == 'LINE':
                    page_number = item['Page']
                    text = item['Text']
                    # if "notes" in text.lower():  # Adjust this check based on the structure of your text
                    #     continue  # Skip this line
                    if page_number not in extracted_text:
                        extracted_text[page_number] = ""
                    extracted_text[page_number] += text + '\n'

            next_token = result.get('NextToken')
            if not next_token:
                break
        combined_text = ""
        for page_number in extracted_text.keys():
            if page_number in extracted_text:
                combined_text += f"Page {page_number}:\n{extracted_text[page_number]}\n"
        return combined_text.strip()
    else:
        # Handle the case where the job failed
        print(f"Textract job failed with status: {status}")
        return None
