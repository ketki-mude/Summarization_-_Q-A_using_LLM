import boto3
import os
from dotenv import load_dotenv
from datetime import datetime

# Load environment variables
load_dotenv()

AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION = os.getenv("AWS_REGION")
S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME")

# Initialize S3 client
s3_client = boto3.client(
    's3',
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    region_name=AWS_REGION
)

def ensure_s3_folder_exists(bucket: str, folder_path: str):
    """
    Ensure the specified folder exists in the S3 bucket.

    Args:
        bucket: The S3 bucket name.
        folder_path: The folder path to check and create if needed.
    """
    try:
        s3_client.head_object(Bucket=bucket, Key=folder_path)
    except Exception as e:
        if 'Not Found' in str(e):
            s3_client.put_object(Bucket=bucket, Key=folder_path)

def upload_pdf_to_s3(file_content: bytes, original_filename: str, document_id: str) -> str:
    """
    Upload a PDF to the S3 bucket under the path: documents/pdf/

    Args:
        file_content: The binary content of the PDF file.
        original_filename: The original filename of the PDF.
        document_id: Unique identifier for the document.

    Returns:
        The S3 URL of the uploaded PDF.
    """
    pdf_folder = "documents/pdf/"
    ensure_s3_folder_exists(S3_BUCKET_NAME, pdf_folder)

    pdf_key = f"{pdf_folder}{document_id}_{original_filename}"

    s3_client.put_object(Bucket=S3_BUCKET_NAME, Key=pdf_key, Body=file_content, ContentType='application/pdf')

    pdf_url = f"https://{S3_BUCKET_NAME}.s3.{AWS_REGION}.amazonaws.com/{pdf_key}"
    return pdf_url

def upload_markdown_to_s3(markdown_content: str, document_id: str, base_name: str) -> str:
    """
    Upload Markdown content to the S3 bucket under the path: documents/markdown/

    Args:
        markdown_content: The Markdown content extracted from the PDF.
        document_id: Unique identifier for the document.
        base_name: Base name of the original PDF file.

    Returns:
        The S3 URL of the uploaded Markdown.
    """
    markdown_folder = "documents/markdown/"
    ensure_s3_folder_exists(S3_BUCKET_NAME, markdown_folder)

    markdown_key = f"{markdown_folder}{document_id}_{base_name}.md"

    s3_client.put_object(Bucket=S3_BUCKET_NAME, Key=markdown_key, Body=markdown_content.encode("utf-8"), ContentType='text/markdown')

    markdown_url = f"https://{S3_BUCKET_NAME}.s3.{AWS_REGION}.amazonaws.com/{markdown_key}"
    return markdown_url

def list_documents_from_s3() -> list:
    """
    List all available PDF and Markdown documents in the S3 bucket.

    Returns:
        A list of document keys available in the S3 bucket.
    """
    document_keys = []
    folders = ["documents/pdf/", "documents/markdown/"]

    for folder in folders:
        response = s3_client.list_objects_v2(Bucket=S3_BUCKET_NAME, Prefix=folder)

        for item in response.get("Contents", []):
            document_keys.append(item["Key"])

    return document_keys
