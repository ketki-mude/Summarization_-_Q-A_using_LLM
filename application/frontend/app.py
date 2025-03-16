import streamlit as st
import boto3
import os
from dotenv import load_dotenv
import requests

# Load environment variables
load_dotenv()

# AWS S3 Configuration
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION = os.getenv("AWS_REGION")
S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME")

# Initialize S3 client
s3 = boto3.client(
    's3',
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    region_name=AWS_REGION
)

def list_s3_files():
    """List files from S3 bucket."""
    response = s3.list_objects_v2(Bucket=S3_BUCKET_NAME)
    return [item['Key'] for item in response.get('Contents', [])]

def upload_to_s3(file):
    """Upload a file to S3 bucket."""
    s3.upload_fileobj(file, S3_BUCKET_NAME, file.name)
    st.success(f"Uploaded {file.name} to {S3_BUCKET_NAME}")

def main():
    st.title("PDF Uploader and Viewer")

    # Upload new PDF
    uploaded_file = st.file_uploader("Upload a PDF file", type="pdf")
    if uploaded_file is not None:
        upload_to_s3(uploaded_file)

    st.write("---")

    # Select from existing PDFs in S3
    st.subheader("Select Existing PDF from S3")
    s3_files = list_s3_files()

    if s3_files:
        selected_file = st.selectbox("Choose a PDF:", s3_files)
        if st.button("Download PDF"):
            response = s3.get_object(Bucket=S3_BUCKET_NAME, Key=selected_file)
            st.download_button(label="Download PDF", data=response['Body'].read(), file_name=selected_file)
    else:
        st.warning("No files found in S3 bucket.")

if __name__ == "__main__":
    main()
