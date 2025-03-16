import os
import sys
from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pathlib import Path
from datetime import datetime
import io
from dotenv import load_dotenv

# Add the backend directory to the Python path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from pdf_extract import PDFExtractor
from s3_utils import upload_pdf_to_s3, upload_markdown_to_s3

# Load environment variables
load_dotenv()

# Initialize FastAPI app
app = FastAPI(title="PDF Extractor API")

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Initialize PDF extractor
pdf_extractor = PDFExtractor()

@app.get("/")
async def root():
    return {"message": "PDF Extractor API is running"}

@app.post("/upload_pdf")
async def upload_pdf(file: UploadFile = File(...)):
    """Upload and process a PDF file"""
    if not file.filename.lower().endswith(".pdf"):
        raise HTTPException(status_code=400, detail="File must be a PDF")
    
    # Read file content
    file_content = await file.read()
    pdf_buffer = io.BytesIO(file_content)
    
    # Extract text from PDF
    try:
        markdown_output = pdf_extractor.extract_pdf(pdf_buffer, file.filename)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to process PDF: {str(e)}")
    
    # Get base name for file naming
    base_name = Path(file.filename).stem
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    document_id = f"{base_name}_{timestamp}"
    
    # Upload PDF to S3
    try:
        pdf_url = upload_pdf_to_s3(file_content, file.filename, document_id)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to upload PDF to S3: {str(e)}")
    
    # Upload extracted text to S3 as Markdown
    try:
        markdown_url = upload_markdown_to_s3(markdown_output, document_id, base_name)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to upload Markdown to S3: {str(e)}")
    
    return {
        "document_id": document_id,
        "original_filename": file.filename,
        "pdf_url": pdf_url,
        "markdown_url": markdown_url,
        "processing_date": timestamp
    }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)