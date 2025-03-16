import os
import io
from pathlib import Path
from datetime import datetime
import fitz  # PyMuPDF
from dotenv import load_dotenv
from .s3_utils import upload_pdf_to_s3, upload_markdown_to_s3

# Load environment variables
load_dotenv()

class PDFExtractor:
    def extract_pdf(self, pdf_buffer: io.BytesIO, original_filename: str) -> str:
        """Extract text from PDF and return as Markdown."""
        try:
            # Open the PDF with PyMuPDF
            pdf_document = fitz.open(stream=pdf_buffer, filetype="pdf")
            text = ""
            for page_num in range(len(pdf_document)):
                page = pdf_document.load_page(page_num)
                text += page.get_text()

            return text

        except Exception as e:
            raise Exception(f"Failed to process PDF with PyMuPDF: {str(e)}")

if __name__ == "__main__":
    extractor = PDFExtractor()

    # Load a sample PDF file for testing
    pdf_path = "sample.pdf"
    with open(pdf_path, "rb") as f:
        pdf_content = io.BytesIO(f.read())

    try:
        markdown_output = extractor.extract_pdf(pdf_content, pdf_path)
        print("Extracted Markdown:\n", markdown_output)

        # Get base name for file naming
        base_name = Path(pdf_path).stem
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        document_id = f"{base_name}_{timestamp}"

        # Upload PDF to S3
        pdf_url = upload_pdf_to_s3(pdf_content.getvalue(), pdf_path, document_id)
        print(f"Uploaded PDF to S3: {pdf_url}")

        # Upload extracted text to S3 as Markdown
        markdown_url = upload_markdown_to_s3(markdown_output, document_id, base_name)
        print(f"Uploaded extracted text to S3: {markdown_url}")

    except Exception as e:
        print("Error during extraction:", str(e))
