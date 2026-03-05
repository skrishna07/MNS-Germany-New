from azure.core.credentials import AzureKeyCredential
from azure.ai.formrecognizer import DocumentAnalysisClient
from dotenv import load_dotenv, find_dotenv
import os
import logging

from ReusableCodes.ReadExcelConfig import create_main_config_dictionary

import re

# Load environment variables
load_dotenv(find_dotenv())
endpoint = os.environ.get('endpoint')
key = os.environ.get('key')


# Function to check if a PDF is financial based on headers and fields
def analyze_read(pdf_path, headers, fields, contents):
    print(headers)
    try:
        # Initialize DocumentAnalysisClient
        document_analysis_client = DocumentAnalysisClient(
            endpoint=endpoint, credential=AzureKeyCredential(key)
        )

        # Read the PDF and analyze the document
        with open(pdf_path, "rb") as pdf:
            poller = document_analysis_client.begin_analyze_document("prebuilt-document", pdf)
            result = poller.result()

        header_keywords = [keyword.strip().lower() for keyword in headers]
        field_keywords = [keyword.strip().lower() for keyword in fields]
        content_keywords = [keyword.strip().lower() for keyword in contents]

        # Initialize a flag to check for the presence of 'ifrs'
        financial_pages = {}

        for page_number, page in enumerate(result.pages):
            # Reconstruct the page content from lines or words
            page_content = " ".join([line.content for line in page.lines]).lower() if page.lines else ""
            if not page_content:
                continue

            # Check for headers and fields
            page_has_header = any(header in page_content for header in header_keywords)
            page_field_count = sum(page_content.count(field) for field in field_keywords)
            page_has_content_keyword = any(content in page_content for content in content_keywords)

            # Log header and field count, skip if content keyword is present
            if page_has_header:
                logging.info(f"Page {page_number + 1}: Found header, Fields: {page_field_count}")
                if page_has_content_keyword:
                    logging.info(f"Page {page_number + 1}: Contains content keyword, skipping page.")
                    continue

            # A page is considered financial if it has headers and at least four fields
            if page_has_header and page_field_count >= 4:
                logging.info(f"Financial page found: Page {page_number + 1}")
                financial_pages[page_number] = page_field_count
            else:
                logging.info(f"Page {page_number + 1} is not a financial page.")

        # Select the page with the maximum fields from the financial pages
        if financial_pages:
            max_page_number = max(financial_pages, key=financial_pages.get)
            logging.info(f"Selected page: {max_page_number + 1}")

            # Determine the range of pages to extract
            start_page = max(0, max_page_number - 2)  # 2 pages before
            end_page = min(len(result.pages), max_page_number + 3)  # 4 pages after (total 6 pages including the selected page)

            # Combine content from the range of pages
            combined_content = ""
            for page_index in range(start_page, end_page):
                page = result.pages[page_index]
                page_content = " ".join([line.content for line in page.lines]).strip()
                combined_content += f"--- Page {page_index + 1} ---\n{page_content}\n"

            return combined_content
        else:
            logging.info("No financial pages found.")
            return None

    except Exception as e:
        logging.error(f"An error occurred during document analysis: {e}")
        return None

def extract_whole_pdf_data(pdf_path):
    # sample document
    print("extract_whole_pdf_data called")
    document_analysis_client = DocumentAnalysisClient(
        endpoint=endpoint, credential=AzureKeyCredential(key)
    )
    with open(pdf_path, "rb") as pdf:
        poller = document_analysis_client.begin_analyze_document(
            "prebuilt-document", pdf)
        result = poller.result()

    extracted_text = result.content
    return extracted_text