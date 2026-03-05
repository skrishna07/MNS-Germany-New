from azure.core.credentials import AzureKeyCredential
from azure.ai.formrecognizer import DocumentAnalysisClient
from dotenv import load_dotenv, find_dotenv
import os
import logging

# Load environment variables
load_dotenv(find_dotenv())
endpoint = os.environ.get('endpoint')
key = os.environ.get('key')

# Function to analyze the PDF for specific keywords
def analyze_read_auditors(pdf_path):
    try:
        # Initialize DocumentAnalysisClient
        document_analysis_client = DocumentAnalysisClient(
            endpoint=endpoint, credential=AzureKeyCredential(key)
        )

        # Read the PDF and analyze the document
        with open(pdf_path, "rb") as pdf:
            poller = document_analysis_client.begin_analyze_document("prebuilt-document", pdf)
            result = poller.result()

        # Variables to store search results
        page_with_auditing_keywords = None
        page_with_audit_findings = None

        # First search for "auditing company" or "auditing firm"
        for page_number, page in enumerate(result.pages):
            page_content = " ".join([line.content for line in page.lines]).lower() if page.lines else ""

            if "auditing company" in page_content or "auditing firm" in page_content:
                page_with_auditing_keywords = page_number
                logging.info(f"Found 'Auditing Company' or 'Auditing Firm' on Page {page_number + 1}")
                break  # Stop searching after finding the keywords

        # If not found, search for "audit findings"
        if page_with_auditing_keywords is None:
            for page_number, page in enumerate(result.pages):
                page_content = " ".join([line.content for line in page.lines]).lower() if page.lines else ""

                if "audit findings" in page_content:
                    page_with_audit_findings = page_number
                    logging.info(f"Found 'Audit Findings' on Page {page_number + 1}")
                    break  # Stop searching after finding "audit findings"

        # Extract content based on what was found
        if page_with_auditing_keywords is not None:
            selected_page = result.pages[page_with_auditing_keywords]
        elif page_with_audit_findings is not None:
            selected_page = result.pages[page_with_audit_findings]
        else:
            logging.info("No relevant keywords found in the document.")
            return None

        # Extract content from the selected page
        selected_page_content = " ".join([line.content for line in selected_page.lines]).strip()

        # Determine if the page contains "Consolidated" information
        if "consolidated balance sheet" in selected_page_content.lower() or \
           "consolidated income statement" in selected_page_content.lower() or \
           "consolidated" in selected_page_content.lower():
            selected_page_content += " Consolidated"
        else:
            selected_page_content += " Standalone"

        return selected_page_content

    except Exception as e:
        logging.error(f"An error occurred during document analysis: {e}")
        return None


