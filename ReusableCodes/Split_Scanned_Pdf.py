import pytesseract
from PyPDF2 import PdfReader, PdfWriter
from pdf2image import convert_from_path
import os
import shutil
import sys

# from Split_Scanned_Pdf import fields

# Set path to Tesseract OCR executable
pytesseract.pytesseract.tesseract_cmd = r"C:\Program Files\Tesseract-OCR\tesseract.exe"

def extract_text_with_ocr(pdf_path, page_number, temp_dir):
    """Converts a PDF page to an image and extracts text using OCR."""
    images = convert_from_path(pdf_path, first_page=page_number + 1, last_page=page_number + 1, dpi=300)
    image_path = os.path.join(temp_dir, f"page_{page_number + 1}.png")
    images[0].save(image_path, "PNG")
    text = pytesseract.image_to_string(image_path)

    # Ensure file exists before attempting to delete
    if os.path.exists(image_path):
        os.remove(image_path)  # Clean up the temporary image
    return text

def count_fields_in_text(text, fields):
    """Counts occurrences of fields in the text."""
    return sum(field.lower() in text.lower() for field in fields)

def is_contents_page(text, contents_keywords):
    """Checks if the page is a contents page based on specific keywords."""
    return any(keyword.lower() in text.lower() for keyword in contents_keywords)

def split_pdf_based_on_headers_and_fields1(pdf_path, output_pdf_path, headers, fields, contents_keywords,
                                          pages_to_extract=3):
    # Read the PDF
    reader = PdfReader(pdf_path)
    total_pages = len(reader.pages)

    # Directory to temporarily store images
    temp_dir = "temp_images"
    os.makedirs(temp_dir, exist_ok=True)

    best_page = None
    max_fields_count = 0

    # Loop through each page in the PDF
    for page_number in range(total_pages):
        print("Page_number", page_number + 1)
        # Extract text using OCR
        text = extract_text_with_ocr(pdf_path, page_number, temp_dir)

        # Skip contents pages
        if is_contents_page(text, contents_keywords):
            continue

        # Check if the page contains any header and count fields
        if any(header.lower() in text.lower() for header in headers):
            fields_count = count_fields_in_text(text, fields)
            print("fields_Count : ", fields_count)
            if fields_count > max_fields_count:
                best_page = page_number
                max_fields_count = fields_count

    if best_page is not None:
        # Extract the best page and the next `pages_to_extract - 1` pages
        writer = PdfWriter()
        for i in range(best_page, min(best_page + pages_to_extract, total_pages)):
            writer.add_page(reader.pages[i])

        # Write to the output PDF
        with open(output_pdf_path, "wb") as output_file:
            writer.write(output_file)
            print(f"Header and fields found on page {best_page + 1}. Extracted pages {best_page + 1} to {min(best_page + pages_to_extract, total_pages)}.")
            split_success = True
    else:
        print("No relevant pages found.")
        split_success = False
    # Cleanup temporary directory: delete files and remove the directory
    if os.path.exists(temp_dir):
        try:
            shutil.rmtree(temp_dir)  # Recursively remove the directory and its contents
        except Exception as e:
            print(f"Error removing temporary directory: {e}")
    return split_success
def split_pdf_based_on_headers_and_fields(pdf_path, output_pdf_path, headers, fields, contents_keywords,
                                          pages_to_extract=5):
    # Read the PDF
    reader = PdfReader(pdf_path)
    total_pages = len(reader.pages)

    # Directory to temporarily store images
    temp_dir = "temp_images"
    os.makedirs(temp_dir, exist_ok=True)

    best_page = None
    max_fields_count = 0

    # Loop through each page in the PDF
    for page_number in range(total_pages):
        print("Page_number", page_number + 1)
        # Extract text using OCR
        text = extract_text_with_ocr(pdf_path, page_number, temp_dir)

        # Skip contents pages
        if is_contents_page(text, contents_keywords):
            continue

        # Check if the page contains any header and count fields
        if any(header.lower() in text.lower() for header in headers):
            fields_count = count_fields_in_text(text, fields)
            print("fields_Count : ", fields_count)
            if fields_count > max_fields_count:
                best_page = page_number
                max_fields_count = fields_count

    if best_page is not None:
        # Extract the best page and the next `pages_to_extract - 1` pages
        writer = PdfWriter()
        for i in range(best_page, min(best_page + pages_to_extract, total_pages)):
            writer.add_page(reader.pages[i])

        # Write to the output PDF
        with open(output_pdf_path, "wb") as output_file:
            writer.write(output_file)

        print(
            f"Header and fields found on page {best_page + 1}. Extracted pages {best_page + 1} to {min(best_page + pages_to_extract, total_pages)}.")
        split_success = True
    else:
        print("No relevant pages found.")
        split_success = False

    # Cleanup temporary directory: delete files and remove the directory
    if os.path.exists(temp_dir):
        try:
            shutil.rmtree(temp_dir)  # Recursively remove the directory and its contents
        except Exception as e:
            print(f"Error removing temporary directory: {e}")
    return split_success

# # Define headers and fields
# headers = [ "Revenue","Cost of Sales","Gross profit",
#     "Consolidated Statement of Comprehensive Income",
#     "STATEMENT OF FINANCIAL POSITION"
#     "Statement of comprehensive Income",
#     "balance sheet",
#     "balance sheet (mandatory scheme)",
#     "Consolidated balance sheet",
#     "Consolidated balance sheet (mandatory scheme)",
#     "Balance sheets",
#     "Statement of comprehensive Income",
#     "statement of Profit or loss and other comprehensive Income",
#     "statement of financial position",
#     "CONSOLIDATED STATEMENT OF COMPREHENSIVE INCOME"
# ]
# #
# # fields = [
# #     "Assets", "Revenue","Cost of Sales","Gross profit","staff costs","other operating profit","Tangible Assets", "Intangible assets", "Inventories", "Provisions", "Stocks", "Total Assets", "Total intangible fixed assets",
# #     "Total Liabilities", "concessions, licenses, trademarks and similar rights", "Total financial fixed assets", "Development costs","industrial patents and intellectual property rights",
# #     "Other Income","Inventories","plant and equipment","cash and bank balances", "Tangible Fixed Assets","Fixed assets", "intangible fixed assets", "Financial fixed assets", "Current assets","finished products and goods for resale",
# #     "Total current assets", "industrial patents and intellectual property rights", "subsidiary companies","Total receivables due from third parties",
# #     "Share capital", "Payables", "shareholders' equity", "assets in process of formation and advances","associated companies","raw, ancillary and consumable materials",
# #     "industrial and commercial equipment"
# # ]
# fields=[
#     "Revenue",
#     "Cost of sales",
#     "Gross profit",
#     "Other income",
#     "Selling and distribution costs",
#     "Administrative expenses",
#     "Other operating expenses",
#     "Finance costs",
#     "Profit before income tax",
#     "Income tax expense",
#     "Profit for the financial year, representing total comprehensive income for the financial year"
# ]
# contents_keywords = ["Table of Contents", "Contents"]
#
# # Perform the splitting
# pdf_path = r"C:\Users\BRADSOL\Downloads\Singapore New tags\SPLIT test\Pana Resources Pte.pdf"
# output_pdf_path = r"C:\Users\BRADSOL\Downloads\Singapore New tags\SPLIT test\splitPana Resources Pte.pdf"
# split_pdf_based_on_headers_and_fields(pdf_path, output_pdf_path, headers, fields, contents_keywords)
