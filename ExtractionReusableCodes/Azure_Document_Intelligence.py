import pandas as pd
from azure.ai.formrecognizer import DocumentAnalysisClient
from azure.core.credentials import AzureKeyCredential
import os
from typing import Dict, List, Tuple, Any, Optional
from dotenv import load_dotenv, find_dotenv
load_dotenv(find_dotenv())


def _rect_from_polygon(polygon):
    xs = [p.x for p in polygon]
    ys = [p.y for p in polygon]
    return (min(xs), min(ys), max(xs), max(ys))


def _intersects(a, b):
    ax1, ay1, ax2, ay2 = a
    bx1, by1, bx2, by2 = b
    return not (ax2 < bx1 or bx2 < ax1 or ay2 < by1 or by2 < ay1)


def azure_pdf_to_excel_conversion(local_file_path: object, excel_file_path: object) -> object:
    # Initialize the DocumentAnalysisClient
    endpoint = os.environ.get('azure_form_recognizer_endpoint')
    key = os.environ.get('azure_form_recognier_key')
    document_analysis_client = DocumentAnalysisClient(endpoint=endpoint, credential=AzureKeyCredential(key))
    
    # Open the PDF file and analyze it directly
    with open(local_file_path, "rb") as file:
        poller = document_analysis_client.begin_analyze_document("prebuilt-layout", file)
        result = poller.result()

    # Prepare to collect DataFrames for each table
    table_dataframes = []

    for table_idx, table in enumerate(result.tables):
        # Create a list to hold the rows of the table
        table_data = []

        # Fill in the table data row by row
        for cell in table.cells:
            # Ensure we have a list for each row
            while len(table_data) <= cell.row_index:
                table_data.append([None] * table.column_count)  # Fill with None initially

            # Assign the cell content to the correct position in the row
            table_data[cell.row_index][cell.column_index] = cell.content

        # Create a DataFrame from the table data
        df = pd.DataFrame(table_data)

        # Optionally set the first row as the header (if your table has headers)
        df.columns = df.iloc[0]  # Set the first row as the header
        df = df[1:]  # Remove the header row from the data
        df.reset_index(drop=True, inplace=True)  # Reset index

        # Add the DataFrame to the list
        table_dataframes.append(df)

    # -----------------------------
    # NEW: Build table bounding rects by page (for filtering page text)
    # -----------------------------
    table_rects_by_page = {}
    for t in getattr(result, "tables", []) or []:
        for br in getattr(t, "bounding_regions", []) or []:
            if getattr(br, "polygon", None):
                page_no = br.page_number
                table_rects_by_page.setdefault(page_no, []).append(_rect_from_polygon(br.polygon))

    # -----------------------------
    # NEW: Extract page-wise text (prefer paragraphs; fallback to lines)
    # By default, we exclude text that overlaps tables.
    # -----------------------------
    page_text_blocks = {}  # {page_no: [text1, text2, ...]}

    paragraphs = getattr(result, "paragraphs", None)
    if paragraphs:
        for p in paragraphs:
            txt = (getattr(p, "content", "") or "").strip()
            if not txt:
                continue

            brs = getattr(p, "bounding_regions", None) or []
            if not brs:
                # Rare: no page info. Put into page 1.
                page_text_blocks.setdefault(1, []).append(txt)
                continue

            br = brs[0]
            page_no = br.page_number

            keep = True
            if getattr(br, "polygon", None):
                pr = _rect_from_polygon(br.polygon)
                # exclude text overlapping any table rect on that page
                for tr in table_rects_by_page.get(page_no, []):
                    if _intersects(pr, tr):
                        keep = False
                        break

            if keep:
                page_text_blocks.setdefault(page_no, []).append(txt)
    else:
        # Fallback: use lines from pages
        for pg in getattr(result, "pages", []) or []:
            page_no = pg.page_number
            for ln in getattr(pg, "lines", []) or []:
                txt = (getattr(ln, "content", "") or "").strip()
                if not txt:
                    continue

                keep = True
                if getattr(ln, "polygon", None):
                    lr = _rect_from_polygon(ln.polygon)
                    for tr in table_rects_by_page.get(page_no, []):
                        if _intersects(lr, tr):
                            keep = False
                            break

                if keep:
                    page_text_blocks.setdefault(page_no, []).append(txt)

    page_text = {p: "\n".join(v).strip() for p, v in page_text_blocks.items()}

    # Write the DataFrames to an Excel file
    with pd.ExcelWriter(excel_file_path, engine='openpyxl') as writer:
        for i, df in enumerate(table_dataframes):
            df.to_excel(writer, index=False, sheet_name=f'Table_{i + 1}')  # Write each DataFrame to a separate sheet
        # -----------------------------
        # NEW: Write page text into Excel as an additional sheet
        # -----------------------------
        text_rows = [{"Page": p, "Text": page_text.get(p, "")} for p in sorted(page_text.keys())]
        text_df = pd.DataFrame(text_rows) if text_rows else pd.DataFrame(columns=["Page", "Text"])
        text_df.to_excel(writer, index=False, sheet_name="Page_Text")
    # Return the list of DataFrames
    return table_dataframes, True