import pdfplumber
import fitz  # PyMuPDF


def extract_text_from_readable_pdf(pdf_path):
    full_text = []

    with pdfplumber.open(pdf_path) as pdf:
        for page_num, page in enumerate(pdf.pages):
            full_text.append(f"\n--- Page {page_num + 1} ---\n")

            # Extract tables if any
            tables = page.extract_tables()
            if tables:
                for table in tables:
                    for row in table:
                        full_text.append("\t".join(row))
                    full_text.append("")  # Add a new line after each table

            # Extract text
            text = page.extract_text()
            if text:
                # Clean and format text
                lines = text.split('\n')
                for line in lines:
                    clean_line = line.strip().replace('\t', ' ')
                    full_text.append(clean_line)

    return '\n'.join(full_text)


"""
PDF Text Extractor - Separates Normal and Underlined Text
=========================================================
Uses PyMuPDF to detect underlines drawn as vector paths (thin horizontal lines),
then classifies each line of text as normal or underlined.

Install: pip install pymupdf

Usage:
    python extract_pdf_underlines.py input.pdf
    python extract_pdf_underlines.py input.pdf --output result.json
    python extract_pdf_underlines.py input.pdf --format text
"""


def extract_underline_rects(page):
    """
    Find all thin horizontal lines on the page that represent underlines.
    In this PDF format, underlines are drawn as zero-height vector paths.
    """
    underline_rects = []
    for path in page.get_drawings():
        r = path['rect']
        height = r.y1 - r.y0
        width = r.x1 - r.x0
        # Underlines are very thin (height < 3px) and wider than they are tall
        if height < 3 and width > 5:
            underline_rects.append(r)
    return underline_rects


def is_word_underlined(word, underline_rects, y_tolerance=8):
    """
    Check if a word bbox overlaps horizontally with an underline
    that sits just below the word's bottom edge.
    """
    wx0, wy0, wx1, wy1 = word[0], word[1], word[2], word[3]
    for ul in underline_rects:
        y_match = abs(wy1 - ul.y0) < y_tolerance
        x_overlap = wx0 < ul.x1 and wx1 > ul.x0
        if y_match and x_overlap:
            return True
    return False


def extract_text_by_type(pdf_path):
    """
    Extract text from PDF, separating normal and underlined text per page.

    Returns:
        dict with page keys, each containing:
            - normal_text: list of text lines without underlines
            - underlined_text: list of text lines with underlines
    """
    doc = fitz.open(pdf_path)
    result = {}

    for page_num, page in enumerate(doc):
        page_key = f"page_{page_num + 1}"

        # Step 1: Detect underline vector paths
        underline_rects = extract_underline_rects(page)

        # Step 2: Get all words with bounding boxes
        words = page.get_text("words")  # (x0, y0, x1, y1, text, block, line, word)

        # Step 3: Tag underlined words
        underlined_word_keys = set()
        for word in words:
            if is_word_underlined(word, underline_rects):
                underlined_word_keys.add((word[0], word[1], word[4]))

        # Step 4: Group words into lines by y-coordinate
        lines = {}
        for word in words:
            y_key = round(word[1], 0)
            if y_key not in lines:
                lines[y_key] = []
            lines[y_key].append(word)

        # Step 5: Classify each line as normal or underlined
        normal_lines = []
        underlined_lines = []

        for y_key in sorted(lines.keys()):
            line_words = sorted(lines[y_key], key=lambda w: w[0])
            line_text = " ".join(w[4] for w in line_words)

            has_underline = any(
                (w[0], w[1], w[4]) in underlined_word_keys
                for w in line_words
            )

            if has_underline:
                underlined_lines.append(line_text)
            else:
                normal_lines.append(line_text)

        result[page_key] = {
            "normal_text": normal_lines,
            "underlined_text": underlined_lines
        }

    doc.close()
    return result


def format_as_text(data):
    """Format extracted data as readable plain text."""
    output = []
    for page_key, content in data.items():
        output.append(f"\n{'=' * 60}")
        output.append(f"  {page_key.upper().replace('_', ' ')}")
        output.append(f"{'=' * 60}")

        output.append(f"\n[NORMAL TEXT]")
        for line in content["normal_text"]:
            output.append(f"  {line}")

        output.append(f"\n[UNDERLINED TEXT]")
        if content["underlined_text"]:
            for line in content["underlined_text"]:
                output.append(f"  {line}")
        else:
            output.append("  (none)")

    return "\n".join(output)