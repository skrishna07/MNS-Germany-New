import pdfplumber


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
