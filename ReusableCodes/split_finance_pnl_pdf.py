"""
Improved Financial PDF Extractor
================================
Extracts Balance Sheet and Profit & Loss sections from German/English financial PDFs.

Key improvements over original:
1. Two-pass text extraction: tries pypdf first, falls back to OCR only when needed
2. Cross-statement penalty: penalizes pages that match BOTH BS and P&L terms
3. Mutual exclusivity: ensures BS and P&L don't pick the same/overlapping pages
4. Document order constraint: in HGB format, BS always precedes P&L
5. Header-first scoring: gives much higher weight to actual section headers vs field terms
6. Page boundary clamping: prevents negative indices and out-of-bounds errors
7. Deduplication of keyword lists to avoid score inflation
8. Uses find_end() instead of hardcoded offsets
9. Replaces stray print() with logging
10. Cleaned up error handling to avoid duplicate logging
"""

import re
import logging
import traceback
import sys
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Callable
from pypdf import PdfReader, PdfWriter
from rapidfuzz import fuzz

# Optional OCR imports — only needed if PDFs are scanned
try:
    from pdf2image import convert_from_path
    import pytesseract
    HAS_OCR = True
except ImportError:
    HAS_OCR = False

# -----------------------------
# Global Error Collector
# -----------------------------
errors: List[str] = []


def collect_error(e: Exception):
    tb = traceback.extract_tb(sys.exc_info()[2])[-1]
    file_name = tb.filename
    line_no = tb.lineno
    func_name = tb.name
    message = f"{file_name} | {func_name} | line {line_no} | {str(e)}"
    errors.append(message)
    logging.error(message)


# --- Tesseract path (Windows only, ignored if OCR not available) ---
if HAS_OCR:
    pytesseract.pytesseract.tesseract_cmd = (
        r"C:\Program Files\Tesseract-OCR\tesseract.exe"
    )


# -----------------------------
# Tuning Knobs
# -----------------------------
BS_MIN_FIELD_HITS = 6
PNL_MIN_FIELD_HITS = 6
MIN_HEADER_HITS = 1
MIN_DIGIT_COUNT = 80
OCR_DPI = 300
END_LOOKAHEAD_CAP = 40

# NEW: Minimum text length from pypdf before falling back to OCR
MIN_DIGITAL_TEXT_LENGTH = 50

# NEW: Cross-penalty multiplier — how much to penalize a page for
# matching the opposite statement's headers
CROSS_PENALTY_MULTIPLIER = 12

# NEW: Header weight increased significantly to prioritize actual section headers
HEADER_WEIGHT = 15  # was 8
FIELD_WEIGHT = 3    # unchanged
DIGIT_BONUS_CAP = 10


# -------------------------------------------------------
# Deduplicated keyword lists (removes duplicate entries)
# -------------------------------------------------------
def _dedup(lst: List[str]) -> List[str]:
    """Remove duplicate entries while preserving order."""
    seen = set()
    result = []
    for item in lst:
        key = item.strip().lower()
        if key not in seen:
            seen.add(key)
            result.append(item)
    return result


# Balance Sheet headers — terms that appear as SECTION TITLES
FINANCE_HEADERS_DEFAULT = _dedup([
    "Balance sheet as of",
    "balance sheet",
    "condensed balance sheet",
    "Annual financial statements for the financial year",
    "FIXED ASSETS",
    "Bilanz",
    "Aktiva",
    "Passiva",
    "Handelsbilanz",
    "Steuerbilanz",
    "Jahresabschluss zum",
    "Konzernbilanz",
])

# Balance Sheet field terms — line items that appear within a BS
FINANCE_FIELDS_DEFAULT = _dedup([
    "Non-current assets",
    "Tangible assets",
    "Current assets",
    "Equity",
    "Current Liabilities",
    "Intangible assets",
    "Financial assets",
    "Tangible fixed assets",
    "Inventories",
    "Fixed assets",
    "Total assets",
    "Net worth",
    "Paid-up funds",
    "Provisions",
    "Liabilities",
    "Cash",
    "Prepaid expenses",
    "Deferred tax assets",
    "other assets",
    "Sachanlagen",
    "Land",
    "rights equivalent to land and buildings, including buildings on third-party land",
    "Technical equipment and machinery",
    "Advance payments and assets under construction",
    "shares in affiliated companies",
    "Financial investments",
    "Deferred tax liabilities",
    "Trade accounts payable",
    "Subscribed capital",
    "Capital reserve",
    "Concessions",
    "Goodwill",
    "Shares in affiliated companies",
    "Other loans",
    "Raw materials, auxiliary materials and operating supplies",
    "Equity capital",
    "Provisions for pensions and similar obligations",
    "Liabilities to affiliated companies",
    "Receivables and other assets",
    "cash on hand and credit balances at credit institutions",
    "total assets",
    "financial investments",
    "Other retained earnings",
    "Anlagevermögen",
    "Umlaufvermögen",
    "Eigenkapital",
    "Fremdkapital",
    "Rückstellungen",
    "Verbindlichkeiten",
    "Forderungen",
    "Vorräte",
    "Kassenbestand",
    "Bankguthaben",
    "Finanzanlagen",
    "Immaterielle Vermögensgegenstände",
    "Sonstige Vermögensgegenstände",
    "Gezeichnetes Kapital",
    "Kapitalrücklage",
    "Gewinnrücklagen",
    "Bilanzsumme",
    "Summe Aktiva",
    "Summe Passiva",
])

# P&L headers — terms that appear as SECTION TITLES
PNL_HEADERS_DEFAULT = _dedup([
    "Profit and Loss Statement",
    "INCOME STATEMENT FOR THE",
    "PROFIT AND LOSS STATEMENT",
    "profit and loss statement",
    "condensed profit and loss",
    "Profit and loss account for",
    "Income statement for the financial year",
    "Profit and loss statement for",
    "INCOME STATEMENT",
    "Gewinn- und Verlustrechnung",
    "GuV",
    "Ertragsrechnung",
    "Erfolgsrechnung",
    "Aufwandsrechnung",
])

# P&L field terms — line items that appear within a P&L
PNL_FIELDS_DEFAULT = _dedup([
    "Turnover",
    "Gross profit",
    "Cost of Sales",
    "Income taxes",
    "Sales revenue",
    "Interest and similar expenses",
    "Other taxes",
    "Depreciation",
    "Personnel expenses",
    "Other operating income",
    "Taxes",
    "other operating expenses",
    "other interest and similar income",
    "Wages and salaries",
    "Social security contributions and pension expenses",
    "of which for pension provision",
    "Other capitalized own work",
    "Expenses for raw materials, consumables and supplies and for purchased goods",
    "Depreciation of intangible assets and property, plant and equipment",
    "Taxes on income and profits",
    "gross profit from sales",
    "distribution costs",
    "administrative costs",
    "operating result (EBIT)",
    "earnings before income taxes",
    "Financial result",
    "Earnings before taxes",
    "Gross result",
    "Wages and salaries",
    "social security contributions and expenditure on pensions and support",
    "of which for pensions",
    "on intangible assets and tangible assets",
    "Other operating expenses",
    "Expenses for services received",
    "Income from profit-pooling, profit-transfer and partial profit-transfer agreements",
    "Expenses from loss absorption",
    "Profit after tax",
    "Annual deficit",
    "Annual surplus",
    "Materialaufwand",
    "Umsatzerlöse",
    "Personalaufwand",
    "Abschreibungen",
    "Zinsaufwendungen",
    "Zinserträge",
    "Steuern vom Einkommen und vom Ertrag",
    "Jahresüberschuss",
    "Jahresfehlbetrag",
    "Betriebsergebnis",
    "Finanzergebnis",
    "Sonstige betriebliche Erträge",
    "Sonstige betriebliche Aufwendungen",
    "Rohergebnis",
    "Ergebnis der gewöhnlichen Geschäftstätigkeit",
])

# Terms that signal the END of a financial statement section
STOP_HEADERS_DEFAULT = [
    "Annex",
    "Anhang",
    "Notes to the financial statements",
    "Notes to the balance sheet",
    "Accounting and valuation methods",
    "General information on the annual financial statements",
    "Independent auditor",
    "Bestätigungsvermerk",
    "Development of fixed assets",
    "Anlagespiegel",
    "Auditor's report",
    "Audit opinions",
]

# Terms that indicate a "notes/annex" page (should be penalized for both BS and P&L)
NOTES_INDICATORS = [
    "accounting and valuation methods",
    "general information on the annual financial statements",
    "notes to the balance sheet",
    "notes to the profit and loss",
    "annex",
    "anhang",
    "bilanzierungs- und bewertungsmethoden",
    "erläuterungen zur bilanz",
    "development of fixed assets",
    "anlagespiegel",
]


# -----------------------------
# Helpers
# -----------------------------
def normalize(text: str) -> str:
    """Normalize text for comparison: lowercase, collapse whitespace."""
    text = text or ""
    text = text.lower()
    text = re.sub(r"\s+", " ", text).strip()
    return text


def digit_count(text: str) -> int:
    """Count digits in text."""
    return len(re.findall(r"\d", text or ""))


def is_likely_content_page(text: str, content_keywords: List[str]) -> bool:
    """Check if page is a table of contents (many keywords, few digits)."""
    t = normalize(text)
    if not t:
        return False
    kw_hits = sum(1 for kw in content_keywords if normalize(kw) in t)
    if kw_hits >= 2 and digit_count(t) < 30:
        return True
    return False


def is_notes_page(text: str) -> bool:
    """
    NEW: Check if a page is a notes/annex/accounting-policy page.
    These pages mention financial terms in a descriptive context,
    not as actual financial statement line items.
    """
    t = normalize(text)
    if not t:
        return False
    hits = sum(1 for indicator in NOTES_INDICATORS if normalize(indicator) in t)
    return hits >= 2


def fuzzy_hits(text: str, terms: List[str], threshold: int = 82) -> int:
    """
    Count how many terms match the text.
    Exact substring match = 2 points, fuzzy match = 1 point.
    """
    t = normalize(text)
    if not t:
        return 0

    hits = 0
    for term in terms:
        q = normalize(term)
        if not q:
            continue
        if q in t:
            hits += 2
            continue
        if max(fuzz.partial_ratio(q, t), fuzz.token_set_ratio(q, t)) >= threshold:
            hits += 1
    return hits


# --------------------------------------------------------
# Two-pass text extraction: digital first, OCR fallback
# --------------------------------------------------------
def ocr_page_text(pdf_path: Path, page_idx_0based: int, dpi: int = OCR_DPI) -> str:
    """OCR a single page. Requires pdf2image and pytesseract."""
    if not HAS_OCR:
        logging.warning("OCR libraries not available. Returning empty text.")
        return ""
    try:
        images = convert_from_path(
            str(pdf_path),
            dpi=dpi,
            first_page=page_idx_0based + 1,
            last_page=page_idx_0based + 1,
        )
        if not images:
            return ""
        return pytesseract.image_to_string(images[0]) or ""
    except Exception as e:
        collect_error(e)
        raise


def extract_all_texts(pdf_path: Path, ocr_fallback: bool = True) -> List[str]:
    """
    IMPROVED: Two-pass extraction.
    1. Try pypdf digital text extraction (fast).
    2. If text is too short, fall back to OCR (slow but handles scanned pages).
    """
    try:
        reader = PdfReader(str(pdf_path))
        total_pages = len(reader.pages)

        texts: List[str] = []
        ocr_count = 0

        for i in range(total_pages):
            # Pass 1: try digital extraction
            text = reader.pages[i].extract_text() or ""

            # Pass 2: fall back to OCR if text is too short
            if len(text.strip()) < MIN_DIGITAL_TEXT_LENGTH and ocr_fallback:
                logging.debug(
                    f"Page {i}: digital text too short ({len(text.strip())} chars), "
                    f"trying OCR..."
                )
                try:
                    text = ocr_page_text(pdf_path, i)
                    ocr_count += 1
                except Exception:
                    pass  # keep whatever digital text we got

            texts.append(text)

        logging.info(
            f"Extracted text from {total_pages} pages "
            f"({ocr_count} required OCR fallback)"
        )
        return texts

    except Exception as e:
        collect_error(e)
        raise


def extract_all_texts_ocr_only(pdf_path: Path) -> List[str]:
    """Original OCR-only extraction (kept for backward compatibility)."""
    try:
        reader = PdfReader(str(pdf_path))
        total_pages = len(reader.pages)

        texts: List[str] = []
        for i in range(total_pages):
            texts.append(ocr_page_text(pdf_path, i))
        return texts
    except Exception as e:
        collect_error(e)
        raise


# -------------------------------------------------------
# Scoring + Validation (IMPROVED)
# -------------------------------------------------------
def score_page(
    text: str,
    own_headers: List[str],
    own_fields: List[str],
    opposite_headers: List[str],
    content_keywords: List[str],
) -> int:
    """
    IMPROVED: Generic page scorer with cross-statement penalty.

    - Scores positively for matching own headers/fields.
    - Penalizes for matching the OPPOSITE statement's headers (cross-contamination).
    - Penalizes notes/annex pages that mention terms in descriptive context.
    """
    if not text:
        return -999

    if is_likely_content_page(text, content_keywords):
        return -200

    h = fuzzy_hits(text, own_headers, threshold=82)
    f = fuzzy_hits(text, own_fields, threshold=82)
    d = digit_count(text)

    base_score = (h * HEADER_WEIGHT) + (f * FIELD_WEIGHT) + min(d // 20, DIGIT_BONUS_CAP)

    # NEW: Cross-statement penalty
    opp_h = fuzzy_hits(text, opposite_headers, threshold=82)
    cross_penalty = opp_h * CROSS_PENALTY_MULTIPLIER

    # NEW: Notes/annex page penalty
    notes_penalty = 50 if is_notes_page(text) else 0

    return base_score - cross_penalty - notes_penalty


def validate_page(
    text: str,
    headers: List[str],
    fields: List[str],
    content_keywords: List[str],
    min_header_hits: int = MIN_HEADER_HITS,
    min_field_hits: int = BS_MIN_FIELD_HITS,
    min_digits: int = MIN_DIGIT_COUNT,
) -> bool:
    """
    IMPROVED: Generic page validator.
    Also rejects notes/annex pages.
    """
    if not text or is_likely_content_page(text, content_keywords):
        return False

    # NEW: reject notes/annex pages
    if is_notes_page(text):
        return False

    h = fuzzy_hits(text, headers, threshold=82)
    f = fuzzy_hits(text, fields, threshold=82)
    d = digit_count(text)

    return (h >= min_header_hits) and (f >= min_field_hits) and (d >= min_digits)


# -------------------------------------------------------
# Start + End detection (IMPROVED)
# -------------------------------------------------------
def find_best_start(
    texts: List[str],
    scorer: Callable[[str], int],
    validator: Callable[[str], bool],
    headers: Optional[List[str]] = None,
    fields: Optional[List[str]] = None,
    exclude_pages: Optional[set] = None,
    search_range: Optional[Tuple[int, int]] = None,
    min_field_hits: int = 6,
) -> Optional[int]:
    """
    IMPROVED: Find the best starting page.
    - Can exclude specific pages (for mutual exclusivity).
    - Can restrict search to a range (for document order constraints).
    - NEW: Considers merged adjacent pages when a header is on one page
      but the data continues on the next (common in German financial PDFs
      where the header appears at the bottom of one page).
    """
    try:
        best_idx = None
        best_score = -10_000

        start_i = search_range[0] if search_range else 0
        end_i = search_range[1] if search_range else len(texts)
        exclude = exclude_pages or set()

        for i in range(start_i, end_i):
            if i in exclude:
                continue
            t = texts[i]

            # Standard validation
            if validator(t):
                s = scorer(t)
                logging.debug(f"  Page {i}: score={s} (single)")
                if s > best_score:
                    best_score = s
                    best_idx = i

            # NEW: Adjacent-page merging heuristic
            # If this page has a section header but not enough fields,
            # check if merging with the NEXT page passes validation.
            # This handles the common case where the header ("Profit and
            # Loss Statement") is at the bottom of one page and the data
            # is on the next page.
            if headers and fields and i + 1 < end_i and (i + 1) not in exclude:
                h_hits = fuzzy_hits(t, headers, threshold=82)
                if h_hits >= MIN_HEADER_HITS:
                    merged = (t or "") + " " + (texts[i + 1] or "")
                    if validator(merged):
                        s = scorer(merged)
                        # Slight penalty for merged pages (prefer single-page match)
                        s -= 5
                        logging.debug(f"  Page {i}+{i+1}: score={s} (merged)")
                        if s > best_score:
                            best_score = s
                            best_idx = i

        return best_idx

    except Exception as e:
        collect_error(e)
        raise


def is_stop_page(text: str, stop_headers: List[str]) -> bool:
    """Check if a page signals the end of a section."""
    return fuzzy_hits(text, stop_headers, threshold=82) >= 1


def find_end(
    texts: List[str],
    start_idx: int,
    stop_headers: List[str],
    next_section_start: Optional[int] = None,
    lookahead_cap: int = END_LOOKAHEAD_CAP,
) -> int:
    """
    IMPROVED: Find the end page of a section.
    - Uses stop_headers if provided.
    - Also stops if we reach the next section's start page.
    - Stops at notes/annex pages.
    - Clamps to valid range.
    """
    try:
        n = len(texts)
        last = min(n - 1, start_idx + lookahead_cap)

        # If we know where the next section starts, don't go past it
        if next_section_start is not None and next_section_start > start_idx:
            last = min(last, next_section_start - 1)

        for j in range(start_idx + 1, last + 1):
            tj = texts[j] or ""

            # Stop if this page matches stop headers and has relatively few digits
            # (using a higher threshold than MIN_DIGIT_COUNT since notes pages
            # can have some digits from references, dates, etc.)
            if stop_headers:
                if is_stop_page(tj, stop_headers) and digit_count(tj) < 150:
                    return j - 1

            # NEW: Also stop at notes/annex pages
            if is_notes_page(tj) and digit_count(tj) < 150:
                return j - 1

        return last

    except Exception as e:
        collect_error(e)
        raise


# -------------------------------------------------------
# PDF writing (IMPROVED with bounds clamping)
# -------------------------------------------------------
def write_pdf_range(src_pdf: Path, out_pdf: Path, start: int, end: int) -> None:
    """
    IMPROVED: Write a range of pages from source PDF to output PDF.
    - Clamps start/end to valid page range.
    - Logs (instead of silently swallowing) errors when adding pages.
    """
    try:
        reader = PdfReader(str(src_pdf))
        total_pages = len(reader.pages)

        # Clamp to valid range
        start = max(0, start)
        end = min(end, total_pages - 1)

        if start > end:
            logging.warning(
                f"Invalid page range: start={start}, end={end}. Skipping."
            )
            return

        writer = PdfWriter()
        for p in range(start, end + 1):
            try:
                writer.add_page(reader.pages[p])
            except Exception as e:
                logging.warning(f"Failed to add page {p}: {e}")
                continue

        out_pdf.parent.mkdir(parents=True, exist_ok=True)
        with out_pdf.open("wb") as f:
            writer.write(f)

        logging.info(f"Wrote pages {start}-{end} to {out_pdf}")

    except Exception as e:
        collect_error(e)
        raise


# -------------------------------------------------------
# Main function (IMPROVED)
# -------------------------------------------------------
def create_two_pdfs(
    input_pdf: str,
    finance_headers: Optional[List[str]] = None,
    finance_fields: Optional[List[str]] = None,
    pnl_headers: Optional[List[str]] = None,
    pnl_fields: Optional[List[str]] = None,
    stop_headers: Optional[List[str]] = None,
    content_keywords: Optional[List[str]] = None,
    out_dir: str = ".",
    use_ocr_only: bool = False,
    pages_before: int = 0,
    pages_after: int = 3,
) -> Tuple[Optional[str], Optional[str], bool]:
    """
    IMPROVED main function.

    Changes from original:
    - Uses two-pass text extraction by default (digital + OCR fallback).
    - Applies cross-statement penalty scoring.
    - Enforces mutual exclusivity between BS and P&L pages.
    - Enforces document order (BS before P&L in HGB format).
    - Uses find_end() to determine actual section boundaries.
    - Clamps page ranges to valid bounds.
    - Clears global error list at start of each call.

    Parameters:
        input_pdf: Path to input PDF file.
        finance_headers: Balance sheet header terms (or None for defaults).
        finance_fields: Balance sheet field terms (or None for defaults).
        pnl_headers: P&L header terms (or None for defaults).
        pnl_fields: P&L field terms (or None for defaults).
        stop_headers: Terms that signal the end of a section.
        content_keywords: Terms that indicate a table-of-contents page.
        out_dir: Output directory for extracted PDFs.
        use_ocr_only: If True, use OCR for all pages (legacy behavior).
        pages_before: How many pages before detected start to include.
        pages_after: How many pages after detected start to include.

    Returns:
        (bs_path, pnl_path, success) tuple.
    """
    logging.basicConfig(level=logging.INFO)

    # Clear errors from any previous run
    errors.clear()

    try:
        pdf_path = Path(input_pdf)
        out_dir_path = Path(out_dir)

        original_file_name = pdf_path.name

        # Use defaults if not provided
        f_headers = finance_headers or FINANCE_HEADERS_DEFAULT
        f_fields = finance_fields or FINANCE_FIELDS_DEFAULT
        p_headers = pnl_headers or PNL_HEADERS_DEFAULT
        p_fields = pnl_fields or PNL_FIELDS_DEFAULT
        s_headers = stop_headers if stop_headers is not None else STOP_HEADERS_DEFAULT
        c_keywords = content_keywords or ["Contents", "Table of Contents"]

        # ---- Step 1: Extract text from all pages ----
        if use_ocr_only:
            texts = extract_all_texts_ocr_only(pdf_path)
        else:
            texts = extract_all_texts(pdf_path, ocr_fallback=HAS_OCR)

        total_pages = len(texts)
        logging.info(f"Processing {total_pages} pages from {pdf_path.name}")

        # ---- Step 2: Find Balance Sheet start ----
        bs_start = find_best_start(
            texts,
            scorer=lambda t: score_page(t, f_headers, f_fields, p_headers, c_keywords),
            validator=lambda t: validate_page(
                t, f_headers, f_fields, c_keywords,
                min_field_hits=BS_MIN_FIELD_HITS,
            ),
            headers=f_headers,
            fields=f_fields,
            min_field_hits=BS_MIN_FIELD_HITS,
        )
        if bs_start is not None:
            logging.info(f"Balance Sheet detected at page {bs_start} (0-indexed)")

        # ---- Step 3: Find P&L start ----
        # Don't start searching too far after BS — in some PDFs, BS and P&L
        # are on adjacent or nearby pages. But do exclude the BS start page itself.
        bs_exclude = set()
        if bs_start is not None:
            bs_exclude.add(bs_start)

        # In HGB format, P&L comes after (or on same page as end of) BS.
        # Allow searching from bs_start+1 onward (not bs_start itself).
        pnl_search_start = (bs_start + 1) if bs_start is not None else 0

        pnl_start = find_best_start(
            texts,
            scorer=lambda t: score_page(t, p_headers, p_fields, f_headers, c_keywords),
            validator=lambda t: validate_page(
                t, p_headers, p_fields, c_keywords,
                min_field_hits=PNL_MIN_FIELD_HITS,
            ),
            headers=p_headers,
            fields=p_fields,
            exclude_pages=bs_exclude,
            search_range=(pnl_search_start, total_pages),
            min_field_hits=PNL_MIN_FIELD_HITS,
        )
        if pnl_start is not None:
            logging.info(f"P&L detected at page {pnl_start} (0-indexed)")

        # ---- Step 4: Determine end pages using find_end ----
        bs_out = None
        pnl_out = None

        if bs_start is not None:
            bs_end = find_end(
                texts,
                start_idx=bs_start,
                stop_headers=s_headers,
                next_section_start=pnl_start,
                lookahead_cap=END_LOOKAHEAD_CAP,
            )
            # Also include pages_before the start
            bs_range_start = max(0, bs_start - pages_before)
            bs_range_end = min(total_pages - 1, max(bs_end, bs_start + pages_after))

            bs_out = out_dir_path / f"{original_file_name}_balance_sheet.pdf"
            write_pdf_range(pdf_path, bs_out, bs_range_start, bs_range_end)
            logging.info(
                f"Balance Sheet extracted: pages {bs_range_start}-{bs_range_end}"
            )

        if pnl_start is not None:
            pnl_end = find_end(
                texts,
                start_idx=pnl_start,
                stop_headers=s_headers,
                next_section_start=None,
                lookahead_cap=END_LOOKAHEAD_CAP,
            )
            pnl_range_start = max(0, pnl_start - pages_before)
            pnl_range_end = min(total_pages - 1, max(pnl_end, pnl_start + pages_after))

            pnl_out = out_dir_path / f"{original_file_name}_profit_and_loss.pdf"
            write_pdf_range(pdf_path, pnl_out, pnl_range_start, pnl_range_end)
            logging.info(
                f"P&L extracted: pages {pnl_range_start}-{pnl_range_end}"
            )

        # Convert to string paths for return
        bs_out_str = bs_out.as_posix() if bs_out is not None else None
        pnl_out_str = pnl_out.as_posix() if pnl_out is not None else None

        return bs_out_str, pnl_out_str, True

    except Exception as e:
        collect_error(e)
        if errors:
            raise Exception(
                "Errors occurred during processing:\n\n" + "\n".join(errors)
            )
        raise


# Backward-compatible wrapper matching original function signature
def create_two_pdfs_ocr_only(
    input_pdf: str,
    finance_headers,
    finance_fields,
    pnl_headers,
    pnl_fields,
    stop_headers,
    content_keywords,
    out_dir,
) -> Tuple[Optional[str], Optional[str], bool]:
    """Backward-compatible wrapper that uses the improved logic."""
    return create_two_pdfs(
        input_pdf=input_pdf,
        finance_headers=finance_headers,
        finance_fields=finance_fields,
        pnl_headers=pnl_headers,
        pnl_fields=pnl_fields,
        stop_headers=stop_headers,
        content_keywords=content_keywords,
        out_dir=out_dir,
        use_ocr_only=False,  # Use improved two-pass extraction
        pages_before=0,
        pages_after=3,
    )


# -------------------------------------------------------
# Entry point for testing
# -------------------------------------------------------
# if __name__ == "__main__":
#     import sys
#
#     logging.basicConfig(
#         level=logging.DEBUG,
#         format="%(asctime)s [%(levelname)s] %(message)s",
#     )
#
#     if len(sys.argv) >= 3:
#         input_file = sys.argv[1]
#         output_dir = sys.argv[2]
#     else:
#         input_file = r"C:\Users\BRADSOL123\Documents\MNS-Germany\Germany Financial Format\Germany Financial Format\6.pdf"
#         output_dir = r"C:\Users\BRADSOL123\Documents\MNS-Germany\Germany Financial Format\Germany Financial Format"
#
#     bs, pnl, ok = create_two_pdfs(
#         input_pdf=input_file,
#         out_dir=output_dir,
#     )
#
#     print(f"\nResults:")
#     print(f"  Balance Sheet: {bs}")
#     print(f"  P&L:           {pnl}")
#     print(f"  Success:       {ok}")