# modules/highlighter.py
#
# Highlights extracted values in the PDF by searching for the text
# at the word level (using pdfplumber coordinates) and adding
# highlight annotations (using pypdf).
#
# Uses metadata from field_extractor (_WA_SOURCE, _CP_SOURCE, _IS_BIL_YES)
# to produce smart comments explaining WHY a value was set.

import re
import logging
from pathlib import Path

import pdfplumber
from pypdf import PdfReader, PdfWriter
from pypdf.annotations import Highlight
from pypdf.generic import ArrayObject, FloatObject, NameObject, TextStringObject

logger = logging.getLogger(__name__)

# Colors for each field (RGB 0-1)
FIELD_COLORS = {
    "PST_ISIN":           (1.0, 0.8, 0.0),    # yellow
    "ISSUER":             (0.6, 0.8, 1.0),    # light blue
    "CURRENCY":           (0.6, 1.0, 0.6),    # light green
    "MATURITY":           (1.0, 0.6, 0.6),    # light red
    "CAPITAL_PROTECTION": (0.8, 0.6, 1.0),    # light purple
    "COUPON":             (1.0, 0.9, 0.5),    # gold
    "WORST_OR_AVERAGE":   (1.0, 0.7, 0.4),    # orange
}

# Fields to skip entirely
SKIP_FIELDS = {"CLN", "BIL", "UNDERLYING_ISINS", "BLOOMBERG_TICKERS", "SSPA_TYPE"}


def _search_terms_and_comment(field: str, value, extracted_values: dict) -> list[tuple]:
    """
    Generate (search_term, comment) pairs for a given field.
    The comment explains why this value was highlighted.
    """
    if value is None or value == "" or value == "nan":
        return []
    v = str(value)

    wa_source = extracted_values.get("_WA_SOURCE", "")
    cp_source = extracted_values.get("_CP_SOURCE", "")
    sspa_type = extracted_values.get("SSPA_TYPE", "")

    if field == "PST_ISIN":
        return [(v, f"ISIN produit: {v}")]

    if field == "ISSUER":
        terms = [(v, f"Emetteur: {v}")]
        parts = v.split()
        if len(parts) > 1 and len(parts[0]) > 3:
            terms.append((parts[0], f"Emetteur: {v}"))
        return terms

    if field == "CURRENCY":
        return [(v, f"Devise: {v}")]

    if field == "MATURITY":
        return [(v, f"Maturite: {v}")]

    if field == "COUPON":
        return [(v, f"Coupon: {v}")]

    if field == "CAPITAL_PROTECTION":
        if cp_source == "sspa":
            # Highlight the SSPA code instead of the capital protection value
            comment = f"Capital Protection inferee via SSPA {sspa_type} => {v}%"
            return [(sspa_type, comment)]
        if cp_source == "bil_yes":
            comment = "Produit BIL YES => 100% capital protection"
            return [("BIL YES", comment), ("BIL", comment)]
        # Explicit value found in PDF
        return [(f"{v}%", f"Capital Protection: {v}%"),
                (f"{v} %", f"Capital Protection: {v}%"),
                (v, f"Capital Protection: {v}%")]

    if field == "WORST_OR_AVERAGE":
        if wa_source == "single_underlying":
            comment = "Un seul sous-jacent => W par defaut"
            return [("Underlying", comment),
                    ("Sous-Jacent", comment),
                    ("Basiswert", comment),
                    ("UNDERLYING", comment)]
        if wa_source == "bil_yes":
            comment = "Produit BIL YES => W par defaut"
            return [("BIL YES", comment), ("BIL", comment)]
        # Pattern-based detection
        if v == "W":
            comment = "Worst-of detecte par pattern"
            return [("worst-of", comment), ("worst of", comment),
                    ("Worst", comment), ("Schlechteste", comment),
                    ("Barrier", comment), ("mauvaise", comment)]
        if v == "A":
            comment = "Average detecte par pattern"
            return [("Average", comment), ("average", comment)]

    return [(v, f"{field}: {v}")]


def highlight_pdf(pdf_path: str, output_path: str, extracted_values: dict) -> int:
    """
    Create an annotated copy of the PDF with highlights on extracted values.
    Returns the number of annotations added.
    """
    try:
        reader = PdfReader(pdf_path)
        writer = PdfWriter()
        for page in reader.pages:
            writer.add_page(page)

        count = 0
        with pdfplumber.open(pdf_path) as pdf:
            for page_num, page in enumerate(pdf.pages):
                ph = page.height
                page_words = page.extract_words(x_tolerance=2, y_tolerance=2)
                if not page_words:
                    continue

                for field, value in extracted_values.items():
                    # Skip metadata keys and excluded fields
                    if field.startswith("_") or field in SKIP_FIELDS:
                        continue

                    terms_and_comments = _search_terms_and_comment(
                        field, value, extracted_values
                    )
                    color = FIELD_COLORS.get(field, (0.5, 0.5, 0.5))

                    found = False
                    for term, comment in terms_and_comments:
                        if found:
                            break
                        for w in page_words:
                            if term.lower() in w["text"].lower() and len(term) >= 2:
                                x0 = float(w["x0"]) - 1
                                x1 = float(w["x1"]) + 1
                                y0 = ph - float(w["bottom"]) - 1
                                y1 = ph - float(w["top"]) + 1

                                hl = Highlight(
                                    rect=(x0, y0, x1, y1),
                                    quad_points=ArrayObject([
                                        FloatObject(x0), FloatObject(y1),
                                        FloatObject(x1), FloatObject(y1),
                                        FloatObject(x0), FloatObject(y0),
                                        FloatObject(x1), FloatObject(y0),
                                    ]),
                                )
                                hl[NameObject("/C")] = ArrayObject([
                                    FloatObject(color[0]),
                                    FloatObject(color[1]),
                                    FloatObject(color[2]),
                                ])
                                hl[NameObject("/T")] = TextStringObject(field)
                                hl[NameObject("/Contents")] = TextStringObject(comment)

                                writer.add_annotation(page_number=page_num, annotation=hl)
                                count += 1
                                found = True
                                break

        if count > 0:
            Path(output_path).parent.mkdir(parents=True, exist_ok=True)
            with open(output_path, "wb") as f:
                writer.write(f)

        logger.info(f"Highlighted {count} annotations in {output_path}")
        return count

    except Exception as e:
        logger.error(f"Highlight error: {e}")
        return 0
