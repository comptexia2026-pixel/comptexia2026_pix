# comptexia2026_pix
"""
scripts/extract_pdf_corpus.py
─────────────────────────────────────────────────────────────────────────────
Construit un corpus texte agrégé à partir de tous les PDFs de term sheets.

Usage :
    python scripts/extract_pdf_corpus.py
    python scripts/extract_pdf_corpus.py --input data/pdf_raw --output data/processed/corpus/pdf_corpus.csv

Sortie :
    data/processed/corpus/pdf_corpus.csv
    Colonnes : filename | pdf_path | text | n_pages | n_chars | status
─────────────────────────────────────────────────────────────────────────────
"""

import argparse
import logging
import time
from pathlib import Path

import pandas as pd
import pdfplumber

# ── Configuration du logging ──────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s │ %(levelname)-8s │ %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

# ── Constantes par défaut ─────────────────────────────────────────────────────
DEFAULT_INPUT_DIR = "data/pdf_raw"
DEFAULT_OUTPUT_PATH = "data/processed/corpus/pdf_corpus.csv"


# ─────────────────────────────────────────────────────────────────────────────
# Fonction 1 : extraction texte d'un PDF
# ─────────────────────────────────────────────────────────────────────────────

def extract_text_from_pdf(pdf_path: Path) -> dict:
    """
    Extrait le texte complet d'un PDF avec pdfplumber.

    Args:
        pdf_path : Chemin vers le fichier PDF.

    Returns:
        dict avec les clés :
            - text     (str)  : texte complet concaténé de toutes les pages
            - n_pages  (int)  : nombre de pages du PDF
            - n_chars  (int)  : nombre de caractères dans le texte extrait
            - status   (str)  : "ok" | "empty" | "error:<message>"
    """
    result = {
        "text": "",
        "n_pages": 0,
        "n_chars": 0,
        "status": "ok",
    }

    try:
        with pdfplumber.open(pdf_path) as pdf:
            result["n_pages"] = len(pdf.pages)
            page_texts = []

            for page in pdf.pages:
                try:
                    page_text = page.extract_text(x_tolerance=3, y_tolerance=3)
                    if page_text:
                        page_texts.append(page_text)
                except Exception as page_err:
                    # Une page défaillante ne bloque pas les suivantes
                    log.debug(f"  Page skipped in {pdf_path.name}: {page_err}")

            result["text"] = "\n".join(page_texts).strip()
            result["n_chars"] = len(result["text"])

            if result["n_chars"] == 0:
                result["status"] = "empty"

    except Exception as e:
        result["status"] = f"error:{e}"
        log.warning(f"  ✗ Cannot read {pdf_path.name} → {e}")

    return result


# ─────────────────────────────────────────────────────────────────────────────
# Fonction 2 : construction du corpus complet
# ─────────────────────────────────────────────────────────────────────────────

def build_corpus(input_dir: str, output_path: str) -> pd.DataFrame:
    """
    Parcourt tous les PDFs d'un dossier, extrait leur texte,
    et construit un DataFrame agrégé sauvegardé en CSV.

    Args:
        input_dir   : Dossier contenant les PDFs (récursif).
        output_path : Chemin du CSV de sortie.

    Returns:
        pd.DataFrame avec une ligne par PDF.
    """
    input_dir = Path(input_dir)
    output_path = Path(output_path)

    # ── Validation du dossier source ──────────────────────────────────────────
    if not input_dir.exists():
        raise FileNotFoundError(
            f"Dossier introuvable : '{input_dir}'\n"
            f"Vérifiez que vos PDFs sont bien dans ce dossier."
        )

    pdf_files = sorted(input_dir.rglob("*.pdf"))

    if not pdf_files:
        log.warning(f"Aucun PDF trouvé dans '{input_dir}'.")
        return pd.DataFrame()

    log.info(f"{'─' * 55}")
    log.info(f"  Dossier source  : {input_dir.resolve()}")
    log.info(f"  PDFs trouvés    : {len(pdf_files)}")
    log.info(f"  Sortie CSV      : {output_path.resolve()}")
    log.info(f"{'─' * 55}")

    # ── Création du dossier de sortie ─────────────────────────────────────────
    output_path.parent.mkdir(parents=True, exist_ok=True)

    # ── Boucle d'extraction ───────────────────────────────────────────────────
    records = []
    n_ok = n_empty = n_error = 0
    t_start = time.time()

    for i, pdf_path in enumerate(pdf_files, start=1):
        # Affichage progression
        pct = i / len(pdf_files) * 100
        log.info(f"[{i:>4}/{len(pdf_files)}] {pct:5.1f}%  {pdf_path.name}")

        extracted = extract_text_from_pdf(pdf_path)

        record = {
            "filename": pdf_path.name,
            "pdf_path": str(pdf_path),
            "text":     extracted["text"],
            "n_pages":  extracted["n_pages"],
            "n_chars":  extracted["n_chars"],
            "status":   extracted["status"],
        }
        records.append(record)

        # Compteurs de statut
        if extracted["status"] == "ok":
            n_ok += 1
        elif extracted["status"] == "empty":
            n_empty += 1
            log.warning(f"  ⚠ Texte vide : {pdf_path.name}")
        else:
            n_error += 1

    # ── Construction et sauvegarde du DataFrame ───────────────────────────────
    df = pd.DataFrame(records)
    df.to_csv(output_path, index=False, encoding="utf-8-sig")

    elapsed = time.time() - t_start

    # ── Résumé final ──────────────────────────────────────────────────────────
    log.info(f"{'─' * 55}")
    log.info(f"  RÉSUMÉ")
    log.info(f"{'─' * 55}")
    log.info(f"  Total traités   : {len(pdf_files)}")
    log.info(f"  ✓ Succès        : {n_ok}")
    log.info(f"  ⚠ Vides         : {n_empty}")
    log.info(f"  ✗ Erreurs       : {n_error}")
    log.info(f"  Durée           : {elapsed:.1f}s  ({elapsed/len(pdf_files):.2f}s/PDF)")
    log.info(f"  Corpus sauvé    : {output_path}")
    log.info(f"{'─' * 55}")

    return df


# ─────────────────────────────────────────────────────────────────────────────
# Point d'entrée
# ─────────────────────────────────────────────────────────────────────────────

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Extrait le texte de tous les PDFs et construit un corpus CSV."
    )
    parser.add_argument(
        "--input", "-i",
        default=DEFAULT_INPUT_DIR,
        help=f"Dossier des PDFs source (défaut : {DEFAULT_INPUT_DIR})",
    )
    parser.add_argument(
        "--output", "-o",
        default=DEFAULT_OUTPUT_PATH,
        help=f"Chemin du CSV de sortie (défaut : {DEFAULT_OUTPUT_PATH})",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()

    try:
        corpus_df = build_corpus(
            input_dir=args.input,
            output_path=args.output,
        )

        if not corpus_df.empty:
            print(f"\n✅ Corpus prêt — {len(corpus_df)} PDFs traités.")
            print(f"   Aperçu :\n{corpus_df[['filename','n_pages','n_chars','status']].head(5)}\n")

    except FileNotFoundError as e:
        log.error(str(e))
        raise SystemExit(1)
