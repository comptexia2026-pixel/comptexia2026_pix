I have merged multiple structured product term sheets into a single dataset to capture real-world variability.

I will provide my current Python extraction script, which uses regex and rule-based logic to extract fields such as ISIN, issuer, maturity, capital protection, currency, etc.

The main issue is that the extraction is not context-aware enough. In particular:

* The script often extracts the maturity or currency of the underlying assets instead of the product itself.
* This happens because the regex matches are not anchored to the correct semantic context (e.g., "Final Redemption Date", "Maturity Date", "Settlement Currency", etc.).

Your task is to significantly improve the extraction logic:

1. Make regex patterns context-aware by using surrounding text (before/after windows).
2. Prioritize matches that are close to relevant keywords such as:

   * "Maturity", "Final Redemption", "Expiry"
   * "Currency", "Settlement Currency", "Denomination"
3. Explicitly avoid matches located in sections related to:

   * "Underlying"
   * "Basket"
   * "Reference Asset"
4. Introduce a scoring system to rank candidates (distance to keywords, position in document, frequency, etc.).
5. Ensure the system selects the product-level information, not underlying-level data.
6. Keep the implementation in Python and consistent with a modular pipeline.
7. Comment the code clearly and explain the improvements.

The goal is to build a robust, production-ready extractor that handles noisy and heterogeneous financial documents.

I will now provide the current code.
