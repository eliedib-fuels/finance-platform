"""
Finance Platform — Budget Rates Loader
Loads FX budget rates from rates.xlsx.

Rate convention (from file):
  rates are TO EUR — local currency × rate = EUR amount
  USD rate = 0.869565 → EUR/USD = 1/0.869565 = 1.15

Derived:
  rate_to_usd = rate_to_eur / usd_eur_rate
  For USD entities: rate_to_usd = 1.0 always
"""

import pandas as pd
import logging
from datetime import date

log = logging.getLogger(__name__)


def load_rates(file_path: str, fiscal_year: int = 2026) -> dict:
    """
    Load budget rates from rates.xlsx into budget_rates table.

    Expected columns:
      Country | Currency | Budget 2026

    Rates are expressed as: local_amount × rate = EUR amount
    USD rate (0.869565) is used to derive EUR/USD cross rate.
    """
    from scripts.db import get_conn

    log.info(f"Loading budget rates from {file_path} for {fiscal_year}")

    df = pd.read_excel(file_path)
    df.columns = [c.strip() for c in df.columns]

    # Find the rate column — handles "Budget 2026" or similar
    rate_col = None
    for col in df.columns:
        if "budget" in col.lower() or str(fiscal_year) in col:
            rate_col = col
            break

    if not rate_col:
        raise ValueError(f"Cannot find rate column for {fiscal_year} in {file_path}")

    df = df.rename(columns={
        "Country":  "country",
        "Currency": "currency_desc",
        rate_col:   "rate_to_eur",
    })

    df["rate_to_eur"] = pd.to_numeric(df["rate_to_eur"], errors="coerce")
    df = df.dropna(subset=["rate_to_eur"])

    # --- Currency mapping ---
    # Map country names to ISO currency codes
    CURRENCY_MAP = {
        "ARGENTINA":      "ARS",
        "BRAZIL":         "BRL",
        "CANADA":         "CAD",
        "COLOMBIA":       "COP",
        "MEXICO":         "MXN",
        "PANAMA":         "USD",   # Balboa pegged to USD
        "UNITED STATES":  "USD",
        "UNITED STATES ": "USD",   # trailing space variant
    }

    df["currency"] = df["country"].astype(str).str.strip().str.upper().map(CURRENCY_MAP)
    unmapped = df[df["currency"].isna()]["country"].tolist()
    if unmapped:
        log.warning(f"Unmapped countries in rates file: {unmapped}")
    df = df.dropna(subset=["currency"])

    # --- Derive USD rate ---
    # USD rate in file = 0.869565 (1 USD = 0.869565 EUR)
    # EUR/USD = 1 / 0.869565 = 1.15
    usd_row = df[df["currency"] == "USD"]
    if usd_row.empty:
        raise ValueError("USD rate not found in rates file — cannot derive cross rates")

    usd_eur_rate = float(usd_row["rate_to_eur"].iloc[0])  # 0.869565
    eur_usd      = 1.0 / usd_eur_rate                      # 1.15

    log.info(f"USD→EUR rate: {usd_eur_rate:.6f} | EUR/USD: {eur_usd:.4f}")

    # --- Compute rate_to_usd for each currency ---
    # rate_to_usd = rate_to_eur / usd_eur_rate
    # (convert to EUR then back to USD)
    df["rate_to_usd"] = df["rate_to_eur"] / usd_eur_rate
    df["usd_eur_rate"] = usd_eur_rate

    # USD entities always 1.0
    df.loc[df["currency"] == "USD", "rate_to_usd"] = 1.0

    # --- Deduplicate (PANAMA and UNITED STATES both USD) ---
    df = df.drop_duplicates(subset=["currency"], keep="first")

    # --- Load to DB ---
    loaded = 0
    with get_conn() as conn:
        for _, row in df.iterrows():
            conn.execute("""
                INSERT OR REPLACE INTO budget_rates
                (fiscal_year, currency, rate_to_eur,
                 rate_to_usd, usd_eur_rate, set_date)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (
                fiscal_year,
                row["currency"],
                row["rate_to_eur"],
                row["rate_to_usd"],
                row["usd_eur_rate"],
                date.today().isoformat(),
            ))
            loaded += 1

    log.info(f"Budget rates loaded: {loaded} currencies for {fiscal_year}")

    # --- Print summary ---
    print(f"\n{'='*50}")
    print(f"BUDGET RATES — {fiscal_year}")
    print(f"{'='*50}")
    print(f"EUR/USD: {eur_usd:.4f}")
    print()
    for _, row in df.iterrows():
        print(f"  {row['currency']:4} → EUR: {row['rate_to_eur']:.6f} "
              f"| USD: {row['rate_to_usd']:.6f}")
    print(f"{'='*50}\n")

    return {
        "loaded":       loaded,
        "fiscal_year":  fiscal_year,
        "eur_usd_rate": eur_usd,
    }


def get_rate(currency: str, fiscal_year: int,
             rate_type: str = "usd") -> float:
    """
    Get a specific budget rate from DB.

    Args:
        currency:   ISO code e.g. "CAD", "MXN"
        fiscal_year: e.g. 2026
        rate_type:  "usd" or "eur"

    Returns:
        Float rate or 1.0 as fallback with warning.
    """
    from scripts.db import query

    col = "rate_to_usd" if rate_type == "usd" else "rate_to_eur"

    df = query(f"""
        SELECT {col} as rate
        FROM budget_rates
        WHERE currency = ? AND fiscal_year = ?
    """, (currency.upper(), fiscal_year))

    if df.empty or df["rate"].iloc[0] is None:
        log.warning(f"No {rate_type} rate found for {currency} {fiscal_year} — using 1.0")
        return 1.0

    rate = float(df["rate"].iloc[0])
    if rate == 0.0:
        log.warning(f"Rate for {currency} {fiscal_year} is placeholder (0.0) — "
                    f"EUR/USD amounts will be incorrect until rate is set")
        return 1.0

    return rate


def get_usd_eur_rate(fiscal_year: int) -> float:
    """Get EUR/USD rate for a given year."""
    from scripts.db import query

    df = query("""
        SELECT usd_eur_rate FROM budget_rates
        WHERE currency = 'USD' AND fiscal_year = ?
        LIMIT 1
    """, (fiscal_year,))

    if df.empty:
        log.warning(f"No USD/EUR rate for {fiscal_year} — using 1.15")
        return 1.15

    return float(df["usd_eur_rate"].iloc[0])


def convert_to_usd(amount_local: float,
                   currency: str,
                   fiscal_year: int) -> tuple:
    """
    Convert local amount to USD and EUR using budget rates.

    Returns: (amount_usd, amount_eur, rate_used_usd, rate_used_eur)
    """
    if currency == "USD":
        eur_rate = get_rate("USD", fiscal_year, "eur")
        amount_eur = amount_local * eur_rate
        return amount_local, amount_eur, 1.0, eur_rate

    usd_rate = get_rate(currency, fiscal_year, "usd")
    eur_rate = get_rate(currency, fiscal_year, "eur")

    amount_usd = amount_local * usd_rate
    amount_eur = amount_local * eur_rate

    return amount_usd, amount_eur, usd_rate, eur_rate
