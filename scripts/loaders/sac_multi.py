"""
Finance Platform — SAC Multi-Period Loader
Reads the new SAC format with all months as columns.

Format:
  Row 1: FPA_GROUP
  Row 3: Measures | MTD LC
  Row 4: Version  | Actual
  Row 5: Date     | 202401 | 202402 | ... (period headers)
  Row 6: Account  | Organisation (column headers)
  Row 7+: data rows

Currency prefixes in amounts:
  $      → USD (577, 569, 682, 684)
  CA$    → CAD (1033, 0879)
  MX$    → MXN (568)
  GY$    → GYD (755)
  –      → zero

Usage:
  python scripts/loaders/sac_multi.py
  python scripts/loaders/sac_multi.py --file data/SAC_all_periods.xlsx
  python scripts/loaders/sac_multi.py --file data/SAC_all_periods.xlsx --dry-run
  python scripts/loaders/sac_multi.py --file data/SAC_all_periods.xlsx --year 2025
  python scripts/loaders/sac_multi.py --file data/SAC_all_periods.xlsx --period 202601
"""

import pandas as pd
import re
import sys
import argparse
import logging
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

log = logging.getLogger(__name__)

# Sheet → AD50 line mapping
SHEETS = {
    '1- Billing':               '01',
    '2 - Wip':                  '02',
    '4 - IG revenue':           '04',
    '5 - IG subcon':            '05',
    '8 - Ext sub':              '08',
    '7 - Pers costs':           '07',
    '9 - Other costs':          '09',
    '10 -funct neutralization': '10',
}

# FX budget rates by year (→ EUR)
# Will be loaded from budget_rates table
BUDGET_RATES_FALLBACK = {
    2026: {'CAD': 0.628931, 'MXN': 0.044106, 'GYD': 0.0041,
           'USD': 0.869565, 'EUR': 1.0},
    2025: {'CAD': 0.628931, 'MXN': 0.044106, 'GYD': 0.0041,
           'USD': 0.869565, 'EUR': 1.0},
    2024: {'CAD': 0.628931, 'MXN': 0.044106, 'GYD': 0.0041,
           'USD': 0.869565, 'EUR': 1.0},
}

# Entity suffix rules
ENTITY_SUFFIX = {
    '0577': 'C', '1033': 'C', '0879': 'C', '0865': 'C',
    '0568': 'S', '0569': 'S', '0682': 'S', '0684': 'S',
    '0755': 'C',
}

# Currency prefix → currency code
CCY_PREFIX = {
    '$':    'USD',
    'CA$':  'CAD',
    'MX$':  'MXN',
    'GY$':  'GYD',
    'EUR':  'EUR',
    '€':    'EUR',
}


def parse_amount(val) -> float:
    """
    Parse amount string with currency prefix.
    '$146,488.45' → 146488.45
    'CA$978.09'   → 978.09
    '–'           → 0.0
    '-'           → 0.0
    """
    if val is None:
        return 0.0
    s = str(val).strip()
    if s in ('–', '-', '', 'nan', 'None'):
        return 0.0
    # Remove currency prefix and commas
    s = re.sub(r'^[A-Z]*\$|^€', '', s)
    s = s.replace(',', '').strip()
    try:
        return float(s)
    except ValueError:
        return 0.0


def get_currency_from_amount(val) -> str:
    """Detect currency from amount string."""
    s = str(val).strip()
    for prefix, ccy in sorted(CCY_PREFIX.items(),
                               key=lambda x: -len(x[0])):
        if s.startswith(prefix):
            return ccy
    return None


def get_budget_rates(fiscal_year: int) -> dict:
    """Load budget rates from DB, fall back to hardcoded."""
    try:
        from scripts.db import query
        df = query("""
            SELECT currency, rate_to_eur, rate_to_usd, usd_eur_rate
            FROM budget_rates
            WHERE fiscal_year = ?
        """, (fiscal_year,))
        if not df.empty:
            rates = {}
            usd_eur = float(df['usd_eur_rate'].iloc[0])
            for _, row in df.iterrows():
                rates[row['currency']] = {
                    'to_eur': float(row['rate_to_eur']),
                    'to_usd': float(row['rate_to_usd']),
                }
            rates['USD'] = {'to_eur': usd_eur, 'to_usd': 1.0}
            return rates
    except Exception:
        pass

    # Fallback
    yr_rates = BUDGET_RATES_FALLBACK.get(
        fiscal_year, BUDGET_RATES_FALLBACK[2026]
    )
    usd_eur  = yr_rates['USD']
    return {
        ccy: {'to_eur': rate, 'to_usd': rate / usd_eur}
        for ccy, rate in yr_rates.items()
    }


def parse_sheet(file_path: str, sheet: str,
                ad50_line: str,
                target_periods: list = None) -> pd.DataFrame:
    """
    Parse one sheet from multi-period SAC file.
    Returns long-format DataFrame with one row per BU/period.

    target_periods: list of YYYYMM ints to load (None = all)
    """
    df_raw = pd.read_excel(file_path, sheet_name=sheet,
                           header=None)

    # ── Find period header row ────────────────────────────────────────────────
    def to_period_str(v) -> str:
        """Convert 202401 or 202401.0 or '202401' to '202401'."""
        try:
            f = float(v)
            if f == int(f):
                return str(int(f))
        except (ValueError, TypeError):
            pass
        return str(v).strip()

    period_row  = None
    account_row = None

    for i, row in df_raw.iterrows():
        vals = [to_period_str(v) for v in row.values]
        # Period row: has 3+ YYYYMM values
        numeric = [v for v in vals if re.match(r'^20\d{4}$', v)]
        if len(numeric) >= 3:
            period_row = i
            continue
        # Account/Org header row
        if 'Account' in vals and 'Organisation' in vals:
            account_row = i
            break

    if period_row is None or account_row is None:
        log.warning(f"Cannot find headers in sheet {sheet} "
                    f"(period_row={period_row}, "
                    f"account_row={account_row})")
        return pd.DataFrame()

    # ── Map column index → period YYYYMM ─────────────────────────────────────
    period_cols = {}
    for j, val in enumerate(df_raw.iloc[period_row].values):
        s = to_period_str(val)
        if re.match(r'^20\d{4}$', s):
            yyyymm = int(s)
            if target_periods is None or yyyymm in target_periods:
                period_cols[j] = yyyymm

    if not period_cols:
        log.warning(f"No matching periods in sheet {sheet}")
        return pd.DataFrame()

    # Get account and org column indices
    header_vals = [str(v).strip() for v in
                   df_raw.iloc[account_row].values]
    try:
        acct_col = header_vals.index('Account')
        org_col  = header_vals.index('Organisation')
    except ValueError:
        log.warning(f"No Account/Organisation columns in {sheet}")
        return pd.DataFrame()

    # Data rows
    data = df_raw.iloc[account_row + 1:].reset_index(drop=True)

    # Forward fill account column
    data[acct_col] = data[acct_col].ffill()

    # Build long-format rows
    rows = []
    for _, row in data.iterrows():
        org_val = str(row.iloc[org_col]).strip()
        if org_val in ('nan', '', 'None'):
            continue

        # Match standard 7-digit BUs AND alphanumeric lease BUs
        # e.g. 0577009C, 0577OPTC, 0577H61C, 0577BEAC
        bu_match = re.match(r'^(\d{4}[A-Z0-9]{3}[CS])', org_val)
        if not bu_match:
            continue

        bu_raw  = bu_match.group(1)
        suffix  = bu_raw[-1]
        bu_code = bu_raw[:-1]
        entity  = bu_code[:4]

        if bu_code[:4] == '1033':
            log.info(f"DEBUG 1033: bu_raw={bu_raw} suffix={suffix} entity={entity} cfg={ENTITY_SUFFIX.get(entity)}")

        # Check valid suffix for entity
        expected_suffix = ENTITY_SUFFIX.get(entity)
        if expected_suffix and suffix != expected_suffix:
            continue

        # Parse account
        acct_raw = str(row.iloc[acct_col]).strip()
        if '.PROD.' in acct_raw or '.NPBO.' in acct_raw:
            account_code = acct_raw.split('.')[0]
        else:
            account_code = acct_raw.split(' ')[0][:6]

        # Get ad50 label
        ad50_label = org_val  # full org string as label

        # Process each period column
        for col_idx, yyyymm in period_cols.items():
            amount_raw = row.iloc[col_idx]
            amount_lc  = parse_amount(amount_raw)

            if amount_lc == 0.0:
                continue

            # Detect currency from amount string
            ccy = get_currency_from_amount(amount_raw)
            if ccy is None:
                # Infer from entity
                ccy_map = {
                    '0577': 'USD', '1033': 'CAD', '0879': 'CAD',
                    '0865': 'CAD', '0568': 'MXN', '0569': 'USD',
                    '0682': 'USD', '0684': 'USD', '0755': 'GYD',
                }
                ccy = ccy_map.get(entity, 'USD')

            fiscal_year   = int(str(yyyymm)[:4])
            fiscal_period = int(str(yyyymm)[4:])

            rows.append({
                'bu_code':       bu_code,
                'bu_name':       org_val,
                'entity':        entity,
                'account_code':  account_code,
                'ad50_line':     ad50_line,
                'ad50_label':    acct_raw,
                'fiscal_year':   fiscal_year,
                'fiscal_period': fiscal_period,
                'amount_lc':     amount_lc,
                'currency':      ccy,
                'plan_type':     'ACTUAL',
                'source':        'SAC_MULTI',
            })

    return pd.DataFrame(rows)


def apply_fx(df: pd.DataFrame) -> pd.DataFrame:
    """Apply budget FX rates to compute USD and EUR amounts."""
    df = df.copy()
    df['amount_usd'] = 0.0
    df['amount_eur'] = 0.0

    # Hardcoded fallback rates for currencies not in budget_rates table
    HARDCODED = {
        'GYD': {'to_usd': 0.0041 / 0.869565, 'to_eur': 0.0041},
        'USD': {'to_usd': 1.0,                'to_eur': 0.869565},
    }

    for (fy, ccy), grp in df.groupby(['fiscal_year', 'currency']):
        rates = get_budget_rates(int(fy))
        r     = rates.get(ccy) or HARDCODED.get(ccy) or \
                {'to_usd': 1.0, 'to_eur': 0.869565}
        mask  = (df['fiscal_year'] == fy) & (df['currency'] == ccy)
        df.loc[mask, 'amount_usd'] = df.loc[mask, 'amount_lc'] * r['to_usd']
        df.loc[mask, 'amount_eur'] = df.loc[mask, 'amount_lc'] * r['to_eur']

    return df


def load_sac_multi(file_path: str,
                   target_periods: list = None,
                   dry_run: bool = False) -> dict:
    """
    Load multi-period SAC file into sac_detail table.

    Args:
        file_path:      Path to SAC Excel file
        target_periods: List of YYYYMM to load (None = all)
        dry_run:        Parse only, no DB write
    """
    from scripts.db import get_conn

    print(f"\n{'='*60}")
    print(f"SAC MULTI-PERIOD LOADER")
    print(f"File: {Path(file_path).name}")
    if target_periods:
        print(f"Periods: {target_periods}")
    else:
        print(f"Periods: ALL")
    print(f"{'='*60}")

    all_dfs = []

    for sheet, ad50 in SHEETS.items():
        print(f"  Parsing {ad50} {sheet}...", end='')
        try:
            df = parse_sheet(file_path, sheet, ad50, target_periods)
            if df.empty:
                print(f" 0 rows")
                continue
            all_dfs.append(df)
            periods_found = df['fiscal_year'].astype(str) + '/' + \
                            df['fiscal_period'].apply(lambda x: f'{x:02d}')
            print(f" {len(df):,} rows "
                  f"({periods_found.nunique()} periods)")
        except Exception as e:
            print(f" ERROR: {e}")
            continue

    if not all_dfs:
        print("No data parsed")
        return {'rows': 0}

    df_all = pd.concat(all_dfs, ignore_index=True)
    print(f"\nTotal rows before FX: {len(df_all):,}")

    # Apply FX rates
    df_all = apply_fx(df_all)

    # Summary by period
    print(f"\n{'Period':<10} {'Lines':>6} {'Rows':>8} {'kUSD':>12}")
    print('-'*40)
    for (fy, fp), grp in sorted(df_all.groupby(
            ['fiscal_year', 'fiscal_period'])):
        rev_mask = grp['ad50_line'].isin(['01','02','04','05'])
        rev      = grp.loc[rev_mask, 'amount_usd'].sum()
        print(f"{fy}/{fp:02d}     "
              f"{grp['ad50_line'].nunique():>6} "
              f"{len(grp):>8,} "
              f"{rev/1000:>11,.0f}K")

    if dry_run:
        print(f"\n[DRY RUN — no DB write] Total rows: {len(df_all):,}")
        return {'rows': len(df_all), 'dry_run': True}

    # Write to DB — delete and replace per period
    periods = df_all[['fiscal_year','fiscal_period']].drop_duplicates()

    with get_conn() as conn:
        deleted = 0
        for _, row in periods.iterrows():
            cur = conn.execute("""
                DELETE FROM sac_detail
                WHERE fiscal_year  = ?
                  AND fiscal_period = ?
            """, (int(row.fiscal_year), int(row.fiscal_period)))
            deleted += cur.rowcount

        print(f"\nDeleted {deleted:,} existing rows")

        # Insert in chunks
        CHUNK = 500
        inserted = 0
        cols = ['bu_code','bu_name','entity','account_code',
                'ad50_line','ad50_label','fiscal_year',
                'fiscal_period','amount_lc','currency',
                'amount_usd','amount_eur','plan_type','source']
        cols = [c for c in cols if c in df_all.columns]

        for i in range(0, len(df_all), CHUNK):
            chunk = df_all[cols].iloc[i:i+CHUNK]
            chunk.to_sql('sac_detail', conn,
                         if_exists='append', index=False)
            inserted += len(chunk)

        print(f"Inserted {inserted:,} rows")

    print(f"\n✅ SAC multi-period load complete")
    return {'rows': len(df_all), 'periods': len(periods)}


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Load multi-period SAC file'
    )
    parser.add_argument('--file', required=True,
                        help='Path to SAC multi-period Excel file')
    parser.add_argument('--periods', type=str, default=None,
                        help='Comma-separated YYYYMM list e.g. 202601,202602')
    parser.add_argument('--year', type=int, default=None,
                        help='Load all periods for a specific year')
    parser.add_argument('--dry-run', action='store_true')
    args = parser.parse_args()

    # Build target periods list
    target = None
    if args.periods:
        target = [int(p.strip()) for p in args.periods.split(',')]
    elif args.year:
        target = [args.year * 100 + m for m in range(1, 13)]

    if not Path(args.file).exists():
        print(f"File not found: {args.file}")
        sys.exit(1)

    load_sac_multi(args.file, target_periods=target,
                   dry_run=args.dry_run)