"""Diagnose GL file — shows all unique BU codes"""
import sys
import pandas as pd

file = sys.argv[1] if len(sys.argv) > 1 \
    else "data/jan_feb/577 GL Details_Jan26.xlsx"

print(f"Reading: {file}\n")
xl = pd.ExcelFile(file)
sheet = "GL Detail" if "GL Detail" in xl.sheet_names else 0
df_raw = pd.read_excel(file, header=None, sheet_name=sheet)

# Find header row
header_row = None
for i, row in df_raw.iterrows():
    vals = [str(v).strip().lower() for v in row.values]
    if "account" in vals and "period" in vals:
        header_row = i
        break

if header_row is None:
    print("Cannot find header row")
    sys.exit(1)

df_raw.columns = df_raw.iloc[header_row].astype(str).str.strip()
df = df_raw.iloc[header_row + 1:].reset_index(drop=True)

print(f"Company: {df['Company'].iloc[0] if 'Company' in df.columns else '?'}")
print(f"Total rows: {len(df):,}")
print()

# Get BU col
bu_col = next((c for c in df.columns
               if "business unit" in c.lower()), None)
if not bu_col:
    print("No BU column found")
    sys.exit(1)

# Classify accounts
df["first"] = df["Account"].astype(str).str.strip().str[0]
df_pl = df[df["first"].isin(["6","7","8","9"])]

bus = df_pl[bu_col].astype(str).str.strip().replace(
    ["nan","","0","None"], None
).dropna().unique()

print(f"Unique P&L BU codes ({len(bus)}):")
for bu in sorted(bus):
    rows = len(df_pl[df_pl[bu_col].astype(str).str.strip() == bu])
    print(f"  {bu:<15} ({rows:4} rows)")
