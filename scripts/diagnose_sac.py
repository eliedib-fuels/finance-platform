import sys, pandas as pd
sys.path.insert(0, '.')

file = sys.argv[1] if len(sys.argv) > 1 else "data/SAC_2024_2026.xlsx"
xl = pd.ExcelFile(file)
print(f"Sheets: {xl.sheet_names[:5]}")
print()

df = pd.read_excel(file, sheet_name=xl.sheet_names[0], header=None, nrows=12)
print(f"First sheet: {xl.sheet_names[0]}")
print(f"Shape: {df.shape}")
print()
for i, row in df.iterrows():
    vals = [str(v).strip() for v in row.values if str(v).strip() not in ('nan','')]
    if vals:
        print(f"Row {i}: {vals[:8]}")
