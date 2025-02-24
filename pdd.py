import pdfplumber
import pandas as pd

pdf_path = r"C:\Users\amrit\OneDrive\Desktop\Amri\vscode\Balancesheet_Rep 1.pdf"

with pdfplumber.open(pdf_path) as pdf:
    page = pdf.pages[0]  # Extracting first page
    table = page.extract_table()

df = pd.DataFrame(table)  

df.dropna(how="all", inplace=True)

print(df.to_markdown(index=False, headers=[]))
