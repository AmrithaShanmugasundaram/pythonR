import mysql.connector
import pdfplumber
import pandas as pd

pdf_path = r"C:\Users\amrit\OneDrive\Desktop\Amri\vscode\Balancesheet_Rep 1.pdf"

conn = mysql.connector.connect(
    host="localhost",
    user="root",
    password="Train@1234",
    database="balance_sheet"
)
cursor = conn.cursor()

with pdfplumber.open(pdf_path) as pdf:
    page = pdf.pages[0]
    table = page.extract_table()

df = pd.DataFrame(table)
df.dropna(how="all", inplace=True) 
df = df.iloc[1:].reset_index(drop=True)  

for i, row in df.iterrows():
    sql = "INSERT INTO balance_sheet (assest_name, notes, 31dec2021, 31dec2020) VALUES (%s, %s, %s, %s)"
    values = (
        row[0], 
        row[1], 
        str(row[2]).replace(",", "") if pd.notna(row[2]) else None,  
        str(row[3]).replace(",", "") if pd.notna(row[3]) else None
    )

    cursor.execute(sql, values)

conn.commit()
cursor.close()
conn.close()

print("Successfully inserted!")
