from flask import Flask, request, jsonify
import mysql.connector
import pdfplumber
import pandas as pd
import os

app = Flask(__name__)

def get_db_connection():
    return mysql.connector.connect(
        host="localhost",
        user="root",
        password="Train@1234",
        database="balance_sheet1"
    )

def process_pdf_and_insert(pdf_path):
    conn = get_db_connection()
    cursor = conn.cursor()

    with pdfplumber.open(pdf_path) as pdf:
        page = pdf.pages[0]  
        table = page.extract_table()

    print("Extracted Table:", table)

    if not table:
        print("No table extracted from PDF.")
        return "No table found in PDF!"

    df = pd.DataFrame(table)
    df.dropna(how="all", inplace=True)
    df = df.iloc[1:].reset_index(drop=True)  

    print("DataFrame after processing:\n", df)

    for _, row in df.iterrows():
        sql = "INSERT INTO balance_sheet (assest_name, notes, 31dec2021, 31dec2020) VALUES (%s, %s, %s, %s)"
        values = (
            row[0],
            row[1],
            str(row[2]).replace(",", "") if pd.notna(row[2]) else None,
            str(row[3]).replace(",", "") if pd.notna(row[3]) else None
        )
        print("Inserting row:", values)  
        cursor.execute(sql, values)

    conn.commit()
    cursor.close()
    conn.close()

    return "Successfully inserted!"


@app.route('/upload', methods=['POST'])
def upload_pdf():
    if 'file' not in request.files:
        return jsonify({"error": "No file provided"}), 400

    file = request.files['file']
    
    if file.filename == '':
        return jsonify({"error": "No selected file"}), 400

    file_path = os.path.join("uploads", file.filename)
    os.makedirs("uploads", exist_ok=True)
    file.save(file_path)

    result = process_pdf_and_insert(file_path)

    return jsonify({"message": result})



if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8081, debug=True)