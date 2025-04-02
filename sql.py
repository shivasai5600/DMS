import pymysql
import pandas as pd
import numpy as np

# Ensure you have installed openpyxl for reading Excel files
# pip install openpyxl

# Database connection details
db_config = {
    "host": "localhost",
    "user": "root",
    "password": "root",
    "database": "dms",
}

# Table name
table_name = "print_data"

# Load Excel file
csv_file_path = "C:/Users/hpadmin/PycharmProjects/ChatbotProject/dms/printer_master_data.xlsx"
df = pd.read_excel(csv_file_path, engine="openpyxl")

# Replace empty strings and NaN values with None (NULL in MySQL)
df.replace(r'^\s*$', np.nan, regex=True, inplace=True)  # Convert empty spaces to NaN
df = df.where(pd.notnull(df), None)  # Convert NaN to None

# Rename columns (replace spaces & special characters with underscores)
df.columns = df.columns.str.replace(r'\W+', '_', regex=True)

print(df.head())  # Debugging

# Connect to MySQL
connection = pymysql.connect(**db_config)
cursor = connection.cursor()

# Create table dynamically (modify data types if needed)
create_table_query = f"""
CREATE TABLE IF NOT EXISTS {table_name} (
    ID INT PRIMARY KEY AUTO_INCREMENT,
    {', '.join([f'`{col}` VARCHAR(255) NULL' for col in df.columns])}
);
"""
cursor.execute(create_table_query)

# Prepare column names dynamically with backticks (exclude ID)
columns = [col for col in df.columns]  # Exclude 'ID'
columns_str = ", ".join([f"`{col}`" for col in columns])  # Wrap column names in backticks
placeholders = ", ".join(["%s"] * len(columns))  # Generate placeholders for values

# Insert query with dynamic column names (IGNORE to avoid duplicates)
insert_query = f"INSERT IGNORE INTO {table_name} ({columns_str}) VALUES ({placeholders})"

# Insert data
for _, row in df.iterrows():
    values = tuple(None if pd.isna(x) else x for x in row)  # Convert NaN to None
    try:
        cursor.execute(insert_query, values)
    except pymysql.MySQLError as e:
        print(f"Error inserting row: {row.to_dict()} | Error: {e}")

# Commit & close
connection.commit()
cursor.close()
connection.close()

print("Data successfully inserted into MySQL.")