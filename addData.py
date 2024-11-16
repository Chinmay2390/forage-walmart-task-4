import pandas as pd
import sqlite3

# Step 1: Load data from spreadsheets
spreadsheet_0 = pd.read_csv("spreadsheet_0.csv")
spreadsheet_1 = pd.read_csv("spreadsheet_1.csv")
spreadsheet_2 = pd.read_csv("spreadsheet_2.csv")

# Step 2: Establish a connection to the SQLite database
conn = sqlite3.connect("shipping_data.db")
cursor = conn.cursor()

# Step 3: Insert data from spreadsheet_0 into the database
spreadsheet_0.to_sql("spreadsheet_0", conn, if_exists="replace", index=False)

# Step 4: Merge spreadsheet_1 and spreadsheet_2
merged_data = pd.merge(
    spreadsheet_1,
    spreadsheet_2,
    left_on="shipping_id",
    right_on="id",
    how="inner"
)

# Step 5: Add quantity column and transform data into the correct format
merged_data["quantity"] = merged_data["weight"] // merged_data["unit_weight"]
formatted_data = merged_data[[
    "shipping_id", "product_name", "origin", "destination", "quantity"
]]

# Step 6: Insert data into the database
formatted_data.to_sql("shipment_data", conn, if_exists="replace", index=False)

# Commit and close the connection
conn.commit()
conn.close()

print("Data has been successfully loaded into the database.")
