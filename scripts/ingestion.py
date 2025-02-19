# THIS IS NOT AN EXECUTABLE SCRIPT - JUST FOR DOCUMENTATION
# REFER EDA/eda.ipynb for execution

import pandas as pd
import re
import ast
import csv

file_path = 'reviews_Clothing_Shoes_and_Jewelry_5.csv'
df = pd.read_csv(file_path, 
                 encoding="utf-8"
                 )  # Read all columns as strings to avoid parsing errors

df = df.drop(columns=['Unnamed: 0'])

df.to_csv('updated_reviews.csv', index=False)

file_path = 'metadata_category_clothing_shoes_and_jewelry_only.csv'

# Read CSV while handling bad lines and escaping quote issues
df = pd.read_csv(file_path, 
                 encoding="utf-8", 
                 dtype=str)  



# Function to extract the first category
def extract_first_category(category_list):
    if isinstance(category_list, str):  # Ensure it's a string before conversion
        try:
            category_list = ast.literal_eval(category_list)  # Convert string to list
            if isinstance(category_list, list) and len(category_list) > 0:
                return category_list[0][0]# Get the first category
        except (SyntaxError, ValueError):
            return None  # Return None if conversion fails
    return None  # Return None for invalid values

# Apply function to the categories column
df['categories'] = df['categories'].apply(extract_first_category)


#Rmove white spaces 
df['title'] = df['title'].str.strip()
df['description'] = df['description'].str.strip()

# Remove special characters (non-alphanumeric characters) from 'title' and 'description' columns
df['title'] = df['title'].apply(lambda x: re.sub(r'[^a-zA-Z0-9\s]', '', str(x)))
df['description'] = df['description'].apply(lambda x: re.sub(r'[^a-zA-Z0-9\s]', '', str(x)))

df.to_csv('updated_metadata.csv', index=False)

# Define the input and output file paths
input_file_path = "updated_metadata.csv"
cleaned_file_path = "updated_metadata_cleaned_final.csv"

# Function to clean JSON-like fields
def clean_json_field(field):
    """
    Ensures JSON-like fields are properly formatted with double quotes and no unescaped commas.
    """
    if isinstance(field, str):
        field = field.replace("'", '"')  # Convert single quotes to double quotes
        field = field.replace(", ", "; ")  # Temporarily replace commas inside JSON to prevent column splitting issues
    return field

# Process the file and clean it
with open(input_file_path, "r", encoding="utf-8") as infile, open(cleaned_file_path, "w", encoding="utf-8", newline='') as outfile:
    reader = csv.reader(infile, delimiter=",", quotechar='"')
    writer = csv.writer(outfile, delimiter=",", quotechar='"', quoting=csv.QUOTE_MINIMAL)
    
    for row in reader:
        if len(row) == 10:  # Ensure we only modify valid rows
            row[2] = clean_json_field(row[2])  # Clean salesrank column
            row[8] = clean_json_field(row[8])  # Clean related column
        writer.writerow(row)

print(f"Cleaned CSV file saved at: {cleaned_file_path}")

df = pd.read_csv("updated_metadata_cleaned_final.csv")