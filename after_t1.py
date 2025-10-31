import pandas as pd
import datetime
import os

def identify_litigators_and_create_reports(input_folder="t1 input", output_folder="t1 output", result_folder="after t1 output"):
    # Ensure all necessary directories exist
    if not os.path.exists(result_folder):
        os.makedirs(result_folder)

    # Find and read the 'all_clean'
    clean_file_path = os.path.join(input_folder, next((f for f in os.listdir(input_folder) if "all_clean" in f and f.endswith('.xlsx')), None))
    if clean_file_path is None:
        print("No 'all_clean' found in the input folder.")
        return
    clean_data = pd.read_excel(clean_file_path)

    # Find and read the 'Litigator scrubbing' file
    litigator_file_path = os.path.join(output_folder, next((f for f in os.listdir(output_folder) if "Litigator scrubbing" in f and f.endswith('.xlsx')), None))
    if litigator_file_path is None:
        print("No 'Litigator scrubbing' file found in the output folder.")
        return
    litigator_data = pd.read_excel(litigator_file_path)

    # Identify the numbers not in 'all_clean' and the associated IDs
    missing_numbers = set(litigator_data['Numbers']) - set(clean_data['Numbers'])
    missing_ids = litigator_data[litigator_data['Numbers'].isin(missing_numbers)]['ID'].unique()

    # Find and read the 'T1Skiptrace BST_out' file
    t1_file_path = os.path.join(output_folder, next((f for f in os.listdir(output_folder) if "T1Skiptrace BST_out" in f and f.endswith('.xlsx')), None))
    if t1_file_path is None:
        print("No 'T1Skiptrace BST_out' file found in the output folder.")
        return
    t1_data = pd.read_excel(t1_file_path)

    # Phone columns to check for Non Hits
    phone_columns = [
        'Phone Number', 'Phone Number2', 'Phone Number3', 'Phone Number4', 'Phone Number5',
        'Phone Number6', 'Phone Number7', 'Phone Number8',
        'Phone Number9'
    ]

    # Remove rows with missing IDs from the 'T1Skiptrace BST_out' file and save
    t1_data_cleaned = t1_data[~t1_data['ID'].isin(missing_ids)]
    cleaned_t1_file_path = os.path.join(result_folder, "T1Skiptrace BST_out_Cleaned.xlsx")
    t1_data_cleaned.to_excel(cleaned_t1_file_path, index=False)
    print(f"Cleaned T1Skiptrace file saved successfully at {cleaned_t1_file_path}")

    # Save the IDs of litigators and their associated data
    flagged_litigators_data = t1_data[t1_data['ID'].isin(missing_ids)]
    flagged_litigators_file_path = os.path.join(result_folder, "Flagged_Litigators.xlsx")
    flagged_litigators_data.to_excel(flagged_litigators_file_path, index=False)
    print(f"Flagged Litigators file saved successfully at {flagged_litigators_file_path}")

    # Identify Non Hits
    non_hits = t1_data_cleaned[phone_columns].isnull().all(axis=1)
    non_hits_data = t1_data_cleaned[non_hits]

    # Prepare column rename mapping for Non Hits
    # column_rename = {
    #     "Folio": "Folio",
    #     "First Name": "First Name",
    #     "Last Name": "Last Name",
    #     "Mailing Address": "Mailing Address",
    #     "Mailing city": "Mailing City",
    #     "Mailing state": "Mailing State",
    #     "Mailing zip": "Mailing Zip",
    #     "Property Address": "Property Address",
    #     "Property city": "Property City",
    #     "Property State": "Property State",
    #     "Property zip": "Property Zip"
    # }
    # non_hits_final = non_hits_data[list(column_rename.keys())].rename(columns=column_rename)

    # Check and transfer values from First Name to Last Name if Last Name is empty
    non_hits_data.loc[non_hits_data['Last Name'].isna(), 'Last Name'] = non_hits_data['First Name']
    non_hits_data.loc[non_hits_data['Last Name'] == non_hits_data['First Name'], 'First Name'] = ""

    non_hits_file_path = os.path.join(result_folder, "Non_Hits.xlsx")
    non_hits_data.to_excel(non_hits_file_path, index=False)
    print(f"Non Hits file saved successfully at {non_hits_file_path}")

    # Create the 'Import Flagged Litigators' file
    selected_columns = [
        'Folio', 'Property Address', 'Property zip', 'Tags', 'Number source'
    ]
    additional_columns = {
        'Property Skip Trace': 'BST',
        'Tags2': 'Possible Litigator',
        'Action Plan': '30'
    }

    # Prepare data for 'Import Flagged Litigators' file
    import_flagged_litigators_data = flagged_litigators_data[selected_columns].copy()
    for col, value in additional_columns.items():
        import_flagged_litigators_data[col] = value

    # Save 'Import Flagged Litigators'
    import_flagged_litigators_file_path = os.path.join(result_folder, "Import_Flagged_Litigators.xlsx")
    import_flagged_litigators_data.to_excel(import_flagged_litigators_file_path, index=False)
    print(f"'Import Flagged Litigators' file saved successfully at {import_flagged_litigators_file_path}")

def create_import_t1_skiptrace_file(output_folder="t1 output", result_folder="after t1 output"):
    # Ensure the result directory exists
    if not os.path.exists(result_folder):
        os.makedirs(result_folder)

    # Define all columns to be included in the output
    final_columns = [
        'Folio', 'Property Address', 'Property zip',
        'Phone Number', 'Phone Type', 'Phone Number2', 'Phone Type2',
        'Phone Number3', 'Phone Type3', 'Phone Number4', 'Phone Type4',
        'Phone Number5', 'Phone Type5', 'Phone Number6', 'Phone Type6','Phone Number7', 'Phone Type7',
        'Phone Number8', 'Phone Type8', 'Phone Number9', 'Phone Type9',
        'Email1', 'Email2','Email3', 'Email4', 'Email5', 'Tags', 'Number source', 'Tags2'

    ]

    # Additional columns to append
    additional_columns = {
        'Property Skip Trace': 'BST',

    }

    # Path to the cleaned T1 Skiptrace file
    cleaned_t1_file_path = os.path.join(result_folder, "T1Skiptrace BST_out_Cleaned.xlsx")

    # Read the 'T1Skiptrace BST_out_Cleaned.xlsx' file
    try:
        t1_data_cleaned = pd.read_excel(cleaned_t1_file_path)
    except Exception as e:
        print(f"An error occurred while reading the file: {e}")
        return

    # Attempt to read the clean and litigator files to find missing numbers
    clean_file_path = os.path.join("t1 input", "all_clean.xlsx")
    litigator_file_path = os.path.join(output_folder, "Litigator scrubbing.xlsx")

    try:
        clean_data = pd.read_excel(clean_file_path)
        litigator_data = pd.read_excel(litigator_file_path)
    except Exception as e:
        print(f"Failed to read input files: {e}")
        return

    # Identify Numbers not in the 'all_clean' but in 'Litigator scrubbing'
    missing_numbers = set(litigator_data['Numbers']) - set(clean_data['Numbers'])

    # Select only the rows from litigator_data that have the missing Numbers
    flagged_litigators_data = litigator_data[litigator_data['Numbers'].isin(missing_numbers)]

    # Save the flagged litigators to a new Excel file
    testing_flagged_litigators_path = os.path.join(result_folder, "Testing_Flagged_Litigators.xlsx")
    try:
        flagged_litigators_data[['ID', 'Numbers']].to_excel(testing_flagged_litigators_path, index=False)
        print(f"Testing Flagged Litigators file saved successfully at {testing_flagged_litigators_path}")
    except Exception as e:
        print(f"Failed to save 'Testing Flagged Litigators' file: {e}")

    # Continue with existing logic to finalize the import T1 skiptrace file
    try:
        # Add missing columns with None values to ensure all expected columns are present
        for column in final_columns:
            if column not in t1_data_cleaned.columns:
                t1_data_cleaned[column] = None

        # Filter rows to include only those with any non-empty phone information
        t1_data_cleaned = t1_data_cleaned.dropna(subset=[col for col in final_columns if 'Phone' in col], how='all')

        # Reorder the DataFrame according to the final_columns list
        t1_data_cleaned = t1_data_cleaned[final_columns]

        # Add additional columns with predefined values
        for column, value in additional_columns.items():
            t1_data_cleaned[column] = value

        # Save the processed data to an Excel file
        import_t1_skiptrace_file_path = os.path.join(result_folder, "Import_T1_Skiptrace.xlsx")
        t1_data_cleaned.to_excel(import_t1_skiptrace_file_path, index=False)
        print(f"'Import T1 Skiptrace' file saved successfully at {import_t1_skiptrace_file_path}")
    except Exception as e:
        print(f"Failed to process and save 'Import T1 Skiptrace' file: {e}")



# Run the function
identify_litigators_and_create_reports()
create_import_t1_skiptrace_file()



"""def identify_litigators_and_create_reports(input_folder="t1 input", output_folder="t1 output", result_folder="after t1 output"):
    # Ensure all necessary directories exist
    if not os.path.exists(result_folder):
        os.makedirs(result_folder)

    # Find and read the 'clean file'
    clean_file_path = os.path.join(input_folder, next((f for f in os.listdir(input_folder) if "clean file" in f and f.endswith('.xlsx')), None))
    if clean_file_path is None:
        print("No 'clean file' found in the input folder.")
        return
    clean_data = pd.read_excel(clean_file_path)

    # Find and read the 'Litigator scrubbing' file
    litigator_file_path = os.path.join(output_folder, next((f for f in os.listdir(output_folder) if "Litigator scrubbing" in f and f.endswith('.xlsx')), None))
    if litigator_file_path is None:
        print("No 'Litigator scrubbing' file found in the output folder.")
        return
    litigator_data = pd.read_excel(litigator_file_path)

    # Identify Numbers not in common
    clean_numbers = set(clean_data['Numbers'])
    litigator_numbers = set(litigator_data['Numbers'])
    unique_numbers = litigator_numbers.symmetric_difference(clean_numbers)
    flagged_ids = litigator_data[litigator_data['Numbers'].isin(unique_numbers)]['ID'].unique()

    # Find and read the 'T1Skiptrace BST_out' file
    t1_file_path = os.path.join(output_folder, next((f for f in os.listdir(output_folder) if "T1Skiptrace BST_out" in f and f.endswith('.xlsx')), None))
    if t1_file_path is None:
        print("No 'T1Skiptrace BST_out' file found in the output folder.")
        return
    t1_data = pd.read_excel(t1_file_path)

    # Phone columns to check for Non Hits
    phone_columns = [
        'Phone  2', 'Phone  2', 'Phone  3', 'Phone  4', 'Phone  5',
        'Phone Number6', 'Phone Number7', 'Phone Number8',
        'Phone Number9', 'Phone Number10', 'Phone Number11', 'Phone Number12', 'Phone Number13', 'Phone Number14'
    ]

    # Remove rows with flagged IDs from the 'T1Skiptrace BST_out' file and save
    t1_data_cleaned = t1_data[~t1_data['ID'].isin(flagged_ids)]
    cleaned_t1_file_path = os.path.join(result_folder, "T1Skiptrace BST_out_Cleaned.xlsx")
    t1_data_cleaned.to_excel(cleaned_t1_file_path, index=False)
    print(f"Cleaned T1Skiptrace file saved successfully at {cleaned_t1_file_path}")

    # Save the IDs of litigators and their associated data
    flagged_litigators_data = t1_data[t1_data['ID'].isin(flagged_ids)]
    flagged_litigators_file_path = os.path.join(result_folder, "Flagged_Litigators.xlsx")
    flagged_litigators_data.to_excel(flagged_litigators_file_path, index=False)
    print(f"Flagged Litigators file saved successfully at {flagged_litigators_file_path}")

    # Identify Non Hits
    non_hits = t1_data_cleaned[phone_columns].isnull().all(axis=1)
    non_hits_data = t1_data_cleaned[non_hits]

    # Prepare column rename mapping for Non Hits
    column_rename = {
        "First Name": "First Name",
        "Last Name": "Last Name",
        "Mailing Address": "MailingAddress",
        "Mailing city": "MailingCity",
        "Mailing state": "MailingState",
        "Mailing zip": "MailingZip",
        "Property Address": "PropertyAddress",
        "Property city": "PropertyCity",
        "Property State": "PropertyState",
        "Property zip": "PropertyZip"
    }
    non_hits_final = non_hits_data[list(column_rename.keys())].rename(columns=column_rename)
    
    # Check and transfer values from First Name to Last Name if Last Name is empty
    non_hits_final.loc[non_hits_final['Last Name'].isna(), 'Last Name'] = non_hits_final['First Name']
    non_hits_final.loc[non_hits_final['Last Name'] == non_hits_final['First Name'], 'First Name'] = ""
    
    non_hits_file_path = os.path.join(result_folder, "Non_Hits.xlsx")
    non_hits_final.to_excel(non_hits_file_path, index=False)
    print(f"Non Hits file saved successfully at {non_hits_file_path}")

    # Create the 'Import Flagged Litigators' file
    selected_columns = [
        'FOLIO', 'Property Address', 'Property zip', 'Bankrupcy',
        'Estate', 'Golden Address', 'Golden city', 'Golden State', 'Golden Zip'
    ]
    additional_columns = {
        'Property Skip Trace': 'BST',
        'Number Source': 'T1Skiptrace',
        'Phone  skip trace': 'BST',
        'TAG': 'T1Skiptrace',
        'Note': 'Possible Litigator',
        'Action Plan': '30'
    }

    # Prepare data for 'Import Flagged Litigators' file
    import_flagged_litigators_data = flagged_litigators_data[selected_columns].copy()
    for col, value in additional_columns.items():
        import_flagged_litigators_data[col] = value
    
    # Save 'Import Flagged Litigators'
    import_flagged_litigators_file_path = os.path.join(result_folder, "Import_Flagged_Litigators.xlsx")
    import_flagged_litigators_data.to_excel(import_flagged_litigators_file_path, index=False)
    print(f"'Import Flagged Litigators' file saved successfully at {import_flagged_litigators_file_path}")
"""
