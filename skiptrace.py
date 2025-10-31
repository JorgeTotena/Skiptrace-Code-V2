import os
import pandas as pd

def skiptrace_process(input_folder="input", output_folder="output"):
    # Create the output directory if it doesn't exist
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)
    
    # List all Excel files that contain "SMS" or "Cold Calling" in their names
    input_files = [f for f in os.listdir(input_folder) if f.endswith('.xlsx') and ('SMS' in f or 'Cold Calling' in f or 'Sms' in f or 'CC' in f or 'Cold Call' in f)]
    if not input_files:
        print("No SMS or Cold Calling files found in the input folder.")
        return

    all_data = {}

    # Columns to be processed with new names and the desired order
    desired_columns = {
        'FOLIO': 'Folio', 'OWNER FULL NAME': 'Owner Full Name', 'OWNER FIRST NAME': 'Owner First Name', 
        'OWNER LAST NAME': 'Owner Last Name', 'ADDRESS': 'Property Address', 'CITY': 'Property City', 
        'STATE': 'Property State', 'ZIP': 'Property Zip', 'MAILING ADDRESS': 'Mailing Address', 
        'MAILING CITY': 'Mailing City', 'MAILING STATE': 'Mailing State', 'MAILING ZIP': 'Mailing Zip'
    }
    
    column_order = [
        'Folio', 'Owner Full Name', 'Owner First Name', 'Owner Last Name',
        'Mailing Address', 'Mailing City', 'Mailing State', 'Mailing Zip',
        'Property Address', 'Property City', 'Property State', 'Property Zip'
    ]

    # Process each file
    for input_file in input_files:
        input_path = os.path.join(input_folder, input_file)
        
        # Read the Excel file
        try:
            data = pd.read_excel(input_path)
        except Exception as e:
            print(f"An error occurred when reading the Excel file {input_file}: {e}")
            continue

        # Check if 'TAGS' column exists and process accordingly
        if 'TAGS' in data.columns:
            # Define the phone number columns dynamically based on the data frame
            phone_columns = [col for col in data.columns if 'PHONE NUMBER' in col]

            # Filter the data for rows where "TAGS" does NOT contain "Skiptrace" and all phone numbers are empty
            condition_no_skiptrace = ~data['TAGS'].str.contains('Skiptrace', na=False)
            condition_no_phones = data[phone_columns].isnull().all(axis=1) if phone_columns else True
            filtered_data = data[condition_no_skiptrace & condition_no_phones]

            # Rename and keep only the desired columns if they exist in the filtered data
            filtered_data = filtered_data.rename(columns=desired_columns)
            filtered_data = filtered_data[[new_col for new_col in column_order if new_col in filtered_data.columns]]

            # Ensure the columns are in the desired order
            filtered_data = filtered_data.reindex(columns=column_order)

            # Store data in dictionary for later use
            all_data[input_file] = filtered_data
        else:
            print(f"The column 'TAGS' is missing in {input_file}, skipping this file.")

    # Eliminate duplicates between SMS and Cold Calling based on mailing criteria
    if 'SMS.xlsx'in all_data and 'Cold Calling.xlsx' in all_data:
    # if 'SMS.xlsx'in all_data or 'SMS.csv' in all_data and 'Cold Calling.xlsx' in all_data or 'Cold Calling.csv' in all_data:    
        sms_data = all_data['SMS.xlsx']
        cold_calling_data = all_data['Cold Calling.xlsx']

        # Create combined criteria for MAILING ADDRESS + MAILING ZIP
        duplicate_criteria = ['Mailing Address', 'Mailing Zip']

        # Identify duplicates based on MAILING ADDRESS + MAILING ZIP
        cold_calling_duplicates = cold_calling_data[duplicate_criteria]
        sms_duplicates = sms_data[sms_data[duplicate_criteria].isin(cold_calling_duplicates.to_dict('list')).all(axis=1)]

        # Remove identified duplicates from SMS data
        sms_data = sms_data[~sms_data.index.isin(sms_duplicates.index)]

        # Update the SMS data after removing duplicates
        all_data['SMS.xlsx'] = sms_data

        print(f"Removed {len(sms_duplicates)} duplicate entries from SMS based on mailing criteria.")

    # Save each filtered dataset to an Excel file in the output folder and print count
    for file_name, data in all_data.items():
        output_file_name = file_name.replace('.xlsx', ' -BST.xlsx')  # Append 'BST' before the file extension
        output_path = os.path.join(output_folder, output_file_name)
        try:
            data.to_excel(output_path, index=False)
            print(f"Output file created at {output_path}")
            print(f"Total properties processed for skiptrace {file_name}: {data.shape[0]}")
        except Exception as e:
            print(f"Failed to save the output file {output_file_name}: {e}")

# Uncomment the following line to run the function
skiptrace_process()
