import os
import pandas as pd
import numpy as np
import datetime

current_date = datetime.date.today()
needed_format = current_date.strftime("%Y-%m")
def integrate_skiptrace_data(input_folder="t1 input", output_folder="t1 output"):
    # ... (el código inicial para encontrar y leer los archivos sigue igual) ...
    # Ensure the output directory exists
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)

    # Find the T1Skiptrace and Cold Calling or SMS files in the input folder
    t1_files = [f for f in os.listdir(input_folder) if "BST_out" in f and f.endswith('.xlsx')]
    calling_sms_files = [f for f in os.listdir(input_folder) if (
                 "Cold Calling" in f or "SMS" in f or "Sms" in f or "CC" in f or "Cold Call" in f) and f.endswith(
         '.xlsx')]

    if not t1_files:
        print("No T1Skiptrace BST_out file found.")
        return
    if not calling_sms_files:
         print("No Cold Calling or SMS file found.")
         return

    # Read the Excel files
    t1_file_path = os.path.join(input_folder, t1_files[0])
    calling_sms_file_path = os.path.join(input_folder, calling_sms_files[0])
    try:
        t1_data = pd.read_excel(t1_file_path)
        calling_sms_data = pd.read_excel(calling_sms_file_path)
    except Exception as e:
        print(f"Failed to read the files: {e}")
        return

    # Verify the number of rows matches
    # if len(t1_data) != len(calling_sms_data):
    #     print("The number of rows in T1Skiptrace BST_out does not match the Cold Calling/SMS file.")
    #     return

    # --- INICIO DE CAMBIOS PARA REORDENAR ---

    # Hacemos un vlookup para traer el folio desde la lista de los fulfillments
    calling_sms_data['Property_ID'] = calling_sms_data['Property Address'].astype(str) + '' + calling_sms_data['Property Zip'].astype(str)
    t1_data['Property_ID'] = t1_data['address'].astype(str) + '' + t1_data['zip'].astype(str)
    list_data_vlookup = calling_sms_data[['Property_ID', 'Folio']]
    vlookup = pd.merge(
        t1_data,
        list_data_vlookup,
        how='left',
        on='Property_ID'
    ).drop('Property_ID', axis=1)  # Limpiamos la clave duplicada
    t1_data = vlookup
    # 1. Creamos un nuevo DataFrame vacío que construiremos en el orden correcto
    ordered_df = pd.DataFrame()
    ordered_df['ID'] = range(1, len(t1_data) + 1)
    # ordered_df['Folio'] = calling_sms_data['Folio']

    # 2. Agregamos las columnas que no cambian de nombre o que tienen un renombrado simple
    simple_rename_mapping = {
        'Property_ID': "Property_ID",
        'Folio': 'Folio',
        'first_name': 'First Name',
        'last_name': 'Last Name',
        'mail_address': 'Mailing Address',
        'mail_city': 'Mailing city',
        'mail_state': 'Mailing state',
        'mailing_zip': 'Mailing zip',
        'address': 'Property Address',
        'city': 'Property city',
        'state': 'Property State',
        'zip': 'Property zip',
        'primary_phone': 'Phone Number',
        'primary_phone_type': 'Phone Type',
        'PH: Phone1': 'Phone Number',
        'PH: Phone1 Type': 'Phone Type',
        'Email-1': 'Email1',
        'Email-2': 'Email2',
        'Email-3': 'Email3',
        'Email-4': 'Email4',
        'Email-5': 'Email5',
    }

    for original_name, new_name in simple_rename_mapping.items():
        if original_name in t1_data.columns:
            ordered_df[new_name] = t1_data[original_name]

    # 3. Mismo mapa que antes para manejar los teléfonos
    phone_mapping = {
        'Mobile-1': {'num': 'Phone Number2', 'type': 'Phone Type2', 'value': 'Mobile'},
        'Mobile-2': {'num': 'Phone Number3', 'type': 'Phone Type3', 'value': 'Mobile'},
        'Mobile-3': {'num': 'Phone Number4', 'type': 'Phone Type4', 'value': 'Mobile'},
        'Mobile-4': {'num': 'Phone Number5', 'type': 'Phone Type5', 'value': 'Mobile'},
        'Mobile-5': {'num': 'Phone Number6', 'type': 'Phone Type6', 'value': 'Mobile'},
        'Landline-1': {'num': 'Phone Number7', 'type': 'Phone Type7', 'value': 'Landline'},
        'Landline-2': {'num': 'Phone Number8', 'type': 'Phone Type8', 'value': 'Landline'},
        'Landline-3': {'num': 'Phone Number9', 'type': 'Phone Type9', 'value': 'Landline'},
    }

    # 4. Iteramos sobre el mapa y agregamos las columnas de NÚMERO y TIPO en pares
    for original_col, new_names in phone_mapping.items():
        if original_col in t1_data.columns:
            new_num_col = new_names['num']
            new_type_col = new_names['type']
            phone_type_value = new_names['value']

            # Agregamos la columna de número al nuevo DataFrame
            ordered_df[new_num_col] = t1_data[original_col]

            # Agregamos la columna de tipo JUSTO DESPUÉS, con la misma lógica de antes
            ordered_df[new_type_col] = np.where(
                ordered_df[new_num_col].notna(),
                phone_type_value,
                ''
            )

    # 5. Reemplazamos el DataFrame original con nuestro nuevo DataFrame ordenado
    t1_data = ordered_df
    t1_data['Tags'] = 'T1.2Skiptrace'
    t1_data['Number source'] = 'T1.2'
    t1_data['Tags2'] = f'Skipped {needed_format}'


    # --- FIN DE CAMBIOS ---

    # El resto del código para preparar el archivo "Litigator" sigue igual...
    phone_columns = [
        'Phone Number', 'Phone Number2', 'Phone Number3', 'Phone Number4', 'Phone Number5',
        'Phone Number6', 'Phone Number7', 'Phone Number8', 'Phone Number9'
    ]

    existing_phone_columns = [col for col in phone_columns if col in t1_data.columns]

    print("Cleaning phone numbers...") #this code helps cleaning the phone numbers of weird characters
    for col in existing_phone_columns:
        t1_data[col] = t1_data[col].astype(str).str.replace(r'\D', '', regex=True)
        t1_data[col] = t1_data[col].replace('', np.nan)

    litigator_data = t1_data[['ID'] + existing_phone_columns]
    litigator_data = litigator_data.set_index('ID')
    litigator_data = litigator_data.stack().reset_index(name='Numbers').drop('level_1', axis=1)
    litigator_data = litigator_data[litigator_data['Numbers'].notnull()]

    # ...y la sección para guardar los archivos también sigue igual.
    t1_output_path = os.path.join(output_folder, f"modified_{os.path.basename(t1_file_path)}")
    litigator_output_path = os.path.join(output_folder, "Litigator scrubbing.xlsx")
    try:
        t1_data.to_excel(t1_output_path, index=False)
        # calling_sms_data.to_excel(t1_output_path, index=False)
        print(f"Modified file saved successfully at {t1_output_path}")
        litigator_data.to_excel(litigator_output_path, index=False)
        print(f"Litigator scrubbing file saved successfully at {litigator_output_path}")
    except Exception as e:
        print(f"Failed to save the modified files: {e}")


# Uncomment the following line to run the function
integrate_skiptrace_data()