import os
import sys
import traceback
import pandas as pd
from datetime import datetime
from functions import  log, send_mail, download_pdf, db_connection
import configparser
import log_details



def check_increment_data(excel_path):

    """
    Compares data from an Excel file extracted data from website with data from a database table to identify new and deleted records.
    This function performs the following steps:
    1. Reads data from a database table and an Excel file stored with all records from website.
    2. Cleans and standardizes column names for both datasets.
    3. Identifies new records in the Excel file that are not present in the database.
    4. Identifies deleted records in the database that are not present in the Excel file.
    5. Logs the results and saves the new and deleted records to separate Excel files.
    6. Updates a log table with the status of the operation.
    7. Sends an email notification in case of errors.

    Raises:
        Exception: If any error occurs during the execution, the exception details are logged, 
                an email is sent, and the program exits.
    Notes:
        - The function uses configurations and utilities from the `log_details` module.
        - The function assumes specific column names ('order_type', 'order_no', 'name_of_party', 
        'ra_file_no',) for comparison between the database and Excel data.
        - The function saves new data to an incremental Excel file and deleted data to a separate file.
        - If no new data is found, the function logs a success message and exits.
        - If deleted data is found but no new data exists, the function logs the deleted count and exits.
    """
   
    print("check_increment_data function is called")

    try:
        config = configparser.ConfigParser()
        config.read('config.ini')
       
        with db_connection.db_connection() as connection:
            query = f"SELECT * FROM {config['general']['table_name']}"
            database_df = pd.read_sql(query, con=connection)
         # Filter out records with removal_date
        database_df.drop(columns=['source_name',  'updated_date', 'date_scraped'])
       
        excel_df = pd.read_excel(excel_path)
        excel_df = excel_df.drop_duplicates()
    
        excel_df.columns = excel_df.columns.str.strip().str.lower().str.replace(' ', '_').str.replace('.', '')

        print("database columns", database_df.columns)
        print("excel columns", excel_df.columns)


        if not database_df.empty:
            new_data = excel_df.merge(database_df[['order_type', 'order_no','name_of_party', 'ra_file_no']], on=['order_type', 'order_no','name_of_party', 'ra_file_no'], how='left', indicator=True).query('_merge == "left_only"').drop(columns=['_merge'])
        else:
            new_data = excel_df

        # Find deleted data
        # deleted_data = database_df.merge(excel_df[['order_type', 'order_no','name_of_party', 'ra_file_no']], on=['order_type', 'order_no','name_of_party', 'ra_file_no'], how="left", indicator=True).query('_merge == "left_only"').drop(columns=['_merge'])
        # deleted_data_df = pd.DataFrame(deleted_data)
        # deleted_data_df.to_excel("deleted_data.xlsx", index=False)
        
        new_data_df = pd.DataFrame(new_data)
        new_data_df.to_excel("new_data_today_123.xlsx", index=False)
        # Add diagnostic prints
        print("\nafter dropping columns:")
        print("Database DataFrame shape:", database_df.shape)
        print("Excel DataFrame shape:", excel_df.shape)

         # Safely compare values
        print("\nSample values comparison:")
        common_cols = set(database_df.columns) & set(excel_df.columns)
        for col in common_cols:
            print(f"\nColumn: {col}")
            db_value = repr(database_df[col].iloc[0]) if not database_df.empty else "No data"
            excel_value = repr(excel_df[col].iloc[0]) if not excel_df.empty else "No data"
            print("Database first value:", db_value)
            print("Excel first value:", excel_value)
        
        database_df.columns = database_df.columns.str.strip().str.lower()
        excel_df.columns = excel_df.columns.str.strip().str.lower()

        # Print the missing rows in database and Excel
        print("Rows in Excel but not in database (New Data):\n")
        print(new_data)

        # print(f"Rows in database but not in excel (Deleted Data):   {len(deleted_data)}")

        log_details.no_data_avaliable = len(new_data)
        # log_details.deleted_source_count = len(deleted_data)
         
        print( "missing rows in database", len(new_data))

        if  log_details.deleted_source_count > 0 and len(new_data) == 0:
            log_details.log_list[1] = "Success"
            log_details.log_list[3] = f"{log_details.deleted_source_count} data are deleted in the website"
            log.insert_log_into_table(log_details.log_list)
            print("log table in check increment when deleted count presents====", log_details.log_list)
            log_details.log_list = [None] * 8
    
            sys.exit()
        
        if len(new_data) == 0:
            log_details.log_list[1] = "Success"
           
            log_details.log_list[3] = "no new data"
            log.insert_log_into_table(log_details.log_list)
            print("log table====", log_details.log_list)
            log_details.log_list = [None] *8
            sys.exit()
  
        current_date = datetime.now().strftime("%Y-%m-%d")
        increment_file_name = f"incremental_excel_sheet_{current_date}.xlsx"
        increment_data_excel_path = os.path.join(config['file_paths']['increment_data_excel_path'], increment_file_name)   
        
        # Save to Excel file
        new_data_df = pd.DataFrame(new_data)
        new_data_df.to_excel(increment_data_excel_path, index=False)
        print(f"New data saved to {increment_data_excel_path}")
        download_pdf.download_pdf(increment_data_excel_path)

    except Exception as e:
            traceback.print_exc()
            log_details.log_list[1] = "Failure"
            log_details.log_list[2] = "error in checking in incremental part"
            log.insert_log_into_table(log_details.log_list)
            print("checking incremental part error:", log_details.log_list)
            send_mail.send_email("ras_sez checking incremental part error", e)
            log_details.log_list = [None] * 8
            exc_type, exc_obj, exc_tb = sys.exc_info()
            print(f"Error occurred at line {exc_tb.tb_lineno}:")
            print(f"Exception Type: {exc_type}")
            print(f"Exception Object: {exc_obj}")
            print(f"Traceback: {exc_tb}")
            sys.exit()

