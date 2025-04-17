import pandas as pd
from mysql.connector import Error
import mysql.connector

import sys 
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from functions import log, send_mail, db_connection
import traceback
import os

import configparser
import log_details




def insert_final_data_to_mysql(final_excel_sheets_path):

    """
    Inserts incremental data from an Excel file into a MySQL database table.
    This function reads data from the specified Excel file, processes it, and inserts it into the 
    `dgft_ras_sez` table in the MySQL database. It also handles logging, error reporting, and email 
    notifications in case of failures.
    Args:
        final_excel_sheets_path (str): The file path of the Excel sheet containing the data to be inserted.
    Raises:
        Exception: If any error occurs during the process, it is logged, and an email notification is sent.
    Workflow:
        1. Reads the Excel file into a pandas DataFrame.
        2. Cleans the column names and replaces NaN values with None.
        3. Establishes a connection to the MySQL database.
        4. Iterates through each row of the DataFrame and inserts the data into the database.
        5. Logs the success or failure of the operation.
        6. Sends an email notification in case of an error.
        7. Closes the database connection and exits the script.
    Notes:
        - The function assumes the existence of a `log_details` module for database connection 
          and logging configurations.
        - The `log` and `send_mail` modules are used for logging and email notifications, respectively.
        - The `dgft_ras_sez` table schema must match the columns in the Excel file.
    Example:
        insert_final_data_to_mysql("path/to/excel_file.xlsx")
    """

    print("insert_final_data_to_mysqll function is called")

    connection = None
    cursor = None
    try:
        config = configparser.ConfigParser()
        config.read('config.ini')

        # Read the Excel file into a DataFrame
        df = pd.read_excel(final_excel_sheets_path)
        # df = df.iloc[:]
        df.columns = df.columns.str.strip().str.lower().str.replace(' ', '_').str.replace('.', '')
        print("df_ columns    ", df)
        

         # Replace NaN with None to store NULL in MySQL
        df = df.where(pd.notna(df), None)
        
        
        # Establish database connection
        connection = db_connection.db_connection()
        if not connection.is_connected():
            raise Error("Failed to connect to database")
        cursor = connection.cursor()
       

        count = 0
        for index, row in df.iterrows():
            # Convert row to dictionary and ensure all NaN values are None
            row_dict = row.to_dict()
            for key in row_dict:
                if pd.isna(row_dict[key]):
                    row_dict[key] = None
            
            insert_query = f"""
                INSERT INTO {config['general']['table_name']}(source_name, office, order_type, order_no, order_date, name_of_party, ra_file_no, category, iec, 
                  issued_by, text_of_order, attachment, 
                pdf_name, pdf_path)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            
            values = (
                'dgft_ras_sez',
                row_dict.get("office"),
                row_dict.get("order_type"),
                row_dict.get("order_no"),
                row_dict.get("order_date"),
                row_dict.get("name_of_party"),
                row_dict.get("ra_file_no"),
                row_dict.get("category"),
                row_dict.get("iec"),
                row_dict.get("issued_by"),
                row_dict.get("text_of_order"),
                row_dict.get("attachment"),
                row_dict.get("pdf_name"),
                row_dict.get("pdf_path")
               
                )
            
            # Check for any remaining NaN values
            clean_values = []
            for val in values:
                if pd.isna(val):  # Catch any remaining NaN values
                    clean_values.append(None)
                else:
                    clean_values.append(val)
            
            # Execute the SQL with cleaned values
            cursor.execute(insert_query, tuple(clean_values))
            count += 1
            print(f"Row {count} has been successfully inserted into the MySQL database.")
        connection.commit()


        log_details.log_list[1] = "Success"
        log_details.no_data_scraped = count
        log_details.newly_added_count = count
        log_details.log_list[3] = f"{log_details.newly_added_count} new data"
        
        print("log table in insert final data to mysql function====", log_details.log_list)
        log.insert_log_into_table(log_details.log_list)
        log_details.log_list = [None] * 8
        print("Data has been successfully inserted into the MySQL database.")
        sys.exit()
    except Exception as e:
        print(f"Error: {str(e)}")
        if connection and connection.is_connected():
            connection.rollback()
        print(f"Error: {str(e)}")
        traceback.print_exc()

        log_details.log_list[1] = "Failure"
        log_details.log_list[2] = "error in ras sez Data inserted into the database error"
        log.insert_log_into_table(log_details.log_list)
        print("database insertion error:", log_details.log_list)
        log_details.log_list = [None] * 8
        send_mail.send_email("ras sez Data inserted into the database error", e)

        exc_type, exc_obj, exc_tb = sys.exc_info()
        print(f"Error occurred at line {exc_tb.tb_lineno}:")
        print(f"Exception Type: {exc_type}")
        print(f"Exception Object: {exc_obj}")
        print(f"Traceback: {exc_tb}")
         
        sys.exit("script error")

       
    finally:
        if cursor:
            cursor.close()
        if connection and connection.is_connected():
            connection.close()




