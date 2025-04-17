import re
import os
import sys
import requests
import calendar
import traceback
import pandas as pd
from functions import insert_final_data_to_mysql, log, send_mail

from urllib.parse import unquote
import time
from datetime import datetime
import log_details
import configparser


def set_pdf_path(order_date, download_folder):
    
    """
    Generates a directory path for storing PDFs based on the given order date.
    This function creates a folder structure organized by year and month 
    (e.g., "download_folder/2023/October") based on the provided `order_date`. 
    If the `order_date` is missing, invalid, or in an unsupported format, 
    the PDF will be stored in a predefined "Invalid_Date" folder.
    Args:
        order_date (str): The date associated with the order. Expected formats:
                          - "YYYY-MM-DD"
                          - "DD/MM/YYYY"
        download_folder (str): The base directory where the folders will be created.
    Returns:
        str: The path to the directory where the PDF should be stored. 
             Returns the "Invalid_Date" folder path if the date is invalid or unsupported.
             Returns `None` if an exception occurs.
    Raises:
        None: Any exceptions are caught and logged, and the function returns `None`.
    Notes:
        - The function ensures that the required directories are created if they do not exist.
        - The returned path uses forward slashes ("/") for compatibility.
    Examples:
        >>> set_pdf_path("2023-10-15", "/path/to/downloads")
        '/path/to/downloads/2023/October'
        >>> set_pdf_path("15/10/2023", "/path/to/downloads")
        '/path/to/downloads/2023/October'
        >>> set_pdf_path("", "/path/to/downloads")
        '/path/to/downloads/Invalid_Date'
    """

    try:
        # Predefined folder for missing or invalid dates
        invalid_date_folder = os.path.join(download_folder, "Invalid_Date")
        os.makedirs(invalid_date_folder, exist_ok=True)

        if not order_date or not isinstance(order_date, str):
            print(f"Missing or invalid order_date ({order_date}). Storing in 'Invalid_Date' folder.")
            return invalid_date_folder.replace("\\", "/")  # Return predefined folder

        # Extract year and month
        if "-" in order_date:  # Format: YYYY-MM-DD
            parts = order_date.split("-")
        elif "/" in order_date:  # Format: DD/MM/YYYY
            parts = order_date.split("/")
            parts = [parts[2], parts[1], parts[0]]  # Rearranging to YYYY-MM-DD format
        else:
                print(f"Unsupported date format ({order_date}). Storing in 'Invalid_Date' folder.")
                return invalid_date_folder.replace("\\", "/")

        if len(parts) != 3 or not all(part.isdigit() for part in parts[:2]):
            print(f"Unsupported date format ({order_date}). Storing in 'Invalid_Date' folder.")
            return invalid_date_folder.replace("\\", "/")

        year, month = parts[0], parts[1]
        
        month_name = calendar.month_name[int(month)]
        
        # Create folder structure
        year_path = os.path.join(download_folder, year)
        month_year_path = os.path.join(year_path, month_name)
        os.makedirs(month_year_path, exist_ok=True)

        # print(f"Directory created or already exists: {month_year_path}")
        return month_year_path.replace("\\", "/")

    except Exception as e:
        print(f"Error in set_pdf_path: {e}")
        return None


def download_pdf(increment_data_excel_path):
    
    """
    Downloads PDF files from URLs specified in an Excel file, organizes them into a folder structure
    based on the order date, and updates the Excel file with the downloaded PDF details.
    Args:
        increment_data_excel_path (str): The file path to the Excel file containing the data with PDF URLs.
    The Excel file is expected to have the following columns:
        - 'attachment': The URL of the PDF file to download.
        - 'order_date': The date associated with the PDF, used for organizing the folder structure.
    Behavior:
        - Creates a download folder if it does not exist.
        - Initializes 'pdf_name' and 'pdf_path' columns in the Excel file if they are missing.
        - Iterates through each row in the Excel file to process the PDF URLs.
        - Skips rows with invalid or malformed URLs.
        - Generates sanitized filenames for the PDFs.
        - Organizes PDFs into a folder structure based on the year and month of the 'order_date'.
        - Downloads the PDF if it does not already exist in the target folder.
        - Updates the Excel file with the downloaded PDF's name and relative path.
        - Saves the updated Excel file to a new location.
        - Inserts the final data into a MySQL database.
    Exceptions:
        - Handles errors during PDF download, file saving, and database insertion.
        - Logs errors and sends email notifications in case of failures.
        - Exits the program if a critical error occurs.
    Notes:
        - Requires external modules such as `pandas`, `os`, `requests`, and `re`.
        - Relies on configurations and helper functions from `log_details`, `set_pdf_path`, 
          `insert_final_data_to_mysql`, `log`, and `send_mail`.
    Example:
        download_pdf("path/to/excel_file.xlsx")
    """
    print("download pdf function is called")

    try:
        config = configparser.ConfigParser()
        config.read('config.ini')

        df = pd.read_excel(increment_data_excel_path)
        
        base_folder = config['file_paths']['base_folder']
        # Define download folder
        download_folder = os.path.join(base_folder, "ras_sez")
        os.makedirs(download_folder, exist_ok=True)  # Ensure folder exists

        # Initialize columns for PDF details if missing
        if 'pdf_name' not in df.columns:
            df['pdf_name'] = ''
        if 'pdf_path' not in df.columns:
            df['pdf_path'] = ''    

        for index, row in df.iterrows():

            time.sleep(1)
            print(f"{index+1}. Processing PDF...")

            pdf_url = row.get("attachment")  # URL of the PDF file
            order_date = row.get("order_date")  # Date from the row

            if not pdf_url.endswith(".pdf"):
                print(f"Skipping row {index+1}: Invalid PDF URL")
                continue  # Skip invalid URLs

            # Generate a valid filename
            # pdf_filename = re.sub(r'://|[\\/:"*?<>|]', '_', pdf_url)

             # Extract unique identifier and file name
            url_parts = pdf_url.split("/")
            if len(url_parts) < 2:
                print(f"Skipping row {index+1}: Malformed URL")
                continue

            unique_id = url_parts[-2]  # Extract the unique identifier
            file_name = url_parts[-1]  # Extract the actual file name

            # Decode URL encoding (replace %20 with spaces)
            file_name = unquote(file_name)

            
            # Replace spaces with underscores
            file_name = file_name.replace(" ", "_")

            # Sanitize file name to remove invalid characters
            pdf_filename = re.sub(r'[\\/:"*?<>|]', '_', f"{unique_id}_{file_name}")

            print(f"Generated PDF filename: {pdf_filename}")

            # Get the structured path for saving PDFs
            yr_month_path = set_pdf_path(order_date, download_folder)
            if not yr_month_path:
                print(f"Skipping row {index+1}: Invalid date format")
                df.at[index, "pdf_path"] = "Invalid date format"
                df.at[index, "pdf_name"] = "Invalid date format"
                continue

            pdf_path = os.path.join(yr_month_path, pdf_filename)

            # Extract the relative path (for database storage or logs)
            start_index = pdf_path.find("/pdf_download")
            extracted_pdf_path = pdf_path[start_index:] if start_index != -1 else pdf_path
            extracted_pdf_path = extracted_pdf_path.replace("\\", "/")

            if os.path.exists(pdf_path):
                print(f"Skipping: {pdf_filename} already exists.")
                df.at[index, "pdf_path"] = extracted_pdf_path
                df.at[index, "pdf_name"] = pdf_filename
                continue  # Skip to the next iteration if file exists
            print("pdf_url -----------------",pdf_url)
            try:
                # Download the PDF
                response = requests.get(pdf_url, stream=True)
                if response.status_code == 200:

                    # Save the PDF
                    with open (pdf_path, 'wb') as pdf_file:
                        pdf_file.write(response.content)
                    print(f"Downloaded: {pdf_filename}")

                    # Update DataFrame
                    df.at[index, 'pdf_name'] = pdf_filename
                    df.at[index, 'pdf_path'] = extracted_pdf_path
                else:
                    print(f"Failed to download {pdf_url} (Status Code: {response.status_code})")

            except Exception as e:
                print(f"Error downloading {pdf_url}: {e}")

        try:
            current_date = datetime.now().strftime("%Y-%m-%d")
            final_excel_sheet_name = f"final_excel_sheet_{current_date}.xlsx"
    
            # Build full path using os.path.join
            final_exceL_sheet_path = os.path.join(config['file_paths']['final_excel_sheet_path'], final_excel_sheet_name)
                
            df.to_excel(final_exceL_sheet_path, index=False) 

            print(f"Data saved to {final_exceL_sheet_path}.")
            insert_final_data_to_mysql.insert_final_data_to_mysql(final_exceL_sheet_path)
        except Exception as e:
            print("An error occurred while saving the data:")
            traceback.print_exc()

    except Exception as e:
        traceback.print_exc()
        print(f"An error occurred during PDF download: {e}") 
        log_details.log_list[1] = "Failure"
        log_details.log_list[2] = "error in pdf download part"
        log.insert_log_into_table(log_details.log_list)
        print("error in pdf download part:", log_details.log_list)
        send_mail.send_email("error in pdf download part:", e)
        log_details.log_list = [None] * 8
        exc_type, exc_obj, exc_tb = sys.exc_info()
        print(f"Error occurred at line {exc_tb.tb_lineno}:")
        print(f"Exception Type: {exc_type}")
        print(f"Exception Object: {exc_obj}")
        print(f"Traceback: {exc_tb}")
        sys.exit()

