
import sys
import traceback
import log_details
from functions import extract_all_data_in_website,log,check_increment_data

"""
RAS SEZ incremental Data Extraction

A script to extract orders and action data from the Directorate General of Foreign Trade (DGFT) website.
Specifically targets orders passed by DGFT Regional Authorities & SEZs.

Key Features:
- Extracts orders related to foreign trade regulation violations
- Collects data from DGFT HQ and DGFT RAs & SEZ sources
- Captures order details including type, IEC, party name, file numbers and dates
- Downloads associated order attachments
- Handles different source states (Active, Hibernated, Inactive)

Data Sources:
- DGFT RAs & SEZ Orders: https://www.dgft.gov.in/CP/?opt=order-passed-rasz

The script provides comprehensive monitoring of penalties and regulatory actions
against companies under DGFT jurisdiction.
Data points to collect: 
"""


def main():

    """
    Main execution module for RAS SEZ Historical data extraction.
    This module controls the flow of the application based on the source status defined in log_details.
    It orchestrates the data extraction process and handles different states of the source system.
    Dependencies:
        - log_details: Contains source status and logging information
        - extract_all_data_in_website: Module for data extraction
        - log: Module for logging operations
        - sys: For system operations and exit handling
        - traceback: For exception handling
    States handled:
        - Active: Proceeds with data extraction
        - Hibernated: Logs status and exits
        - Inactive: Logs status, clears log list and exits
    Returns:
        None
    Raises:
        SystemExit: On Hibernated or Inactive states
    Usage:
        Called as the main entry point of the application to control data extraction flow
        based on source system status.


        Orders by the DGFT RAs & SEZ: 
            1.	Office 
            2.	Order Type 
            3.	Order No
            4.	Order Date 
            5.	Name of Party 
            6.	RA File No
            7.	Category
            8.	IEC
            9.	Issued by 
            10.	Text of Order 
            11.	Attachment

    """

    print("main function is called")
 
    if log_details.source_status == "Active":

        extract_all_data_in_website.extract_all_data_in_website()
        # excel_path = r"C:\Users\Premkumar.8265\Desktop\ras_sez_historical\data\first_excel_sheet\first_excel_sheet_2025-04-11.xlsx"
        # check_increment_data.check_increment_data(excel_path)
     
        print("finsihed")

    elif log_details.source_status == "Hibernated":
        log_details.log_list[1] = "not run"
        print(log_details.log_list)
        log.insert_log_into_table(log_details.log_list)

        exc_type, exc_obj, exc_tb = sys.exc_info()
        print(f"Error occurred at line {exc_tb.tb_lineno}:")
        print(f"Exception Type: {exc_type}")
        print(f"Exception Object: {exc_obj}")
        print(f"Traceback: {exc_tb}")
        sys.exit()
            
    elif log_details.source_status == "Inactive":
        log_details.log_list[1] = "not run"
       
        traceback.print_exc()
        print(log_details.log_list)
        log.insert_log_into_table(log_details.log_list)
        
        print(log_details.log_list)
        log_details.log_list = [None] * 4
        traceback.print_exc()
        exc_type, exc_obj, exc_tb = sys.exc_info()
        print(f"Error occurred at line {exc_tb.tb_lineno}:")
        print(f"Exception Type: {exc_type}")
        print(f"Exception Object: {exc_obj}")
        print(f"Traceback: {exc_tb}")
        sys.exit()


if __name__ == "__main__":
    main()
