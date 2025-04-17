from functions import get_data_count_database, db_connection
import sys
import json
from datetime import datetime
import log_details
import configparser


def insert_log_into_table(log_list):
    
    """
    Inserts log data into the 'dgft_log' table in the database.
    This function takes a list of log details, constructs a dictionary of values, 
    and inserts them into the 'dgft_log' table. It uses a database connection 
    and cursor to execute the SQL query. If an exception occurs, it prints 
    detailed error information.
    Args:
        log_list (list): A list containing log details. Expected indices:
            - log_list[1]: Script status (optional).
            - log_list[2]: Failure reason (optional).
            - log_list[3]: Comments (optional).
    Raises:
        Exception: If an error occurs during the database operation, the exception 
        details are printed, including the line number, exception type, and traceback.
    Notes:
        - The function relies on external configurations and modules such as 
        `log_details` and `get_data_count_database`.
        - The `removal_date` is set to the current date if `deleted_source_count` 
        is available in `log_details`.
        - The database connection is closed after the operation.
    """

    print("insert_log_into_table function is called")
    
    try:
        config = configparser.ConfigParser()
        config.read('config.ini')

        removal_date = datetime.now().strftime('%Y-%m-%d')
        connection = db_connection.db_connection()
        cursor = connection.cursor()

        query = f"""
            INSERT INTO {config['general']['log_table_name']} (source_name, script_status, data_available, data_scraped, total_record_count, failure_reason, comments, source_status, newly_added_count, deleted_source, deleted_source_count, removal_date)
            VALUES (%(source_name)s, %(script_status)s, %(data_available)s, %(data_scraped)s, %(total_record_count)s, %(failure_reason)s, %(comments)s, %(source_status)s, %(newly_added_count)s, %(deleted_source)s, %(deleted_source_count)s, %(removal_date)s)
        """
        values = {
            'source_name': log_details.source_name if log_details.source_name else None,
            'script_status': log_list[1] if log_list[1] else None,
            'data_available': log_details.no_data_avaliable if log_details.no_data_avaliable else None,
            'data_scraped': log_details.no_data_scraped if log_details.no_data_scraped else None,
            'total_record_count': get_data_count_database.get_data_count_database(),
            'failure_reason': log_list[2] if log_list[2] else None,
            'comments': log_list[3] if log_list[3] else None,
            'source_status': log_details.source_status,
            'newly_added_count': log_details.newly_added_count if log_details.newly_added_count else None,
            # 'updated_source_count': log_details.updated_count if log_details.updated_count else None,
            'deleted_source': log_details.deleted_source if log_details.deleted_source else None,
            'deleted_source_count': log_details.deleted_source_count if log_details.deleted_source_count else None,
            'removal_date' : removal_date if log_details.deleted_source_count else None
        }

        cursor.execute(query, values)
        print("log list ====", values)
        connection.commit()
        connection.close()

    except Exception as e:
        print("Error in insert_log_into_table function:", e)
        exc_type, exc_obj, exc_tb = sys.exc_info()
        print(f"Error occurred at line {exc_tb.tb_lineno}:")
        print(f"Exception Type: {exc_type}")
        print(f"Exception Object: {exc_obj}")
        print(f"Traceback: {exc_tb}")
           