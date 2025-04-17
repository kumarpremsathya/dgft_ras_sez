from functions import db_connection
import configparser



def get_data_count_database():
    """
    Retrieves the total count of records from a specified database table.
    This function establishes a connection to the database using the configuration
    provided in the `ras_sez_config` module. It executes a SQL query to count the
    total number of records in the table specified by `ras_sez_config.table_name`.
    Returns:
        int: The total number of records in the specified database table.
    Raises:
        ValueError: If the query does not return any results.
        Exception: If there is an error during database connection or query execution.
    Note:
        Ensure that `ras_sez_config.db_connection()` and `ras_sez_config.table_name`
        are correctly configured before calling this function.
    """
    print("get_data_count_database function is called")

    try:

        config = configparser.ConfigParser()
        config.read('config.ini')

        # Function to get the total records in the database table
        connection = db_connection.db_connection()
        cursor = connection.cursor()

        # SQL query to get the count of records in the specified table
        query = f"SELECT COUNT(*) FROM {config['general']['table_name']}"
        cursor.execute(query)
        result = cursor.fetchone()
        print("Result from database query:", result)
        if result:
            return result[0]
        else:
            raise ValueError("Query did not return any results")
    except Exception as e:
        print("Error fetching data count from database:", e)
        raise
