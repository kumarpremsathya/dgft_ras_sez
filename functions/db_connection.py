from selenium import webdriver
from datetime import datetime
import mysql.connector
import configparser
import os
import sys


def db_connection():
    
    """
    Establishes a connection to the MySQL database using configuration from 'config.ini'.
    Returns:
        mysql.connector.connection.MySQLConnection: A connection object to the MySQL database if successful.
    Raises:
        Exception: If the config.ini file cannot be read or if database connection fails.
            Specific error message will be printed to console.
    Notes:
        - Requires a valid config.ini file with [database] section containing:
            - host
            - user
            - password
            - database
            - auth_plugin
        - Uses mysql.connector to establish database connection
    """

    try:
        config = configparser.ConfigParser()
        if not config.read('config.ini'):
            raise Exception("Could not read config.ini file")

        connection = mysql.connector.connect(
            host = config['database']['host'],
            user = config['database']['user'],
            password = config['database']['password'],
            database = config['database']['database'],
            auth_plugin = config['database']['auth_plugin']  
        )
       
        return connection
    except Exception as e:
        print(f"Error: {str(e)}")