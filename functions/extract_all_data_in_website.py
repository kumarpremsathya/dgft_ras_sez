import time
import traceback
import pandas as pd
import os
from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from bs4 import BeautifulSoup
from datetime import datetime
from selenium.webdriver.common.keys import Keys
from functions import check_increment_data, log, send_mail
import sys
from selenium.common.exceptions import TimeoutException, WebDriverException, NoSuchElementException
import configparser
import log_details
config = configparser.ConfigParser()
config.read('config.ini')


try: 
    # Open the URL and maximize the browser window
    # driver = webdriver.Chrome()

    chrome_options = webdriver.ChromeOptions()

    # Enable headless mode
    chrome_options.add_argument("--headless=new")  # Use new headless mode for better compatibility
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--window-size=1920,1080")  # Set window size to avoid hidden elements
    
    # service = Service(chrome_driver_path)
    driver = webdriver.Chrome(options=chrome_options)
           
    driver.get(config['url']['dgft_url'])
    driver.maximize_window()
    time.sleep(20)

except(TimeoutException, WebDriverException, NoSuchElementException) as e:
    raise Exception("Website not opened correctly") from e


def handle_security_warning(page):
    """Check for security warning and handle it."""
    try:
    
        if "j_spring_security_logout" in driver.current_url:
            print("Security warning detected! Refreshing page...")
            # driver.refresh()
            driver.delete_all_cookies()
            driver.get(config['url']['dgft_url'])
            # driver.execute_script("location.reload();")
            zero_indexed_page = page - 1 
            js_code = f"""
                $('#metaTable').DataTable().page({zero_indexed_page}).draw(false);
            """
            driver.execute_script(js_code)
            time.sleep(25)  # Wait for refresh
    except Exception as e:
        print(f"Error handling security warning: {e}")
        raise e
    


def extract_all_data_in_website():

    """
    Extracts data from a paginated table on a dgft website and saves it to an Excel file.
    This function uses Selenium to control a web browser and BeautifulSoup to parse
    the HTML content of a webpage. It extracts table data, including headers and rows,
    and saves the data to an Excel file with a timestamp. The function handles pagination
    and continues scraping until no next page is found.
    Returns:
        pd.DataFrame: A DataFrame containing the extracted table data.
    Raises:
        Exception: If the website cannot be opened or if an error occurs during data extraction.
    Workflow:
        1. Initializes a headless Chrome WebDriver.
        2. Opens the target URL and maximizes the browser window.
        3. Scrapes table data from the webpage, including headers and rows.
        4. Handles pagination by clicking the "next page" button until no more pages are available.
        5. Saves the extracted data to an Excel file with a timestamp.
        6. Logs errors and sends email notifications in case of failures.
    Notes:
        - The function uses configuration values from `log_details` for table tags and URL.
        - The extracted data includes a hyperlink from the last column of each row, if available.
        - Errors are logged and email notifications are sent using the `log` and `send_mail` modules.
    Exceptions Handled:
        - TimeoutException, WebDriverException, NoSuchElementException: Raised if the website cannot be opened.
        - General exceptions during data extraction and saving are logged and handled gracefully.
    Dependencies:
        - Selenium WebDriver for browser automation.
        - BeautifulSoup for HTML parsing.
        - pandas for data manipulation and saving to Excel.
        - `log_details` for configuration values.
        - `log` and `send_mail` modules for logging and notifications.
    Example:
        >>> df = extarct_first_part_table()
        >>> print(df.head())
    """
    print("extract_all_data_in_website function is called") 

    try:
 
        config = configparser.ConfigParser()
        config.read('config.ini')       
        
        table_data = []  # Store extracted data
        headers = []  # Initialize headers list
        
        time.sleep(20)
        page_count = driver.find_element(By.XPATH,config['xpaths']['page_count'])
        total_page = page_count.text

        print('total_page',total_page)
        
        for page in range(1, (int(total_page)+1)):  # Resume from last extracted page
     
            zero_indexed_page = page - 1  # Convert to DataTables page index (0-based)

            
            max_retries = 3  # Maximum number of retries for JavaScript execution
            for attempt in range(max_retries):
                try:
                    # JavaScript to navigate to the desired page
                    js_code = f"""
                        $('#metaTable').DataTable().page({zero_indexed_page}).draw(false);
                    """

                    driver.execute_script(js_code)
                    # print(f"JavaScript executed successfully on attempt {attempt + 1}")
                    break  # Success
                except Exception as e:
                    print(f"Attempt {attempt + 1} failed: {e}")
                    time.sleep(3)

            time.sleep(3)  # Wait for table to update

            print(f"Successfully jumped to page {page} using JavaScript")
            handle_security_warning(page)  # Check for security warning

            # Wait until table is updated
            # WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.ID, "metaTable")))
            page_source = driver.page_source

            # Parse the page content using BeautifulSoup to extract table data
            soup = BeautifulSoup(page_source, 'html.parser')
        
            # Extract the table from the webpage
            table = soup.find(config['table_tags']['table_tag'])
            headers = []  # List to store the table headers
            rows = table.find_all(config['table_tags']['tr_tag'])  # Find all rows in the table

            # Extract table headers (first row)
            header_row = rows[0]
            header_columns = header_row.find_all(config['table_tags']['th_tag'])
            for col in header_columns:
                headers.append(col.text.strip())  # Append cleaned header names
            
        
            for row in rows[1:]:  # Skip the header row
                columns = row.find_all(config['table_tags']['td_tag'])
                
                # Extract text from all columns except the last one
                row_data = [col.text.strip() for col in columns[:-1]]
                
                # Extract href from the last column
                href = columns[-1].find("a")["href"] if columns[-1].find("a") else "No Link"
                
                row_data.append(href)  # Append extracted href
                table_data.append(row_data)   

            # Save the extracted data to an Excel file
            df = pd.DataFrame(table_data, columns=headers)

            current_date = datetime.now().strftime("%Y-%m-%d")

            first_excel_sheet_name = f"first_excel_sheet_{current_date}.xlsx"
            
            # Build full path using os.path.join
            first_exceL_sheet_path = os.path.join(config['file_paths']['first_excel_sheet_path'], first_excel_sheet_name)
                
            df.to_excel(first_exceL_sheet_path, index=False)

        print(f"Data saved to {first_exceL_sheet_path}.")
        check_increment_data.check_increment_data(first_exceL_sheet_path)

    except AttributeError as e:  # If table is None
        handle_security_warning()   

    except Exception as e:
        traceback.print_exc()
        log_details.log_list[1] = "Failure"
        if str(e) == "Website not opened correctly":
            log_details.log_list[3] = "Website is not opened"
        else:
            log_details.log_list[3] = "Error in first part table data extraction part" 
        
        log.insert_log_into_table(log_details.log_list)
        print("error in data extraction part======", log_details.log_list)
        log_details.log_list = [None] * 8

        send_mail.send_email("ras sez extract data in website error", e)
        exc_type, exc_obj, exc_tb = sys.exc_info()
        print(f"Error occurred at line {exc_tb.tb_lineno}:")
        print(f"Exception Type: {exc_type}")
        print(f"Exception Object: {exc_obj}")
        print(f"Traceback: {exc_tb}")
        sys.exit("script error")         
                
















# def click_next_page(driver):

#     """Handles pagination by clicking the 'Next' button to load the next page."""
#     try:
#         config = configparser.ConfigParser()
#         config.read('config.ini')

#         # Find the 'Next' button 
#         driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")

#         time.sleep(1)
#         next_button = driver.find_element(By.XPATH, config['xpaths']['next_button_xpath'])
#         # actions = ActionChains(driver)
#         # actions.move_to_element(next_button).perform()

#         if "disabled" in next_button.get_attribute("class"):
#             print("Reached the last page. No more pages to scrape.")
#             return False
#         next_button.click()  # Click 'Next' to load the next page
#           # Wait for the next page to load
#         return True
#     except Exception as e:
#         print(f"Error or no more pages found: {e}")
#         return False



# def extarct_first_part_table():

#     """
#     Extracts data from a paginated table on a dgft website and saves it to an Excel file.
#     This function uses Selenium to control a web browser and BeautifulSoup to parse
#     the HTML content of a webpage. It extracts table data, including headers and rows,
#     and saves the data to an Excel file with a timestamp. The function handles pagination
#     and continues scraping until no next page is found.
#     Returns:
#         pd.DataFrame: A DataFrame containing the extracted table data.
#     Raises:
#         Exception: If the website cannot be opened or if an error occurs during data extraction.
#     Workflow:
#         1. Initializes a headless Chrome WebDriver.
#         2. Opens the target URL and maximizes the browser window.
#         3. Scrapes table data from the webpage, including headers and rows.
#         4. Handles pagination by clicking the "next page" button until no more pages are available.
#         5. Saves the extracted data to an Excel file with a timestamp.
#         6. Logs errors and sends email notifications in case of failures.
#     Notes:
#         - The function uses configuration values from `log_details` for table tags and URL.
#         - The extracted data includes a hyperlink from the last column of each row, if available.
#         - Errors are logged and email notifications are sent using the `log` and `send_mail` modules.
#     Exceptions Handled:
#         - TimeoutException, WebDriverException, NoSuchElementException: Raised if the website cannot be opened.
#         - General exceptions during data extraction and saving are logged and handled gracefully.
#     Dependencies:
#         - Selenium WebDriver for browser automation.
#         - BeautifulSoup for HTML parsing.
#         - pandas for data manipulation and saving to Excel.
#         - `log_details` for configuration values.
#         - `log` and `send_mail` modules for logging and notifications.
#     Example:
#         >>> df = extarct_first_part_table()
#         >>> print(df.head())
#     """
    
#     try:    
#         try:
#             config = configparser.ConfigParser()
#             config.read('config.ini')
#             # Initialize WebDriver to control the browser

#             chrome_options = webdriver.ChromeOptions()

#             # Enable headless mode
#             chrome_options.add_argument("--headless=new")  # Use new headless mode for better compatibility
#             chrome_options.add_argument("--disable-gpu")
#             chrome_options.add_argument("--no-sandbox")
#             chrome_options.add_argument("--disable-dev-shm-usage")
#             chrome_options.add_argument("--window-size=1920,1080")  # Set window size to avoid hidden elements
            
#             driver = webdriver.Chrome(options=chrome_options)

#             # driver = webdriver.Chrome()
#             # Open the URL and maximize the browser window
            
#             driver.get(config['url']['dgft_url'])
#             driver.maximize_window()
#         except(TimeoutException, WebDriverException, NoSuchElementException) as e:
#             raise Exception("Website not opened correctly") from e

#         page = 1
#         table_data = []
#         try:
#             while True:
#                 print(f"Scraping data from page {page}...")
           
#                 # Get the page source (HTML content) from the webpage
#                 page_source = driver.page_source

#                 # Parse the page content using BeautifulSoup to extract table data
#                 soup = BeautifulSoup(page_source, 'html.parser')
            
#                 # Extract the table from the webpage
#                 table = soup.find(config['xpaths']['table_tag'])
#                 headers = []  # List to store the table headers
#                 rows = table.find_all(config['xpaths']['tr_tag'])  # Find all rows in the table

#                 # Extract table headers (first row)
#                 header_row = rows[0]
#                 header_columns = header_row.find_all(config['xpaths']['th_tag'])
#                 for col in header_columns:
#                     headers.append(col.text.strip())  # Append cleaned header names
            
                
        
#                 for row in rows[1:]:  # Skip the header row
#                     columns = row.find_all(config['xpaths']['td_tag'])
                    
#                     # Extract text from all columns except the last one
#                     row_data = [col.text.strip() for col in columns[:-1]]
                    
#                     # Extract href from the last column
#                     href = columns[-1].find("a")["href"] if columns[-1].find("a") else "No Link"
                    
#                     row_data.append(href)  # Append extracted href
#                     table_data.append(row_data)   
                
#                         # Check if there is a next page, and if so, continue scraping
                
                
#                 # Save the extracted data to an Excel file
#                 df = pd.DataFrame(table_data, columns=headers)
                
            
#                 # Save to Excel
#                 # df.to_excel("first_part_new.xlsx", index=False)
#                 # print(f"Data successfully extracted and saved to first part xlsx.")
        

#                 if not click_next_page(driver):
#                     break  # Break the loop if no next page is found

#                 page += 1  # Increment the page number for the next iteration

            
#             return df 
#         except Exception as e:
#             print("An error occurred while saving the data:")
#             traceback.print_exc()
            
#         finally:
#             driver.quit()
#     except Exception as e:
#         traceback.print_exc()
#         log_details.log_list[1] = "Failure"
#         if str(e) == "Website not opened correctly":
#             log_details.log_list[3] = "Website is not opened"
#         else:
#             log_details.log_list[3] = "Error in first part table data extraction part" 
        
#         log.insert_log_into_table(log_details.log_list)
#         print("error in data extraction part======", log_details.log_list)
#         log_details.log_list = [None] * 8

#         send_mail.send_email("ras sez extract data in website error", e)
#         exc_type, exc_obj, exc_tb = sys.exc_info()
#         print(f"Error occurred at line {exc_tb.tb_lineno}:")
#         print(f"Exception Type: {exc_type}")
#         print(f"Exception Object: {exc_obj}")
#         print(f"Traceback: {exc_tb}")
#         sys.exit("script error")
    
       


# def extract_second_part_table():

#     try:

#         try :

#             config = configparser.ConfigParser()
#             config.read('config.ini')

#             # Initialize WebDriver with Chrome options
#             chrome_options = webdriver.ChromeOptions()
#             chrome_options.add_argument("--headless=new")
#             chrome_options.add_argument("--disable-gpu")
#             chrome_options.add_argument("--no-sandbox")
#             chrome_options.add_argument("--disable-dev-shm-usage")
#             chrome_options.add_argument("--window-size=1920,1080")
            
#             driver = webdriver.Chrome(options=chrome_options)
#             # driver = webdriver.Chrome()

#             driver.refresh()

#             # Open the URL and maximize the browser window
#             driver.get(config['url']['dgft_url'])
#             driver.maximize_window()
        
#         except(TimeoutException, WebDriverException, NoSuchElementException) as e:
#             raise Exception("Website not opened correctly") from e
        
#         # Skip to page particular page number if needed
#         start_page = 600
#         current_page = 1
        
#         print(f"Navigating to page {start_page}...")
        
#         # Method 1: Try using direct page input if available
#         try:
#             # Look for page input field
#             WebDriverWait(driver, 20).until(
#                 EC.presence_of_element_located((By.CLASS_NAME, "pagination_button"))
#             )
            
#             # Check if there's a direct page input or a JavaScript page switcher
#             # This looks for a page input element or a JavaScript function call
#             # Try clicking on the "..." ellipsis that often appears in pagination
#             ellipsis = driver.find_element(By.ID, "metaTable_ellipsis")
#             if ellipsis:
#                 ellipsis.click()
#                 time.sleep(1)
                
#                 # Look for page input after clicking ellipsis
#                 page_input = driver.find_element(By.CSS_SELECTOR, "input.paginate_input")
#                 if page_input:
#                     page_input.clear()
#                     page_input.send_keys(str(start_page))
#                     page_input.send_keys(Keys.ENTER)
#                     time.sleep(3)  # Wait for page to load
#                     current_page = start_page
#                     print(f"Successfully jumped to page {start_page}")
#         except Exception as e:
#             print(f"Could not directly navigate to page {start_page}: {e}")
            
#             # Method 2: If direct navigation fails, try using the "javascript:" link
#             try:
#                 # Looking at your screenshot, there's a JavaScript function in href
#                 # Click on a specific page number through JavaScript
#                 js_code = f"document.querySelector('a[href*=\"javascript:\"][aria-controls=\"metaTable\"]').click(); setTimeout(function() {{ $('#metaTable').DataTable().page({start_page-1}).draw(false); }}, 500);"
#                 driver.execute_script(js_code)
#                 time.sleep(3)
#                 current_page = start_page
#                 print(f"Successfully jumped to page {start_page} using JavaScript")
#             except Exception as e:
#                 print(f"Could not use JavaScript to navigate: {e}")
                
#                 # Method 3: If all else fails, go through pages one by one (but faster)
#                 print(f"Falling back to sequential navigation to page {start_page}...")
#                 while current_page < start_page:
#                     try:
#                         # Try to find the "Next" button
#                         next_button = driver.find_element(By.ID, "metaTable_next")
#                         if "disabled" in next_button.get_attribute("class"):
#                             print("Reached the last page before target page.")
#                             break
#                         next_button.click()
#                         current_page += 1
                        
#                         # Print progress every 50 pages
#                         if current_page % 50 == 0:
#                             print(f"Navigated to page {current_page}...")
                        
#                         # No need to wait as long between pages when just navigating
#                         time.sleep(0.5)
#                     except Exception as e:
#                         print(f"Error navigating sequentially: {e}")
#                         break
        
#         # Start the actual scraping from the current page
#         page = current_page
#         table_data = []
#         try:
#             while True:
#                 print(f"Scraping data from page {page}...")
#                 # Get the page source (HTML content) from the webpage
#                 page_source = driver.page_source

#                 # Parse the page content using BeautifulSoup to extract table data
#                 soup = BeautifulSoup(page_source, 'html.parser')
            
#                 # Extract the table from the webpage
#                 table = soup.find(config['xpaths']['table_tag'])
#                 headers = []  # List to store the table headers
#                 rows = table.find_all(config['xpaths']['tr_tag'])  # Find all rows in the table

#                 # Extract table headers (first row)
#                 header_row = rows[0]
#                 header_columns = header_row.find_all(config['xpaths']['th_tag'])
#                 for col in header_columns:
#                     headers.append(col.text.strip())  # Append cleaned header names
                
#                 # Extract data from each row of the table
#                 for row in rows[1:]:  # Skip the header row
#                     columns = row.find_all(config['xpaths']['td_tag'])
                    
#                     # Extract text from all columns except the last one
#                     row_data = [col.text.strip() for col in columns[:-1]]
                    
#                     # Extract href from the last column
#                     href = columns[-1].find("a")["href"] if columns[-1].find("a") else "No Link"
                    
#                     row_data.append(href)  # Append extracted href
#                     table_data.append(row_data)   
                
        
                
#                 # Save the extracted data to an Excel file
#                 df = pd.DataFrame(table_data, columns=headers)
                
                
#                 # Save to Excel
#                 # df.to_excel("second_part_new.xlsx", index=False)
#                 # print(f"Data successfully extracted and saved to second part xlsx.")
            
                
#                 if not click_next_page(driver):
#                     break  # Break the loop if no next page is found

#                 page += 1  # Increment the page number for the next iteration
#             return df 
#         except Exception as e:
#             print("An error occurred while saving the data:")
#             traceback.print_exc()
#     except Exception as e:
#         traceback.print_exc()
#         log_details.log_list[1] = "Failure"
#         if str(e) == "Website not opened correctly":
#             log_details.log_list[3] = "Website is not opened"
#         else:
#             log_details.log_list[3] = "Error in second part table data extraction part" 
        
#         log.insert_log_into_table(log_details.log_list)
#         print("error in data extraction part======", log_details.log_list)
#         log_details.log_list = [None] * 8

#         send_mail.send_email("ras sez extract data in website error", e)
#         exc_type, exc_obj, exc_tb = sys.exc_info()
#         print(f"Error occurred at line {exc_tb.tb_lineno}:")
#         print(f"Exception Type: {exc_type}")
#         print(f"Exception Object: {exc_obj}")
#         print(f"Traceback: {exc_tb}")
#         sys.exit("script error")
    
#     finally:
#         driver.quit()
       


# def extract_all_data_in_website():
#     try:

#         config = configparser.ConfigParser()
#         config.read('config.ini')
#         # Get dataframes from both functions

#         df1 = extarct_first_part_table()
#         df2 = extract_second_part_table()
      

#         # Combine dataframes if both are valid
#         if df1 is not None and df2 is not None:
#             combined_df = pd.concat([df1, df2], ignore_index=True)
            
#             # Remove any duplicate rows if they exist
#             combined_df = combined_df.drop_duplicates()
#             current_date = datetime.now().strftime("%Y-%m-%d")

#             first_excel_sheet_name = f"first_excel_sheet_{current_date}.xlsx"
            
#             # Build full path using os.path.join
#             first_exceL_sheet_path = os.path.join(config['file_paths']['first_excel_sheet_path'], first_excel_sheet_name)
                
#             combined_df.to_excel(first_exceL_sheet_path, index=False)

#             print(f"Combined data successfully saved to {first_exceL_sheet_path}")
#             check_increment_data.check_increment_data(first_exceL_sheet_path)
#         else:
#             print("Error: One or both dataframes are empty")
#             return None
#     except Exception as e:
#         traceback.print_exc()
#         print(f"Error occurred during data extraction: {e}")

