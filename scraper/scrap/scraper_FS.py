import decimal
import os
import django
import time #as sleep_time
import csv
from datetime import datetime #, time
from seleniumwire import webdriver  # Importing Selenium Wire
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import Select
from selenium.webdriver.chrome.service import Service
from pytz import timezone
from .models import StockTransaction  # Django model
from scraper.proxies import PROXIES


CHROMEDRIVER_PATH = r"c:\Users\Arjun\Desktop\project2\chromedriver.exe"  # Update this path

def create_driver():
    """Creates a Selenium Wire WebDriver instance with a proxy."""
    options = webdriver.ChromeOptions()
    seleniumwire_options = {}
    service = Service(executable_path=CHROMEDRIVER_PATH)  # Use the custom path
    driver = webdriver.Chrome(service=service, options=options, seleniumwire_options=seleniumwire_options)
    return driver

def change_proxy(driver, proxy):
    driver.proxy= {
            "http": f"socks5://{proxy}",
            "https": f"socks5://{proxy}",
        }
    
    #configuration of Proxy IP log in CSV file
def initialize_csv(filename="proxy1_results.csv"):
    """Creates the CSV file with headers and a blank line only once."""
    if not os.path.exists(filename):  # Check if file exists
        with open(filename, mode='w', newline='', encoding="utf-8") as file:
            writer = csv.writer(file, delimiter='\t')
            writer.writerow(["𝐓𝐢𝐦𝐞𝐬𝐭𝐚𝐦𝐩".ljust(25), "𝐏𝐫𝐨𝐱𝐲".ljust(25), "𝐒𝐭𝐚𝐭𝐮𝐬".ljust(15)])  # Headers with padding
            writer.writerow([])  # Add a blank line for spacing

def log_results_to_csv(results, filename="proxy1_results.csv"):
    """Logs results to a CSV file."""
    with open(filename, mode='a', newline='', encoding="utf-8") as file:
        writer = csv.writer(file, delimiter='\t')
        writer.writerow([results[0].ljust(25), results[1].ljust(25), results[2].ljust(15)])  # Add padding

def safe_decimal(value):
    try:
        # Remove commas, spaces, and any other non-numeric characters if necessary
        value = value.replace(',', '').strip()
        return decimal.Decimal(value)
    except (decimal.InvalidOperation, ValueError) as e:
        print(f"Error converting value '{value}' to Decimal: {e}")
        return decimal.Decimal(0)  # Return a default value (0) in case of error



def scrape_nepse_FS():
    """Scrapes stock price data from NEPSE using rotating proxies within a time window."""
    
    while True:
        current_time = datetime.now().time()
        start_time = datetime.strptime("11:00:00", "%H:%M:%S").time()
        end_time = datetime.strptime("17:00:00", "%H:%M:%S").time()

        if start_time <= current_time <= end_time:
            print("Starting NEPSE scraping within the allowed time window...")
            scrape_loop()  # Calls the function that handles the proxy-based scraping
        else:
            print("Outside scraping time. Waiting until 11 AM...")
            time.sleep(1200)  # Check again in 20 minutes
def scrape_loop():
    """Runs the actual scraping function in a loop, stopping after given_time."""
    
    end_time = datetime.strptime("17:00:00", "%H:%M:%S").time()

    while True:
        current_time = datetime.now().time()
        if current_time > end_time:
            print("Stopping NEPSE scraping. Exiting...")
            break  # Exit the loop after given_time 

        driver = create_driver()
        
        for proxy in PROXIES:
            change_proxy(driver, proxy)
            print(f"Using Proxy: {proxy}")
            status = "Error: Initialization"  # Default status

            try:
                driver.get("https://www.nepalstock.com/floor-sheet")
                print("Waiting for page to load...")

                # Continuously check time while waiting
                for _ in range(10):
                    if datetime.now().time() > end_time:
                        print("Time exceeded during page load. Exiting...")
                        driver.quit()
                        return  # Stop function immediately
                    time.sleep(1)

              # Extract last updated time
                updated_time_element = WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "div.table__asofdate span"))
                )

                # If updated_time_element is already a string, just strip and process it
                timestamp_str = updated_time_element.text.strip().replace('As of ', '')  # No need for .text

                # Now parse the datetime string correctly
                last_updated_datetime = datetime.strptime(timestamp_str, '%b %d, %Y, %I:%M:%S %p')

                # Select 500 rows
                select_element = WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "select.ng-pristine"))
                )
                Select(select_element).select_by_value("500")

                # Click the filter button
                filter_button = WebDriverWait(driver, 10).until(
                    EC.element_to_be_clickable((By.CSS_SELECTOR, "button.box__filter--search"))
                )
                filter_button.click()

                print("Waiting for table data to load...")
                
                # Continuously check time while waiting
                for _ in range(5):
                    if datetime.now().time() > end_time:
                        print("Time exceeded during data load. Exiting...")
                        driver.quit()
                        return  # Stop function immediately
                    time.sleep(1)

                # Extract table data
                table = WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "table.table-striped tbody"))
                )


                # Save data to the database
                for tr in table.find_elements(By.TAG_NAME, "tr"):
                    if datetime.now().time() > end_time:
                        print("Time exceeded during database save. Exiting...")
                        driver.quit()
                        return  # Stop function immediately
                    
                    columns = [td.text.strip() for td in tr.find_elements(By.TAG_NAME, "td")]
                    quantity = int(columns[5].replace(',', '').strip())

                    if len(columns) >= 8:
                      entry = StockTransaction(
                        SN=columns[0],
                        contract_no=columns[1],  # Corrected field name
                        stock_symbol=columns[2],  # Corrected field name
                        buyer=columns[3],  # Corrected field name
                        seller=columns[4],  # Corrected field name
                        quantity=quantity,  # Corrected field type
                        rate=safe_decimal(columns[6]),  # Use safe_decimal to convert rate
                        amount=safe_decimal(columns[7]),  # Use safe_decimal to convert amount
                        timestamp=last_updated_datetime  # Corrected field name
                    )
                    entry.save()

                print("Stock data saved successfully.")
                status = "Success✅"

            except Exception as e:
                print(f"Error with proxy {proxy}: {str(e)}. Trying next proxy...")
                status = f"Error: Error❌"

            finally:
                initialize_csv()
                timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                log_results_to_csv([timestamp, proxy, status])  # Log timestamp, proxy, and status

            if datetime.now().time() > end_time:
                print("Time exceeded after proxy loop. Exiting...")
                driver.quit()
                return  # Stop function immediately
        
        driver.quit()
       
        print("For Loop completed. Waiting 1 minute before restarting...")
        # Check time before sleeping
        for _ in range(60):  # 1 minute = 60 seconds
            if datetime.now().time() > end_time:
                print("Time exceeded during sleep. Exiting...")
                return  # Stop function immediately
            time.sleep(1)
