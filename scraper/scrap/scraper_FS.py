import decimal
import os
import time
import csv
from datetime import datetime
from seleniumwire import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import Select
from selenium.webdriver.chrome.service import Service
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
    driver.proxy = {
        "http": f"socks5://{proxy}",
        "https": f"socks5://{proxy}",
    }

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
        value = value.replace(',', '').strip()
        return decimal.Decimal(value)
    except (decimal.InvalidOperation, ValueError) as e:
        print(f"Error converting value '{value}' to Decimal: {e}")
        return decimal.Decimal(0)  # Return a default value (0) in case of error

def scrape_nepse_FS():
    """Scrapes stock price data from NEPSE using rotating proxies within a time window."""
    while True:
        current_time = datetime.now().time()
        start_time = datetime.strptime("15:00:00", "%H:%M:%S").time()
        end_time = datetime.strptime("23:30:00", "%H:%M:%S").time()

        if start_time <= current_time <= end_time:
            print("Starting NEPSE scraping within the allowed time window...")
            scrape_loop()  # Calls the function that handles the proxy-based scraping
        else:
            print("Outside scraping time. Waiting until 11 AM...")
            time.sleep(1200)  # Check again in 20 minutes

def scrape_loop():
    """Runs the actual scraping function in a loop, stopping after given_time."""
    end_time = datetime.strptime("23:30:00", "%H:%M:%S").time()
    total_pages_saved = 0  # Counter to track the number of pages saved

    for proxy in PROXIES:
        driver = create_driver()
        change_proxy(driver, proxy)
        print(f"Using Proxy: {proxy}")
        status = "Error: Initialization"  # Default status

        # Initialize page tracking
        current_page = 1  # Start from the first page
        retry_attempts = 10  # Number of retry attempts for the same page

        while True:
            current_time = datetime.now().time()
            if current_time > end_time:
                print(f"Stopping NEPSE scraping. Total pages saved: {total_pages_saved}. Exiting...")
                driver.quit()
                return  # Exit the loop after given_time

            try:
                driver.get("https://www.nepalstock.com/floor-sheet")
                print("Waiting for page to load...")

                for _ in range(10):
                    if datetime.now().time() > end_time:
                        print("Time exceeded during page load. Exiting...")
                        driver.quit()
                        return
                    time.sleep(1)

                # Select 500 rows
                select_element = WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "select.ng-pristine"))
                )
                Select(select_element).select_by_value("500")

                # Click the filter button
                filter_button = WebDriverWait(driver, 20).until(
                    EC.element_to_be_clickable((By.CSS_SELECTOR, "button.box__filter--search"))
                )
                filter_button.click()

                print("Waiting for table data to load...")

                for _ in range(5):
                    if datetime.now().time() > end_time:
                        print("Time exceeded during data load. Exiting...")
                        driver.quit()
                        return
                    time.sleep(1)

                # Navigate to the current page
                if current_page > 1:
                    for _ in range(current_page - 1):
                        try:
                            next_button = WebDriverWait(driver, 5).until(
                                EC.element_to_be_clickable((By.CSS_SELECTOR, "li.pagination-next a"))
                            )
                            driver.execute_script("arguments[0].click();", next_button)
                            print(f"Navigating to page {current_page}...")
                            time.sleep(2)
                        except:
                            print(f"Failed to navigate to page {current_page}. Exiting...")
                            driver.quit()
                            return

                # Scrape the current page
                while True:
                    table = WebDriverWait(driver, 30).until(
                        EC.presence_of_element_located((By.CSS_SELECTOR, "table.table-striped tbody"))
                    )

                    for tr in table.find_elements(By.TAG_NAME, "tr"):
                        if datetime.now().time() > end_time:
                            print("Time exceeded during database save. Exiting...")
                            driver.quit()
                            return

                        columns = [td.text.strip() for td in tr.find_elements(By.TAG_NAME, "td")]
                        quantity = int(columns[5].replace(',', '').strip())

                        if len(columns) >= 8:
                            entry = StockTransaction(
                                SN=columns[0],
                                contract_no=columns[1],
                                stock_symbol=columns[2],
                                buyer=columns[3],
                                seller=columns[4],
                                quantity=quantity,
                                rate=safe_decimal(columns[6]),
                                amount=safe_decimal(columns[7]),
                            )
                            entry.save()

                    total_pages_saved += 1  # Increment page count
                    print(f"Stock data saved successfully. Total pages saved: {total_pages_saved}")
                    status = "Success✅"

                    # Click the "Next" button if available
                    try:
                        next_button = WebDriverWait(driver, 5).until(
                            EC.element_to_be_clickable((By.CSS_SELECTOR, "li.pagination-next a"))
                        )
                        driver.execute_script("arguments[0].click();", next_button)
                        print("Moving to the next page...")
                        current_page += 1  # Update the current page
                        time.sleep(2)
                    except:
                        print("No more pages to scrape. Exiting pagination loop.")
                        break

            except Exception as e:
                print(f"Error with proxy {proxy} on page {current_page}: {e}. Retrying the same page...")
                status = f"Error: {e}❌"
                retry_attempts -= 1
                if retry_attempts > 0:
                    print(f"Retrying page {current_page}... Attempts left: {retry_attempts}")
                    continue  # Retry the same page
                else:
                    print(f"Max retry attempts reached for page {current_page}. Moving to the next proxy...")
                    break  # Move to the next proxy

            finally:
                initialize_csv()
                timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                log_results_to_csv([timestamp, proxy, status])

            if datetime.now().time() > end_time:
                print(f"Time exceeded after proxy loop. Total pages saved: {total_pages_saved}. Exiting...")
                driver.quit()
                return

        driver.quit()
        print(f"Proxy {proxy} completed. Total pages saved: {total_pages_saved}. Moving to the next proxy...")

scrape_nepse_FS()