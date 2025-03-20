import decimal
import os
import time
import csv
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import Select
from selenium.webdriver.chrome.service import Service

# Path to the ChromeDriver executable
CHROMEDRIVER_PATH = r"c:\Users\Arjun\Desktop\project2\chromedriver.exe"

def create_driver():
    """Creates a Selenium WebDriver instance."""
    options = webdriver.ChromeOptions()
    service = Service(executable_path=CHROMEDRIVER_PATH)  # Use the custom path
    driver = webdriver.Chrome(service=service, options=options)
    return driver

def initialize_csv(filename="floor_sheet_data.csv"):
    """Creates the CSV file with headers if it doesn't exist."""
    if not os.path.exists(filename):  # Check if file exists
        with open(filename, mode='w', newline='', encoding="utf-8") as file:
            writer = csv.writer(file, delimiter='\t')
            writer.writerow(["SN", "Contract No", "Stock Symbol", "Buyer", "Seller", "Quantity", "Rate", "Amount"])
            writer.writerow([])  # Add a blank line for spacing

def log_results_to_csv(data, filename="floor_sheet_data.csv"):
    """Logs scraped data to a CSV file."""
    with open(filename, mode='a', newline='', encoding="utf-8") as file:
        writer = csv.writer(file, delimiter='\t')
        writer.writerow(data)

def safe_decimal(value):
    """Converts a string to a Decimal safely."""
    try:
        value = value.replace(',', '').strip()
        return decimal.Decimal(value)
    except (decimal.InvalidOperation, ValueError) as e:
        print(f"Error converting value '{value}' to Decimal: {e}")
        return decimal.Decimal(0)  # Return a default value (0) in case of error

def scrape_page(driver):
    """Scrapes data from the current page of the floor sheet."""
    try:
        # Wait for the table to load
        table = WebDriverWait(driver, 30).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "table.table-striped tbody")))
    except Exception as e:
        print(f"Error locating table: {e}")
        return []

    rows_data = []
    for tr in table.find_elements(By.TAG_NAME, "tr"):
        columns = [td.text.strip() for td in tr.find_elements(By.TAG_NAME, "td")]
        if len(columns) >= 8:
            # Extract and process data
            sn = columns[0]
            contract_no = columns[1]
            stock_symbol = columns[2]
            buyer = columns[3]
            seller = columns[4]
            quantity = int(columns[5].replace(',', '').strip())
            rate = safe_decimal(columns[6])
            amount = safe_decimal(columns[7])

            # Append the row data
            rows_data.append([sn, contract_no, stock_symbol, buyer, seller, quantity, rate, amount])
    return rows_data

def scrape_nepse_floor_sheet():
    """Main function to scrape the NEPSE floor sheet."""
    start_time = datetime.strptime("11:00:00", "%H:%M:%S").time()
    end_time = datetime.strptime("15:20:00", "%H:%M:%S").time()

    # Set to track unique rows based on multiple fields
    unique_rows = set()

    # Initialize CSV file
    initialize_csv()

    # Create the driver once and reuse it
    driver = create_driver()

    try:
        while True:
            current_time = datetime.now().time()

            if start_time <= current_time <= end_time:
                print("Starting NEPSE scraping within the allowed time window...")
                driver.get("https://www.nepalstock.com/floor-sheet")
                time.sleep(5)  # Allow the page to load

                # Select 500 rows only once
                select_element = WebDriverWait(driver, 20).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "select.ng-pristine"))
                )
                Select(select_element).select_by_value("500")

                # Click the filter button only once
                filter_button = WebDriverWait(driver, 20).until(
                    EC.element_to_be_clickable((By.CSS_SELECTOR, "button.box__filter--search"))
                )
                filter_button.click()
                time.sleep(5)  # Allow data to load

                # Scrape Page 1
                print("Scraping Page 1...")
                page1_data = scrape_page(driver)
                new_records_count = 0  # Counter for new records

                for row in page1_data:
                    # Create a unique key for the row
                    row_key = tuple(row[1:])  # Exclude SN (first column) for uniqueness check

                    if row_key not in unique_rows:  # Check for duplicates
                        log_results_to_csv(row)
                        unique_rows.add(row_key)  # Add to unique rows set
                        new_records_count += 1  # Increment the counter

                if new_records_count > 0:
                    print(f"Saved {new_records_count} new records from Page 1.")
                else:
                    print("Scraping Page 1 but saving no new records.")

                # Wait for 5 seconds before the next scrape
                print("Waiting for 5 seconds before the next scrape...")
                time.sleep(5)

            else:
                # Calculate the time difference to dynamically wait
                now = datetime.now()
                next_start = datetime.combine(now.date(), start_time)
                
                if current_time > end_time:
                    # If current time is past end time, wait until the next day's start time
                    next_start = next_start.replace(day=now.day + 1)

                wait_seconds = (next_start - now).total_seconds()
                print(f"Outside scraping time. Waiting for {wait_seconds // 3600:.0f} hours and {wait_seconds % 3600 // 60:.0f} minutes until next start time...")
                time.sleep(wait_seconds)  # Dynamically wait until start time

    except Exception as e:
        print(f"Error during scraping: {e}")
    finally:
        # Quit the driver only after all scraping is done
        driver.quit()

if __name__ == "__main__":
    scrape_nepse_floor_sheet()
