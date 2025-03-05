import os
import django
import time #as sleep_time
import csv
import subprocess
from datetime import datetime #, time
from seleniumwire import webdriver  # Importing Selenium Wire
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import Select
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from pytz import timezone
from .models import StockData  # Django model
import webbrowser
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger


# List of proxy IP addresses
PROXIES = [
   
    #   NEPAL
    "43.245.95.178:80",
    "182.93.85.225:80",
    "43.245.93.193:80",
    "103.232.228.138:80",
    "110.34.8.110:80",
    "110.34.13.4:80",
    "202.166.197.177:80",
    "182.50.65.145:80",
    "182.93.82.191:80",
    "182.50.65.169:80",
    "43.245.93.241:80",
    "103.98.131.194:80",
    "49.236.212.123:80",
    "103.1.93.184:80",
    "202.79.57.59:80",
    "103.117.92.18:80",
    "103.117.92.33:80",
    "103.180.240.191:80",
    "110.44.118.77:80",
    "202.166.207.176:80",
    "43.245.95.178:53805",
    "202.166.197.177:2121",
    "182.93.75.162:8080",
    "110.44.115.83:8080",
    "182.93.85.225:8080",
    "110.34.8.110:8080",
    "49.236.212.134:8888",
    "103.1.93.184:55443",
    "36.253.18.38:8181",
    "43.245.93.193:53805",
    "103.232.228.138:8080",
    "110.34.13.4:8080",
    "110.44.126.27:8080",
    "182.50.65.145:8080",
    "103.167.229.147:8080",
    "116.90.236.149:8080",
    "182.93.82.191:8080",
    "182.50.65.169:8080",
    "43.245.93.241:53805",
    "103.98.131.194:8081",
    "49.236.212.123:8080",
    "202.79.57.59:8080",
    "202.70.82.253:8080",
    "103.1.93.55:80",
    "103.117.92.18:8080",
    "103.117.92.33:8080",
    "103.1.93.55:32650",
    "103.180.240.44:1080",
    "202.51.68.205:80",
    "103.180.240.191:8080",
    "103.124.97.12:8080",
    "182.93.75.77:8080",
    "110.44.118.77:8080",
    "36.253.8.21:80",
    "202.166.207.176:52429",
    "103.254.185.195:53281",
    "103.153.232.41:8080",
    "182.93.80.242:8080",
    "103.1.94.210:1427",
    "103.153.233.12:8080",
    "202.70.82.253:80",
    "36.253.8.13:80",
    "202.70.82.253:5678",
    "103.104.233.78:8080",
    "110.34.1.180:32650",
    "202.166.220.143:55443",
    "202.52.1.7:8080",
    "110.34.3.74:8080",
    "103.104.233.78:80",
    "103.117.95.136:41890",
    "103.1.94.113:1427",
    "124.41.213.164:8080",
    "43.245.85.252:32650",
    "103.235.199.179:9812",
    "103.10.29.36:32650",
    "202.166.220.176:8080",
    "182.93.95.37:8087",
    "43.245.85.252:80",
    "182.93.65.140:8080",
    "103.41.173.97:32650",
    "103.153.233.20:8080",
    "124.41.213.225:8080",
    "139.5.73.71:8080",
    "110.34.1.180:80",
    "202.52.231.94:8080",
    "103.214.77.126:5000",
    "124.41.213.166:8080",
    "103.168.86.2:8080",
    "117.121.224.50:32650",
    "117.121.224.50:80",
    "49.236.212.20:32650",
    "182.93.65.239:8080",
    "103.1.93.46:32650",
    "103.1.93.46:80",
    "122.254.84.195:8080",
    "103.192.76.116:32650",
    "103.1.93.9:32650",
    "202.166.223.82:41890",
    "124.41.213.221:41890",
    "49.236.212.176:80",
    "43.245.94.229:4996",
    "202.52.234.218:8080",
    "49.236.212.11:80",
    "49.236.212.176:32650",
    "116.90.238.49:8085",
    "49.236.212.11:32650",
    "163.53.27.177:8080",
    "103.1.93.111:80",
    "103.1.93.42:80",
    "49.236.212.97:32650",
    "43.245.94.229:4995",
    "103.1.93.73:80",
    "103.153.232.241:8080",
    "103.10.29.35:80",
    "103.153.232.1:8080",
    "49.236.212.174:80",
    "49.236.212.101:80",
    "116.90.232.50:41890",
    "103.41.173.47:3128",
    "103.148.23.22:8080",
    "182.93.85.225:8080",
    "43.245.95.178:53805",
    "43.245.93.193:53805",
    "103.167.229.147:8080",
    "202.166.197.177:2121",
    "110.44.115.83:8080",
    "182.93.85.225:8080",
    "110.34.8.110:8080",
    "49.236.212.134:8888",
    "103.1.93.184:55443",
    "182.93.75.162:8080",
    "36.253.18.38:8181",
    "43.245.93.193:53805",
    "103.232.228.138:8080",
    "103.167.229.147:8080",
    "43.245.95.178:53805",
    "110.34.13.4:8080",
    "182.50.65.145:8080",
    "110.44.126.27:8080",
    "116.90.236.149:8080",
    "182.93.82.191:8080",
    "182.50.65.169:8080",
    "43.245.93.241:53805",
    "103.98.131.194:8081",
    "49.236.212.123:8080",
    "202.79.57.59:8080",
    "202.70.82.253:8080",
    "103.1.93.55:80",
    "103.117.92.18:8080",
    "103.117.92.33:8080",
    "103.1.93.55:32650",
    "202.51.68.205:80",
    "103.180.240.191:8080",
    "110.44.118.77:8080",
    "124.41.213.174:5678",
    "103.28.86.241:57230",
    "43.245.94.229:4996",
    "110.44.119.154:8111",
    "110.44.126.189:8111",
    "103.134.219.197:9050",
    "103.129.135.213:9050",
    "182.93.84.136:30052",
    "182.93.84.136:21206",
    "150.107.207.137:57230",
    "202.79.52.244:5678",
    "49.236.212.134:8888",
    "182.93.82.8:8080",
    "182.93.75.162:8080",
    "202.79.47.194:1080",
    "202.166.197.177:2121",
    "150.107.207.137:57230",
    "202.166.211.89:60606",
    "202.79.47.159:10800",
    "110.34.1.178:7777",
    "202.166.219.80:4153",
    "124.41.213.174:5678",
    "103.28.86.241:57230",
    "124.41.240.203:37704",
    "103.180.240.44:1080",
    "110.34.1.180:32650",
    "36.253.18.38:8181",
    "182.93.75.77:8080",
    "38.54.71.67:80",
    "103.75.148.9:4145",
    "103.75.149.97:4145",
    "103.1.93.184:55443",
    "202.166.203.23:39054",
    "202.70.67.93:80",
    "103.114.26.67:1080",
    "103.235.199.93:42033",
    "124.41.213.201:54806",
    "103.213.125.25:4153",
    "27.111.18.202:4153",
    "202.45.146.210:80",
    "103.28.86.241:57230",
    "116.90.229.186:35561",
    "103.75.149.105:4145",
    "124.41.211.211:43979",
    "43.245.94.226:4153",
    "103.28.86.241:61954",
    "124.41.213.201:39272",
    "202.70.84.201:4153",
    "124.41.211.196:52050",
    
    # NEPAL

 
    "193.239.86.249:3128",
    "159.8.114.37:80",
    "185.123.101.174:3128",
    "222.129.38.21:57114",
    "185.236.202.205:3128",
    "193.56.255.179:3128",
    "35.180.188.216:80",
    "106.45.221.168:3256",
    "113.121.240.114:3256",
    "193.34.95.110:8080",
    "84.17.51.235:3128",
    "180.183.97.16:8080",
    "193.239.86.247:3128",
    "185.189.112.157:3128",
    "121.206.205.75:4216",
    "103.114.53.2:8080",
    "139.180.140.254:1080",
    "84.17.51.241:3128",
    "84.17.51.240:3128",
    "185.189.112.133:3128", 
    "81.12.119.171:8080",
    "37.120.140.158:3128",
    "159.89.113.155:8080",
    "104.248.146.99:3128",
    "185.236.202.170:3128",
    "67.205.190.164:8080",
    "46.21.153.16:3128",
    "51.158.172.165:8811",
    "84.17.35.129:3128",
    "85.214.244.174:3128",
    "104.248.59.38:80",
    "12.156.45.155:3128",
    "161.202.226.194:8123",
    "167.172.109.12:41491",
    "167.172.109.12:39533",
    "115.221.242.131:9999",
    "125.87.82.86:3256",
    "159.8.114.37:8123",
    "183.164.254.8:4216",
    "169.57.157.146:8123",
    "94.100.18.111:3128",
    "18.141.177.23:80",
    "193.56.255.181:3128",
    "116.242.89.230:3128",
    "188.166.252.135:8080",
    "103.28.121.58:3128",
    "103.28.121.58:80",
    "119.84.215.127:3256",
    "217.172.122.14:8080",
    "79.122.230.20:8080",
    "167.172.109.12:46249",
    "176.113.73.102:3128",
    "88.99.10.252:1080",
    "167.172.109.12:37355",
    "193.239.86.248:3128",
    "113.195.224.222:9999",
    "112.98.218.73:57658",
    "15.207.196.77:3128",
    "223.113.89.138:1080",
    "36.7.252.165:3256",
    "113.100.209.184:3128",
    "185.38.111.1:8080",
    "121.40.51.48:1080",
    "121.205.213.141:4216",
    "37.200.66.166:9051",
    "69.163.161.209:38713",
    "159.65.69.186:9200",
    "121.205.215.44:4216",
    "27.153.141.90:4216",
    "218.6.105.152:4216",
    "140.237.14.92:4216",
    "66.135.227.181:4145",
    "207.97.174.134:1080",
    "121.206.205.75:4216",
    "154.72.204.78:8080",
    "106.52.2.26:1080",
    "115.231.175.80:3000",
    "47.115.42.157:8044",
    "113.128.33.60:53405",
    "106.52.187.222:1080",
    "60.13.42.157:1080",
    "222.129.36.115:57114",
    "222.129.38.21:57114",
    "222.129.37.77:57114",
    "117.68.147.8:3000",
    "184.185.2.45:4145",
    "123.56.89.191:1081",
    "165.22.43.8:30081",
    "195.93.173.58:9050",
    "172.104.4.144:9050",
    "183.164.226.253:4216",
    "222.129.32.173:57114",
    "219.147.112.150:1080",
    "218.64.122.99:7302",
    "222.129.35.9:57114",
    "188.165.233.121:9151",
    "98.162.96.41:4145",
    "51.195.201.48:9095",
    "116.202.103.223:29210",
    "119.187.146.163:1080",
    "43.224.10.43:6667",
    "43.224.8.116:6667",
    "98.190.102.40:4145",
    "208.113.222.205:57226",
    "175.24.2.65:1080",
    "220.248.188.75:17211",
    "66.135.227.178:4145",
    "103.109.57.42:3629",
    "98.190.102.62:4145",
    "124.115.21.11:1080",
    "208.113.152.12:32690",
    "114.96.218.231:3000",
    "54.215.46.91:20087",
    "188.120.245.247:12432",
    "188.227.224.110:9051",
    "98.126.23.24:2846",
    "101.132.36.83:3129",
    "188.166.104.152:49981",
    "138.68.73.161:1080",
    "114.99.200.41:3000",
    "172.104.240.74:9053",
    "43.224.10.13:6667",
    "192.111.135.21:4145",
    "150.129.52.74:6667",
    "222.129.36.157:57114",
    "124.65.117.38:7302",
    "208.113.155.120:41154",
    "188.166.34.137:9000",
    "135.181.203.208:80",
    "222.129.32.188:57114",
    "113.120.61.189:43644",
    "51.11.240.222:8085",
    "140.143.164.213:1080",
    "115.221.245.167:1080",
    "98.185.94.76:4145",
    "112.98.218.73:57658",
    "188.166.30.17:8888",
    "37.120.133.137:3128",
    "37.120.222.132:3128",
    "89.249.65.191:3128",
    "144.91.118.176:3128",
    "95.216.17.79:3888",
    "85.214.94.28:3128",
    "185.123.143.251:3128",
    "167.172.109.12:39452",
    "176.113.73.104:3128",
    "51.158.68.133:8811",
    "185.123.143.247:3128",
    "95.111.226.235:3128",
    "176.113.73.99:3128",
    "206.189.130.107:8080",
    "79.110.52.252:3128",
    "13.229.107.106:80",
    "118.99.108.4:8080",
    "13.229.47.109:80",
    "169.57.157.148:80",
    "51.158.68.68:8811",
    "167.172.109.12:40825",
    "119.81.189.194:80",
    "119.81.189.194:8123",
    "3.24.178.81:80",
    "119.81.71.27:80",
    "119.81.71.27:8123",
    "185.236.203.208:3128",

    "24.249.199.4:4145",#accepted
    "72.195.34.41:4145",#accepted
    "72.195.34.42:4145",#accepted
    "72.195.114.169:4145",#accepted
    "174.64.199.79:4145",#accepted
    "174.75.211.222:4145",#accepted
    "174.77.111.197:4145",#accepted
    "184.178.172.14:4145",#accepted
    "184.181.217.210:4145",#accepted
    "192.111.129.150:4145",#accepted
    "192.111.130.2:4145",#accepted
    "192.111.137.35:4145",#accepted
    "192.111.139.162:4145",#accepted
    "192.111.139.165:4145",#accepted
    "198.8.94.170:4145",#accepted
    "192.252.209.155:14455",#accepted
    "184.178.172.18:15280",#accepted
    "184.178.172.18:15280", #accepted
    "184.178.172.28:15294",#accepted
    "184.178.172.5:15303",#accepted
    "192.252.215.5:16137",#accepted
    "192.111.129.145:16894",#accepted
    "222.129.33.141:57114",#accepted
    "222.129.36.92:57114",#accepted

]

CHROMEDRIVER_PATH = r"c:\Users\Arjun\Desktop\project2\chromedriver.exe"  # Update this path


def create_driver():
    """Creates a Selenium Wire WebDriver instance with a proxy."""
    options = webdriver.ChromeOptions()
    seleniumwire_options = {
        
    }
    
    service = Service(executable_path=CHROMEDRIVER_PATH)  # Use the custom path
    driver = webdriver.Chrome(service=service, options=options, seleniumwire_options=seleniumwire_options)
    return driver

def change_proxy(driver, proxy):
    driver.proxy= {
            "http": f"socks5://{proxy}",
            "https": f"socks5://{proxy}",
        }
    
    #configuration of Proxy IP log in CSV file
def initialize_csv(filename="proxy_results.csv"):
    """Creates the CSV file with headers and a blank line only once."""
    if not os.path.exists(filename):  # Check if file exists
        with open(filename, mode='w', newline='', encoding="utf-8") as file:
            writer = csv.writer(file, delimiter='\t')
            writer.writerow(["𝐓𝐢𝐦𝐞𝐬𝐭𝐚𝐦𝐩".ljust(25), "𝐏𝐫𝐨𝐱𝐲".ljust(25), "𝐒𝐭𝐚𝐭𝐮𝐬".ljust(15)])  # Headers with padding
            writer.writerow([])  # Add a blank line for spacing

def log_results_to_csv(results, filename="proxy_results.csv"):
    """Logs results to a CSV file."""
    with open(filename, mode='a', newline='', encoding="utf-8") as file:
        writer = csv.writer(file, delimiter='\t')
        writer.writerow([results[0].ljust(25), results[1].ljust(25), results[2].ljust(15)])  # Add padding



def scrape_nepse_data():
    """Scrapes stock price data from NEPSE using rotating proxies within a time window."""
    
    while True:
        current_time = datetime.now().time()
        start_time = datetime.strptime("10:00:00", "%H:%M:%S").time()
        end_time = datetime.strptime("11:05:00", "%H:%M:%S").time()

        if start_time <= current_time <= end_time:
            print("Starting NEPSE scraping within the allowed time window...")
            scrape_loop()  # Calls the function that handles the proxy-based scraping
        else:
            print("Outside scraping time. Waiting until 10 AM...")
            time.sleep(1200)  # Check again in 20 minutes
def scrape_loop():
    """Runs the actual scraping function in a loop, stopping after given_time."""
    
    end_time = datetime.strptime("11:05:00", "%H:%M:%S").time()

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
                driver.get("https://www.nepalstock.com/today-price")
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
                    EC.presence_of_element_located((By.XPATH, "//div[contains(@class, 'table__asofdate')]/span"))
                )
                last_updated_time = updated_time_element.text.strip().replace("As of ", "")
                last_updated_datetime = datetime.strptime(last_updated_time, '%b %d, %Y, %I:%M:%S %p')

                # Select 500 rows
                select_element = WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((By.TAG_NAME, "select"))
                )
                Select(select_element).select_by_value("500")

                # Click the filter button
                filter_button = WebDriverWait(driver, 10).until(
                    EC.element_to_be_clickable((By.CLASS_NAME, "box__filter--search"))
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
                    EC.presence_of_element_located((By.TAG_NAME, "table"))
                )


                # Save data to the database
                for tr in table.find_elements(By.TAG_NAME, "tbody")[0].find_elements(By.TAG_NAME, "tr"):
                    if datetime.now().time() > end_time:
                        print("Time exceeded during database save. Exiting...")
                        driver.quit()
                        return  # Stop function immediately
                    
                    data = [td.text.strip() for td in tr.find_elements(By.TAG_NAME, "td")]

                    if len(data) >= 15:
                        stock_entry = StockData(
                            serial_number=data[0],
                            symbol=data[1],
                            close_price=data[2],
                            open_price=data[3],
                            high_price=data[4],
                            low_price=data[5],
                            total_traded_quantity=data[6],
                            total_traded_value=data[7],
                            total_trades=data[8],
                            last_traded_price=data[9],
                            previous_close_price=data[10],
                            average_traded_price=data[11],
                            week_52_high=data[12],
                            week_52_low=data[13],
                            market_capitalization=data[14],
                            timestamp=last_updated_datetime
                        )
                        stock_entry.save()

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
