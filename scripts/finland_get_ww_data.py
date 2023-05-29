
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
import time
import os
import glob

# Setup webdriver
webdriver_service = Service(ChromeDriverManager().install())
chrome_options = Options()
chrome_options.add_argument('--headless') 

download_dir = os.path.join(os.environ['GITHUB_WORKSPACE'], 'data', 'Finland') 

# Ensure the subdirectory exists
os.makedirs(download_dir, exist_ok=True)

# Set up Chrome to automatically download files to the specified directory
chrome_options.add_experimental_option('prefs', {
    'download.default_directory': download_dir,
    'download.prompt_for_download': False,
})
driver = webdriver.Chrome(service=webdriver_service, options=chrome_options)


# Open the webpage
driver.get('https://www.thl.fi/episeuranta/jatevesi/wastewater_weekly_report.html')  

# Locate the 'Data' tab using its href attribute
data_tab = driver.find_element(By.XPATH, '//a[@href="#data"]')

# Wait for a short while for the page to load the data
time.sleep(15)

# Click the 'Data' tab to open it
data_tab.click()

# Wait for a short while for the page to load the data
time.sleep(5)

# Locate the CSV download button
csv_button = driver.find_element(By.CSS_SELECTOR, '.dt-button.buttons-csv.buttons-html5')

# Click the button to start CSV download
csv_button.click()

time.sleep(5)

# Find the latest downloaded CSV file
list_of_files = glob.glob(download_dir + '/*.csv')
latest_file = max(list_of_files, key=os.path.getctime)

# Rename the CSV file
os.rename(latest_file, os.path.join(download_dir, 'fi_wastewater_data.csv'))


# Close the driver
driver.quit()
