
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
chrome_options.add_argument('--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/99.0.4844.84 Safari/537.36')


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

time.sleep(2)

# print the current URL
print("Current URL:", driver.current_url)

# print the page source
print("Page Source:", driver.page_source[:200]) 


# Locate the 'Data' tab using its href attribute
data_tab = driver.find_element(By.XPATH, '//a[@href="#data"]')

# Wait for a short while for the page to load the data
time.sleep(15)

# Click the 'Data' tab to open it
data_tab.click()

# Wait for a short while for the page to load the data
time.sleep(5)

# Locate the CSV download buttons
csv_buttons = driver.find_elements(By.CSS_SELECTOR, '.dt-button.buttons-csv.buttons-html5')
print(f'CSV Buttons Found: {len(csv_buttons)}')

if len(csv_buttons) >= 2:
    # Click first button to start CSV download (assuming this is 'wastewater')
    csv_buttons[0].click()
    time.sleep(10)  # wait for the download to complete

    # Find the latest downloaded CSV file (should be 'wastewater')
    list_of_files = glob.glob(download_dir + '/*.csv')
    latest_file = max(list_of_files, key=os.path.getctime)
    
    # Rename the CSV file
    os.rename(latest_file, download_dir + '/fi_wastewater_data.csv')
    
    # Click second button to start CSV download (assuming this is 'estimated infections')
    csv_buttons[1].click()
    time.sleep(10)  # wait for the download to complete

    # Find the latest downloaded CSV file (should be 'estimated infections')
    list_of_files = glob.glob(download_dir + '/*.csv')
    latest_file = max(list_of_files, key=os.path.getctime)
    
    # Rename the CSV file
    os.rename(latest_file, download_dir + '/FinlandEstimCasesSewersheds.csv')
else:
    print('Error: Less than 2 CSV buttons found.')

# Close the driver
driver.quit()
