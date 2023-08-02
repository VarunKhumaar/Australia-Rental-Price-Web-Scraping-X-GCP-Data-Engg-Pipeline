from selenium import webdriver
import pandas as pd
from selenium.webdriver.common.keys import Keys
import time
from selenium.webdriver.chrome.options import Options
from Domain_links import links
from google.cloud import storage
import os

# Configure Chrome options for headless browsing
chrome_options = Options()
chrome_options.add_argument("--headless")  # Ensure GUI is off
chrome_options.add_argument("--no-sandbox")
chrome_options.add_argument("--disable-dev-shm-usage")

# Set path to chromedriver as per your configuration
webdriver_path = 'C:/Users/varun/Downloads/Resume/Projects/Domain/chromedriver.exe'

#page = 1
property_list = []


#### EXTRACT ####
for i in links:
    print(i)
    page = 1
    while page <= 3:
        url = i + str(page)
        print(url)
        browser = webdriver.Chrome(executable_path=webdriver_path, options=chrome_options)
        browser.get(url)

        if len(browser.find_elements_by_class_name('css-hgk76f'))==0 and len(browser.find_elements_by_class_name('css-rxp4mi'))==0:
            browser.quit()
            print("Page is empty")
            page=51

        else:
            properties = browser.find_elements_by_class_name('css-hgk76f')
            print(len(properties))
            for p in properties:
                address = p.find_element_by_xpath('.//*[@data-testid="address-wrapper"]').text
                price = p.find_element_by_xpath('.//*[@data-testid="listing-card-price"]').text
                try:    features = p.find_element_by_xpath('.//*[@data-testid="property-features-wrapper"]').text
                except: features = 0
                type = p.find_element_by_class_name('css-693528').text
                p_item = {"Type": type, "Features": features, "Price": price, 'Address':address, 'Url':url}
                property_list.append(p_item)
            
            properties = browser.find_elements_by_class_name('css-rxp4mi')
            print(len(properties))
            for p in properties:
                address = p.find_element_by_xpath('.//*[@data-testid="address-wrapper"]').text
                price = p.find_element_by_xpath('.//*[@data-testid="listing-card-price"]').text
                try:    features = p.find_element_by_xpath('.//*[@data-testid="property-features-wrapper"]').text
                except: features = 0
                type = p.find_element_by_class_name('css-693528').text
                p_item = {"Type": type, "Features": features, "Price": price, 'Address':address, 'Url':url}
                property_list.append(p_item)

            page+=1
            # Close the browser
            browser.quit()
    
    # Uncomment the below code if code stops due to memory issues (takes 4-6 hours to run - as its scraping data from 2500 pages, LOL)
    # The below code will save the data as CSVs in each loop. Later, can edit the 'links' file to resume where it stopped

    #mt = pd.DataFrame(property_list)
    #filename = ''.join(e for e in i if e.isalnum()) + ".csv"
    #path = 'C:/Users/varun/Downloads/Resume/Projects/Domain/Outputs/'
    #mt.to_csv(path+filename, index=False)

print("Loop ended") 
#####


#### TRANSFORM ####
df = pd.DataFrame(property_list)
df = df.drop_duplicates(keep='first')
df.to_excel('output1.xlsx', index=False)

df['Features'] = df['Features'].str.replace('\n', ' ').str.replace('−', '')
df['Beds'] = df['Features'].str.extract(r'(.{2})Bed').fillna(0)
df['Bath'] = df['Features'].str.extract(r'(.{2})Bath').fillna(0)
df['Parking'] = df['Features'].str.extract(r'(.{2})Park').fillna(0)
df.loc[(df['Beds'] == 0) & (df['Type'].isin(['Studio', 'Apartment / Unit / Flat'])), 'Beds'] = 1

df['Rent per Week'] = df['Price'].str.replace(',', '').str.replace('per', '').str.replace('week', '').str.replace(' ', '').str.replace('$', 'de').str.extract(r"(?<=de)(\d+)")

address_data = pd.DataFrame(df['Address'].str[:-5].str.split(', \n').str[-1])
address_data['Address'] = address_data['Address'].str.rsplit(' ', n=1).str.join('$')
df['Suburb'] = address_data['Address'].str.split('$').str[0]
df['State'] = address_data['Address'].str.split('$').str[-1]
df['Post code'] = df['Address'].str[-4:]

df.dropna(inplace=True)


#### LOAD ####

# Loading the transformed data to GCP
# https://www.youtube.com/watch?v=7ACwDV-lQ1I (can use this for reference)
df.to_csv('Domain_data.csv', index=False)
os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="C:/Users/varun/Downloads/Resume/Projects/Domain/data-engg-391707-9149d6f1fd72.json"
from google.cloud import storage

# Specify a bucket name and other details
bucket_name = 'domain_data_engg'
source_file_path = 'C:/Users/varun/Downloads/Resume/Projects/Domain//Domain_data.csv'
destination_blob_path = 'Domain.csv'

# Define a special function
def upload_to_storage(bucket_name: str, source_file_path: str, destination_blob_path: str):
  """Uploads a file to the bucket."""
  storage_client = storage.Client()
  bucket = storage_client.get_bucket(bucket_name)
  blob = bucket.blob(destination_blob_path)
  blob.upload_from_filename(source_file_path)
  print(f'The file {source_file_path} is uploaded to GCP bucket path: {destination_blob_path}')
  return None

# Run the function!
upload_to_storage(bucket_name, source_file_path, destination_blob_path)



# Use the code in main.py and requirements.txt to create a Google Cloud Function 
# As soon as the Domain.csv is loaded in GCP cloud storage bucket, the cloud function is triggered to load the csv to Big query
# Finally the Big query table can be connected to a BI tool like Tableau/Power BI