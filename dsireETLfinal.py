import pandas as pd
import requests
import zipfile
import io
import datetime
import os
import shutil
import time # time module for delays
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.chrome.options import Options as ChromeOptions
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, WebDriverException

DSIRE_ARCHIVE_PAGE_URL = "https://www.dsireusa.org/resources/database-archives/"

TEMP_DATA_DIR = "C:\\Users\\kiran\\OneDrive\\Documents\\dsirelocal\\temp_dsire_downloads"
os.makedirs(TEMP_DATA_DIR, exist_ok=True)

FIPS_LOOKUP_FILE = "C:\\Users\\kiran\\OneDrive\\Documents\\dsirelocal\\appalachian_county_fips_lookup.csv"

OUTPUT_FILE_PATH = "C:\\Users\\kiran\\OneDrive\\Documents\\dsirelocal\\Monthly_DSIRE_Appalachian_Cleaned.xlsx"
OUTPUT_SHEET_NAME = "Appalachian_Master_DB"

CSVS_TO_LOAD = [
    "program.csv",
    "state_info_content.csv",
    "contact.csv", 
]

CHROMEDRIVER_PATH = "C:\\Users\\kiran\\OneDrive\\Documents\\dsirelocal\\chromedriver.exe"

def get_latest_dsire_zip_url(archive_page_url, driver_path):
    print(f"Searching for latest DSIRE ZIP on: {archive_page_url} using Selenium...")
    
    options = ChromeOptions()
    options.add_argument("--headless")
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36")

    service = ChromeService(executable_path=driver_path)
    
    driver = None
    try:
        driver = webdriver.Chrome(service=service, options=options)
        driver.get(archive_page_url)

        wait = WebDriverWait(driver, 20)
        wait.until(EC.presence_of_element_located((By.XPATH, "//a[contains(@href, 'ncsolarcen-prod.s3.amazonaws.com/fullexports/dsire-')]")))
        
        page_source = driver.page_source
        soup = BeautifulSoup(page_source, 'html.parser')

        all_links = soup.find_all('a', href=True)

        latest_zip_url = None
        latest_month_year = None

        print("--- Debug: Found potential ZIP links (from Selenium-rendered page) ---")
        for link in all_links:
            href = link.get('href')
            if href and "ncsolarcen-prod.s3.amazonaws.com/fullexports/dsire-" in href and href.endswith('.zip'):
                print(f"DEBUG: Found S3 ZIP candidate: {href}")

                try:
                    file_name = href.split('/')[-1] 
                    month_year_part = file_name[6:13]
                    
                    current_link_date = datetime.datetime.strptime(month_year_part, "%Y-%m").date()
                    
                    if latest_month_year is None or current_link_date > latest_month_year:
                        latest_month_year = current_link_date
                        latest_zip_url = href
                        print(f"DEBUG: New latest ZIP URL candidate based on date: {latest_zip_url}")

                except ValueError: 
                    continue 
                except Exception as e:
                    continue 

        if latest_zip_url:
            print(f"Found latest DSIRE ZIP URL: {latest_zip_url}")
            return latest_zip_url
        else:
            print("Warning: Could not find any suitable DSIRE ZIP link with the expected S3 URL and filename pattern after Selenium render.")
            print("Please check the DSIRE archive page manually for changes to link patterns if this persists.")
            return None

    except TimeoutException:
        print("Error: Selenium timed out waiting for the download links to appear. Page content might be taking too long to load.")
        return None
    except WebDriverException as e:
        print(f"Error with WebDriver (Selenium): {e}")
        print("Please ensure chromedriver.exe is in the specified path and matches your Chrome browser version.")
        return None
    except Exception as e:
        print(f"An unexpected error occurred during URL scraping (Selenium): {e}")
        return None
    finally:
        if driver:
            driver.quit()

def download_and_extract_dsire_zip(zip_url, temp_dir, csv_list):
    if not zip_url:
        print("No ZIP URL provided for download.")
        return None

    print(f"Downloading DSIRE ZIP from: {zip_url}")
    try:
        response = requests.get(zip_url, stream=True)
        response.raise_for_status()

        zip_file_name = zip_url.split('/')[-1]
        zip_file_path = os.path.join(temp_dir, zip_file_name)
        with open(zip_file_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        print(f"Downloaded '{zip_file_path}' successfully.")

        extracted_dfs = {}
        with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
            for csv_name in csv_list:
                try:
                    with zip_ref.open(csv_name) as f:
                        extracted_dfs[os.path.splitext(csv_name)[0]] = pd.read_csv(io.BytesIO(f.read()), encoding='utf-8', low_memory=False)
                        print(f"Extracted and loaded '{csv_name}'.")
                except KeyError:
                    print(f"Warning: '{csv_name}' not found in the ZIP file. Skipping.")
                except Exception as e:
                    print(f"Error loading '{csv_name}' from zip: {e}")
        return extracted_dfs

    except requests.exceptions.RequestException as e:
        print(f"Error downloading ZIP file: {e}")
        print("Please check the URL and your internet connection.")
        return None
    except zipfile.BadZipFile:
        print("Error: Downloaded file is not a valid ZIP archive.")
        return None
    except Exception as e:
        print(f"An unexpected error occurred during download/extraction: {e}")
        return None

def load_appalachian_fips_lookup(file_path):
    print(f"Loaded Appalachian FIPS lookup from: {file_path}")
    try:
        lookup_df = pd.read_csv(file_path, encoding='utf-8')
        
        lookup_df.rename(columns={
            'COUNTY': 'County',
            'State ID': 'State ID',
            'STATE': 'State',
            'FIPS': 'FIPS',
            'Is_Appalachian': 'Is_Appalachian'
        }, inplace=True, errors='ignore')

        for col in ['County', 'State']:
            if col in lookup_df.columns:
                lookup_df[col] = lookup_df[col].astype(str).str.strip().str.title()
        
        if 'FIPS' in lookup_df.columns:
            lookup_df['FIPS'] = lookup_df['FIPS'].astype(str).str.strip().str.zfill(5)
        
        if 'State ID' in lookup_df.columns:
            lookup_df['State ID'] = pd.to_numeric(lookup_df['State ID'], errors='coerce')
            lookup_df.dropna(subset=['State ID'], inplace=True)
            lookup_df['State ID'] = lookup_df['State ID'].astype(int)

        return lookup_df
    except FileNotFoundError:
        print(f"Error: Appalachian FIPS lookup file '{file_path}' not found.")
        print("Please create this file with COUNTY, State ID, STATE, FIPS, Is_Appalachian columns.")
        exit()
    except Exception as e:
        print(f"Error loading Appalachian FIPS lookup file: {e}")
        exit()


def run_dsire_etl():
    print("--- Starting Full DSIRE ETL Process ---")

    zip_url = get_latest_dsire_zip_url(DSIRE_ARCHIVE_PAGE_URL, CHROMEDRIVER_PATH)
    if zip_url is None:
        print("ETL process aborted: Could not find latest DSIRE ZIP URL.")
        return

    dsire_dfs = download_and_extract_dsire_zip(zip_url, TEMP_DATA_DIR, CSVS_TO_LOAD)
    if dsire_dfs is None:
        print("ETL process aborted due to download/extraction error.")
        return

    required_dsire_csvs_from_zip = ['program', 'state_info_content', 'contact'] # Added 'contact'
    if not all(df_name in dsire_dfs for df_name in required_dsire_csvs_from_zip):
        print(f"Error: Not all required DSIRE CSVs ({', '.join(required_dsire_csvs_from_zip)}) were loaded from the ZIP.")
        print("Please ensure they exist in the DSIRE ZIP and are listed in CSVS_TO_LOAD.")
        return

    df_program = dsire_dfs['program']
    df_state_info_content = dsire_dfs['state_info_content']
    df_contact = dsire_dfs['contact'] # Load df_contact

    master_df = load_appalachian_fips_lookup(FIPS_LOOKUP_FILE)
    
    master_df.insert(0, 'ID', range(1, 1 + len(master_df)))

    print("\n--- Initial Master DataFrame based on Appalachian FIPS Lookup ---")
    print("First 5 rows:")
    print(master_df.head())
    print("\nColumns:")
    print(master_df.columns.tolist())
    print("\n")


    if 'state_id' in df_program.columns:
        df_program['state_id'] = pd.to_numeric(df_program['state_id'], errors='coerce').fillna(-1).astype(int)
        
        program_data_to_merge = df_program[[
            'state_id', 'name', 'code', 'summary', 'websiteurl', 'administrator', 'fundingsource', 'budget'
        ]].copy()
        program_data_to_merge.rename(columns={
            'name': 'Program Name',
            'code': 'Code',
            'summary': 'Program Summary',
            'websiteurl': 'Program Website URL',
            'administrator': 'Administrator', 
            'fundingsource': 'Funding Source', 
            'budget': 'Budget' 
        }, inplace=True, errors='ignore')
        
        program_data_to_merge.drop_duplicates(subset=['state_id'], inplace=True)

        master_df = pd.merge(
            master_df,
            program_data_to_merge,
            left_on='State ID', 
            right_on='state_id', 
            how='left', 
            suffixes=('_master', '_program_data')
        )
        print("Merged with program data (Program Name, Code, Summary, Website, Admin, Funding Source, Budget).")
    else:
        print("Warning: 'state_id' column not found in program.csv. Skipping Program Data merge.")


    if 'state_id' in df_state_info_content.columns:
        df_state_info_content['state_id'] = pd.to_numeric(df_state_info_content['state_id'], errors='coerce').fillna(-1).astype(int)

        state_info_data_to_merge = df_state_info_content[[
            'state_id', 'introduction', 'history', 'renewable_portfolio_standard', 'organizations', 'programs', 'footnotes'
        ]].copy()
        state_info_data_to_merge.rename(columns={
            'introduction': 'State Info Intro',
            'history': 'State Info History',
            'renewable_portfolio_standard': 'Renewable Portfolio Standard',
            'organizations': 'Organizations', 
        }, inplace=True, errors='ignore')

        state_info_data_to_merge.drop_duplicates(subset=['state_id'], inplace=True)

        master_df = pd.merge(
            master_df,
            state_info_data_to_merge,
            left_on='State ID', 
            right_on='state_id', 
            how='left', 
            suffixes=('_master', '_stateinfo')
        )
        print("Merged with state_info_content data.")
    else:
        print("Warning: 'state_id' column not found in state_info_content.csv. Skipping State Info merge.")


    if 'state_id' in df_contact.columns:
        df_contact['state_id'] = pd.to_numeric(df_contact['state_id'], errors='coerce').fillna(-1).astype(int)

        contact_data_to_merge = df_contact[[
            'state_id', 'first_name', 'last_name', 'organization_name', 'phone', 'email', 'website_url', 'address', 'city', 'zip'
        ]].copy()
        contact_data_to_merge.rename(columns={
            'first_name': 'Contact First Name',
            'last_name': 'Contact Last Name',
            'organization_name': 'Contact Organization Name',
            'phone': 'Contact Phone',
            'email': 'Contact Email',
            'website_url': 'Contact Website URL',
            'address': 'Contact Address',
            'city': 'Contact City',
            'zip': 'Contact Zip'
        }, inplace=True, errors='ignore')

        contact_data_to_merge.drop_duplicates(subset=['state_id'], inplace=True)

        master_df = pd.merge(
            master_df,
            contact_data_to_merge,
            left_on='State ID', 
            right_on='state_id', 
            how='left', 
            suffixes=('_master', '_contact')
        )
        print("Merged with contact data.")
    else:
        print("Warning: 'state_id' column not found in contact.csv. Skipping Contact Data merge.")

    print("\nMaster DataFrame columns after all merges:")
    print(master_df.columns.tolist())
    master_df.info()
    print("\n")

    print("--- Starting Comprehensive Data Cleaning and Transformation ---")

    # Remove HTML tags and strip whitespace from relevant text columns
    text_columns_to_clean = [
        'State Info Intro',
        'State Info History',
        'Renewable Portfolio Standard',
        'Program Summary',
        'Organizations',
        'Program Website URL', 
        'Administrator', 
        'Funding Source', 
        'Budget', 
        'Contact Organization Name', 
        'Contact Phone', 
        'Contact Email', 
        'Contact Website URL', 
        'Contact Address', 
        'Contact City', 
        'Contact Zip' 
    ]
    for col in text_columns_to_clean:
        if col in master_df.columns:
            master_df[col] = master_df[col].astype(str).str.replace(r'<[^>]*>', '', regex=True)
            master_df[col] = master_df[col].str.strip()
            master_df[col] = master_df[col].replace('nan', '', regex=False)
            print(f"Cleaned HTML, whitespace, and 'nan' from '{col}'.")
    print("\n")

    # Handle Missing Values 
    for col in master_df.columns:
        if master_df[col].dtype == 'object':
            master_df[col] = master_df[col].fillna('Not Specified').astype(str).replace('nan', 'Not Specified')
    
    numeric_cols_to_fill_zero = ['Budget'] # Add other numeric columns here if they are expected to have NaNs
    for col in numeric_cols_to_fill_zero:
        if col in master_df.columns:
            master_df[col] = pd.to_numeric(master_df[col], errors='coerce').fillna(0) # Fill numeric NaNs with 0
            print(f"Filled missing numeric values in '{col}' with 0.")

    # Final FIPS code cleanup (ensure it's a 5-digit string)
    if 'FIPS' in master_df.columns:
        master_df['FIPS'] = master_df['FIPS'].astype(str).str.strip().str.zfill(5).replace('Not Specified', '') 
        print("Ensured 'FIPS' is a 5-digit string.")
    else:
        print("Warning: 'FIPS' column not found after FIPS merge. FIPS might be missing in output.")

    columns_to_drop_after_merge = [
        'state_id_program_data', 
        'state_id_stateinfo', 
        'state_id_contact', 
        'introduction', 'history', 'renewable_portfolio_standard', 'organizations', 'programs', 'footnotes', 
        'name', 'code', 'summary', 'websiteurl', 'administrator', 'fundingsource', 'budget', 
        'first_name', 'last_name', 'organization_name', 'phone', 'email', 'website_url', 'address', 'city', 'zip', 
        # Columns from lookup that are now redundant (their data is in the final columns)
        'county_name_lookup',
        'state_id_numeric_lookup',
        'state_full_name_lookup',
        'fips_code_lookup',
        'is_appalachian_lookup',
    ]

    for col in columns_to_drop_after_merge:
        if col in master_df.columns:
            master_df.drop(columns=[col], inplace=True, errors='ignore')
            print(f"Dropped redundant column '{col}'.")
    print("\n")

    # Excel file has columns in a specific order.
    desired_order = [
        'ID', 'County', 'State ID', 'FIPS', 'State', 
        'Program Name', 'Code', 'Program Website URL', 'Program Summary', 
        'State Info Intro', 'State Info History', 'Renewable Portfolio Standard',
        'Administrator', 'Funding Source', 'Budget', 'Organizations', 
        'Contact First Name', 'Contact Last Name', 'Contact Organization Name',
        'Contact Phone', 'Contact Email', 'Contact Website URL',
        'Contact Address', 'Contact City', 'Contact Zip',
        'Is_Appalachian'
    ]

    # adding any missing as 'Not Specified' if they should be
    for col in desired_order:
        if col not in master_df.columns:
            master_df[col] = 'Not Specified'
            print(f"Added missing desired column '{col}' with 'Not Specified' values.")

    final_columns = [col for col in desired_order if col in master_df.columns]
    master_df = master_df[final_columns].copy()

    print("Missing values after comprehensive cleaning and merging:")
    print(master_df.isnull().sum())
    print("\n")

    print("--- Data already focused on Appalachian Counties ---")
    print(f"Final DataFrame contains {len(master_df)} rows for Appalachian counties.")
    print("First 5 rows of final Appalachian Master DB:")
    print(master_df.head())
    print("\n")

    # --- Output Cleaned and Filtered Data ---
    print(f"--- Saving cleaned and filtered data to: {OUTPUT_FILE_PATH} (Sheet: '{OUTPUT_SHEET_NAME}') ---")
    try:
        os.makedirs(os.path.dirname(OUTPUT_FILE_PATH), exist_ok=True)
        master_df.to_excel(OUTPUT_FILE_PATH, sheet_name=OUTPUT_SHEET_NAME, index=False)
        print("Cleaned and filtered data saved successfully!\n")
    except Exception as e:
        print(f"An error occurred while saving the Excel file: {e}\n")

    # Clean up temporary directory
    print(f"Cleaning up temporary directory: {TEMP_DATA_DIR}")
    try:
        # Give a small delay to ensure all file handles are released
        time.sleep(1) 
        shutil.rmtree(TEMP_DATA_DIR)
        print("Temporary directory cleaned.\n")
    except OSError as e:
        print(f"Error removing temporary directory {TEMP_DATA_DIR}: {e}\n")

    print("--- Full DSIRE ETL Process Complete ---")

# --- Execute ETL Process ---
if __name__ == "__main__":
    try:
        import requests
        from bs4 import BeautifulSoup
        from selenium import webdriver
        from selenium.webdriver.chrome.service import Service as ChromeService
        from selenium.webdriver.chrome.options import Options as ChromeOptions
        from selenium.webdriver.common.by import By
        from selenium.webdriver.support.ui import WebDriverWait
        from selenium.webdriver.support import expected_conditions as EC
        from selenium.common.exceptions import TimeoutException, WebDriverException
    except ImportError:
        print("Required libraries 'requests', 'beautifulsoup4', and 'selenium' not found.")
        print("Please install them using: pip install requests beautifulsoup4 selenium")
        exit()

    run_dsire_etl()