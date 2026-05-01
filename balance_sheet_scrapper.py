import requests
import pandas as pd
import json

def scrape_with_session():
    company_id = "57" 
    # List of categories to fetch
    categories = ["Borrowings", "Other+Liabilities", "Other+Assets"]
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36...',
        'X-Requested-With': 'XMLHttpRequest',
        'Cookie': 'YOUR_ACTUAL_COOKIE_STRING_HERE'
    }

    all_dfs = []

    for parent in categories:
        url = f"https://www.screener.in/api/company/{company_id}/schedules/?parent={parent}&section=balance-sheet&consolidated=true"
        response = requests.get(url, headers=headers)
        
        if response.status_code == 200:
            data = response.json()
            # If the response is a dictionary of categories (like your Borrowings log):
            if isinstance(data, dict):
                temp_df = pd.DataFrame(data).transpose()
                temp_df.index.name = 'Category'
                all_dfs.append(temp_df.reset_index())
        else:
            print(f"Failed to fetch {parent}. Status: {response.status_code}")

    if all_dfs:
        final_df = pd.concat(all_dfs, ignore_index=True)
        # Clean numeric formatting
        for col in final_df.columns[1:]:
            final_df[col] = final_df[col].astype(str).str.replace(',', '')
            final_df[col] = pd.to_numeric(final_df[col], errors='coerce').fillna(0)
            
        final_df.to_csv('adani_ports_detailed_scraped_2.csv', index=False)
        print("SUCCESS: Full breakdown saved to adani_ports_detailed_scraped_2.csv")

if __name__ == "__main__":
    scrape_with_session()