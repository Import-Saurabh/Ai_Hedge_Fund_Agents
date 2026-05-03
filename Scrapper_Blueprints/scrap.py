import httpx
from bs4 import BeautifulSoup
import pandas as pd

def scrape_screener_all_tables(company_symbol):
    url = f"https://www.screener.in/company/{company_symbol}/consolidated/"
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36"
    }

    print(f"Fetching data for {company_symbol}...")
    response = httpx.get(url, headers=headers, follow_redirects=True)
    
    if response.status_code != 200:
        print(f"Failed to fetch data. Status code: {response.status_code}")
        return

    soup = BeautifulSoup(response.text, 'lxml')
    
    # We will store all dataframes in a dictionary
    all_data = {}

    # Screener tables are wrapped in sections with specific IDs
    sections = [
        ('Quarters', 'quarters'),
        ('Profit_Loss', 'profit-loss'),
        ('Compounded_Growth', 'growth-numbers'),
        ('Balance_Sheet', 'balance-sheet'),
        ('Cash_Flow', 'cash-flow'),
        ('Ratios', 'ratios'),
        ('Shareholding', 'shareholding')
    ]

    for sheet_name, section_id in sections:
        section = soup.find('section', id=section_id)
        if not section:
            continue
            
        table = section.find('table')
        if not table:
            continue

        # Using Pandas read_html to quickly convert the <table> tag
        # We use [0] because read_html returns a list of dataframes
        df = pd.read_html(str(table))[0]
        
        # Clean the first column (often has '+' buttons for sub-rows)
        df.iloc[:, 0] = df.iloc[:, 0].str.replace(r'\s*\+\s*', '', regex=True).str.strip()
        
        all_data[sheet_name] = df
        print(f"Successfully scraped: {sheet_name}")

    # Save to Excel
    file_name = f"{company_symbol}_Full_Data.xlsx"
    with pd.ExcelWriter(file_name, engine='openpyxl') as writer:
        for sheet, df in all_data.items():
            df.to_excel(writer, sheet_name=sheet, index=False)
            
    print(f"\n✅ All tables saved to {file_name}")

if __name__ == "__main__":
    scrape_screener_all_tables("ADANIPORTS")