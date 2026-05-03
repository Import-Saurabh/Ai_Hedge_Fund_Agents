import requests
import pandas as pd
import io
import os

class ScreenerProDetailed:
    def __init__(self):
        self.base_url = "https://www.screener.in/api/company/57/schedules/"
        self.session = requests.Session()
        # Pro headers to mimic a browser session
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
            'X-Requested-With': 'XMLHttpRequest',
            'Referer': 'https://www.screener.in/company/ADANIPORTS/consolidated/'
        })

    def get_safe_path(self, filename):
        """Checks if file is open in Excel/another app and returns a safe name."""
        if not os.path.exists(filename):
            return filename
        try:
            # Try to rename it to itself. If it fails, the file is open.
            os.rename(filename, filename)
            return filename
        except OSError:
            name, ext = os.path.splitext(filename)
            return f"{name}_NEW{ext}"

    def fetch_schedules(self):
        # The specific categories you requested
        targets = [
            {"name": "Borrowings", "parent": "Borrowings"},
            {"name": "Other Liabilities", "parent": "Other+Liabilities"},
            {"name": "Other Assets", "parent": "Other+Assets"}
        ]
        
        all_dfs = []

        for item in targets:
            print(f"Fetching: {item['name']}...")
            
            # Using your exact URL format (consolidated= empty)
            url = f"{self.base_url}?parent={item['parent']}&section=balance-sheet&consolidated="
            
            response = self.session.get(url)
            
            if response.status_code == 200:
                data = response.json()
                if data and isinstance(data, dict):
                    df = pd.DataFrame(data).transpose()
                    df.index.name = 'Sub-Category'
                    df.reset_index(inplace=True)
                    df.insert(0, 'Parent_Category', item['name'])
                    all_dfs.append(df)
                    print(f"  Successfully extracted {len(df)} rows.")
                else:
                    print(f"  Warning: {item['name']} returned no data. (Likely restricted for guest users)")
            else:
                print(f"  Error {response.status_code} for {item['name']}")

        if all_dfs:
            final_df = pd.concat(all_dfs, ignore_index=True)
            
            # Numeric cleaning for year columns
            for col in final_df.columns:
                if any(x in col for x in ['Mar', 'Sep', '20']):
                    final_df[col] = (final_df[col].astype(str)
                                     .str.replace(',', '')
                                     .replace('nan', '0'))
                    final_df[col] = pd.to_numeric(final_df[col], errors='coerce').fillna(0)

            # File safety check
            save_path = self.get_safe_path('ADANIPORTS_full_breakdown.csv')
            final_df.to_csv(save_path, index=False)
            print(f"\n--- SUCCESS ---")
            print(f"File saved to: {save_path}")
        else:
            print("No data was retrieved.")

if __name__ == "__main__":
    scraper = ScreenerProDetailed()
    scraper.fetch_schedules()