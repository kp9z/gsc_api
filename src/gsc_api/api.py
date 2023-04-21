from datetime import datetime, timedelta
from googleapiclient import discovery
from google.oauth2 import service_account
import re
import pandas as pd

class GoogleSeachConsoleAPI:
    def __init__(self,domain) -> None:
        self.service = self._get_service()
        self.default_site = self._format_domain(domain)

    def _format_domain(self, domain):
        pattern = r'(http(s)?:\/\/)'
        replacement = r'sc-domain:'
        return re.sub(pattern, replacement, domain)
        
    def _get_service(self):
        SCOPES = ['https://www.googleapis.com/auth/webmasters.readonly']
        credentials = service_account.Credentials.from_service_account_file('gsc_keys.json', scopes = SCOPES)
        service = discovery.build('searchconsole', 'v1', credentials=credentials)
        return service
    
    def get_sites(self):
        response = self.service.sites().list().execute()
        return response
    
    def get_sitemap(self):
  
        response = self.service.sitemaps().list(siteUrl=self.default_site).execute()
        return response
    
    def inspect_url(self,url):
        body = {
            "inspectionUrl": url,
            "siteUrl": self.default_site,
        }
        response = self.service.urlInspection().index().inspect(body=body).execute()
        return response
    
    def convert_to_df(self, data,dimensions):
        df = pd.DataFrame.from_records(data)
        df[dimensions] = df['keys'].apply(pd.Series)
        df = df.drop(columns=['keys'])
        
        return df
  
    def get_search_analytics(self,dimensions = ['query','page'], start_row= 0, row_limit = 25000,end_date=None,duration=30):
        if not end_date: 
            end_date = datetime.today()
        else: 
            end_date = datetime.strptime(end_date, '%Y-%m-%d')
        start_date = end_date - timedelta(days=duration)

        body = {
            "startDate": start_date.strftime('%Y-%m-%d'),
            "endDate": end_date.strftime('%Y-%m-%d'),
            "dimensions": dimensions,
            "rowLimit": row_limit,
            "dataState": "FINAL",
            "startRow": start_row,
        }
        response = self.service.searchanalytics().query(siteUrl=self.default_site, body=body).execute()
        return response

    def get_search_analytics_all(self,max_export=None,row_limit = 25_000):
        dimensions = ['query','page']
        start_row= 0
        data = []

        while (start_row%row_limit == 0) or (start_row == 0):
            print(f'Exporting up to {start_row+row_limit} rows')
            response = self.get_search_analytics(dimensions,start_row, row_limit)
            start_row += len(response['rows'])
            data.extend(response['rows'])

            if max_export and start_row > max_export:
                break

        return data


if __name__ == "__main__":
    api = GoogleSeachConsoleAPI(
        domain = 'https://google.com'
    )
    print(api.get_sites())
