import pandas as pd
from googleapiclient import discovery
from google.oauth2 import service_account

from datetime import datetime, timedelta
import re

class GoogleSeachConsoleAPI:
    """
    A class for interacting with the Google Search Console API.
    Attributes:
    service (googleapiclient.discovery.Resource): A resource object for interacting with the API.
    default_site (str): The default site to query.

    Methods:
    _format_domain(domain): Formats a domain name for use in the API.
    _get_service(): Retrieves a resource object for interacting with the API.
    get_sites(): Retrieves a list of sites associated with the authenticated user.
    get_sitemap(): Retrieves a list of sitemaps for the default site.
    inspect_url(url): Inspects a URL for information.
    convert_to_df(data, dimensions): Converts API response data to a pandas DataFrame.
    get_search_analytics(dimensions, start_row, row_limit, end_date, duration): Retrieves search analytics data for the default site.
    get_search_analytics_all(max_export, row_limit): Retrieves all available search analytics data for the default site.
    """
    def __init__(self,domain) -> None:
        """
        Initializes a GoogleSeachConsoleAPI object.

        Args:
            domain (str): The default site to query.

        Returns:
            None

        """
        self.service = self._get_service()
        self.default_site = self._format_domain(domain)

    def _format_domain(self, domain):
        """
        Formats a domain name for use in the API.

        Args:
            domain (str): The domain name to format.

        Returns:
            str: The formatted domain name.

        """
        pattern = r'(http(s)?:\/\/)'
        replacement = r'sc-domain:'
        return re.sub(pattern, replacement, domain)
        
    def _get_service(self):
        """
        Retrieves a resource object for interacting with the API.

        Returns:
            googleapiclient.discovery.Resource: A resource object for interacting with the API.

        """
        SCOPES = ['https://www.googleapis.com/auth/webmasters.readonly']
        credentials = service_account.Credentials.from_service_account_file('gsc_keys.json', scopes = SCOPES)
        service = discovery.build('searchconsole', 'v1', credentials=credentials)
        return service

    def get_sites(self):
        """
        Retrieves a list of sites associated with the authenticated user.

        Returns:
            dict: A dictionary containing information about the sites.

        """
        response = self.service.sites().list().execute()
        return response

    def get_sitemap(self):
        """
        Retrieves a list of sitemaps for the default site.

        Returns:
            dict: A dictionary containing information about the sitemaps.

        """
        response = self.service.sitemaps().list(siteUrl=self.default_site).execute()
        return response

    def inspect_url(self,url):
        """
        Inspects a URL for information.

        Args:
            url (str): The URL to inspect.

        Returns:
            dict: A dictionary containing information about the URL.

        """
        body = {
            "inspectionUrl": url,
            "siteUrl": self.default_site,
        }
        response = self.service.urlInspection().index().inspect(body=body).execute()
        return response

    def convert_to_df(self, data,dimensions):
        """
        Converts API response data to a pandas DataFrame.

        Args:
            data (list): The API response data to convert.
            dimensions (list): The dimensions to include in the DataFrame.

        Returns:
            pandas.DataFrame: A DataFrame containing the API response data.

        """
        df = pd.DataFrame.from_records(data)
        df[dimensions] = df['keys'].apply(pd.Series)
        df = df.drop(columns=['keys'])
        
        return df

    def get_search_analytics(self,dimensions = ['query','page'], start_row= 0, row_limit = 25000,end_date=None,duration=30):
        """
        Retrieves search analytics data for the default site.

        Args:
            dimensions (list): The dimensions to include in the response data.
            start_row (int): The starting row to retrieve.
            row_limit (int): The maximum number of rows to retrieve.
            end_date (str): The end date for the query in the format 'YYYY-MM-DD'.
            duration (int): The duration of the query in days.

        Returns:
            dict: A dictionary containing the search analytics data.

        """
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
        """
        Retrieves all available search analytics data for the default site.

        Args:
            max_export (int): The maximum number of rows to retrieve.
            row_limit (int): The maximum number of rows to retrieve per query.

        Returns:
            list: A list containing all available search analytics data.

        """
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
