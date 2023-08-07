"""Module for extracting stocks data from PSE Edge"""

# Author: Rey Anthony Masilang


import pandas as pd
import bs4 as bs
import json
from requests import Session
from datetime import datetime
from typing import List


COMPANY_SEARCH_URL = 'https://edge.pse.com.ph/companyDirectory/search.ax'
COMPANY_NAME_SEARCH_URL = 'https://edge.pse.com.ph/autoComplete/searchCompanyNameSymbol.ax?term={}'
STOCK_DATA_URL = 'https://edge.pse.com.ph/common/DisclosureCht.ax'

COMPANY_SEARCH_HEADERS = {
    'Accept': '*/*',
    'Accept-Encoding': 'gzip, deflate, br',
    'Accept-Language': 'en-US,en;q=0.9',
    'Connection': 'keep-alive',
    'Content-Length': '13',
    'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
    'Host': 'edge.pse.com.ph',
    'Origin': 'https://edge.pse.com.ph',
    'Referer': 'https://edge.pse.com.ph/companyDirectory/form.do',
    'Sec-Fetch-Mode': 'cors',
    'Sec-Fetch-Site': 'same-origin',
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.79 Safari/537.36',
    'X-Requested-With': 'XMLHttpRequest'
}

STOCK_DATA_HEADERS = {
    'Accept': 'application/json, text/javascript, */*; q=0.01',
    'Accept-Encoding': 'gzip, deflate, br',
    'Accept-Language': 'en-US,en;q=0.9',
    'Connection': 'keep-alive',
    'Content-Length': '85',
    'Content-Type': 'application/json',
    'Host': 'edge.pse.com.ph',
    'Origin': 'https://edge.pse.com.ph',
    'Referer': 'https://edge.pse.com.ph/companyPage/stockData.do?cmpy_id={company_id}',
    'Sec-Fetch-Mode': 'cors',
    'Sec-Fetch-Site': 'same-origin',
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.79 Safari/537.36',
    'X-Requested-With': 'XMLHttpRequest'
}

COMPANY_SEARCH_DEFAULT_PAYLOAD = {
    'pageNo': '',
    'companyId': '',
    'keyword': '',
    'sortType': '',
    'dateSortType': 'DESC',
    'cmpySortType': 'DESC',
    'symbolSortType': 'ASC',
    'sector': 'ALL',
    'subsector': 'ALL'
}


def get_listed_companies() -> pd.DataFrame:
    """Extracts a list of all PSE-listed companies.
    
    Returns
    -------
    companies_df : pandas.DataFrame
        A complete list of companies including common details for each.
    
    """
    
    with Session() as s:
        
        extract_df_list: List[pd.DataFrame] = []
        
        # Get first page
        payload = dict(COMPANY_SEARCH_DEFAULT_PAYLOAD)
        payload['pageNo'] = 1
        
        r = s.post(COMPANY_SEARCH_URL, data=payload, headers=COMPANY_SEARCH_HEADERS)
        
        extract_df = pd.read_html(r.text)[0]
        extract_df['Retrieved At'] = r.headers['Date']
        extract_df_list.append(extract_df)
        
        soup = bs.BeautifulSoup(r.text, 'html5lib')
        page_count = max([int(x.text) for x in soup.findAll('a', href='#') if x.text.isdigit()])

        for page_num in range(2, page_count+1):
            payload['pageNo'] = page_num
            r = s.post(COMPANY_SEARCH_URL, data=payload, headers=COMPANY_SEARCH_HEADERS)
            extract_df = pd.read_html(r.text)[0]
            extract_df['Retrieved At'] = r.headers['Date']
            extract_df_list.append(extract_df)

    companies_df = pd.concat(extract_df_list, axis=0)
    companies_df = companies_df.rename(columns={
        'Company Name':'company_name',
        'Stock Symbol':'symbol',
        'Sector':'sector',
        'Subsector':'subsector',
        'Listing Date':'listing_date',
        'Retrieved At':'extracted_at'
    })
    companies_df['listing_date'] = pd.to_datetime(companies_df['listing_date'], utc=True).dt.strftime('%Y-%m-%d')
    companies_df['extracted_at'] = pd.to_datetime(companies_df['extracted_at'], utc=True).dt.strftime('%Y-%m-%d %H:%M:%S')
    companies_df['company_name'] = companies_df['company_name'].str.replace('\'','\'\'')
    companies_df = companies_df[['symbol','company_name','sector','subsector','listing_date','extracted_at']]
    
    return companies_df
    

def get_company_info(symbol: str) -> dict:
    """Extracts company info from PSE Edge.
    
    Parameters
    ----------
    symbol : str
        The ticker symbol for the company to extract market price data for.
    
    Returns
    -------
    company_info : dict
        A dictionary of company details.
    
    """
    
        
    with Session() as s:
        
        # Search company name
        r = s.get(COMPANY_NAME_SEARCH_URL.format(symbol))
        company_name, company_id = (
            pd.DataFrame(r.json())
            .query("symbol=='{}'".format(symbol))
            .loc[:,['cmpyNm','cmpyId']]
            .iloc[0]
        )
        
        search_results = r.json()
        response_dict = [x for x in search_results if x['symbol']==symbol][0]
        company_id = response_dict['cmpyId']
        company_name = response_dict['cmpyNm']
        
        # Get company metadata
        payload = dict(COMPANY_SEARCH_DEFAULT_PAYLOAD)
        payload['companyId'] = company_id
        payload['keyword'] = company_id
        payload['sortType'] = 'cmpy'

        r = s.post(COMPANY_SEARCH_URL, data=payload, headers=COMPANY_SEARCH_HEADERS)

        soup = bs.BeautifulSoup(r.text, 'html5lib')
        table_elements = soup.findAll('td')
        
        # Compile company info
        company_info = {'symbol':symbol,
                        'company_name':company_name,
                        'company_id':company_id}
        
        # Extract PSE Edge security ID
        attribute_str = list(table_elements[0].children)[0].get('onclick')
        _, company_info['security_id'] = attribute_str.replace('cmDetail(','').replace(');return false;','').replace("'","").split(',')

        # Extract sector
        company_info['sector'] = table_elements[2].text
        
        # Extract subsector
        company_info['subsector'] = table_elements[3].text
        
        # Extract listing date
        company_info['listing_date'] = pd.to_datetime(table_elements[4].text, utc=True).strftime('%Y-%m-%d')
        
    return company_info


def get_stock_data(symbol: str, start_date: datetime = None, end_date: datetime = None) -> pd.DataFrame:
    """Extracts daily stock market prices.
    
    Parameters
    ----------
    symbol : str
        The ticker symbol for the company to extract market price data for.
    
    start_date : datetime.datetime
        The starting date of the price data to be extracted. If not specified,
        the listing date of the company is used.
    
    end_date : datetime.datetime
        The latest date of the price data to be extracted. If not specified,
        the date today is used.
    
    Returns
    -------
    prices_df : pandas.DataFrame
        A DataFrame which contains daily closing price of the specified stock.
        
    """
    
    
    with Session() as s:
        try:
            company_info = get_company_info(symbol)
            
            # Impute dates
            if start_date is None:
                start_date = company_info['listing_date']

            if end_date is None:
                end_date = datetime.now().strftime('%Y-%m-%d')

            payload = {
                'cmpy_id': company_info['company_id'],
                'security_id': company_info['security_id'],
                'startDate': pd.to_datetime(start_date, utc=True).strftime('%m-%d-%Y'),
                'endDate': pd.to_datetime(end_date, utc=True).strftime('%m-%d-%Y'),
            }

            headers = dict(STOCK_DATA_HEADERS)
            headers['Referer'] = headers['Referer'].format(company_id=company_info['company_id'])
            r = s.post(STOCK_DATA_URL, json=payload, headers=headers)

            chart_data = r.json()['chartData']
            extracted_at = r.headers['Date']

            if len(chart_data) == 0:
                prices_df = pd.DataFrame(columns=['symbol','date','open','high','low','close'])

            else:
                prices_df = pd.DataFrame(chart_data)
                prices_df['symbol'] = symbol
                prices_df['CHART_DATE'] = pd.to_datetime(prices_df['CHART_DATE'], utc=True)
                prices_df = prices_df.rename(columns={
                    'OPEN':'open',
                    'HIGH':'high',
                    'LOW':'low',
                    'CLOSE':'close',
                    'CHART_DATE':'date'
                })
                prices_df['date'] = pd.to_datetime(prices_df['date'], utc=True).dt.strftime('%Y-%m-%d')
                prices_df['extracted_at'] = pd.to_datetime(extracted_at, utc=True).strftime('%Y-%m-%d %H:%M:%S')
                prices_df = prices_df[['symbol','date','open','high','low','close','extracted_at']]

        except:
            print(f'Failed to get company info from PSE Edge for: {symbol}.')
            prices_df = pd.DataFrame(columns=['symbol','date','open','high','low','close','extracted_at'])
        
        return prices_df