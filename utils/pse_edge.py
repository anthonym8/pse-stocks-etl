"""Module for extracting stocks data from PSE Edge"""

# Author: Rey Anthony Masilang


import pandas as pd
import bs4 as bs
import json
from requests import Session
from datetime import date, datetime


def get_listed_companies():
    """Extracts a list of all PSE-listed companies.
    
    Parameters
    ----------
    None
    
    Returns
    -------
    companies_df : pandas.DataFrame
        A complete list of companies including common details for each.
    
    """
    
    company_search_url = 'https://edge.pse.com.ph/companyDirectory/search.ax'
    
    with Session() as s:
        
        HEADERS = {
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
        
        payload = {
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
        
        df_list = []
        
        # Get first page
        page_num = 1
        payload['pageNo'] = page_num
        r = s.post(company_search_url, data=payload, headers=HEADERS)
        df_list.append(pd.read_html(r.text)[0])
        
        soup = bs.BeautifulSoup(r.text, 'html5lib')
        page_count = max([int(x.text) for x in soup.findAll('a', href='#') if x.text.isdigit()])

        for page_num in range(2, page_count+1):
            payload['pageNo'] = page_num
            r = s.post(company_search_url, data=payload, headers=HEADERS)
            df_list.append(pd.read_html(r.text)[0])

    companies_df = pd.concat(df_list, axis=0)
    companies_df = companies_df.rename(columns={
        'Company Name':'company_name',
        'Stock Symbol':'symbol',
        'Sector':'sector',
        'Subsector':'subsector',
        'Listing Date':'listing_date'
    })
    companies_df['listing_date'] = pd.to_datetime(companies_df['listing_date']).dt.strftime('%Y-%m-%d')
    companies_df = companies_df[['symbol','company_name','sector','subsector','listing_date']]
    
    return companies_df
    

def get_company_info(symbol):
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
    
    company_name_search_url = 'https://edge.pse.com.ph/autoComplete/searchCompanyNameSymbol.ax?term={}'
    company_search_url = 'https://edge.pse.com.ph/companyDirectory/search.ax'
    
    with Session() as s:
        
        # Search company name
        r = s.get(company_name_search_url.format(symbol))
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
        HEADERS = {
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

        payload = {
            'pageNo': '',
            'companyId': '{}'.format(company_id),
            'keyword': '{}'.format(company_id),
            'sortType': 'cmpy',
            'dateSortType': 'DESC',
            'cmpySortType': 'DESC',
            'symbolSortType': 'ASC',
            'sector': 'ALL',
            'subsector': 'ALL'
        }

        r = s.post(company_search_url, data=payload, headers=HEADERS)

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
        company_info['listing_date'] = pd.to_datetime(table_elements[4].text).strftime('%Y-%m-%d')
        
    return company_info


def get_stock_data(symbol, start_date=None, end_date=None):
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
    
    
    stock_data_url = 'https://edge.pse.com.ph/common/DisclosureCht.ax'

    with Session() as s:
        company_info = get_company_info(symbol)
        
        # Impute dates
        if start_date is None:
            start_date = company_info['listing_date']
        
        if end_date is None:
            end_date = datetime.now().strftime('%Y-%m-%d')

        # Get stock data
        STOCK_DATA_HEADERS = {
            'Accept': 'application/json, text/javascript, */*; q=0.01',
            'Accept-Encoding': 'gzip, deflate, br',
            'Accept-Language': 'en-US,en;q=0.9',
            'Connection': 'keep-alive',
            'Content-Length': '85',
            'Content-Type': 'application/json',
            'Host': 'edge.pse.com.ph',
            'Origin': 'https://edge.pse.com.ph',
            'Referer': 'https://edge.pse.com.ph/companyPage/stockData.do?cmpy_id={}'.format(company_info['company_id']),
            'Sec-Fetch-Mode': 'cors',
            'Sec-Fetch-Site': 'same-origin',
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.79 Safari/537.36',
            'X-Requested-With': 'XMLHttpRequest'
        }

        payload = {
            'cmpy_id': company_info['company_id'],
            'security_id': company_info['security_id'],
            'startDate': pd.to_datetime(start_date).strftime('%m-%d-%Y'),
            'endDate': pd.to_datetime(end_date).strftime('%m-%d-%Y'),
        }

        r = s.post(stock_data_url, json=payload, headers=STOCK_DATA_HEADERS)
        
        chart_data = r.json()['chartData']
        
        if len(chart_data) == 0:
            prices_df = pd.DataFrame(columns=['symbol','date','open','high','low','close'])
            
        else:
            prices_df = pd.DataFrame(chart_data)
            prices_df['symbol'] = symbol
            prices_df['CHART_DATE'] = pd.to_datetime(prices_df['CHART_DATE'])
            prices_df = prices_df.rename(columns={
                'OPEN':'open',
                'HIGH':'high',
                'LOW':'low',
                'CLOSE':'close',
                'CHART_DATE':'date'
            })
            prices_df = prices_df[['symbol','date','open','high','low','close']]
            prices_df['date'] = pd.to_datetime(prices_df['date']).dt.strftime('%Y-%m-%d')

        return prices_df