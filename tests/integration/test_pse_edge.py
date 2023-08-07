import pytest
from src.utils.pse_edge import get_listed_companies, get_company_info, get_stock_data


@pytest.fixture
def pse_company_list():
    return get_listed_companies()

@pytest.fixture
def sample_company_info():
    return get_company_info('JFC')

@pytest.fixture
def sample_stock_data_no_dates():
    return get_stock_data('JFC')

def test__get_listed_companies__result_count(pse_company_list):
    assert pse_company_list.shape[0] > 200
        
def test__get_listed_companies__column_list(pse_company_list):
    COLUMN_NAMES = ['symbol', 'company_name', 'sector', 'subsector', 'listing_date', 'extracted_at']
    assert pse_company_list.columns.tolist() == COLUMN_NAMES
    
def test__get_listed_companies__column_types(pse_company_list):
    COLUMN_TYPES = ['object','object','object','object','object','object']
    assert pse_company_list.dtypes.astype('str').tolist() == COLUMN_TYPES
    
def test__get_company_info__key_list(sample_company_info):
    KEYS = ['symbol','company_name','company_id','security_id',
            'sector','subsector','listing_date']
    assert list(sample_company_info.keys()) == KEYS
    
def test__get_company_info__value_types(sample_company_info):
    VALUE_TYPES = [str,str,str,str,str,str,str]
    assert [type(v) for k,v in sample_company_info.items()] == VALUE_TYPES
    
def test__get_company_info__exact_output(sample_company_info):
    EXPECTED_OUTPUT = {
        'symbol': 'JFC',
        'company_name': 'Jollibee Foods Corporation',
        'company_id': '86',
        'security_id': '158',
        'sector': 'Industrial',
        'subsector': 'Food, Beverage & Tobacco',
        'listing_date': '1993-07-14'
    }
    assert sample_company_info == EXPECTED_OUTPUT

def test__get_stock_data__column_list(sample_stock_data_no_dates):
    COLUMN_NAMES = ['symbol','date','open','high','low','close','extracted_at']
    assert sample_stock_data_no_dates.columns.tolist() == COLUMN_NAMES
    
def test__get_stock_data__column_types(sample_stock_data_no_dates):
    COLUMN_TYPES = ['object','object','float64','float64','float64','float64','object']
    assert sample_stock_data_no_dates.dtypes.astype('str').tolist() == COLUMN_TYPES

def test__get_stock_data__result_count(sample_stock_data_no_dates):
    assert sample_stock_data_no_dates.shape[0] > 3000
    
def test__get_stock_data__with_dates__exact_output():
    SYMBOL = 'JFC'
    START_DATE = '2022-03-01'
    END_DATE = '2022-03-01'
    EXPECTED_OUTPUT = [{
        'symbol':SYMBOL,
        'date':START_DATE,
        'open':241.4,
        'high':245,
        'low':240,
        'close':240
    }]
    
    actual_output = get_stock_data(SYMBOL, START_DATE, END_DATE)
    actual_output = actual_output[['symbol','date','open','high','low','close']]
    assert actual_output.to_dict('records') == EXPECTED_OUTPUT
