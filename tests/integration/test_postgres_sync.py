import pandas as pd
from datetime import datetime
from src.etl.postgres_sync import PSECompaniesDataset, DailyStockPriceDataset


COMPANIES_DATASET_OBJECT_ATTRIBUTES = ['source_data', 'db_data', 'symbols']
PRICE_DATASET_OBJECT_ATTRIBUTES = ['symbols', 'db_latest_dates']
COMPANY_FIELDS = ['symbol','company_name','sector','subsector','listing_date','extracted_at']
PRICE_DATA_FIELDS = []


companies_dataset = PSECompaniesDataset()
price_dataset = DailyStockPriceDataset(symbols=['SM','BDO'])  # Sample companies


# PSECompaniesDataset
def test_companies_dataset__object_attributes():
    for attr in COMPANIES_DATASET_OBJECT_ATTRIBUTES:
        assert hasattr(companies_dataset, attr), f'Missing attribute: "{attr}"'

def test_companies_dataset__source_data__is_df():
    assert type(companies_dataset.source_data) == pd.DataFrame
    
def test_companies_dataset__source_data__has_complete_fields():
    assert companies_dataset.source_data.columns.tolist() == COMPANY_FIELDS
    
def test_companies_dataset__source_data__min_size():
    assert companies_dataset.source_data.shape[0] > 250   # More than 250 listed companies

def test_companies_dataset__db_data__is_df():
    assert type(companies_dataset.db_data) == pd.DataFrame
    
def test_companies_dataset__db_data__has_complete_fields():
    assert set(COMPANY_FIELDS).issubset(companies_dataset.db_data.columns.tolist())
        
def test_companies_dataset__symbols__is_list():
    assert type(companies_dataset.symbols) == list
    
def test_companies_dataset__symbols__correct_values():
    assert sorted(companies_dataset.symbols) == companies_dataset.source_data.symbol.sort_values().tolist()
    
def test_companies_dataset__delete_all_records():
    if companies_dataset.db_data.shape[0] == 0:
        companies_dataset.sync_table()  # Insert records before testing
        
    companies_dataset._delete_all_records()
    assert companies_dataset.db_data.shape[0] == 0
    
def test_companies_dataset__sync_table():
    if companies_dataset.db_data.shape[0] > 0:
        companies_dataset._delete_all_records()  # Clear table before testing

    companies_dataset.sync_table()
    assert companies_dataset.db_data.shape[0] == companies_dataset.source_data.shape[0]
    

# DailyStockPriceDataset
def test_price_dataset__object_attributes():
    for attr in PRICE_DATASET_OBJECT_ATTRIBUTES:
        assert hasattr(price_dataset, attr), f'Missing attribute: "{attr}"'
        
def test_price_dataset__symbols__is_list():
    assert type(price_dataset.symbols) == list
        
def test_price_dataset__db_latest_dates__is_dict():
    assert type(price_dataset.db_latest_dates) == dict
    
def test_price_dataset__delete_records():
    price_dataset.sync_table(lookback_days=30)  # Insert records before testing
    price_dataset._delete_records(condition="True")
    assert price_dataset.db_latest_dates == {}
    
def test_price_dataset__insert_records():
    SAMPLE_DATA = pd.DataFrame([{'symbol':'BDO',
                                 'date':datetime(3000,1,1).date(),
                                 'open':9999,
                                 'high':9999,
                                 'low':9999,
                                 'close':9999,
                                 'extracted_at':datetime.utcnow()}])
    price_dataset._insert_price_data_to_db(df=SAMPLE_DATA)
    price_dataset._get_db_latest_dates()
    assert price_dataset.db_latest_dates['BDO'] == datetime(3000,1,1).date()
    
    # Clean up
    price_dataset._delete_records(condition="symbol='BDO' AND date='3000-01-01'")
    
def test_price_dataset__sync_table():
    price_dataset._delete_records(condition="True")  # Clear table
    price_dataset.sync_table()
