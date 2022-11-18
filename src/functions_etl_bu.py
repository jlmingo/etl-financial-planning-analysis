import calendar
import pandas as pd
from inputs import YEAR

def get_revenue(path, sheet_name, year):
    print(f"Reading {sheet_name}")
    
    # prepare column naming
    # generate_
    list_of_months = [x[0:3] + "-" + str(YEAR)[-2:] for x in list(calendar.month_name) if x != ""]
    list_of_unnamed = []
    for i in range(6, 6+12+1):
        list_of_unnamed.append(f"Unnamed: {i}")
    float_types = 12 * ["float64"]
    dtype = dict(zip(list_of_unnamed, float_types))
    
    #read file
    df = pd.read_excel(path, sheet_name=sheet_name, usecols="B:R", skiprows=range(76), dtype=dtype)
    
    #check first and last value
    assert df.iloc[0, 0] == "Contract Revenue"
    assert df.iloc[-1, 0] == "EBITDA"
    
    #drop columns
    col_drop = ["Unnamed: 2", "Unnamed: 3", "Unnamed: 4", "Unnamed: 5"]
    df.drop(col_drop, inplace=True, axis=1)
    
    #rename columns
    dict_rename_columns = {"Unnamed: 1": "PL_Account"}
    dict_rename_unnamed = dict(zip(list_of_unnamed, list_of_months))
    dict_rename_columns.update(dict_rename_unnamed)
    df.rename(columns=dict_rename_columns, inplace=True)
    
    #drop values where the PL_Account is a general category or subtotal
    drop_values = [
        "Total Revenue", "Gross Profit", "Operations & Maintenance",
        "Other Professional Services", "Environmental and Social",
        "Insurances", "Local / Municipal HoldCo Taxes", "Local / Municipal Plant Taxes",
        "Payroll - Other", "Office Costs (Lease, Utilities, IT, etc.)", "EBITDA"
    ]
    
    idx_drop = df[df.PL_Account.isin(drop_values)].index
    df.drop(idx_drop, inplace=True)
    df.reset_index(drop=True, inplace=True)
    df.dropna(how='all', inplace=True)

    #pending: ensure no values of drop_values are within "subcategory"
    valid_pl_amounts = set(df_dim_accounts.PL_Acount.unique())
    unique_pl_amounts = set(df.PL_Account.unique())
    difference = unique_pl_amounts.difference(valid_pl_amounts)
    assert difference == {}, difference
    
    df["sheet_name"] = sheet_name
    
    #pending: get company name
    #pending: get FX and convert to USD
    #pending: check that company names are within allowed values
    #pending: check that PL_Account names are within allowed values (subcategories)
    
    return df
    
def check_nulls(df, col):
    assert df[df[col].isnull()].shape[0] == 0, f"Warning, nulls in {col}"