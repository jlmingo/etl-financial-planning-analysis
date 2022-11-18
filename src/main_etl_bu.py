import pandas as pd
from numpy import nan as np_nan
from datetime import datetime, date
import calendar
from inputs import path_asset_management_input, path_dim_company, path_dim_accounts, YEAR
#from functions_etl_bu import get_revenue, check_nulls

#TODO: add cash flow figures

#functions
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
    col_drop = ["Unnamed: 2", "Unnamed: 4", "Unnamed: 5"]
    df.drop(col_drop, inplace=True, axis=1)
    
    #rename columns
    dict_rename_columns = {"Unnamed: 1": "PL_Account", "Unnamed: 3": "Text_Description"}
    dict_rename_unnamed = dict(zip(list_of_unnamed, list_of_months))
    dict_rename_columns.update(dict_rename_unnamed)
    df.rename(columns=dict_rename_columns, inplace=True)
    
    #drop values where the PL_Account is a general category or subtotal
    drop_values = [
        "Total Revenue", "Gross Profit", "Operations & Maintenance",
        "Other Professional Services", "Environmental and Social",
        "Insurances", "Local / Municipal HoldCo Taxes", "Local / Municipal Plant Taxes",
        "Payroll - Other", "Office Costs (Lease, Utilities, IT, etc.)", "EBITDA", np_nan
    ]
    
    idx_drop = df[df.PL_Account.isin(drop_values)].index
    df.drop(idx_drop, inplace=True)
    df.reset_index(drop=True, inplace=True)
    df.dropna(how='all', inplace=True)

    #pending: ensure no values of drop_values are within "subcategory"
    valid_pl_amounts = set(df_dim_accounts.PL_Description_SubCategory.unique())
    unique_pl_amounts = set(df.PL_Account.unique())
    difference = unique_pl_amounts.difference(valid_pl_amounts)
    assert difference == set({}), difference
    
    df["Project_Name"] = sheet_name
    
    #pending: get company name
    #pending: get FX and convert to USD
    #pending: check that company names are within allowed values
    #pending: check that PL_Account names are within allowed values (subcategories)
    
    return df
    
def check_nulls(df, col):
    assert df[df[col].isnull()].shape[0] == 0, f"Warning, nulls in {col}"

#initialize dimensions
#dim_accounts
dtype_accounts = {
    "PL_Account_Category": "int",
    "PL_Account_SubCategory": "int"
}
df_dim_accounts = pd.read_excel(path_dim_accounts, sheet_name="Dataload", dtype=dtype_accounts)

#selector
df_selector = pd.read_excel(path_asset_management_input, sheet_name="SPV Selector", skiprows=1)
df_selector = df_selector[df_selector.SPV.notnull()]
companies_spv_selector = set(df_selector.SPV.unique())

#companies
df_dim_company = pd.read_excel(path_dim_company, sheet_name="Dataload", dtype={"Net_Suite_Code": "int32"})
companies_dim_company = set(df_dim_company.Company_Name.unique())

#FX
#TODO: confirm FX values to be used
dict_fx = {
    "Currency": ["USD", "EUR", "COP", "CLP"],
    "FX": [1, 1, 5000, 950]
}
df_fx = pd.DataFrame(dict_fx)

#TODO: REMOVE THIS WHEN EVERYTHING ITS OK! filter out companies that are not in dim_company
df_selector = df_selector[df_selector.SPV.isin(companies_dim_company)]
#TODO: ASSERT TO ENSURE PL_ACCOUNTS ARE INCLUDED IN DIM_ACCOUNTS
#TODO: check that companies in tab have allowed values in DIM_COMPANY

#iterate over sheets
list_df = []
for sheet in df_selector.PROJECT.unique():
    df = get_revenue(path_asset_management_input, sheet, YEAR)
    list_df.append(df)
list_of_months = [x[0:3] + "-" + str(YEAR)[-2:] for x in list(calendar.month_name) if x != ""]
df_concat = pd.concat(list_df, ignore_index=True)
print(df_concat.shape)
df_unpivot = (pd.melt(
    df_concat,
    id_vars=["PL_Account", "Project_Name", "Text_Description"],
    value_vars=list_of_months,
    var_name='Date',
    value_name='LC_Amount'))
df_unpivot["Date"] = pd.to_datetime(df_unpivot.Date, format="%b-%y")
df_unpivot = df_unpivot[df_unpivot.LC_Amount.notnull()]
df_unpivot = df_unpivot[df_unpivot.LC_Amount != 0]
df_unpivot.reset_index(inplace=True, drop=True)
df_unpivot["LC_Amount"] = df_unpivot["LC_Amount"].round(4)

#add company name
df_selector.rename(columns={"PROJECT": "Project_Name", "SPV": "Company_Name"}, inplace=True)
selected_merge = ["Project_Name", "Company_Name"]
df_unpivot = df_unpivot.merge(df_selector[selected_merge], on="Project_Name", how="left")
check_nulls(df_unpivot, "Company_Name")

#add currency
selected_merge = ["Company_Name", "Currency"]
df_unpivot = df_unpivot.merge(df_dim_company[selected_merge], on="Company_Name", how="left")
check_nulls(df_unpivot, "Currency")

#add fx
df_unpivot = df_unpivot.merge(df_fx, on="Currency", how="left")
check_nulls(df_unpivot, "FX")

#add USD amount
df_unpivot["USD_Amount"] = df_unpivot["LC_Amount"] / df_unpivot["FX"]
df_unpivot["USD_Amount"] = df_unpivot["USD_Amount"].round(4)

#drop columns
df_unpivot.drop(["Currency", "FX"], axis=1, inplace=True)

#order columns
ordered_columns = [
    "PL_Account", "Date",
    "LC_Amount", "USD_Amount", "Company_Name",
    "Project_Name", "Text_Description"
]
print(df_unpivot.columns)
df_unpivot = df_unpivot[ordered_columns]

#output file
df_unpivot.to_csv("../output/BU23_data.csv", index=False)