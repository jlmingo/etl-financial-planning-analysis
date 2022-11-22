import calendar
import pandas as pd
from numpy import nan as np_nan

#general functions
def check_nulls(df, col):
    assert df[df[col].isnull()].shape[0] == 0, f"Warning, nulls in {col}"

#revenue and opex functions
def get_revenue_opex(path_asset_management_input, sheet_name, year, df_dim_accounts):
    print(f"Reading {sheet_name}")
    
    # prepare column naming
    # generate_
    list_of_months = [x[0:3] + "-" + str(year)[-2:] for x in list(calendar.month_name) if x != ""]
    list_of_unnamed = []
    for i in range(6, 6+12+1):
        list_of_unnamed.append(f"Unnamed: {i}")
    float_types = 12 * ["float64"]
    dtype = dict(zip(list_of_unnamed, float_types))
    
    #read file
    df = pd.read_excel(path_asset_management_input, sheet_name=sheet_name, usecols="B:R", skiprows=range(76), dtype=dtype)
    
    #drop nan values
    drop_values = [
        np_nan
    ]
    
    idx_drop = df[df["Unnamed: 1"].isin(drop_values)].index
    df.drop(idx_drop, inplace=True)
    df.reset_index(drop=True, inplace=True)

    #check first and last value
    assert df.iloc[0, 0] == "Contract Revenue", df.iloc[0, 0]
    assert df.iloc[-1, 0] == "EBITDA", df.iloc[-1, 0]
    
    #drop columns
    col_drop = ["Unnamed: 2", "Unnamed: 3", "Unnamed: 5"]
    df.drop(col_drop, inplace=True, axis=1)
    
    #rename columns
    dict_rename_columns = {"Unnamed: 1": "PL_Account", "Unnamed: 4": "Text_Description"}
    dict_rename_unnamed = dict(zip(list_of_unnamed, list_of_months))
    dict_rename_columns.update(dict_rename_unnamed)
    df.rename(columns=dict_rename_columns, inplace=True)
    
    #drop values where the PL_Account is a general category or subtotal
    #TODO: remove Other Professional Services from list when origin data is corrected
    drop_values = [
        "Total Revenue", "Gross Profit", "EBITDA", "Other Professional Services"
    ]
    
    idx_drop = df[df.PL_Account.isin(drop_values)].index
    df.drop(idx_drop, inplace=True)
    df.reset_index(drop=True, inplace=True)

    #drop rows where all values are null
    df.dropna(how='all', inplace=True)
    df.reset_index(drop=True, inplace=True)

    #drop rows where values in months are null
    df.dropna(subset=list_of_months, inplace=True)
    df.reset_index(drop=True, inplace=True)

    #pending: ensure no values of drop_values are within "subcategory"
    valid_pl_amounts = set(df_dim_accounts.PL_Description_SubCategory.unique())
    unique_pl_amounts = set(df.PL_Account.unique())
    difference = unique_pl_amounts.difference(valid_pl_amounts)
    assert difference == set({}), f"Warning, some PL_Values are not within difference: {difference}"
    
    df["Project_Name"] = sheet_name
    
    #pending: get company name
    #pending: get FX and convert to USD
    #pending: check that company names are within allowed values
    #pending: check that PL_Account names are within allowed values (subcategories)
    
    return df

def transform_revenue_opex(df_concat, list_of_months, df_selector, df_dim_project, df_fx):
    #iterate over sheets
    df = (pd.melt(
        df_concat,
        id_vars=["PL_Account", "Project_Name", "Text_Description"],
        value_vars=list_of_months,
        var_name='Date',
        value_name='LC_Amount'))
    df["Date"] = pd.to_datetime(df.Date, format="%b-%y")
    df = df[df.LC_Amount.notnull()]
    df = df[df.LC_Amount != 0]
    df.reset_index(inplace=True, drop=True)
    df["LC_Amount"] = df["LC_Amount"].round(4)

    #add company name
    #df_selector.rename(columns={"PROJECT": "Project_Name", "SPV": "Company_Name"}, inplace=True)
    #selected_merge = ["Project_Name", "Company_Name"]
    #df = df.merge(df_selector[selected_merge], on="Project_Name", how="left")
    #check_nulls(df, "Company_Name")

    #add currency
    selected_merge = ["Project_Name", "Currency"]
    df = df.merge(df_dim_project[selected_merge], on="Project_Name", how="left")
    check_nulls(df, "Currency")

    #add fx
    df = df.merge(df_fx, on="Currency", how="left")
    check_nulls(df, "FX")

    #add USD amount
    df["USD_Amount"] = df["LC_Amount"] / df["FX"]
    df["USD_Amount"] = df["USD_Amount"].round(4)

    #drop unnecesary columns
    df.drop(["Currency", "FX"], axis=1, inplace=True)

    #order columns
    ordered_columns = [
        "PL_Account", "Date",
        "LC_Amount", "USD_Amount", "Project_Name",
        "Text_Description"
    ]
    df = df[ordered_columns]

    return df

#capex and devex functions

