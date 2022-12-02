import calendar
import pandas as pd
from numpy import nan as np_nan
import numpy as np
import datetime

#general functions
def check_nulls(df, col):
    assert df[df[col].isnull()].shape[0] == 0, f"Warning, nulls in {col}."

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
    if sheet_name != "GASKELL":
        df = pd.read_excel(path_asset_management_input, sheet_name=sheet_name, usecols="B:R", skiprows=range(76), dtype=dtype)
    elif sheet_name == "GASKELL":
        df = pd.read_excel(path_asset_management_input, sheet_name=sheet_name, usecols="B:R", skiprows=range(88), dtype=dtype)

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
    #TODO: check if the below works (Other Professional Services is now removed)
    drop_values = [
        "Total Revenue", "Gross Profit", "EBITDA"
    ]
    
    idx_drop = df[df.PL_Account.isin(drop_values)].index
    df.drop(idx_drop, inplace=True)
    df.reset_index(drop=True, inplace=True)

    #substitute zeros by nulls
    # df[list_of_months] = df[list_of_months].replace(0, np.nan)

    #drop rows where all values are null
    df.dropna(how='all', inplace=True)
    df.reset_index(drop=True, inplace=True)

    #drop rows where values in months are null
    df = df[~df[list_of_months].isnull().all(1)]
    
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

#TODO: remove df_selector from parameters, comments and the main execution
def transform_revenue_opex(df_concat, list_of_months, df_selector, df_dim_company, df_fx):

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
    df = df.merge(df_dim_company[selected_merge], on="Project_Name", how="left")
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
def get_capex_sheet_data(df, sheet_name, year, only_devex_tabs):
    
    print(f"Executing transform function for {sheet_name}")


    #df = pd.read_excel(path_capex, sheet_name=sheet_name, skiprows=range(3))
    
    #prepare columns and renames
    list_of_months = [x[0:3] + "-" + str(year)[-2:] for x in list(calendar.month_name) if x != ""]
    cols_selected_year = [col for col in df.columns if type(col) == datetime.datetime and col.year == year]
    rename_selected_year = [col.strftime("%b-%y") for col in df.columns if type(col) == datetime.datetime and col.year == year]
    # cols_selected_year = [col for col in df.columns if type(col) == datetime.datetime]
    # rename_selected_year = [col.strftime("%b-%y") for col in df.columns if type(col) == datetime.datetime ]
    dict_rename_dates = dict(zip(cols_selected_year, rename_selected_year))
    df.rename(columns=dict_rename_dates, inplace=True)
    
    #Select and rename columns
    selected_columns = [
        "Project Name ",
        "Cash Item ", "Milestones", "%Milestone", 
    ]
    selected_columns.extend(rename_selected_year)
    df = df[selected_columns]
    rename_columns = {
        "Project Name ": "Project_Name",
        "Cash Item ": "Cash_Item"
    }
    df.rename(columns=rename_columns, inplace=True)

    #TODO remove this manual step when data is corrected in the source
    #correct USA
    if sheet_name == "USA":
        df["Project_Name"] = "USA"

    #Correct project names
    if sheet_name == "ALTEN GREENFIELD":
        df["Project_Name"] = "ALTEN GREENFIELD"

    #devex data
    a = df[df.Cash_Item == "Development Payments "].index
    assert len(a) == 1
    a = a[0]

    b = df[df.Cash_Item == "Total Cash Flow Developmemnt "].index
    assert len(b) == 1
    b = b[0]

    df_devex = df.iloc[a+1:b,:].copy()
    df_devex["Investment_Type"] = "DEVEX"

    #correct VAT lines in Investment_Type for devex
    df_devex["Investment_Type"] = np.where(
        df_devex["Cash_Item"].str.contains("VAT"),
        "VAT DEVEX",
        df_devex["Investment_Type"]
    )

    #capex data - only for selected tabs
    if sheet_name not in only_devex_tabs:
        a = df[df.Cash_Item == "EPC Payments "].index
        assert len(a) == 1
        a = a[0]

        b = df[df.Cash_Item == "Total Cash Flow EPC "].index
        assert len(b) == 1
        b = b[0]

        df_capex = df.iloc[a+1:b, :].copy()
        df_capex["Investment_Type"] = "CAPEX"

        #correct VAT lines in Investment_Type for devex
        df_capex["Investment_Type"] = np.where(
            df_capex["Cash_Item"].str.contains("VAT"),
            "VAT CAPEX",
            df_capex["Investment_Type"]
        )

        #concat dataframes
        df = pd.concat([df_devex, df_capex], ignore_index=True)
        df.reset_index(drop=True, inplace=True)
    
    else:
        df = df_devex
    
    #correct VAT lines in Investment_Type
    #df["Investment_Type"] = np.where(
    #    df["Cash_Item"].str.contains("VAT"),
    #    "VAT",
    #    df["Investment_Type"]
    #)

    #melt table
    id_vars_values = ["Project_Name", "Cash_Item", "Milestones", "%Milestone", "Investment_Type"]
    df = (pd.melt(
        df,
        id_vars=id_vars_values,
        value_vars=rename_selected_year,
        var_name="Date",
        value_name="Amount"))
    
    #correct data where there is a dash instead of a 0
    df.loc[:, "Amount"] = np.where(df.Amount == "-", 0, df.Amount)
    df["Amount"] = df["Amount"].astype("float64")
    df.Amount.fillna(0, inplace=True)
    df.loc[:, "Amount"] = df.Amount.round(4)
    df = df[df.Amount != 0]

    #correct date
    df["Date"] = pd.to_datetime(df.Date, format="%b-%y")
    
    return df

#capex and devex functions
def get_total_capex(df, sheet_name, year, only_devex_tabs):
    
    print(f"Executing transform function for {sheet_name}")


    #df = pd.read_excel(path_capex, sheet_name=sheet_name, skiprows=range(3))
    
    #prepare columns and renames
    list_of_months = [x[0:3] + "-" + str(year)[-2:] for x in list(calendar.month_name) if x != ""]
    cols_selected_year = [col for col in df.columns if type(col) == datetime.datetime and col.year == year]
    rename_selected_year = [col.strftime("%b-%y") for col in df.columns if type(col) == datetime.datetime and col.year == year]
    dict_rename_dates = dict(zip(cols_selected_year, rename_selected_year))
    df.rename(columns=dict_rename_dates, inplace=True)
    
    #Select and rename columns
    selected_columns = [
        "Project Name ",
        "Cash Item ", "Milestones", "%Milestone", 
    ]
    selected_columns.extend(rename_selected_year)
    df = df[selected_columns]
    rename_columns = {
        "Project Name ": "Project_Name",
        "Cash Item ": "Cash_Item"
    }
    df.rename(columns=rename_columns, inplace=True)

    #TODO remove this manual step when data is corrected in the source
    #correct USA
    if sheet_name == "USA":
        df["Project_Name"] = "USA"

    #Correct project names
    if sheet_name == "ALTEN GREENFIELD":
        df["Project_Name"] = "ALTEN GREENFIELD"

    #devex data
    a = df[df.Cash_Item == "Development Payments "].index
    assert len(a) == 1
    a = a[0]

    b = df[df.Cash_Item == "Total Cash Flow Developmemnt "].index
    assert len(b) == 1
    b = b[0]

    df_devex = df.iloc[a+1:b,:].copy()
    df_devex["Investment_Type"] = "DEVEX"

    #correct VAT lines in Investment_Type for devex
    df_devex["Investment_Type"] = np.where(
        df_devex["Cash_Item"].str.contains("VAT"),
        "VAT DEVEX",
        df_devex["Investment_Type"]
    )

    #capex data - only for selected tabs
    if sheet_name not in only_devex_tabs:
        a = df[df.Cash_Item == "EPC Payments "].index
        assert len(a) == 1
        a = a[0]

        b = df[df.Cash_Item == "Total Cash Flow EPC "].index
        assert len(b) == 1
        b = b[0]

        df_capex = df.iloc[a+1:b, :].copy()
        df_capex["Investment_Type"] = "CAPEX"

        #correct VAT lines in Investment_Type for devex
        df_capex["Investment_Type"] = np.where(
            df_capex["Cash_Item"].str.contains("VAT"),
            "VAT CAPEX",
            df_capex["Investment_Type"]
        )

        #concat dataframes
        df = pd.concat([df_devex, df_capex], ignore_index=True)
        df.reset_index(drop=True, inplace=True)
    
    else:
        df = df_devex
    
    #correct VAT lines in Investment_Type
    #df["Investment_Type"] = np.where(
    #    df["Cash_Item"].str.contains("VAT"),
    #    "VAT",
    #    df["Investment_Type"]
    #)

    #melt table
    id_vars_values = ["Project_Name", "Cash_Item", "Milestones", "%Milestone", "Investment_Type"]
    df = (pd.melt(
        df,
        id_vars=id_vars_values,
        value_vars=rename_selected_year,
        var_name="Date",
        value_name="Amount"))
    
    #correct data where there is a dash instead of a 0
    df.loc[:, "Amount"] = np.where(df.Amount == "-", 0, df.Amount)
    df["Amount"] = df["Amount"].astype("float64")
    df.Amount.fillna(0, inplace=True)
    df.loc[:, "Amount"] = df.Amount.round(4)
    df = df[df.Amount != 0]

    #correct date
    df["Date"] = pd.to_datetime(df.Date, format="%b-%y")
    
    return df