import calendar
import pandas as pd
from numpy import nan as np_nan
import numpy as np
import datetime

#general functions
def check_nulls(df, col):
    assert df[df[col].isnull()].shape[0] == 0, f"Warning, nulls in {col}. \n {df[df[col].isnull()]}"

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

    #drop rows where all values in months are 0
    df = df[~(df[list_of_months] == 0).all(1)]
    
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
    
    #vamos a coger enero 2024 y lo vamos a asumir como capex incurrido en 2023 pero no pagado hasta 2024
    #prepare columns and renames
    cols_selected_year = [col for col in df.columns if type(col) == datetime.datetime and (type(col) == datetime.datetime) and ((col.year == year) or (col.year == year+1 and col.month == 1))]
    rename_selected_year = [col.strftime("%b-%y") for col in df.columns if type(col) == datetime.datetime and ((col.year == year) or (col.year == year+1 and col.month == 1))]
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

    if sheet_name == "AS13":
            df["Project_Name"] = "AS13"

    if sheet_name == "AS2":
        df["Project_Name"] = "AS2"

    if sheet_name == "AS6":
            df["Project_Name"] = "AS6"

    if sheet_name == "AS1":
            df["Project_Name"] = "AS1"

    if sheet_name == "PMGDs":
            df["Project_Name"] = "ACQ_CHILE"

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
    
    #filter 2024 data diferent from capex
    condition_filter = (df.Date.dt.year == 2024) & (df.Investment_Type != "CAPEX") 
    index_drop = df[condition_filter].index
    df.drop(index_drop, inplace=True)
    df.reset_index(drop=True, inplace=True)

    #set deferment flag
    df["Deferment_Flag"] = np.where(df["Date"].dt.year == 2024, "DEFERRED_23_PAID_24", "DEFERRED_23_PAID_23")
    df["Date"] = np.where(df["Date"].dt.year == 2024, df["Date"] + pd.DateOffset(months=-1), df["Date"])
    
    #set deferment flag for Gaskell or Zaratan Devex
    condition_gaskell = (df["Project_Name"] == "Gaskell") & (df["Date"].dt.month <= 2)
    condition_zaratan_devex = ((df["Project_Name"] == "ZARATAN") & (df["Investment_Type"] == "DEVEX"))
    df["Deferment_Flag"] = np.where(condition_gaskell | condition_zaratan_devex, "DEFERRED_22_PAID_23", df["Deferment_Flag"])
    #df["Deferment_Flag"] = np.where(condition_zaratan_devex, "DEFERRED_22_PAID_23", df["Deferment_Flag"])

    #correct for VAT
    df["Deferment_Flag"] = np.where(~df["Investment_Type"].isin(["CAPEX", "DEVEX"]), np.nan, df["Deferment_Flag"])

    return df

def get_capex_sheet_data_2024(df, sheet_name, year):
    
    print(f"Executing transform function for {sheet_name}")
    
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

    #capex data
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


def get_total_capex(path, sheet_name, only_devex_tabs):
    print(f"Reading {sheet_name}...")
    select_cols = [
        "Project Name ", "Cash Item ", "Total "
    ]

    select_lines = [
        "VAT payments ", "VAT Received", "Total Cash Flow Developmemnt ", "Total Cash Flow EPC "
    ]
    
    #read dataframe
    df = pd.read_excel(path, sheet_name=sheet_name, skiprows=range(3), usecols=select_cols)
    
    rename_cols = {
        "Project Name ": "Project_Name",
        "Cash Item ": "Cash_Item",
        "Total ": "Amount"
    }
    df = df.rename(columns=rename_cols)
    df = df[df["Cash_Item"].isin(select_lines)]
    
    print("Project Name shown in tab:")
    print(df.Project_Name.unique())

    #drop rows beyond total cash flow development index for sheets in only devex tabs
    index_devex = df[df.Cash_Item == "Total Cash Flow Developmemnt "].index[0]
    df.loc[0:index_devex, "Investment_Type"] = "DEVEX"
    if sheet_name in only_devex_tabs:
        df = df[df.index <= index_devex]
        df.reset_index(drop=True, inplace=True)
    else:
        df.loc[index_devex+1:, "Investment_Type"] = "CAPEX"
    
    #set VAT values
    df["Investment_Type"] = np.where(
    (df["Investment_Type"] == "DEVEX") & (df["Cash_Item"].str.contains("VAT")),
    "VAT DEVEX",
    np.where(
        (df["Investment_Type"] == "CAPEX") & (df["Cash_Item"].str.contains("VAT")),
        "VAT CAPEX",
        df["Investment_Type"]
        )
    )

    df["Amount"] = df["Amount"].round(4)
    df = df[df.Amount != 0]
    df = df[df.Amount.notnull()]
    
    df.reset_index(drop=True, inplace=True)

    if sheet_name == "USA":
        df["Project_Name"] = "USA"

    #Correct project names
    if sheet_name == "ALTEN GREENFIELD":
        df["Project_Name"] = "ALTEN GREENFIELD"

    if sheet_name == "AS13":
                df["Project_Name"] = "AS13"

    if sheet_name == "AS2":
        df["Project_Name"] = "AS2"

    if sheet_name == "AS6":
            df["Project_Name"] = "AS6"

    if sheet_name == "AS1":
            df["Project_Name"] = "AS1"

    return df

def return_col_dates(df, year):
    list_of_months = [x[0:3] + "-" + str(year)[-2:] for x in list(calendar.month_name) if x != ""]
    cols_selected_year = [col for col in df.columns if type(col) == datetime.datetime and col.year == year]
    rename_selected_year = [col.strftime("%b-%y") for col in df.columns if type(col) == datetime.datetime and col.year == year]
    dict_rename_dates = dict(zip(cols_selected_year, rename_selected_year))
    return dict_rename_dates, cols_selected_year

def process_financing(path_financing, dict_params, year):
    sheet_name = dict_params["sheet_name"]
    investment_type = dict_params["Investment_Type"]
    skiprows = dict_params["skiprows"]
    
    df = pd.read_excel(path_financing, sheet_name=sheet_name, skiprows=skiprows)
    
    dict_rename_dates, cols_selected_year = return_col_dates(df, year)
    df.rename(columns=dict_rename_dates, inplace=True)

    select_cols = ["Project_Name"]
    select_cols.extend(dict_rename_dates.values())
    df = df[select_cols]

    for col in df.columns:
        if "Unnamed" in col:
            df.drop(col, inplace=True, axis=1)
    
    if "Total" in df.columns:
        df.drop("Total", inplace=True, axis=1)

    if "Has Cash Flow from Debt in 2023" in df.columns:
        df.drop("Has Cash Flow from Debt in 2023", inplace=True, axis=1)

    if "Debt Inflow First Date" in df.columns:
        df.drop("Debt Inflow First Date", inplace=True, axis=1)

    df = df[df.Project_Name != "TOTAL"]
    df.reset_index(drop=True, inplace=True)
    
    list_of_months = [x[0:3] + "-" + str(year)[-2:] for x in list(calendar.month_name) if x != ""]
    
    df = df[~(df[list_of_months] == 0).all(1)]
    df = df[~df[list_of_months].isnull().all(1)]
    df.reset_index(drop=True, inplace=True)
    
    df["Investment_Type"] = investment_type

    return df


def return_col_dates_all_years(df):
    years = set([col.year for col in df.columns if type(col) == datetime.datetime])
    total_list_month = []
    for year in years:
        list_of_months = [x[0:3] + "-" + str(year)[-2:] for x in list(calendar.month_name) if x != ""]
        total_list_month.extend(list_of_months)
    cols_selected_year = [col for col in df.columns if type(col) == datetime.datetime]
    rename_selected_year = [col.strftime("%b-%y") for col in df.columns if type(col) == datetime.datetime]
    dict_rename_dates = dict(zip(cols_selected_year, rename_selected_year))
    return dict_rename_dates

def get_capex_usa_by_project(path_usa_by_project, year):
    
    #read excel
    df = pd.read_excel(path_usa_by_project, sheet_name="Summary", skiprows=range(1))
    
    #remove rows corresponding to totals
    df = df.iloc[20:]
    df.reset_index(drop=True, inplace=True)
    
    #rename columns with dates
    dict_rename_dates = return_col_dates_all_years(df)
    df.rename(columns=dict_rename_dates, inplace=True)
    df.rename(columns={"Unnamed: 0": "Project_Name", "Unnamed: 1": "FNTP_Date"}, inplace=True)
    df.drop(["Unnamed: 2", "Unnamed: 3"], inplace=True, axis=1)
    
    #get values in USD from USD millions
    df[list(dict_rename_dates.values())] = df[list(dict_rename_dates.values())].fillna(0).multiply(1000000)
    
    #get indexes for projects
    idx = df[df['Project_Name']=="Development Costs"].index
    idx_projects = [i-1 for i in idx]
    
    #indexes for each relevant line
    index_dev_costs = 1
    index_devfee_retainer = 2
    index_post_ntp_capex = 3
    index_down_payment_modules = 4
    index_down_payment_epc = 5
    index_inc_fntp = 8
    index_inc_equity = 15
    index_inc_debt = 16

    selected_indexes = [1, 2, 3, 4, 5, 8, 15, 16]

    def get_selected_indexes(idx, selected_indexes):
        return [idx+selected_index for selected_index in selected_indexes]
    
    #iterate over dataframes to get information from every project
    list_df = []
    for project_idx in idx_projects:
        #get project name
        project_name = df.loc[project_idx, "Project_Name"]

        #get indexes for project's dataframe
        selected_indexes_projects = get_selected_indexes(project_idx, selected_indexes)
        df_project = df[df.index.isin(selected_indexes_projects)].copy().reset_index(drop=True)

        #get fntp date
        fntp_date = df_project.loc[df_project[df_project.Project_Name == "FNTP Flag"].index[0], "FNTP_Date"]
        df_project["FNTP_Date"] = fntp_date

        #rename columns and assign project name
        df_project.rename(columns={"Project_Name": "Cash_Item"}, inplace=True)
        df_project["Project_Name"] = project_name

        #append dataframe to list
        list_df.append(df_project)

    df_concat = pd.concat(list_df, ignore_index=True)
    df_concat = df_concat[df_concat.Cash_Item != "FNTP Flag"].reset_index(drop=True)
    
    #mapping values for Investment_Type column
    map_cash_item = {
    'Development Costs': "DEVEX",
    'DevFee + Retainer': "DEVEX",
    'Post NTP-CapEx': "CAPEX",
    'Down Payment on Modules': "CAPEX",
    'Down Payment on EPC': "CAPEX",
    'Actual Monthly Debt Raise': "Debt",
    'Actual Monthly Equity Raise': "Equity"
    }
    
    #replacement values for lines written differently from origin
    replace_values = {
        "Retainer + DevFee": "DevFee + Retainer",
        "Post-NTP CAPEX": "Post NTP-CapEx"
    }
    for key, value in replace_values.items():
        df_concat["Cash_Item"] = df_concat["Cash_Item"].replace(key, value, regex=False)
    s1 = set(df_concat.Cash_Item.unique())
    s2 = set(map_cash_item.keys())
    assert s1.issubset(s2), s1.difference(s2)
    
    #create Investment_Type column
    df_concat["Investment_Type"] = ""
    df_concat["Investment_Type"] = df_concat["Cash_Item"].map(map_cash_item)
    
    #melt dataframe
    id_vars_values = ["Project_Name", "Investment_Type", "FNTP_Date", "Cash_Item"]
    df_melt = pd.melt(
        df_concat,
        id_vars=id_vars_values,
        value_vars=dict_rename_dates.values(),
        var_name="Date",
        value_name="USD_Amount"
    )
    
    #datetime format
    df_melt["Date"] = pd.to_datetime(df_melt["Date"], format="%b-%y")
    df_melt["FNTP_Date"] = pd.to_datetime(df_melt["FNTP_Date"], format="%b-%y")
    
    #rounding and remove zeros
    df_melt["USD_Amount"] = df_melt["USD_Amount"].round(4)
    df_melt = df_melt[df_melt["USD_Amount"] != 0]
    df_melt.reset_index(drop=True, inplace=True)

    df_melt["Developer"] = "USA"

    df_melt["LC_Amount"] = df_melt["USD_Amount"]

    df = df_melt.copy()

    #assign Deferment_Flag to capex 2024 (converting it to capex 2023)
    #set deferment flag
    condition_jan_24 = (df["Date"].dt.year == 2024) & (df["Date"].dt.month == 1) & (df["Investment_Type"] == "CAPEX")
    condition_deferment = (df["Date"].dt.year == 2024) & (df["Date"].dt.month == 1) & (df["Investment_Type"] == "CAPEX")
    df["Deferment_Flag"] = np.where(condition_deferment, "DEFERRED_23_PAID_24", "DEFERRED_23_PAID_23")
    df["Date"] = np.where(condition_jan_24, df["Date"] + pd.DateOffset(months=-1), df["Date"])

    df["Deferment_Flag"] = np.where(df["Investment_Type"].isin(["Debt", "Equity"]), np.nan, df["Deferment_Flag"])

    return df

def calculate_adjustment_to_capex(df):
    df_adj = df.copy()
    
    df_adj = df_adj[df_adj["FNTP_Date"].notnull()]

    ## adjustment to devex before financial closing ##
    df_adj = df_adj[(df_adj.Date <= df_adj["FNTP_Date"]) & (df_adj.Investment_Type == "DEVEX")]
    
    group_cols = [col for col in df_adj.columns if col not in ["Date", "USD_Amount", "LC_Amount"]]
    df_adj.drop("Date", inplace=True, axis=1)
    df_adj = df_adj.groupby(group_cols, as_index=False, dropna=False).sum()

    #adjustment to capex
    df_adj["Investment_Flag"] = "WIP_to_PPE"
    df_adj["Date"] = df_adj["FNTP_Date"]
    #df_adj.rename(columns={"FNTP_Date": "Date"}, inplace=True)
    df_adj_capex = df_adj.copy()
    df_adj_capex["Investment_Type"] = "CAPEX"

    #adjustment to devex
    df_adj["USD_Amount"] = df_adj["USD_Amount"].multiply(-1)
    df_adj["LC_Amount"] = df_adj["LC_Amount"].multiply(-1)

    #total adjustment until financial close
    df_adj = pd.concat([df_adj, df_adj_capex], ignore_index=True)
    df_adj.reset_index(drop=True, inplace=True)
    
    ## adjustments post FNTP ##
    df_adj_2 = df.copy()
    df_adj_2 = df_adj_2[(df_adj_2.Date > df_adj_2["FNTP_Date"]) & (df_adj_2.Investment_Type == "DEVEX")]
    df_adj_2["Investment_Flag"] = "WIP_to_PPE"
    #df_adj_2.rename(columns={"FNTP_Date": "Date"}, inplace=True)

    df_adj_2_capex = df_adj_2.copy()
    df_adj_2_capex["Investment_Type"] = "CAPEX"

    #adjustment to devex
    df_adj_2["USD_Amount"] = df_adj_2["USD_Amount"].multiply(-1)
    df_adj_2["LC_Amount"] = df_adj_2["LC_Amount"].multiply(-1)

    #total adjustment until financial close
    df_adj_2 = pd.concat([df_adj_2, df_adj_2_capex], ignore_index=True)
    df_adj_2.reset_index(drop=True, inplace=True)
    list_concat = [dataframe for dataframe in [df_adj, df_adj_2] if not dataframe.empty]
    df_final = pd.concat(list_concat, ignore_index=True)
    
    df_final = df_final[df_final.USD_Amount != 0].reset_index(drop=True)
    df_final = df_final[df_final.notnull()].reset_index(drop=True)
    
    return df_final