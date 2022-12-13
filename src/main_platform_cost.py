import pandas as pd
import calendar
import datetime
import numpy as np
from inputs import path_platform_cost, df_dim_accounts, df_dim_department, df_dim_company, YEAR
from functions_etl_bu import return_col_dates

#read main data
df_platform_cost = pd.read_excel(path_platform_cost, sheet_name="Dataload", skiprows=range(8))

dict_rename_dates, cols_selected_year = return_col_dates(df_platform_cost, 2023)

selected_cols = [
    "PL_Account", "Legal_Entity", "Service_Detail",
    "Department_Code"
]
selected_cols.extend(cols_selected_year)
df_platform_cost = df_platform_cost[selected_cols]

#check integrity - companies
companies = set(df_platform_cost.Legal_Entity.unique())
companies_dim = set(df_dim_company.Company_Name.unique())
assert companies == companies_dim, f"Warning, check integrity: {companies.difference(companies_dim)}"

#check integrity - departments
departments = set(df_platform_cost.Department_Code.unique())
departments_dim = set(df_dim_department.Department_Code.unique())
assert departments == departments_dim, f"Warning, check integrity: {departments.difference(departments)}"

#check integrity - pl_accounts
pl_accounts = set(df_platform_cost.PL_Account.unique())
pl_accounts_dim = set(df_dim_accounts.PL_Account.unique())
assert pl_accounts == pl_accounts_dim, f"Warning, check integrity: {pl_accounts.difference(pl_accounts_dim)}"