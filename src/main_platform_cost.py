import pandas as pd
import calendar
import datetime
import os
import numpy as np
from inputs import (path_platform_cost, df_dim_accounts, df_dim_department,
df_dim_company, YEAR, path_revenue_opex, output_path, scenario)
from functions_etl_bu import return_col_dates, check_nulls

#read main data
df_platform_cost = pd.read_excel(path_platform_cost, sheet_name="Dataload_Alt_FX", skiprows=range(8))
df_platform_cost = df_platform_cost[df_platform_cost.PL_Account.notnull()]
dict_rename_dates, cols_selected_year = return_col_dates(df_platform_cost, YEAR)

selected_cols = [
    "PL_Account", "Legal_Entity", "Service_Detail",
    "Department_Code"
]
selected_cols.extend(cols_selected_year)
df_platform_cost = df_platform_cost[selected_cols]

#check integrity - companies
companies = set(df_platform_cost.Legal_Entity.unique())
companies_dim = set(df_dim_company.Company_Name.unique())
assert companies.issubset(companies_dim), f"Warning, check integrity: {companies.difference(companies_dim)}"

#check integrity - departments
departments = set(df_platform_cost.Department_Code.unique())
departments_dim = set(df_dim_department.CODE.unique())
assert departments.issubset(departments_dim), f"Warning, check integrity: {departments.difference(departments_dim)}"

#check integrity - pl_accounts
pl_accounts = set(df_platform_cost.PL_Account.unique())
pl_accounts_dim = set(df_dim_accounts.PL_Description_SubCategory.unique())
assert pl_accounts.issubset(pl_accounts_dim), f"Warning, check integrity: {pl_accounts.difference(pl_accounts_dim)}"

#rename columns
df_platform_cost.rename(columns={"Legal_Entity": "Company_Name", "Service_Detail": "Text_Description"}, inplace=True)

#look for project_name
df_platform_cost = df_platform_cost.merge(df_dim_company[["Project_Name", "Company_Name", "Company_Type"]], on="Company_Name", how="left")
check_nulls(df_platform_cost, "Company_Name")
check_nulls(df_platform_cost, "Company_Type")

#look for subcategory level_1 detail
df_dim_accounts.rename(columns={"PL_Description_SubCategory": "PL_Account"}, inplace=True)
df_platform_cost = df_platform_cost.merge(df_dim_accounts[["PL_Account", "LEVEL_1"]], on="PL_Account", how="left")
check_nulls(df_platform_cost, "LEVEL_1")

#assert all costs are within operating cost
assert set(df_platform_cost.LEVEL_1.unique()) == {"Operating Cost"}, set(df_platform_cost.LEVEL_1.unique()).difference({"Operating Cost"})

#assert all companies are HoldCo
assert set(df_platform_cost.Company_Type.unique()) == {"HoldCo"}, set(df_platform_cost.Company_Type.unique()).difference({"HoldCo"})

#rename date columns
#prepare columns and renames
list_of_months = [x[0:3] + "-" + str(YEAR)[-2:] for x in list(calendar.month_name) if x != ""]
cols_selected_year = [col for col in df_platform_cost.columns if type(col) == datetime.datetime and col.year == YEAR]
rename_selected_year = [col.strftime("%b-%y") for col in df_platform_cost.columns if type(col) == datetime.datetime and col.year == YEAR]
dict_rename_dates = dict(zip(cols_selected_year, rename_selected_year))
df_platform_cost.rename(columns=dict_rename_dates, inplace=True)

col_drop = ["Company_Type", "LEVEL_1", "Company_Name"]
df_platform_cost.drop(col_drop, axis=1, inplace=True)
print(df_platform_cost)
print(df_platform_cost.columns)

# treat data
df_platform_cost = (pd.melt(
        df_platform_cost,
        id_vars=["PL_Account", "Project_Name", "Text_Description"],
        value_vars=list_of_months,
        var_name='Date',
        value_name='USD_Amount'))

df_platform_cost["Date"] = pd.to_datetime(df_platform_cost.Date, format="%b-%y")
df_platform_cost = df_platform_cost[df_platform_cost.USD_Amount.notnull()]
df_platform_cost = df_platform_cost[df_platform_cost.USD_Amount != 0]
df_platform_cost.reset_index(inplace=True, drop=True)
df_platform_cost["USD_Amount"] = df_platform_cost["USD_Amount"].round(4).multiply(-1)

#get revenue opex file
df_revenue_opex = pd.read_parquet(path_revenue_opex)

#concat and output
df_revenue_opex_platform = pd.concat([df_revenue_opex, df_platform_cost], ignore_index=True)
print(df_revenue_opex_platform.columns)


#OUTPUT revenue-opex + platform_cost
statement_line = "revenue_opex_platform"
output_path_csv = os.path.join(output_path, scenario + "_" + statement_line + ".csv")
output_path_parquet = os.path.join(output_path, scenario + "_" + statement_line + ".parquet")
df_revenue_opex_platform.to_csv(output_path_csv, index=False)
df_revenue_opex_platform.to_parquet(output_path_parquet, index=False)
