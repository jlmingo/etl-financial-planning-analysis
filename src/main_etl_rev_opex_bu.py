import pandas as pd
import os
from numpy import nan as np_nan
from datetime import datetime, date
import calendar
from inputs import (path_asset_management_input, path_amortization, YEAR,
df_dim_accounts, df_selector, df_dim_company, dim_fx, companies_dim_company, output_path, scenario)
from functions_etl_bu import check_nulls, get_revenue_opex, transform_revenue_opex

#TODO: add cash flow figures
#TODO: REMOVE df_selector filtered by SPV WHEN COMPANIES ARE OK! filter out companies that are not in dim_company and convert to assert
companies_not_in_dim_company = df_selector[~df_selector.Project_Name.isin(companies_dim_company)].Project_Name.unique()
if len(companies_not_in_dim_company) > 0:
    print("Companies in SPV selector but not in dim_company (these will not be included):")
    print(companies_not_in_dim_company)
df_selector = df_selector[df_selector.Project_Name.isin(companies_dim_company)]
#TODO: ASSERT TO ENSURE PL_ACCOUNTS ARE INCLUDED IN DIM_ACCOUNTS
#TODO: check that companies in tab have allowed values in DIM_COMPANY

#get revenue and opex from all sheets
list_df = []
for sheet_name in df_selector.Project_Name.unique():
    df = get_revenue_opex(path_asset_management_input, sheet_name, YEAR, df_dim_accounts)
    list_df.append(df)
list_of_months = [x[0:3] + "-" + str(YEAR)[-2:] for x in list(calendar.month_name) if x != ""]
df_concat = pd.concat(list_df, ignore_index=True)

#transform df_concat
df_revenue_opex = transform_revenue_opex(df_concat, list_of_months, df_selector, df_dim_company, dim_fx)

#add amortization
df_amortization = pd.read_excel(path_amortization, sheet_name="Dataload")
list_of_months = [x[0:3] + "-" + str(YEAR)[-2:] for x in list(calendar.month_name) if x != ""]
cols_selected_year = [col for col in df_amortization.columns if type(col) == datetime and col.year == YEAR]
rename_selected_year = [col.strftime("%b-%y") for col in df_amortization.columns if type(col) == datetime and col.year == YEAR]
dict_rename_dates = dict(zip(cols_selected_year, rename_selected_year))
df_amortization.rename(columns=dict_rename_dates, inplace=True)
df_amortization = df_amortization.melt(
    id_vars = ["Project_Name", "PL_Account"],
    value_vars = list_of_months,
    var_name = "Date",
    value_name = "USD_Amount"
)
df_amortization = df_amortization[df_amortization.USD_Amount != 0]
df_amortization.reset_index(drop=True, inplace=True)

#correct date
df_amortization["Date"] = pd.to_datetime(df_amortization.Date, format="%b-%y")

#concat
df_revenue_opex = pd.concat([df_revenue_opex, df_amortization], ignore_index=True)

#eliminate Colombia
idx_drop = df_revenue_opex[(df_revenue_opex.Project_Name.str.contains("LOS LLANOS")) & (df_revenue_opex.Date.dt.month > 6)].index
df_revenue_opex.drop(idx_drop, inplace=True)
df_revenue_opex.reset_index(drop=True, inplace=True)

#output file
statement_line = "revenue_opex"
output_path_csv = os.path.join(output_path, scenario + "_" + statement_line + ".csv")
output_path_parquet = os.path.join(output_path, scenario + "_" + statement_line + ".parquet")
df_revenue_opex.to_csv(output_path_csv, index=False)
df_revenue_opex.to_parquet(output_path_parquet, index=False)