import pandas as pd
import numpy as np
import os
from functions_etl_bu import get_capex_sheet_data, check_nulls
from inputs import (df_dim_company, dim_fx, YEAR, path_capex_global, dim_country_currency,
dim_project_capex, path_capex_chile, output_path, scenario, filter_out, companies_dim_company_capex, only_devex_tabs)

#general values
cash_items = {
    "Development Payments ", "Total Cash Flow Developmemnt ",
    "EPC Payments ", "Total Cash Flow EPC "
}

#global capex
excel_file = pd.ExcelFile(path_capex_global)
projects_capex = set(excel_file.sheet_names)

#GLOBAL tabs where only devex is taken
only_devex_tabs = [
    "ALTEN GREENFIELD", "ROLWIND GREENFIELD", "SAN GIULIANO",
    "EN494a", "EN494c", "MOLE", "TP02",
    "CALTO", "CASTELGOFF2", "BOSARO", "ROVIGO", "VALSAMOGGIA",
    "FR01", "TR01", "ENNA1",
    "SIGNORA", "SPARACIA", "VALLATA", "ISCHIA DI CASTRO", "RANDAZZO 1"
]

assert set(only_devex_tabs).issubset(projects_capex), "Warning, review values."

#exclude detected tabs not to be included
exclude_tabs = ["SUMMARY USD", "SUMMARY LCY", "INDEX", "PROJECT DB", "SCENARIOS", "BAJO I", "BAJO II", "ASSUMPTIONS"]

#get values only for analysis
projects_to_analyze = [project for project in projects_capex if project not in exclude_tabs and ">>>" not in project]

###################                      ###################
################### PROCESS GLOBAL CAPEX ###################
###################                      ###################

list_df = []
list_sheets_not_used = []
for sheet_name in projects_to_analyze:
    print("\n")
    print(f"///READING {sheet_name}///")
    df = pd.read_excel(path_capex_global, sheet_name=sheet_name, skiprows=range(3))
    
    #check of Cash Item is in columns
    if "Cash Item " not in df.columns:
        print(f"Warning: {sheet_name} will not be analyzed as column 'Cash Item' have not been detected.")
        list_sheets_not_used.append(sheet_name)
        continue
    
    values_cash_items = set(df["Cash Item "].unique())
    
    #check if all required cash items are present in file
    if cash_items.issubset(values_cash_items):
        print(f"Treating {sheet_name}.")
        try:
            df = get_capex_sheet_data(df.copy(), sheet_name, YEAR, only_devex_tabs)
            list_df.append(df)
        except Exception as e:
            print(f"{sheet_name} will not be treated due to the following exception:")
            print(e)
            list_sheets_not_used.append(sheet_name)
            continue
        
    else:
        print(f"Warning: {sheet_name} will not be analyzed as required cash items have not been detected.")
        list_sheets_not_used.append(sheet_name)

df_capex_global = pd.concat(list_df, ignore_index=True)

#Rename amount as data comes in local currency
df_capex_global.rename(columns={"Amount": "LC_Amount"}, inplace=True)

#checks
dif = set(df_capex_global.Project_Name.unique()).difference(set(dim_project_capex.Project_Name.unique()))
assert dif == set({}), dif

#get country for each and developer
df_capex_global = df_capex_global.merge(dim_project_capex[["Project_Name", "Developer", "Country"]], on="Project_Name", how="left")
check_nulls(df_capex_global, "Developer")
check_nulls(df_capex_global, "Country")

#get currency and fx
df_capex_global = df_capex_global.merge(dim_country_currency[["Country", "Currency"]], on="Country", how="left")
check_nulls(df_capex_global, "Currency")

df_capex_global = df_capex_global.merge(dim_fx[["Currency", "FX"]], on="Currency", how="left")
check_nulls(df_capex_global, "FX")

df_capex_global["USD_Amount"] = df_capex_global["LC_Amount"] / df_capex_global["FX"]

#drop columns not used
col_drop = ["Country", "Currency", "FX"]
df_capex_global.drop(col_drop, axis=1, inplace=True)

#TODO add check for sheets not used

df_capex_global.to_parquet("../output/BU23_capex_global.parquet", index=False)
df_capex_global.to_csv("../output/BU23_capex_global.csv", index=False)

###################                      ###################
################### PROCESS CHILE CAPEX ###################
###################                      ###################

#general values
cash_items = {
    "Development Payments ", "Total Cash Flow Developmemnt ",
    "EPC Payments ", "Total Cash Flow EPC "
}

#chile capex
excel_file = pd.ExcelFile(path_capex_chile)
projects_capex = set(excel_file.sheet_names)

#chile tabs where only devex is taken
# only_devex_tabs = [
#     "ALTEN GREENFIELD", "ROLWIND GREENFIELD", "SAN GIULIANO",
#     "EN494a", "EN494c", "MOLE", "TP02",
#     "CALTO", "CASTELGOFF2", "BOSARO", "ROVIGO", "VALSAMOGGIA",
#     "FR01", "TR01", "ENNA1",
#     "SIGNORA", "SPARACIA", "VALLATA", "ISCHIA DI CASTRO"
# ]

#assert set(only_devex_tabs).issubset(projects_capex), "Warning, review values."

#exclude detected tabs not to be included
exclude_tabs = ["SUMMARY USD", "SUMMARY LCY", "INDEX", "PROJECT DB", "SUMMARY"]

#get values only for analysis
projects_to_analyze = [project for project in projects_capex if project not in exclude_tabs and ">>>" not in project]

list_df = []
list_sheets_not_used = []
for sheet_name in projects_to_analyze:
    print("\n")
    print(f"///READING {sheet_name}///")
    df = pd.read_excel(path_capex_chile, sheet_name=sheet_name, skiprows=range(3))
    
    #check of Cash Item is in columns
    if "Cash Item " not in df.columns:
        print(f"Warning: {sheet_name} will not be analyzed as column 'Cash Item' have not been detected.")
        list_sheets_not_used.append(sheet_name)
        continue
    
    values_cash_items = set(df["Cash Item "].unique())
    
    #check if all required cash items are present in file
    if cash_items.issubset(values_cash_items):
        print(f"Treating {sheet_name}.")
        try:
            df = get_capex_sheet_data(df.copy(), sheet_name, YEAR, only_devex_tabs)
            list_df.append(df)
        except Exception as e:
            print(f"{sheet_name} will not be treated due to the following exception:")
            print(e)
            list_sheets_not_used.append(sheet_name)
            continue
        
    else:
        print(f"Warning: {sheet_name} will not be analyzed as required cash items have not been detected.")
        list_sheets_not_used.append(sheet_name)

df_capex_chile = pd.concat(list_df, ignore_index=True)

#Rename amount as data comes in local currency
df_capex_chile.rename(columns={"Amount": "USD_Amount"}, inplace=True)

#checks
dif = set(df_capex_chile.Project_Name.unique()).difference(set(dim_project_capex.Project_Name.unique()))
assert dif == set({}), dif

#get country for each and developer
df_capex_chile = df_capex_chile.merge(dim_project_capex[["Project_Name", "Developer"]], on="Project_Name", how="left")
check_nulls(df_capex_chile, "Developer")

#get currency and fx - for chile the data is in USD
df_capex_chile["Currency"] = "CLP"


df_capex_chile = df_capex_chile.merge(dim_fx[["Currency", "FX"]], on="Currency", how="left")
check_nulls(df_capex_chile, "FX")

df_capex_chile["LC_Amount"] = df_capex_chile["USD_Amount"] * df_capex_chile["FX"]
df_capex_chile["LC_Amount"] = df_capex_chile["LC_Amount"].round(4)

#drop columns not used
col_drop = ["Currency", "FX"]
df_capex_chile.drop(col_drop, axis=1, inplace=True)

#TODO add check for sheets not used

df_capex_chile.to_parquet("../output/BU23_capex_chile.parquet", index=False)
df_capex_chile.to_csv("../output/BU23_capex_chile.csv", index=False)

#total output
df_capex_total = pd.concat([df_capex_global, df_capex_chile], ignore_index=True)

#filter out projects that will not be included

assert set(filter_out).issubset(set(df_capex_total.Project_Name.unique()))

df_capex_total = df_capex_total[~df_capex_total.Project_Name.isin(filter_out)]
df_capex_total.reset_index(drop=True, inplace=True)
#set flag for and devex capex that should not be considered
#explanation: GASKELL  (devex+capex) and ZARATAN (only devex) were incurred in 2022. In 2023 they will be only cash.
#TODO: check this below! USA does not include GASKELL
condition = (df_capex_total["Project_Name"] == "USA") | ((df_capex_total["Project_Name"] == "ZARATAN") & (df_capex_total["Investment_Type"] == "DEVEX"))
df_capex_total["Only_Cash"] = np.where(condition, "Only_Cash", "Cash_and_Balance")

statement_line="capex_devex"

#OUTPUT FULL CAPEX-DEVEX
# output_path_csv = os.path.join(output_path, scenario + "_" + statement_line + "_full_life_" + ".csv")
# output_path_parquet = os.path.join(output_path, scenario + "_" + statement_line + "_full_life_" + ".parquet")
# df_capex_total.to_csv(output_path_csv, index=False)
# df_capex_total.to_parquet(output_path_parquet, index=False)

#remove colombia 6 last months
idx_drop = df_capex_total[(df_capex_total.Project_Name.str.contains("LOS LLANOS")) & (df_capex_total.Date.dt.month > 6)].index
df_capex_total.drop(idx_drop, inplace=True)
df_capex_total.reset_index(drop=True, inplace=True)

#add capex for Deployment Hedge

#add devex for Hyren

#OUTPUT CAPEX-DEVEX23
df_capex_total = df_capex_total[df_capex_total.Date.dt.year == YEAR]
output_path_csv = os.path.join(output_path, scenario + "_" + statement_line + ".csv")
output_path_parquet = os.path.join(output_path, scenario + "_" + statement_line + ".parquet")
df_capex_total.to_csv(output_path_csv, index=False)
df_capex_total.to_parquet(output_path_parquet, index=False)

#check integrity against dim_company
dif = set(df_capex_total.Project_Name.unique()).difference(companies_dim_company_capex)
assert dif == set({}), dif