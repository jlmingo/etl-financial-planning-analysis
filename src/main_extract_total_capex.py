import pandas as pd
import os
from functions_etl_bu import get_total_capex, check_nulls
from inputs import path_capex_global, path_capex_chile, only_devex_tabs, output_path, scenario, dim_fx, dim_country_currency, dim_project_capex

#extract capex global
projects_capex = pd.ExcelFile(path_capex_global).sheet_names
exclude_tabs = set(["SUMMARY USD", "SUMMARY LCY", "INDEX", "PROJECT DB", "SCENARIOS", "ASSUMPTIONS", "SUMMARY"])
projects_to_analyze = [project for project in projects_capex if project not in exclude_tabs and ">>>" not in project]

list_df = []
for sheet_name in projects_to_analyze:
    try:
        df = get_total_capex(path_capex_global, sheet_name, only_devex_tabs)
        list_df.append(df)
    except Exception as e:
        print(e)
    

#TODO: correct currency



df_concat_global = pd.concat(list_df, ignore_index=True)
df_concat_global.reset_index(drop=True, inplace=True)

df_concat_global.rename(columns={"Amount": "LC_Amount"}, inplace=True)

#get country
df_concat_global = df_concat_global.merge(dim_project_capex[["Project_Name", "Country"]], on="Project_Name", how="left")
check_nulls(df_concat_global, "Country")

#get currency and fx
df_concat_global = df_concat_global.merge(dim_country_currency[["Country", "Currency"]], on="Country", how="left")
check_nulls(df_concat_global, "Currency")

df_concat_global = df_concat_global.merge(dim_fx[["Currency", "FX"]], on="Currency", how="left")
check_nulls(df_concat_global, "FX")

df_concat_global["USD_Amount"] = df_concat_global["LC_Amount"] / df_concat_global["FX"]

amount = df_concat_global[(df_concat_global.Project_Name == "OLIVARES") & (df_concat_global.Investment_Type.isin(["DEVEX", "CAPEX"]))].USD_Amount.sum()
print(amount)
#drop columns not used
col_drop = ["Country", "Currency", "FX"]
df_concat_global.drop(col_drop, axis=1, inplace=True)

#extract capex chile
projects_capex = pd.ExcelFile(path_capex_chile).sheet_names
exclude_tabs = set(["SUMMARY USD", "SUMMARY LCY", "INDEX", "PROJECT DB", "SCENARIOS", "ASSUMPTIONS"])
projects_to_analyze = [project for project in projects_capex if project not in exclude_tabs and ">>>" not in project]

list_df = []
for sheet_name in projects_to_analyze:
    try:
        df = get_total_capex(path_capex_chile, sheet_name, only_devex_tabs)
        list_df.append(df)
    except Exception as e:
        print(e)
    

df_concat_chile = pd.concat(list_df, ignore_index=True)
df_concat_chile.reset_index(drop=True, inplace=True)
df_concat_chile.rename(columns={"Amount": "USD_Amount"}, inplace=True)

#get currency and fx - for chile the data is in USD
df_concat_chile["Currency"] = "CLP"


df_concat_chile = df_concat_chile.merge(dim_fx[["Currency", "FX"]], on="Currency", how="left")
check_nulls(df_concat_chile, "FX")

df_concat_chile["LC_Amount"] = df_concat_chile["USD_Amount"] * df_concat_chile["FX"]
df_concat_chile["LC_Amount"] = df_concat_chile["LC_Amount"].round(4)

#drop columns not used
col_drop = ["Currency", "FX"]
df_concat_chile.drop(col_drop, axis=1, inplace=True)

df_concat_chile.to_parquet("../output/check_chile.parquet", index=False)
#TODO: add local currency

df_capex_final = pd.concat([df_concat_global, df_concat_chile], ignore_index=True)


#remove colombia 6 last months
# idx_drop = df_capex_final[(df_capex_final.Project_Name.str.contains("LOS LLANOS")) & (df_capex_final.Date.dt.month > 6)].index
# df_capex_final.drop(idx_drop, inplace=True)
# df_capex_final.reset_index(drop=True, inplace=True)

#OUTPUT CAPEX-DEVEX23
statement_line = "total_capex_whole_life"
output_path_csv = os.path.join(output_path, scenario + "_" + statement_line + ".csv")
output_path_parquet = os.path.join(output_path, scenario + "_" + statement_line + ".parquet")
df_capex_final.to_csv(output_path_csv, index=False)
df_capex_final.to_parquet(output_path_parquet, index=False)