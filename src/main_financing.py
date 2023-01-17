import pandas as pd
import calendar
import os
import numpy as np
from inputs import path_financing, path_capex_parquet, YEAR, output_path, scenario, path_capex_parquet, path_capex_usa, dim_project_capex, path_revenue_opex_platform
from functions_etl_bu import process_financing, get_capex_usa_by_project, calculate_adjustment_to_capex, check_nulls

list_of_months = [x[0:3] + "-" + str(YEAR)[-2:] for x in list(calendar.month_name) if x != ""]

dict_params = {
    "Equity Schedule": {
        "skiprows": [0],
        "Investment_Type": "Equity",
        "sheet_name": "Equity Schedule"
    },
    "Debt Schedule": {
        "skiprows": [0],
        "Investment_Type": "Debt",
        "sheet_name": "Debt Schedule"
    },
    "Equity Schedule Chile": {
        "skiprows": [0],
        "Investment_Type": "Equity",
        "sheet_name": "Chile Equity Schedule"
    },
    "Debt Schedule Chile": {
        "skiprows": range(13),
        "Investment_Type": "Debt",
        "sheet_name": "Chile Debt Schedule"
    }
}

#process financing
list_df = []
for key, dict_parameters in dict_params.items():
    print(f"Processing {key}")
    df = process_financing(path_financing, dict_parameters, YEAR)
    list_df.append(df)

#process financial expenses linked to capex and devex
dict_params_fin_costs = {
    "skiprows": range(4),
    "Investment_Type": "CAPEX",
    "sheet_name": "Capex Financing Expenses - Debt"
}
print("Processing financing costs asssociated to CAPEX")
df_capex_fin_costs = process_financing(path_financing, dict_params_fin_costs, YEAR)
df_capex_fin_costs["Cash_Item"] = "Financial costs associated to investment"

#generate dataframes with financing of capex fin costs
df_fin_costs_equity = df_capex_fin_costs.copy()
df_fin_costs_equity["Investment_Type"] = "Equity"
df_fin_costs_equity["Cash_Item"] = "Financial costs associated to investment"

df_fin_costs_debt = df_capex_fin_costs.copy()
df_fin_costs_debt["Investment_Type"] = "Debt"
df_fin_costs_debt["Cash_Item"] = "Financial costs associated to investment"

for col in list_of_months:
    df_fin_costs_equity[col] = df_fin_costs_equity[col].multiply(0.35)
    df_fin_costs_debt[col] = df_fin_costs_debt[col].multiply(0.65)

list_df.append(df_capex_fin_costs)
list_df.append(df_fin_costs_equity)
list_df.append(df_fin_costs_debt)

df_concat = pd.concat(list_df, ignore_index=True)

df_melt = (pd.melt(
        df_concat,
        id_vars=["Project_Name", "Investment_Type", "Cash_Item"],
        value_vars=list_of_months,
        var_name='Date',
        value_name='USD_Amount'))

df_melt["USD_Amount"] = df_melt["USD_Amount"].round(4)
df_melt = df_melt[df_melt["USD_Amount"] != 0]
df_melt.reset_index(drop=True, inplace=True)

#correct date
df_melt["Date"] = pd.to_datetime(df_melt.Date, format="%b-%y")

#drop USA lines TODO: add back VAT from USA
df_melt = df_melt[(df_melt["Project_Name"] != "USA")]

#TODO: this is financing for gaskell... modify
# debt_usa = pd.DataFrame({
#     "Project_Name": ["USA"],
#     "Investment_Type": ["Debt"],
#     "Cash_Item": [np.nan],
#     "Date": [pd.to_datetime("2023-01-01")],
#     "USD_Amount": [42590061.8687]
# })

#df_melt = pd.concat([df_melt, debt_usa], ignore_index=True)

#OUTPUT FINANCING
statement_line = "financing"
output_path_csv = os.path.join(output_path, scenario + "_" + statement_line + ".csv")
output_path_parquet = os.path.join(output_path, scenario + "_" + statement_line + ".parquet")
df_melt.to_csv(output_path_csv, index=False)
df_melt.to_parquet(output_path_parquet, index=False)

#OUTPUT CAPEX + FINANCING

#add Investment_Flag (converts DEVEX into CAPEX for projects that achieved financial closing)
df_capex = pd.read_parquet(path_capex_parquet)

#read date for first payment using debt for Spain and Italy
df_debt_payment = pd.read_excel(
    path_financing,
    sheet_name="Capex Financing Expenses - Debt",
    skiprows=range(4),
    usecols=["Project_Name", "Debt Inflow First Date"]
    )
df_debt_payment.rename(columns={"Debt Inflow First Date": "FNTP_Date"}, inplace=True)
df_debt_payment = df_debt_payment[df_debt_payment["FNTP_Date"].notnull()]

df_capex = df_capex.merge(df_debt_payment, how="left", on="Project_Name")
df_capex["FNTP_Date"] = df_capex["FNTP_Date"].replace(False, np.nan)

#calculate adjustments - italy and spain
df_capex_spain_italy = df_capex.copy()
df_capex_spain_italy = df_capex_spain_italy.merge(dim_project_capex[["Project_Name", "Country"]], on="Project_Name", how="left")
check_nulls(df_capex_spain_italy, "Country")
df_capex_spain_italy = df_capex_spain_italy[df_capex_spain_italy.Country.isin(["Spain", "Italy"])].reset_index(drop=True)
df_adjustment_capex_spain_italy = calculate_adjustment_to_capex(df_capex_spain_italy)
df_adjustment_capex_spain_italy.drop("Country", axis=1, inplace=True)

#calculate adjustments - chile
df_capex_chile = df_capex.copy()
print(df_capex_chile)
print(df_capex_chile.columns)
df_capex_chile = df_capex_chile.merge(dim_project_capex[["Project_Name", "Country"]], on="Project_Name", how="left")
df_capex_chile = df_capex_chile[df_capex_chile.Country.isin(["Chile"])].reset_index(drop=True)
df_capex_chile.drop("Country", axis=1, inplace=True)
df_capex_chile_adj = df_capex_chile[df_capex_chile.Investment_Type == "DEVEX"].reset_index(drop=True)
df_capex_chile_adj["Investment_Flag"] = "WIP_to_PPE"
df_capex_chile_adj_capex = df_capex_chile_adj.copy()
df_capex_chile_adj_capex["Investment_Type"] = "CAPEX"
print(df_capex_chile_adj)
print(df_capex_chile_adj.columns)
df_capex_chile_adj["USD_Amount"] = df_capex_chile_adj["USD_Amount"].multiply(-1)
df_capex_chile_adj["LC_Amount"] = df_capex_chile_adj["LC_Amount"].multiply(-1)

df_capex_chile_adj = pd.concat([df_capex_chile_adj, df_capex_chile_adj_capex], ignore_index=True)

print(df_capex_chile_adj)
print(df_capex_chile_adj_capex)

#calculate adjustments - usa excl. gaskell
df_capex_usa = get_capex_usa_by_project(path_capex_usa, YEAR)
df_capex_usa_adj = calculate_adjustment_to_capex(df_capex_usa)

#correct dates for usa
df_capex_usa["Date"] = df_capex_usa["Date"].to_numpy().astype('datetime64[M]')
df_capex_usa_adj["Date"] = df_capex_usa_adj["Date"].to_numpy().astype('datetime64[M]')

#calculate adjustments - gaskell
df_capex_gaskell = df_capex.copy()
df_capex_gaskell = df_capex_gaskell[df_capex_gaskell.Project_Name == "Gaskell"]
df_capex_gaskell_adj = df_capex_gaskell[df_capex_gaskell.Investment_Type == "DEVEX"].reset_index(drop=True)
df_capex_gaskell_adj["Investment_Flag"] = "WIP_to_PPE"
df_capex_gaskell_adj_capex = df_capex_gaskell_adj.copy()
df_capex_gaskell_adj_capex["Investment_Type"] = "CAPEX"
df_capex_gaskell_adj["USD_Amount"] = df_capex_gaskell_adj["USD_Amount"].multiply(-1)
df_capex_gaskell_adj["LC_Amount"] = df_capex_gaskell_adj["LC_Amount"].multiply(-1)

df_capex_gaskell_adj = pd.concat([df_capex_gaskell_adj, df_capex_gaskell_adj_capex], ignore_index=True)

#remove data from USA - TODO: VAT should be kept
condition_1 = (df_capex.Project_Name != "USA")
condition_2 = (df_capex.Project_Name == "USA") & (df_capex.Investment_Type.str.contains("VAT"))
df_capex = df_capex[(condition_1) | (condition_2)]
df_capex.reset_index(drop=True, inplace=True)

list_concat_dataframes = [df_capex, df_capex_usa, df_adjustment_capex_spain_italy, df_capex_chile_adj, df_capex_usa_adj, df_capex_gaskell_adj]
for dataframe in list_concat_dataframes:
    if "FNTP_Date" in dataframe.columns:
        dataframe.drop("FNTP_Date", axis=1, inplace=True)

list_concat_dataframes.append(df_melt)
df_capex_financing = pd.concat(list_concat_dataframes, ignore_index=True)


#add platform cost as equity
df_platform_cost = pd.read_parquet(path_revenue_opex_platform)
df_platform_cost = df_platform_cost[df_platform_cost.Project_Name.str.contains("Platform")].reset_index(drop=True)
df_platform_cost.rename(columns={"PL_Account": "Cash_Item", "Text_Description": "Milestones"}, inplace=True)
df_platform_cost["Investment_Type"] = "Equity"
df_platform_cost["USD_Amount"] = df_platform_cost["USD_Amount"].multiply(-1)

df_capex_financing = pd.concat([df_capex_financing, df_platform_cost], ignore_index=True)

#add alternative date for buffering - 2 months for Spain, Italy and USA except Rocio 1, Rocio2, Rocio3, Olivares, Arroyadas, Zaratan and Gaskell
df_capex_financing = df_capex_financing.merge(dim_project_capex[["Project_Name", "Country"]], on="Project_Name", how="left")
check_nulls(df_capex_financing[~df_capex_financing.Project_Name.str.contains("Platform")], "Country")
print(df_capex_financing.Country.unique())
project_exceptions =  [
    "Gaskell", "ROCIO 1", "ROCIO 2", "ROCIO 3", "LAS ARROYADAS", "ZARATAN", "OLIVARES"
]


condition_project = df_capex_financing.Country.isin(["United States", "Italy", "Spain"]) & ~(df_capex_financing.Project_Name.isin(project_exceptions))
df_capex_financing["Date_alt"] = np.where(condition_project, df_capex_financing["Date"] + pd.DateOffset(months=2), df_capex_financing["Date"])



print(df_capex_financing)
print(df_capex_financing.columns)
statement_line = "capex_devex_financing"
output_path_csv = os.path.join(output_path, scenario + "_" + statement_line + ".csv")
output_path_parquet = os.path.join(output_path, scenario + "_" + statement_line + ".parquet")
df_capex_financing.to_csv(output_path_csv, index=False)
df_capex_financing.to_parquet(output_path_parquet, index=False)