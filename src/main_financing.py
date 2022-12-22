import pandas as pd
import calendar
import os
import numpy as np
from inputs import path_financing, path_capex_parquet, YEAR, output_path, scenario, path_capex_parquet, path_capex_usa
from functions_etl_bu import process_financing, get_capex_usa_by_project

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

#drop USA lines
df_melt = df_melt[df_melt["Project_Name"] != "USA"]

debt_usa = pd.DataFrame({
    "Project_Name": ["USA"],
    "Investment_Type": ["Debt"],
    "Cash_Item": [np.nan],
    "Date": [pd.to_datetime("2023-01-01")],
    "USD_Amount": [42590061.8687]
})

df_melt = pd.concat([df_melt, debt_usa], ignore_index=True)

#OUTPUT FINANCING
statement_line = "financing"
output_path_csv = os.path.join(output_path, scenario + "_" + statement_line + ".csv")
output_path_parquet = os.path.join(output_path, scenario + "_" + statement_line + ".parquet")
df_melt.to_csv(output_path_csv, index=False)
df_melt.to_parquet(output_path_parquet, index=False)

#OUTPUT CAPEX + FINANCING

#add Investment_Type_Reclassified (converts DEVEX into CAPEX for projects that achieved financial closing)
df_capex = pd.read_parquet(path_capex_parquet)

#read date for first payment using debt for Spain and Italy
df_debt_payment = pd.read_excel(
    path_financing,
    sheet_name="Capex Financing Expenses - Debt",
    skiprows=range(4),
    usecols=["Project_Name", "Debt Inflow First Date"]
    )
df_debt_payment.rename(columns={"Debt Inflow First Date": "Debt_Inflow_Date"}, inplace=True)
df_debt_payment = df_debt_payment[df_debt_payment["Debt_Inflow_Date"].notnull()]

df_capex = df_capex.merge(df_debt_payment, how="left", on="Project_Name")
df_capex["Investment_Type_Reclassified"] = np.where(
    (df_capex["Debt_Inflow_Date"].notnull()) & (df_capex["Investment_Type"] == "DEVEX"),
    "CAPEX",
    df_capex["Investment_Type"]
    )

#remove data from USA
df_capex = df_capex[df_capex["Project_Name"] != "USA"]
df_capex.reset_index(drop=True, inplace=True)
df_capex.drop("Debt_Inflow_Date", axis=1, inplace=True)

#add data from USA
df_capex_usa = get_capex_usa_by_project(path_capex_usa, YEAR)




df_capex_financing = pd.concat([df_capex, df_melt, df_capex_usa], ignore_index=True)

#fill blanks in investment_type_reclassified
df_capex_financing["Investment_Type_Reclassified"] = np.where(
    df_capex_financing["Investment_Type_Reclassified"].isnull(),
    df_capex_financing["Investment_Type"],
    df_capex_financing["Investment_Type_Reclassified"]
    )

statement_line = "capex_devex_financing"
output_path_csv = os.path.join(output_path, scenario + "_" + statement_line + ".csv")
output_path_parquet = os.path.join(output_path, scenario + "_" + statement_line + ".parquet")
df_capex_financing.to_csv(output_path_csv, index=False)
df_capex_financing.to_parquet(output_path_parquet, index=False)