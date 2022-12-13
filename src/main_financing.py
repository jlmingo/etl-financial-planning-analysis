import pandas as pd
import calendar
import os
from inputs import path_financing, path_capex_parquet, YEAR, output_path, scenario, path_capex_parquet
from functions_etl_bu import process_financing

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

list_df = []
for key, dict_parameters in dict_params.items():
    print(f"Processing {key}")
    df = process_financing(path_financing, dict_parameters, YEAR)
    list_df.append(df)
df_concat = pd.concat(list_df, ignore_index=True)
list_of_months = [x[0:3] + "-" + str(YEAR)[-2:] for x in list(calendar.month_name) if x != ""]
df_melt = (pd.melt(
        df_concat,
        id_vars=["Project_Name", "Investment_Type"],
        value_vars=list_of_months,
        var_name='Date',
        value_name='USD_Amount'))

df_melt["USD_Amount"] = df_melt["USD_Amount"].round(4)
df_melt = df_melt[df_melt["USD_Amount"] != 0]
df_melt.reset_index(drop=True, inplace=True)

#correct date
df_melt["Date"] = pd.to_datetime(df_melt.Date, format="%b-%y")

#OUTPUT FINANCING
statement_line = "financing"
output_path_csv = os.path.join(output_path, scenario + "_" + statement_line + ".csv")
output_path_parquet = os.path.join(output_path, scenario + "_" + statement_line + ".parquet")
df_melt.to_csv(output_path_csv, index=False)
df_melt.to_parquet(output_path_parquet, index=False)

#OUTPUT CAPEX + FINANCING

df_capex = pd.read_parquet(path_capex_parquet)
df_capex_financing = pd.concat([df_capex, df_melt], ignore_index=True)
statement_line = "capex_devex_financing"
output_path_csv = os.path.join(output_path, scenario + "_" + statement_line + ".csv")
output_path_parquet = os.path.join(output_path, scenario + "_" + statement_line + ".parquet")
df_capex_financing.to_csv(output_path_csv, index=False)
df_capex_financing.to_parquet(output_path_parquet, index=False)