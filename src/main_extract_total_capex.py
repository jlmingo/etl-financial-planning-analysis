#this file generates a dataframe with total capex from excel files - used for amortization calculation
import pandas as pd
from inputs import (scenario, YEAR, path_capex_global, path_capex_chile, filter_out)

#general values
cash_items = "Total Cash Flow EPC "

#TODO add filter to get VAT

#TODO add filter to exclude some companies from Chile

#global capex
excel_file = pd.ExcelFile(path_capex_global)
projects_capex = set(excel_file.sheet_names)

#GLOBAL tabs where only devex is taken
only_devex_tabs = [
    "ALTEN GREENFIELD", "ROLWIND GREENFIELD", "SAN GIULIANO",
    "EN494a", "EN494c", "MOLE", "TP02",
    "CALTO", "CASTELGOFF2", "BOSARO", "ROVIGO", "VALSAMOGGIA",
    "FR01", "TR01", "ENNA1",
    "SIGNORA", "SPARACIA", "VALLATA", "ISCHIA DI CASTRO"
]

#to get capex, only sheets without >>> or not included in devex tabs will be considered
assert set(only_devex_tabs).issubset(projects_capex), "Warning, review values."

#exclude detected tabs not to be included
exclude_tabs = ["SUMMARY USD", "SUMMARY LCY", "INDEX", "PROJECT DB"]

#get values only for analysis
projects_to_analyze = [project for project in projects_capex if project not in exclude_tabs and ">>>" not in project]

#as we are taking only capex, we will also filter these:
projects_to_analyze = [project for project in projects_to_analyze if project not in only_devex_tabs]

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
