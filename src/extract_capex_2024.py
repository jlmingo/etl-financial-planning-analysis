from functions_etl_bu import get_capex_sheet_data_2024
from inputs import path_capex_global, path_capex_chile
import pandas as pd



#general values
YEAR = 2024

cash_items = {
    "Development Payments ", "Total Cash Flow Developmemnt ",
    "EPC Payments ", "Total Cash Flow EPC "
}

#global capex
excel_file = pd.ExcelFile(path_capex_global)
projects_capex = set(excel_file.sheet_names)

#exclude detected tabs not to be included
exclude_tabs = ["SUMMARY USD", "SUMMARY LCY", "INDEX", "PROJECT DB", "SCENARIOS", "BAJO I", "BAJO II", "ASSUMPTIONS"]

#get values only for analysis
projects_to_analyze = [project for project in projects_capex if project not in exclude_tabs and ">>>" not in project]

#global capex 2024
for sheet_name in projects_to_analyze:
    df = pd.read_excel(path_capex_global, sheet_name=sheet_name, skiprows=range(3))
    df = get_capex_sheet_data_2024(df, sheet_name, YEAR)