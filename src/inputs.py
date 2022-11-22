import pandas as pd

#parameters
YEAR=2023

#paths
path_asset_management_input = r"C:\Users\JorgeLopezMingo\Documents\BU23\TEMPLATES\221018_Matrix Renewables 2023 OM Budget - portfolio_v2.xlsx"
path_dim_company = r"C:\Users\JorgeLopezMingo\Matrix Renewables Spain SLU\MATRIX RENEWABLES - Documentos\MATRIX RENEWABLES\1. Company\4. Accounting\Data model\Dimensions\DIM_COMPANY.xlsx"
path_dim_accounts = r"C:\Users\JorgeLopezMingo\Matrix Renewables Spain SLU\MATRIX RENEWABLES - Documentos\MATRIX RENEWABLES\1. Company\4. Accounting\Data model\Dimensions\DIM_PL_ACCOUNT_BU23.xlsx"

#dim_accounts
dtype_accounts = {
    "PL_Account_Category": "int",
    "PL_Account_SubCategory": "int"
}
df_dim_accounts = pd.read_excel(path_dim_accounts, sheet_name="Dataload", dtype=dtype_accounts)

#dataframe for SPV selector
df_selector = pd.read_excel(path_asset_management_input, sheet_name="SPV Selector", skiprows=1)
df_selector.rename(columns={"PROJECT": "Project_Name", "SPV": "Company_Name"}, inplace=True)
print(df_selector)
#TODO: remove this manual step:
df_selector.loc[df_selector.Project_Name=="GASKELL", "Company_Name"] = "Gaskell (Pending Correct Name)"
#print projects with no SPV associated
project_without_spv = df_selector[df_selector.Company_Name.isnull()].Project_Name.unique()
if len(project_without_spv) > 0:
    print("Warning: the following projects have no SPV associated and will not be included:")
    print(project_without_spv)

df_selector = df_selector[df_selector.Company_Name.notnull()]
companies_spv_selector = set(df_selector.Company_Name.unique())

#dim_company
df_dim_project = pd.read_excel(path_dim_company, sheet_name="Dataload")
companies_dim_project = set(df_dim_project.Project_Name.unique())


#FX dataframe
dict_fx = {
    "Currency": ["USD", "EUR", "COP", "CLP"],
    "FX": [1, 1, 5000, 950]
}
dim_fx = pd.DataFrame(dict_fx)

