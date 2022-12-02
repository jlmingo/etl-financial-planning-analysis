import pandas as pd

#parameters
YEAR=2023
scenario="BU23"


#paths
path_asset_management_input = r"C:\Users\JorgeLopezMingo\Matrix Renewables Spain SLU\MATRIX RENEWABLES - Data model\Input Data\Revenue_OPEX\221018_Matrix Renewables 2023 OM Budget - portfolio_v2.xlsx"
path_capex_global = r"C:\Users\JorgeLopezMingo\Matrix Renewables Spain SLU\MATRIX RENEWABLES - Data model\Input Data\CAPEX_DEVEX\FollowUp_Budget CAPEX_Global_budget23.xlsm"
path_capex_chile = r"C:\Users\JorgeLopezMingo\Matrix Renewables Spain SLU\MATRIX RENEWABLES - Data model\Input Data\CAPEX_DEVEX\FollowUp_Budget CAPEX_Chile_budget23.xlsm"
path_amortization = r"C:\Users\JorgeLopezMingo\Matrix Renewables Spain SLU\MATRIX RENEWABLES - Data model\Input Data\Amortization\bu23_amortization.xlsx"
path_dim_project_capex = r"C:\Users\JorgeLopezMingo\Matrix Renewables Spain SLU\MATRIX RENEWABLES - Data model\Dimensions\DIM_PROJECT_CAPEX.xlsx"
path_dim_company = r"C:\Users\JorgeLopezMingo\Matrix Renewables Spain SLU\MATRIX RENEWABLES - Data model\Dimensions\DIM_COMPANY.xlsx"
path_dim_accounts = r"C:\Users\JorgeLopezMingo\Matrix Renewables Spain SLU\MATRIX RENEWABLES - Data model\Dimensions\DIM_PL_ACCOUNT_BU23.xlsx"
output_path = r"C:\Users\JorgeLopezMingo\Matrix Renewables Spain SLU\MATRIX RENEWABLES - Data model\Output"

#dim_accounts
dtype_accounts = {
    "PL_Account_Category": "int",
    "PL_Account_SubCategory": "int"
}
df_dim_accounts = pd.read_excel(path_dim_accounts, sheet_name="Dataload", dtype=dtype_accounts)

#dataframe for SPV selector
df_selector = pd.read_excel(path_asset_management_input, sheet_name="SPV Selector", skiprows=1)
df_selector.rename(columns={"PROJECT": "Project_Name", "SPV": "Company_Name"}, inplace=True)

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
df_dim_company = pd.read_excel(path_dim_company, sheet_name="Dataload")
companies_dim_company = set(df_dim_company.Project_Name.unique())
companies_dim_company_capex = set(df_dim_company.Project_Name_Alt.unique())

#dim_projects
dim_project_capex = pd.read_excel(path_dim_project_capex, sheet_name="Dataload")
dim_project_capex = dim_project_capex[dim_project_capex.Project_Name.notnull()]
assert dim_project_capex["Project_Name"].duplicated().sum() == 0

#FX dataframe
dict_country_currency = {
    "Country": ["United States", "Spain", "Italy", "Colombia", "Chile"],
    "Currency": ["USD", "EUR", "EUR", "COP", "CLP"]
}
dim_country_currency = pd.DataFrame(dict_country_currency)

dict_fx = {
    "Currency": ["USD", "EUR", "COP", "CLP"],
    "FX": [1, 1, 5000, 950]
}
dim_fx = pd.DataFrame(dict_fx)

#filter out capex tabs
filter_out = [
    "LOMA TENDIDA", "FUNDO SAN ISIDRO", "FONTANA", "MONTENEGRO"   
]