import os
import shutil
org = r"C:\Users\JorgeLopezMingo\Matrix Renewables Spain SLU\Operations - 7. 2023 Budget&Forecast\221018_Matrix Renewables 2023 OM Budget - portfolio_v2.xlsx"
dst = r"C:\Users\JorgeLopezMingo\Matrix Renewables Spain SLU\MATRIX RENEWABLES - Data model\Input Data\Revenue_OPEX\221018_Matrix Renewables 2023 OM Budget - portfolio_v2.xlsx"

assert "Operations" not in dst

if os.path.isfile(dst):
    os.remove(dst)
shutil.copy(org, dst)