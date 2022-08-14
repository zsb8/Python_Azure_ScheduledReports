# reStructuredText style.
import io
import zipfile
import platform
import pandas as pd
from .utils import constants, functions_zoho as functions
# from utils import constants, functions_zoho as functions


def pull_zoho_crmdata(module="*", download=True, folder = "default"):
    """
    Runs steps required to download Zoho CRM data and return as a DataFrame, and optionally as files.

    :param (str, list, optional) module: Module name. Or list of module names. Defaults to "*". Default is '*'.
    :param (bool, optional) download: True or False. Download as CSV into data directory. Defaults to True.
    :param (str, list, optional) folder:  The path to save. If user not set specially, Wins will be d:/ , Linux will be /tmp/
    :return dict results: Returns a dictionary indexed by the module name containing DataFrames as values.
    """
    modules_list = constants.ZOHO_MODELS_LIST
    if module == "*":
        module = modules_list
    elif isinstance(module, str):
        module = [module]
    else:
        print("Invalid input type")
        return None
    # Zoho ids often get accidentally converted to float, force str
    forced_types = {"Id": str, "Account_Name": str}
    if set(module).issubset(set(modules_list)):
        header = functions.get_zoho_token_header()
        dict_report_ids = {}
        for m in module:
            print(m)
            dict_report_ids[m] = functions.send_zoho_bulkreadquery(header, m)
        results = {}  # store df's indexed by module
        for r in dict_report_ids:
            print("Downloading ", r)
            response = functions.dl_zoho_results(header, dict_report_ids[r])
            if response is not None:
                # response is a zip file - read into memory and unzip    
                results_raw = io.BytesIO(response.content)
                results_zip = zipfile.ZipFile(results_raw)
                df = pd.read_csv(results_zip.open(
                    results_zip.namelist()[0]), dtype=forced_types, low_memory=False).set_index("Id")
                if download:
                    operation = platform.uname()
                    if folder:
                        path = folder
                    else:
                        if operation[0] == 'Linux':
                            path = '/tmp/'
                        if operation[0] == 'Windows':
                            path = 'd:/'
                    df.to_csv(path+"Zoho_"+r+".csv", encoding='utf-8-sig')
                results[m] = df
            else:
                print(f"No results for {r} module")
                pass
        print("Results generated")
        return results
    else:
        print("Not listed in code as valid module - update if necessary")
        return {}


if __name__ == "__main__":
    pull_zoho_crmdata(module="Accounts", download=False)
