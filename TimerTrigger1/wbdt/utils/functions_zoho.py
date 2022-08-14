import json
import time
import sys
import requests
import pandas as pd
from . import constants, settings
# from utils import constants, settings


def get_zoho_token_header():
    """
    Get the token from zoho POST API for run the query in the next step.
    The token is changed every time.
    Such as "1000.d0e1f7a0e7e91de8b09cae2eced2cad4.67b9c7e7e682a5b31d6e4f54677565cb"

    :return dict header: Include the access_token.
    """
    base_url = constants.ZOHO_URL_TOKEN
    params = settings.CRM_ZOHO
    if params:
        access_token = json.loads(requests.post(base_url, data=params).text)['access_token']
        header = {'Authorization': 'Zoho-oauthtoken ' + access_token, 'Content-Type': 'application/json'}
        return header
    else:
        print("Error! Can't find MongoDB login info in OS environ.")
        sys.exit()


def send_zoho_bulkreadquery(header, module):
    """
    Get the report id (map to the module) through the POST API.

    :param dict header: Include the access_token. It is from get_zoho_token_header().
    :param str module: Such as "Accounts". It if the user input param in the main program.
    :return str report_id:  Such as "3786011000068360004", it is changed every time.
    """
    fields = get_module_settings(header, module, "fields")
    json_query = {"query": {"module": module, "fields": fields, "page": 1}}
    base_url = constants.ZOHO_URL_BULK
    response = json.loads(requests.post(base_url, headers=header, json=json_query).text)
    report_id = response["data"][0]["details"]["id"]
    return report_id


def dl_zoho_results(header, report_id):
    """
    Get the report content only for one module with the report_id.
    The URL such as https://www.zohoapis.com/crm/bulk/v2/read/3786011000068357022/result.

    :param dict header: It is from get_zoho_token_header().
    :param str report_id: Such as "3786011000068353009".
    :return class results: It is the requests.models.Response, such as <Response [200].`
    """
    base_url = constants.ZOHO_URL_BULK
    for t in range(0, 3):  # make 3 attempts to download
        time.sleep(30)  # wait 30 seconds before trying to download
        url = base_url + '/' + report_id + '/result'
        results = requests.get(url, headers=header)
        # will 404 if file hasn't completed running
        if results.status_code != 404:
            print(results.status_code)
            return results
    print('Results could not be generated.')
    return None


def get_module_settings(header, module, mode='fields'):
    """
    Get the files info through ZOHO GET API.
    The URL such as https://www.zohoapis.com/crm/v2/settings/fields?module=Accounts.
    If mode='fields', only return the rows of the webhook=True.

    :param dict header: It is from get_zoho_token_header().
    :param str module: Such as 'Accounts'
    :param (str, optional) mode: Default is 'fields'
    :return  fields or df df_module_metadata:
        fields: Such as ['Account_Name', 'Account_Category', 'isOrganization',...].
        df_module_metadata: The data of entire json.
    """
    base_url = constants.ZOHO_URL_SETTINGS
    response = json.loads(requests.get(base_url + module, headers=header).content)["fields"]
    df_module_metadata = pd.DataFrame(response)
    if mode == "fields":
        # webhook fields cannot be exported
        fields = df_module_metadata[df_module_metadata['webhook']]['api_name'].tolist()
        return fields
    else:
        return df_module_metadata


if __name__ == "__main__":
    get_zoho_token_header()
