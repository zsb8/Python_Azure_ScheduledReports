import pandas as pd
import numpy as np
from .utils import functions


def account_recon():
    new_columns = ['_id', 'name', 'isOrganization', 'subscriptionId', 'isDeleted']

    def _get_two_df():
        """
        zoho-pipeline -- async upsertAccount({ account })  columns includes:
        'account_created'、'account_updated'、'account_subscription_changed'、
        Notes capital letter：['Id1','Account_Name','isOrganization','Subscription_Id','isDeleted']
        Id1 is the index
        """
        zoho_columns = ['Id1', 'Account_Name', 'isOrganization', 'Subscription_Id', 'isDeleted']
        df_zoho = functions.get_df_from_zoho('Accounts', 'Accounts', zoho_columns, new_columns)
        """
        account --> UserService.js ---> topic -->user_profile_updated 
        account --> AccountService.js --> topic-->account_created
        """
        df_mongo = functions.get_df_from_mongo("account", 'account.accounts', new_columns)
        df_mongo = df_mongo.drop_duplicates()
        return df_mongo, df_zoho

    d_mongo, d_zoho = _get_two_df()
    d_mongo = d_mongo.replace(np.nan, '')
    d_zoho = d_zoho.replace(np.nan, '')
    res = d_mongo.merge(d_zoho, how="outer", suffixes=('_mongo', '_zoho'), on='_id', indicator='id_diff')
    res = res.replace(np.nan, '')
    # in Mongo ,but not in Zoho
    diff = res.loc[res["id_diff"] == 'left_only']
    # in Zoho, but not in Mongo
    diff = res.loc[res["id_diff"] == 'right_only']
    df_combine = pd.merge(d_mongo, d_zoho, suffixes=('_mongo', '_zoho'), on='_id')
    df_combine = df_combine.replace(np.nan, '')
    df_new = df_combine.copy()
    for i in new_columns[1: len(new_columns)]:
        df_new = functions.compare_df_columns(df_new, i)
    col1 = ['_id',
            'name_mongo', 'name_zoho',
            'isOrganization_mongo', 'isOrganization_zoho',
            'subscriptionId_mongo', 'subscriptionId_zoho',
            'isDeleted_mongo', 'isDeleted_zoho'
            ]
    df_new = res.merge(df_new, how="left", on=col1)
    df_new = df_new.replace(np.nan, '')
    df_result = functions.arrange_format(df_new, new_columns)
    return df_result


if __name__ == "__main__":
    df_report1 = account_recon()
    # df_report1.to_csv('d:/df_report1.csv')
