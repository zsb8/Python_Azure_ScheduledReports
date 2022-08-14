import re
import pandas as pd
from .utils import functions
import numpy as np
new_columns = ['_id', 'accountId', 'userId', 'email', 'roles', 'isDeleted']


def myre(x):
    # only deal with txt
    try:
        if x.find('[') > -1 and x != "[]":
            pattern = re.compile(r'\w+')
            a = pattern.findall(x)
            try:
                result = a[0]
            except:
                print(a)
                print("出粗了")
                result = ''
        elif x == "[]":
            result = ""
        else:
            result = x
    except:
        print("!!!ERROR")
        print(x)
        print(type(x))
        result = ""
    return result


def get_two_df():
    columns = ['id1', 'Associated_Accounts', 'Contact_Name', 'Email', 'roles', 'isDeleted']
    df_zoho = functions.get_df_from_zoho('Contacts_X_Accounts', 'Contacts_X_Accounts', columns)
    # Note: Associated_Accounts, value is number not str
    df_zoho['Associated_Accounts'] = df_zoho['Associated_Accounts'].apply(lambda x: str(x))
    # Note: sample as the str format as  ["owner"]，not list format，also include nan
    df_zoho = df_zoho.fillna('')
    df_zoho['roles'] = df_zoho['roles'].apply(lambda x: myre(x))
    # Note:  str format: ["owner"] ， []， ‘administrator’
    df_zoho = df_zoho.drop_duplicates()
    """
    async upsertAccountContact({ accountUser })
    join Zoho_Contacts_X_Accounts.Associated_Accounts=Zoho_Accounts.Id
    Zoho_Contacts_X_Accounts.Contact_Name: contact.id,(Zoho_Contacts.csv --> Id) 
    """
    columns = ['Id', 'Id1']
    df_zoho_accounts = functions.get_df_from_zoho('Accounts', 'Accounts')
    df_zoho_accounts = df_zoho_accounts.reset_index()
    df_zoho_accounts = df_zoho_accounts.loc[:, columns]
    df_zoho_accounts = df_zoho_accounts.rename(columns={'Id': 'Associated_Accounts', 'Id1': 'accountId'})
    df_zoho = pd.merge(df_zoho, df_zoho_accounts, on='Associated_Accounts')
    df_zoho.drop('Associated_Accounts', axis=1, inplace=True)
    df_zoho = df_zoho.drop_duplicates()
    columns = ['Id', 'WayBase_ID']
    df_zoho_contact = functions.get_df_from_zoho('Contacts', 'Contacts', columns, reset=True)
    df_zoho_contact = df_zoho_contact.rename(columns={'Id': 'Contact_Name', 'WayBase_ID': 'userId'})
    df_zoho['Contact_Name'] = df_zoho['Contact_Name'].apply(lambda x: str(x))
    df_zoho_contact['Contact_Name'] = df_zoho_contact['Contact_Name'].apply(lambda x: str(x))
    df_zoho = pd.merge(df_zoho, df_zoho_contact, on='Contact_Name')
    df_zoho.drop('Contact_Name', axis=1, inplace=True)
    df_zoho = df_zoho.rename(columns={'id1': '_id', 'Email': 'email'})
    df_zoho = df_zoho.fillna('')
    df_zoho.drop(index=df_zoho.loc[df_zoho['_id'] == ''].index, axis=0, inplace=True)
    df_zoho = df_zoho.drop_duplicates()
    df_zoho = df_zoho.loc[:, new_columns]
    # listing --> @validateCommand(validators.createListing)
    df_mongo = functions.get_df_from_mongo("account", 'account.accountusers', new_columns)
    df_mongo['roles'] = df_mongo['roles'].apply(lambda x: x[0] if len(x)>0 else '')
    df_mongo = df_mongo.drop_duplicates()
    return df_mongo, df_zoho


def account_user_recon():
    d_mongo, d_zoho = get_two_df()
    d_mongo = d_mongo.replace(np.nan, '')
    d_zoho = d_zoho.replace(np.nan, '')
    df_combine = pd.merge(d_mongo, d_zoho, suffixes=('_mongo', '_zoho'), on='_id')
    df_combine = df_combine.replace(np.nan, '')
    df_new = df_combine.copy()
    for i in new_columns[1: len(new_columns)]:
        df_new = functions.compare_df_columns(df_new, i)
    res = d_mongo.merge(d_zoho, how="outer", suffixes=('_mongo', '_zoho'), on='_id', indicator='id_diff')
    res = res.replace(np.nan, '')
    col1 = ['_id',
            'accountId_mongo', 'accountId_zoho',
            'userId_mongo', 'userId_zoho',
            'email_mongo', 'email_zoho',
            'roles_mongo', 'roles_zoho',
            'isDeleted_mongo', 'isDeleted_zoho'
            ]
    df_new = res.merge(df_new, how="left", on=col1)
    df_new = df_new.replace(np.nan, '')
    result = functions.arrange_format(df_new, new_columns)
    return result


if __name__ == "__main__":
    df_report2 = account_user_recon()
    # df_report2.to_csv('d:/df_report2.csv')
