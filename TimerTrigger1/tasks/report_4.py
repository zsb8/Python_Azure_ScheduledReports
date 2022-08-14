import logging
import pyodbc
import pandas as pd
from .utils import functions
from .utils.settings import Setting


def pull_identitydata():
    """
    SQL server database used to mention identities rather than mongodb, however it provides the only
    connection from user id's to emails.
    """
    try:
        driver = "{ODBC Driver 17 for SQL Server}"
        server = "tcp:waybase-prod.database.windows.net"
        database = "identity"
        username = Setting.SQLSERVER_USERNAME
        password = Setting.SQLSERVER_PASSWORD
        connectstr = \
            "DRIVER=" + driver + \
            ";SERVER=" + server + \
            ";DATABASE=" + database + \
            ";UID=" + username + \
            ";PWD=" + password + \
            ";Encrypt=yes;Authentication=ActiveDirectoryPassword"
        sqlclient = pyodbc.connect(connectstr)
        query = "SELECT [Id],[Email] as email,[Name] as first_name,[lastName] as last_name FROM [dbo].[AspNetUsers]"
        df = pd.read_sql(query, sqlclient, index_col="Id")
        df.index.rename("_id", inplace=True)
    except Exception as error:
        logging.info(f'!!!!========SQl query error: {error}')
        df = pd.DataFrame({'A3333333333': [4, 5, 6], 'B3333333333333': [4, 1, 1]})
    return df


def user_recon():
    df_sql = pull_identitydata()
    df_zoho = functions.get_mails_zoho("contacts")
    df_sql = df_sql.loc[:, ['email']]
    df_sql.rename(columns={'email': 'email_SQL'}, inplace=True)
    df_sql['email_SQL'] = df_sql['email_SQL'].apply(lambda x: x.strip())
    df_sql['email_SQL'] = df_sql['email_SQL'].apply(lambda x: x.lower())
    df_sql_distinct = df_sql.drop_duplicates()
    df_zoho = df_zoho.loc[:, ['email']]
    df_zoho.rename(columns={'email': 'email_ZOHO'}, inplace=True)
    df_zoho = df_zoho.fillna('')
    df_zoho['email_ZOHO'] = df_zoho['email_ZOHO'].apply(lambda x: x.strip())
    df_zoho['email_ZOHO'] = df_zoho['email_ZOHO'].apply(lambda x: x.lower())
    df_zoho.drop(index=df_zoho.loc[df_zoho['email_ZOHO'] == ''].index, axis=0, inplace=True)
    df_zoho_distinct = df_zoho.drop_duplicates()
    df_diff = df_sql_distinct.merge(df_zoho_distinct, how="left", left_on='email_SQL', right_on='email_ZOHO',
                                    indicator='i').query("i=='left_only'")
    df_diff = df_diff.loc[:, ['email_SQL']]
    df_diff.rename(columns={'email_SQL': 'email_InSQL_notInZOHO'}, inplace=True)
    df_diff.reset_index(drop=True, inplace=True)
    result = df_diff
    return result


if __name__ == "__main__":
    df_report4 = user_recon()
    # df_report4.to_csv('d:/df_report4.csv')
