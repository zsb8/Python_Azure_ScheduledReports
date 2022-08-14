from ...wbdt import crm, mongodb  # this wbdt need you to installed in you local pc firstly
import pandas as pd


def get_df_from_zoho(db_name, table_name, columns=[], new_columns=[], reset=False):
    df_z = crm.pull_zoho_crmdata(db_name, False)[table_name]
    if reset:
        df_z = df_z.reset_index()
    if columns:
        df_z = df_z.loc[:, columns]
    if new_columns:
        df_z.columns = new_columns
        df_z = df_z.fillna('')
        df_z.drop(index=df_z.loc[df_z['_id'] == ''].index, axis=0, inplace=True)
    df_z = df_z.drop_duplicates()
    return df_z


def get_df_from_mongo(db_name, table_name, new_columns=[], deleteidemtpy=True):
    df_p1 = mongodb.download(db_name, False)[table_name]
    df_p1 = df_p1.reset_index()  # _id is the index
    if new_columns:
        df_mongo = df_p1.loc[:, new_columns]
    else:
        df_mongo = df_p1
    df_mongo = df_mongo.fillna('')
    if deleteidemtpy:
        df_mongo.drop(index=df_mongo.loc[df_mongo['_id'] == ''].index, axis=0, inplace=True)
    return df_mongo


def compare_df_columns(df, column_name):
    # compare mongo and zoho, add one column named diff
    mongo = column_name + '_mongo'
    zoho = column_name + '_zoho'
    differ = column_name + '_diff'
    df.loc[(df[mongo] != df[zoho]), differ] = 'diff'
    df.loc[(df[mongo] == df[zoho]), differ] = 'same'
    if column_name == 'name':
        df.loc[(df['name_zoho'] == df['_id']) & (df['name_diff'] == 'diff'), 'name_diff'] = 'Differ because name=id'
    return df


def set_same_empty(df, column_name):
    mongo = column_name + '_mongo'
    zoho = column_name + '_zoho'
    differ = column_name + '_diff'
    df.loc[(df[differ] == 'same'), [mongo, zoho, differ]] = ''
    return df


def set_left_right_empty(df):
    new_col = list(df.columns)
    new_col.remove('_id')
    new_col.remove('id_diff')
    df.loc[(df['id_diff'] == 'left_only'), new_col] = ''
    df.loc[(df['id_diff'] == 'right_only'), new_col] = ''
    return df


def delete_empty_rows(df, new_columns):
    df.columns = [i.replace('.', '__') for i in df.columns]
    query = ' & '.join([f"{x}==''" for x in new_columns])
    query = query + " & id_diff=='both' "
    query = query.replace(".", "__")
    df.drop(index=df.query(query).index, inplace=True)
    df.columns = [i.replace('__', '.') for i in df.columns]
    return df


def arrange_format(df, new_columns):
    # must include _id
    col_beautiful = make_cols(new_columns)
    df = df[col_beautiful]
    for column in new_columns[1: len(new_columns)]:
        df = set_same_empty(df, column)
    my_list = []
    for i in new_columns:
        a = i + '_diff'
        my_list.append(a)
    del my_list[0]
    df = delete_empty_rows(df, my_list)
    df = set_left_right_empty(df)
    df['id_diff'] = df['id_diff'].apply(lambda x: 'MongoDB_id_only' if x=='left_only' else ('ZOHO_id_only' if x=='right_only' else '') )
    list_cols = list(df.columns)
    list_cols.insert(1, list_cols.pop(list_cols.index('id_diff')))
    df = df.loc[:, list_cols]
    df.reset_index(drop=True, inplace=True)
    return df


def make_cols(col):
    columns = col.copy()
    my_list = ['_id']
    del columns[0]
    for i in columns:
        my_list.append(i+'_mongo')
        my_list.append(i + '_zoho')
        my_list.append(i + '_diff')
    my_list.append('id_diff')
    return my_list


def split_people(x, col):
    result = ''
    if x:
        x = x[0]
        if 'firstName' in x and col == 'firstName':
            result = x['firstName'] if x['firstName'] else ''
        if 'lastName' in x and col == 'lastName':
            result = x['lastName'] if x['lastName'] else ''
        if 'title' in x and col == 'title':
            result = x['title'] if x['title'] else ''
        if 'phoneNumber' in x and col == 'phoneNumber':
            result = x['phoneNumber'] if x['phoneNumber'] else ''
        if 'email' in x and col == 'email':
            result = x['email'] if x['email'] else ''
    return result


def split_email(x):
    #  only for report5
    result = []
    if isinstance(x, list):
        for i in x:
            if isinstance(i, dict):
                for key in i:
                    if key == 'email':
                        if i['email']:   # may <class 'NoneType'>
                            my_str = i['email'].rstrip()
                            my_str2 = my_str.replace("%20", "")
                            result.append(my_str2)
    return result


def get_mails_zoho(table_name=""):
    # download the contacts table from ZOHO
    zoho_columns_contacts = ['First_Name', 'Last_Name', 'Title', 'Email', 'Phone']
    df_zoho_contacts = get_df_from_zoho('Contacts', 'Contacts', zoho_columns_contacts)
    df_zoho_contacts = df_zoho_contacts.reset_index()
    df_zoho_contacts.columns = ['contacts_id', 'first_name', 'last_name', 'title', 'email', 'phone']
    df_zoho_contacts = df_zoho_contacts.fillna('')
    df_zoho_contacts.drop(index=df_zoho_contacts.loc[df_zoho_contacts['contacts_id'] == ''].index, axis=0, inplace=True)
    df_zoho_contacts['contacts_id'] = df_zoho_contacts['contacts_id'].astype(str)
    df_zoho_contacts = df_zoho_contacts.fillna('')
    zoho_columns_contacts_x_orgs = ['Contact_Name', 'Associated_Orgs']
    df_zoho_contacts_x_orgs = get_df_from_zoho('Contacts_X_Orgs', 'Contacts_X_Orgs', zoho_columns_contacts_x_orgs)
    df_zoho_contacts_x_orgs.columns = ['contact_name', 'associated_orgs']
    df_zoho_contacts_x_orgs.drop(index=df_zoho_contacts_x_orgs.loc[df_zoho_contacts_x_orgs['contact_name'] == ''].index,
                                 axis=0, inplace=True)
    df_zoho_contacts_x_orgs.drop(
        index=df_zoho_contacts_x_orgs.loc[df_zoho_contacts_x_orgs['associated_orgs'] == ''].index, axis=0, inplace=True)
    df_zoho_contacts_x_orgs['contact_name'] = df_zoho_contacts_x_orgs['contact_name'].astype(str)
    df_zoho_contacts_x_orgs['associated_orgs'] = df_zoho_contacts_x_orgs['associated_orgs'].astype(str)
    zoho_columns_orgs = ['Name', 'ListingId', 'Listing_Type']
    df_zoho_orgs = get_df_from_zoho('Orgs', 'Orgs', zoho_columns_orgs)
    df_zoho_orgs = df_zoho_orgs.reset_index()
    df_zoho_orgs.columns = ['orgs_id', 'name', 'listingId', 'listing_type']
    df_temp = pd.merge(df_zoho_contacts, df_zoho_contacts_x_orgs, how='inner', suffixes=('_x', '_y'),
                       left_on='contacts_id', right_on='contact_name')
    df_zoho = pd.merge(df_temp, df_zoho_orgs, how='inner', suffixes=('_x', '_y'), left_on='associated_orgs',
                       right_on='orgs_id')
    if table_name == "contacts":
        df_zoho = df_zoho_contacts
    if table_name == "orgs":
        df_zoho = df_zoho_orgs
    return df_zoho


def group_by_df(df, columns):
    # only for report5
    def _concat_func(series):
        a = list()
        a.append(series.values)
        return a
    df_mongo = df.copy()
    df_zoho = df.copy()
    listingid_mongo = columns[0]
    s_mongo_new = df_mongo.groupby(['email'])[listingid_mongo].apply(_concat_func)
    df_mongo_new = s_mongo_new.to_frame()
    df_mongo_new = df_mongo_new.reset_index()
    df_mongo_new[listingid_mongo] = df_mongo_new[listingid_mongo].apply(lambda x: sorted(x[0]))
    listingid_zoho = columns[1]
    s_zoho_new = df_zoho.groupby(['email'])[listingid_zoho].apply(_concat_func)
    df_zoho_new = s_zoho_new.to_frame()
    df_zoho_new = df_zoho_new.reset_index()
    df_zoho_new[listingid_zoho] = df_zoho_new[listingid_zoho].apply(lambda x: sorted(x[0]))
    df_new = pd.merge(df_mongo_new, df_zoho_new, on='email')
    result = df_new
    return result


def format_table(df):
    # only for report5
    def _change_set_for_mongo(my_set):
        if len(my_set) == 0:
            result = None
        elif len(my_set) == 1:
            result = list(my_set)
        else:
            result = list(my_set)
        return result

    def _change_set_for_zoho(my_set):
        if len(my_set) == 0:
            result = None
        elif len(my_set) == 1:
            result = list(my_set)[0]
        else:
            result = list(my_set)
        return result

    def _my_test(mongo, zoho):
        my_dict = {}
        set_mongo = set(mongo)
        set_zoho = set(zoho)
        mongo_new = set_mongo - set_zoho
        same = set_mongo - mongo_new
        zoho_new = set_zoho - same
        my_dict['mongo_new'] = _change_set_for_mongo(mongo_new)
        my_dict['zoho_new'] = _change_set_for_zoho(zoho_new)
        result = my_dict
        return result
    df['dic'] = df.apply(lambda row: _my_test(row['listingid_mongo'], row['listingid_zoho']), axis=1)
    df['listingid_mongo'] = df['dic'].map(lambda x: x['mongo_new'])
    df['listingid_zoho'] = df['dic'].map(lambda x: x['zoho_new'])
    df.drop(['dic', 'listing_diff'], axis=1, inplace=True)
    df_result = df.explode('listingid_mongo')
    return df_result


if __name__ == "__main__":
    pass