import pandas as pd
from .utils import functions
import numpy as np
new_columns = ['_id', 'name', 'phones', 'First_links', 'status', 'reach.type', 'tags.category', 'tags.secondarycategory', 'tags.language']


def get_obj_in_col(df_mongo):
    df_mongo.drop(index=df_mongo.loc[df_mongo['_id'] == ''].index, axis=0, inplace=True)
    df_mongo['phones'] = df_mongo['phones'].apply(lambda x: x[0]['number'] if x else None)
    df_mongo['tags.category'] = df_mongo['tags.category'].apply(
        lambda x: ','.join(f'{i.strip()}' for i in x) if x else None)
    df_mongo['tags.secondarycategory'] = df_mongo['tags.secondarycategory'].apply(lambda x: x[0] if x else None)
    df_mongo['tags.language'] = df_mongo['tags.language'].apply(
        lambda x: ', '.join(f'{i.strip()}' for i in x) if x else None)
    my_result = df_mongo
    return my_result


def get_two_df():
    """
    @DaprSubscriber('listing_updated')
    @DaprSubscriber('listing_claimed')
    @DaprSubscriber('listing_deleted')
    async upsertOrg({ listing })
    ZOHO : orgs : ['ListingId','Account_Name','Name','Phone','Primary_URL','Status','Reach','Primary_Category','Secondary_Category','Languages','Listing_Type']
    """
    zoho_columns = ['ListingId', 'Name', 'Phone', 'Primary_URL', 'Status', 'Reach', 'Primary_Category',
                    'Secondary_Category', 'Languages']
    df_zoho = functions.get_df_from_zoho('Orgs', 'Orgs', zoho_columns, new_columns)
    # listing-->@validateCommand(validators.createListing)
    df_mongo = functions.get_df_from_mongo('listing', 'listing.listings', new_columns)
    df_mongo = get_obj_in_col(df_mongo)
    df_mongo = df_mongo.drop_duplicates()
    return df_mongo, df_zoho


def org_listing_recon():
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
            'name_mongo', 'name_zoho',
            'phones_mongo', 'phones_zoho',
            'First_links_mongo', 'First_links_zoho',
            'status_mongo', 'status_zoho',
            'reach.type_mongo', 'reach.type_zoho',
            'tags.category_mongo', 'tags.category_zoho',
            'tags.secondarycategory_mongo', 'tags.secondarycategory_zoho',
            'tags.language_mongo', 'tags.language_zoho'
            ]
    df_new = res.merge(df_new, how="left", on=col1)
    df_new = df_new.replace(np.nan, '')
    result = functions.arrange_format(df_new, new_columns)
    return result


if __name__ == "__main__":
    df_report3 = org_listing_recon()
    # df_report3.to_csv('d:/df_report3.csv')