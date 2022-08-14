import pandas as pd
import numpy as np
import platform
from pymongo import MongoClient
from . import queries, settings
# from utils import queries, settings


def create_mongo_client() -> object:
    """
    The connection to MongoDB.

    Returns:
    -------
    client : obj
    """
    db_info = settings.DB_INFO
    client = MongoClient(db_info['host'])
    return client


def download_collection(client, db, col, mode='find', query={}, charity_mode="") -> pd.DataFrame:
    """Download data from collection

    Parameters
    ----------
    client : function
        The connection of MongoDB
    db : str
        Database name, such as 'account'.
    col : str
        Collection name, such as 'accounts'.
    mode : str, default='find'
    query ： dict, default={}
    charity_mode : str, default=''

    Returns
    -------
    df,df_empty : DataFrame
    """
    try:
        db = client.get_database(db)
        collection = db[col]
        if mode == 'find':
            cursor = collection.find(query)
        elif mode == 'aggregate':
            if charity_mode == 'mostrecent':
                query = queries.CHARITY_QUERY_MOSTRECENT
            elif charity_mode == 'recent':
                query = queries.CHARITY_QUERY_RECENT
            elif charity_mode == 'other':
                query = queries.CHARITY_QUERY_OTHER
            cursor = collection.aggregate(query)
        else:
            return None
        docs = list(cursor)
        coll_docs = []
        for items in enumerate(docs):
            coll_docs.append(items)
        df = pd.json_normalize(pd.DataFrame(coll_docs)[1]).set_index("_id")
        return df
    except Exception as error:
        # print(f"Error is:\n {error}")
        # print("Problem downloading ", col, " collection")
        return None


def process_collection(df, coll_name, download, download_path) -> pd.DataFrame:
    """Wrapper function for custom processing functions of specific collections.
    If collection is "listings", we need to process more with the special functions.
    Otherwise, it is easy to only return df.

    Parameters
    ----------
    df : DataFrame
        Collection data. Such as the data which got from download_collection().
    coll_name : str
        Collection name to determine specific transformations to apply.
    download: bool
        True means we need to save files, False means we not need to save it.
    download_path : str
        Windows path such as "d:/" , Ubuntu such as "/tmp/.

    Returns
    -------
    df : DataFrame
        Processed collection
    """
    if coll_name == 'listings':
        df = format_listings(df, download, download_path)
    if coll_name == 'programs':
        df = format_programs(df, download, download_path)
    if coll_name == 'causes':
        df = format_cause(df, download, download_path)
    return df


def format_listings(df, download, download_path) -> pd.DataFrame:
    """Listings specific collection formatting and additional table extraction.

    Parameters
    ----------
    df : DataFrame
        Data passed from generic format_collections.
    download: bool
        True means we need to save files, False means we not need to save it.
    download_path : str
        Windows path such as "d:/" , Ubuntu such as "/tmp/.
    Returns
    -------
    df_result : DataFrame
    """
    df2 = df.copy()
    # Process tags with count and first and specific others.
    other_lists = ['phones', 'links']
    for col in [c for c in df.columns if 'tags' in c] + other_lists:
        df2 = df2.join(simplify_list(df2[col]))
    # Useful category groups.
    df2['TopCategory'] = df2['First_category'].dropna().apply(lambda x: x.split("-")[0])
    # External ids and charity number (original data from CRA).
    df_xids = extract_subtable(df2['externalIds'])[['externalIds.source', 'externalIds.value']]
    # Find the max charity number and apply to original listing df.
    df_s_bn = df_xids[df_xids['externalIds.source'] == 'cra']['externalIds.value'].rename('charityNumber').copy()
    df_s_bn = df_s_bn.groupby('_id').max()
    df2 = df2.join(df_s_bn)
    df_locations, df2 = process_location(df2)
    # Languages and ethnicity
    df2 = df2.join(languages_culture_aggregator(df2))
    # Drop unnecessary fields.
    dropcols_startswith = ('logo.', 'photo.')
    dropcols = [c for c in df2.columns if c.startswith(dropcols_startswith)]
    df_result = df2.drop(dropcols, axis=1)
    if download:
        download_to_csv(download_path, df_xids, df_locations, df_result)
    return df_result


def format_programs(df, download, download_path) -> pd.DataFrame:
    """Program specific collection formatting and additional table extraction.
    Only for program.programs

    Parameters
    ----------
    df : DataFrame
        Data passed from generic format_collections.
    download: bool
        True means we need to save files, False means we not need to save it.
    download_path : str
        Windows path such as "d:/" , Ubuntu such as "/tmp/.
    Returns
    -------
    df_result : DataFrame
    """
    df2 = df.copy()
    # Delete the fields do not need to be transformed for now.
    del_lists = ['actions', 'resources', 'weeklyEvents']
    for i in ['logo', 'bannerImage']:
        del_lists += [c for c in df.columns if i in c]
    df2.drop(del_lists, axis=1, inplace=True)
    # Process cols with count and first and specific others.
    extract_list = ['listingIds', 'categories', 'sdgs']
    simplify_list_lists = extract_list + ['links', 'locations']
    for col in simplify_list_lists:
        df2 = df2.join(simplify_list(df2[col]))
    # External  as a individual table. ['listingIds', 'categories', 'sdgs', 'contacts']
    extract_list = extract_list + ['contacts']
    for col in extract_list:
        if col == 'contacts':
            df_col = extract_subtable(df2['contacts'], False)[['contacts.name', 'contacts.title', 'contacts.email']]
        else:
            df_col = df2.loc[:, [col]].dropna().explode(col).dropna()
        if download:
            df_col.to_csv(download_path + "program_programs_" + col + ".csv")
    return df2


def format_cause(df, download, download_path) -> pd.DataFrame:
    """Cause specific collection formatting and additional table extraction.
    Only for cause.causes

    Parameters
    ----------
    df : DataFrame
        Data passed from generic format_collections.
    download: bool
        True means we need to save files, False means we not need to save it.
    download_path : str
        Windows path such as "d:/" , Ubuntu such as "/tmp/.
    Returns
    -------
    df_result : DataFrame
    """
    df2 = df.copy()
    # Delete the fields do not need to be transformed for now.
    del_lists = ['resources']
    for i in ['logo', 'bannerImage']:
        del_lists += [c for c in df.columns if i in c]
    df2.drop(del_lists, axis=1, inplace=True)
    # Process cols with count and first and specific others.
    extract_list = ['categories']
    simplify_list_lists = extract_list + ['links', 'serviceDeliveryRegions']
    for col in simplify_list_lists:
        df2 = df2.join(simplify_list(df2[col]))
    # External as a individual table. ['categories', 'contacts', 'metrics', 'needs']
    extract_list = extract_list + ['contacts', 'metrics', 'needs']
    for col in extract_list:
        if col == 'contacts':
            df_col = extract_subtable(df2['contacts'], False)[['contacts.name',
                                                               'contacts.email']]
        elif col == 'metrics':
            df_col = extract_subtable(df2['metrics'], False)[['metrics.label',
                                                              'metrics.targetValue',
                                                              'metrics.type',
                                                              'metrics.value']]
        elif col == 'needs':
            df_col = extract_subtable(df2['needs'], False)[['needs.description',
                                                                 'needs.title',
                                                                 'needs.type',
                                                                 'needs.action',
                                                                 'needs._id']]
            df_col = df_col.fillna('')

            def _parse_dict(x, field_name):
                result_col = ''
                if x and isinstance(x, dict):
                    if field_name in x:
                        result_col = x[field_name]
                return result_col
            df_col['needs.action.type'] = df_col['needs.action'].apply(lambda x: _parse_dict(x, 'type') if x else np.nan)
            df_col['needs.action.label'] = df_col['needs.action'].apply(lambda x: _parse_dict(x, 'label') if x else np.nan)
            df_col['needs.action.url'] = df_col['needs.action'].apply(lambda x: _parse_dict(x, 'url') if x else np.nan)
            df_col.drop('needs.action', axis=1, inplace=True)
        else:
            df_col = df2.loc[:, [col]].dropna().explode(col).dropna()
        if download:
            df_col.to_csv(download_path + "cause_causes_" + col + ".csv")
    return df2


def process_location(df) -> pd.DataFrame:
    """Extract info from 'location' column.

    Parameters
    ----------
    df : DataFame
        Include the 'location' column.

    Returns
    -------
    df_locations : DataFame
        only are the info of location.
    df_result : DataFrame
        All the columns, include the location info.
    """
    df_locations = extract_subtable(df['locations'], False).rename(columns={
        'locations.types': 'address.type',
        'locations.name': 'address.name',
        'locations.address': 'address'})
    df_locations = df_locations.explode('address.type').set_index(['address.type', 'address.name'], append=True)
    df_locations = pd.DataFrame.from_records(df_locations['address'], index=df_locations.index)
    df_locations.columns = ['address.' + c for c in df_locations.columns]
    """
    The indexes of df_locations are address.type and address.name,
    The columns are ['address.street', 'address.locality', 'address.region', 'address.postalCode', 'address.country']
    The address.type content is 'maped' or 'mailing'.
    Add mapped address to primary DataFrame.
    """
    df_tmp1 = df_locations.loc[:, 'mapped', :]
    list_tmp = list(df_tmp1.index.names)
    del list_tmp[0]
    df_result = df.join(df_tmp1.reset_index(list_tmp))
    return df_locations, df_result


def download_to_csv(download_path, df_xids, df_locations, df) -> bool:
    """ Download data to some CSV files. Include xids, locations, links, tags, and peoples.

    Parameters
    ----------
    download_path:str
    df_xids : DataFrame
    df_locations : DataFrame
    df : DataFrame

    Returns
    -------
    result : bool
    """
    try:
        if download_path:
            df_xids.to_csv(download_path + "listing_listings_externalIds.csv")
            df_locations.to_csv(download_path + "listing_listings_locations.csv")
            # Explode links.
            df_links = pd.DataFrame(df.links.dropna().explode().dropna())
            df_links.to_csv(download_path + "listing_listings_links.csv")
            # Process tags.

            def _process_listingtags(df_listings) -> pd.DataFrame:
                """Explodes tag-type columns, unions them, removes duplicates, then exports.

                Parameters
                ----------
                df_listings : DataFrame
                    Fully processed listings DataFrame.

                Returns
                -------
                df_result : DataFrame
                    Fields(
                        tagValue : (str)  Such as 'all-ages'
                        tagType : (str)  Such as 'tags.age'
                    )
                """
                list_columns = ['ownsProperty', 'statementsOfFaith', 'ethnic']
                tag_type_cols = [t for t in df_listings.columns if t.startswith("tags")] + list_columns
                df_temp = pd.DataFrame()
                for c in tag_type_cols:
                    df2 = pd.DataFrame(df_listings[c]).rename(columns={c: 'tagValue'})
                    df2['tagType'] = c
                    df_temp = df_temp.append(df2.dropna())
                df_result = df_temp.dropna().explode('tagValue').dropna()
                return df_result

            df_tags = _process_listingtags(df)
            df_tags.to_csv(download_path + "listing_listings_tags.csv")
            # Explode people.
            df_people = extract_subtable(df['people'])
            df_people.to_csv(download_path + "listing_listings_people.csv")
            print("Download successful.")
            result = True
        else:
            result = False
    except Exception as error:
        print("Download failed.")
        print(error)
        result = False
    return result


def simplify_list(df_col, count=True, first=True) -> pd.DataFrame:
    """Delete the empty rows in the df_col. Takes a list of value (mainly for tags) and simplifies to a single value.
    Creates a column for providing a summary statistic, it is usually a count number of values being reduced.

    Parameters
    ----------
    df_col : series
        Every column (only 'tags', 'phones' or 'links')flag data in df.
    count : bool, default=True
    first : bool, default=True

    Returns
    -------
    df_result : DataFrame
        The dataset includes 2 new columns(count number and first value).
    """
    if not df_col.empty:
        df_col = df_col.dropna()
        # If column was the result of a json normalization, take the sub-column name.
        col_name = df_col.name.split(".")[1] if "." in df_col.name else df_col.name
        df = pd.DataFrame(df_col)
        if count:
            df['Count_'+col_name] = df[df_col.name].apply(lambda x: len(x) if len(x) > 0 else np.nan)
        if first:
            df['First_'+col_name] = df[df_col.name].apply(lambda x: x[0] if len(x) > 0 else np.nan)
        df_result = df.drop(columns=[df_col.name])
    else:
        df_result = df_col
        print("The simplify_list() did nothing.")
    return df_result


def extract_subtable(data, add_list_pos=True) -> pd.DataFrame:
    """Takes a series containing an embedded table in the form of a list of dicts and creates it as its own DataFrame.

    Parameters
    ----------
    data : series
        Such as _id NaN or _id [{'source': 'cra', 'value': '141049734RR0001'}].
    add_list_pos : bool, default=True
        Used for the 'externalIds' column.

    Returns
    -------
    df_result : DataFrame
        Returned as DataFrame with index.
    """
    if not data.empty:
        df = pd.DataFrame(data.dropna())
        if add_list_pos:
            df['listPos'] = df.apply(lambda x: range(len(x[data.name])), axis=1)
            df.drop(index=df.loc[df[data.name].str.len() == 0].index, axis=0, inplace=True)
            column_list = list(df.columns)    # such as ['externalIds', 'listPos']
            df = df.explode(column_list)
            df = df.dropna().set_index('listPos', append=True)
        else:
            df = df.explode([c for c in df.columns]).dropna()
        df = pd.DataFrame.from_records(df[data.name], index=df.index)
        df.columns = [str(data.name) + "." + str(c) for c in df.columns]
        df_result = df
    else:
        df_result = data
        print("The extract_subtable() has an empty result.")
    return df_result


def languages_culture_aggregator(df_listings) -> pd.DataFrame:
    """Special processor for languages and culture to create proxies for culture as non-Canadian ethnicity.

    Parameters
    ----------
    df_listings : DataFrame

    Returns
    -------
    df_result : DataFrame
        Fields(
            languages : (list) Languages+ combines listing languages with service languages into a single unique list
            Count_languages : (int)  Length of languages list (null if 0)
            nonOffLanguages : (list)
            Count_nonOffLanguages : (int) Length of only non-official languages
            ethnic : (bool)
        )
    """
    if not df_listings.empty:
        listing_languages = df_listings['tags.language'].dropna().explode()
        service_languages = extract_subtable(df_listings['weeklyServices'], add_list_pos=False)['weeklyServices.tags']
        service_languages = service_languages.apply(lambda x: x['language']).explode()
        allnormedlanguages = listing_languages.append(
            service_languages).reset_index().drop_duplicates().set_index('_id').dropna()
        allnormedlanguages.columns = ['tags.language']
        alllanguages = pd.DataFrame(allnormedlanguages.groupby('_id')['tags.language'].apply(list))
        alllanguages = alllanguages.rename(columns={'tags.language': 'languages'})
        alllanguages['Count_languages'] = alllanguages['languages'].apply(len)
        off_langcodes = ['eng', 'fra']
        nonofflangs = allnormedlanguages[~allnormedlanguages['tags.language'].isin(off_langcodes)]
        nonofflangs = pd.DataFrame(nonofflangs.groupby('_id')['tags.language'].apply(list))
        nonofflangs['Count_nonOffLanguages'] = nonofflangs['tags.language'].apply(len)
        nonofflangs = nonofflangs.rename(columns={'tags.language': 'nonOffLanguages'})
        df_ethnic = alllanguages.join(nonofflangs, how='outer')
        df_ethnic = df_ethnic.join(df_listings[df_listings['Count_culture'] > 0][[
            'tags.culture', 'Count_culture']], how='outer')
        df_ethnic['ethnic'] = df_ethnic.apply(lambda x: x['Count_nonOffLanguages'] > 0 or x['Count_culture'] > 0, axis=1)
        df_ethnic = df_ethnic.drop(columns=['tags.culture', 'Count_culture'])
        df_result = df_ethnic
    else:
        df_result = df_listings
        print("The anguages_culture_aggregator() has an empty result.")
    return df_result


def path_save_csv(path):
    """Change the path of saving csv files through judge operation system.
    Windows path such as "d:/" , Ubuntu such as "/tmp/.

    Parameters
    ----------
    path : str

    Returns
    -------
    path: str

    """
    if path:
        pass
    else:
        operation = platform.uname()
        if operation[0] == 'Linux':
            path = '/tmp/'
        if operation[0] == 'Windows':
            path = 'd:/'
    return path

