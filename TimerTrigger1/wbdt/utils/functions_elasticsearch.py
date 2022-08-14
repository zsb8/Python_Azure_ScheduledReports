"""
This module takes a list of listing id's, and:
1) finds the listing's coordinates,
2) return the list of boundaries within which each listing falls
3) for each unique boundary in 2), returns a requested list of census data points
"""
import os
import time
import pandas as pd
import numpy as np
from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search, MultiSearch
from . import constants
# from utils import constants
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
pd.set_option('display.width', 1000)


def generate_es_client():
    es_client = Elasticsearch(
        [constants.ELASTICSEARCH_LOCATION],
        http_auth=(os.environ.get("ES_USERNAME"), os.environ.get("ES_PASSWORD")),
        timeout=10, max_retries=3, retry_on_timeout=True
        )
    return es_client


def listing_lat_long(listing_ids=[]):
    """Download the coordinate(longitude and latitude) of listing.
    Some listing_ids can't find the id or location on ES, so the result rows may less than the number of input ids.
    If the listingId can't match ES, will return NoN in return df.
    If listing_ids=[], will get all from  DB->listing->listings, then return the points of all listing ids.
    Parameters
    ----------
    listing_ids : list
        Such as ['6446e09d-f9c0-4e64-9279-08d691f0f2f6', '68da7a2c-250b-9892-8737-0036719daf71'] or []
        Defaults to all if a list is not specified.

    Returns
    -------
    results: DataFame or None
        Columns are ['listingId', 'point', 'pointStr', 'longitude', 'latitude'].
        Contents such as:
        listingId:6446e09d-f9c0-4e64-9279-08d691f0f2f6
        point:[-79.835696, 43.336962]
        pointStr:[-79.835696, 43.336962]
        longitud:-79.835696
        latitude:43.336962
    """
    if listing_ids:
        pd_listing_ids = pd.Series(listing_ids)
    else:
        from . import functions_mongo
        # from utils import functions_mongo
        client = functions_mongo.create_mongo_client()
        df_coll = functions_mongo.download_collection(client, 'listing', 'listings', 'find', {})
        df_temp = df_coll.reset_index()
        pd_listing_ids = df_temp['_id']  # 2433
    listing_ids_clean = pd_listing_ids.drop_duplicates()
    ms = MultiSearch(using=generate_es_client(), index='web_search_listings')  # web_serach -> @type:"listing"
    """
    2119 later,such as  2119: 1ed3bf84-61e7-4f26-a7ce-402d0034aefc  all can't find info in ES
    2127 5fd801bbaeac9f000fbaa0e8   Notice! the type of this item is bson.objectid.ObjectId, not str
    """
    for i in listing_ids_clean:
        i = str(i)
        ms = ms.add(Search().filter("term", id=i))
    try:
        responses = ms.execute()
        df_res = pd.DataFrame.from_dict(r.hits.hits[0]._source.to_dict() for r in responses if len(r.hits.hits) > 0)
        df_temp = pd.DataFrame({"id": listing_ids})
        df_merge = pd.merge(df_res, df_temp, how='outer')
        results = split_ms_result(df_merge)
    except Exception as error:
        print(f"Get pointStr failed because Data too large or other error etc")
        print(error)
        results = None
    return results


def split_ms_result(df):
    """Split the id and location.point from the df.

    Parameters
    ----------
    df: DataFame
        Columns are ['@timestamp', '@type', 'id', 'createdAt', 'updatedAt', 'isPublic', 'accountId',
        'title', 'fulltext', 'keywords', 'location', 'locations', 'listing']
        Contents such as:
        id:dc229c7d-1e99-b1bf-46c0-1aaaffe0b2b0
        location:{'shape': {'type': 'polygon', 'coordinates': [...   Include point info.

    Returns
    -------
    results: DataFame or None
        Columns are ['listingId', 'point', 'pointStr', 'longitude', 'latitude'].
        Contents such as:
        listingId:6446e09d-f9c0-4e64-9279-08d691f0f2f6
        point:[-79.835696, 43.336962]
        pointStr:[-79.835696, 43.336962]
        longitud:-79.835696
        latitude:43.336962
    """
    result = None
    df_id = df.loc[:, ['id']]
    if not df.empty and set(['id', 'location']).issubset(df.columns):
        df = df[df['id'].notnull()]  # filter out listings that don't have a id
        if not df.empty:
            df['listingId'] = df['id']
            df['point'] = df['location'].apply(pd.Series)['point']
            df = df[df['point'].notnull()]  # filter out listings that don't have a id
            df['pointStr'] = df['point'].apply(str)
            df['longitude'] = df['point'].map(lambda x: x[0])
            df['latitude'] = df['point'].map(lambda x: x[1])
            df_merge = pd.merge(df, df_id, how='outer')
            df_merge['listingId'] = df_merge['id']
            result = df_merge[['listingId', 'point', 'pointStr', 'longitude', 'latitude']]
    return result


def point_boundaries(points, geo_level=""):
    """These boundaries come mostly from Statcan.
    They are ways of dividing up the country into areas for census purposes.
    Each boundary is given a geocode that is an id for the area.

    Parameters
    ----------
    points : list
        Such as [[-79.835696,43.336962],[-104.497882, 52.328702]].
    geo_level : str, default=""

    Returns
    -------
    results: DataFame or None
        Columns are ['level', 'geocode', 'geometry', 'source']. Index is ['pointStr'].
        level: ada. It is a level of geographic aggregation,might be provincial, or a dissemination area.
        geocode: ada-35240056. It is an id for the area.
        geometry: include many lats in a dict,
        source: Statcan. Statistics Canada , gov institution.
    """
    begin_time = int(time.time())
    list_points = del_nan_in_lst(points)
    if len(list_points) > 0:
        size = constants.NS_SPLIT_BLOCK_SIZE
        list_blocks = split_list(list_points, size)
        list_df = []
        j = 1
        for i in list_blocks:
            print(f"This block is the number {j}. Scope from {size*(j-1)} to {size*j-1} ")
            df = ms_boundaries(i, geo_level)
            if df is not None:
                list_df.append(df)
            else:
                print("Try once again.")
                sub_list_blocks = split_list(i, size/4)
                sub_list_df = []
                for sub_i in sub_list_blocks:
                    sub_df = ms_boundaries(sub_i, geo_level)
                    if sub_df is not None:
                        sub_list_df.append(sub_df)
                    else:
                        print("Trying failed. Discard these sub block data")
                        print(f"This block failed, the scope is from {100 * (j - 1)} to {100 * j - 1} ")
                        sub_list_df = []
                        break
                sub_df = pd.concat(sub_list_df) if sub_list_df else None
                if sub_df is not None:
                    list_df.append(pd.concat(sub_list_df))
            j += 1
        try:
            results = pd.concat(list_df)
        except Exception as error:
            print("point_boundaries() error! ")
            print(error)
            results = None
    else:
        results = None
    end_time = int(time.time())
    print(f"Now datetime is {time.ctime()}. point_boundaries() cost  {round((end_time - begin_time) / 60, 2)} minutes. ")
    return results


def ms_boundaries(lst, geo_level):
    """This is the sub program of point_boundaries. Get boundaries data with ElasticSearch.

    Parameters
    ----------
    lst: list
    geo_level: str

    Returns
    -------
    result: DataFame or None
    """
    i = lst
    ms = MultiSearch(using=generate_es_client(), index='boundaries-2016')
    df = pd.DataFrame({"point": i})
    for index, row in df.iterrows():
        p_item = list(row)[0]  # Such as [-79.835696, 43.336962]
        if geo_level == "":
            ms = ms.add(Search().filter("geo_shape", geometry={"shape": {"type": "point", "coordinates": p_item}}))
        else:
            ms = ms.add(Search().filter("geo_shape", geometry={"shape": {"type": "point", "coordinates": p_item}})
                        .filter("term", level=geo_level))
    try:
        responses = ms.execute()
        list_rows = []
        for r in range(len(responses)):
            df_row = pd.DataFrame.from_dict(h._source.to_dict() for h in responses[r].hits.hits)
            df_row["pointStr"] = str(i[r])
            list_rows.append(df_row)
        df_results = pd.concat(list_rows)
        df_results = df_results.set_index(["pointStr", df_results.index])
        result = df_results
    except Exception as error:
        # print(f"This block failed, the scope is from {100 * (j - 1)} to {100 * j - 1} ")
        print(f"point_boundaries() error:\n {error}")
        result = None
    return result


def del_nan_in_lst(lst):
    """If nan in the list, will delete it.

    Parameters
    ----------
    lst: list

    Returns
    -------
    lst: list
    """
    while np.nan in lst:
        lst.remove(np.nan)
    return lst


def split_list(lst, n):
    """Split the list to m blocks, every block is n length.

    Parameters
    ----------
    lst: lsit
        Such as [[-79, 46], [-123, 49], [-99, 50], [-79, 43], [-80, 43], [-79, 43]]
    n: int
        Such as 3

    Returns
    -------
    result: list
        Such as [ --  [[-79, 46], [-123, 49]],  --  [[-99, 50], [-79, 43]],  -- [[-80, 43], [-79, 43]]  ]
    """
    if len(lst) % n == 0:
        m = int(len(lst) / n)
        m = 1 if m == 0 else m
    else:
        m = int(len(lst) / n) + 1
    division = len(lst) / m
    result = [lst[round(division * i):round(division * (i + 1))] for i in range(m)]
    return result


def area_data(geocodes=None, geo_levels=None, data_point_ids=None, pivot=True):
    """Pulls census data stored in Elasticsearch based on criteria provided.
    We take the coordinate for every church and determine what boundary geocode this church exists in.

    Parameters
    ----------
        geocodes: list
            Such as ['ada-35240056'] , and its pure code is 35240056.
        geo_levels: list
            Such as ['ada']. 'ada' is the level which stands for Aggregated Dissemination Area.
        data_point_ids : list
            Data point id's to be retrieved. Such as [743,751,52,51,87,68,1149,1141,1142,37].
            In 98-401-X2016059_English_meta.txt
        pivot : bool
            Transpose, adjust data angle

    Returns
    -------
    results: DataFame or None
        ['total 1141', 'total 1142', 'total 1149', 'total 37', 'total 51',
       'total 52', 'total 68', 'total 743', 'total 751', 'total 87',
       'male 1141', 'male 1142', 'male 1149', 'male 37', 'male 51', 'male 52',
       'male 68', 'male 743', 'male 751', 'male 87', 'female 1141',
       'female 1142', 'female 1149', 'female 37', 'female 51', 'female 52',
       'female 68', 'female 743', 'female 751', 'female 87']
    """
    if type(data_point_ids) != list:
        print("Must pass list of DataPoint Id's")
        return None
    client = generate_es_client()
    if type(geocodes) == list:
        if 0 < len(geocodes) < 1000:
            s = Search(using=client, index='census-2016')\
                .filter("terms", geocode=geocodes).filter("terms", datapointId=data_point_ids)
        else:
            print("For 1000 or more geocodes, use geolevels")
            return None
    elif type(geo_levels) == list:
        s = Search(using=client, index='census-2016').filter("terms", level=geo_levels)\
            .filter("terms", datapointId=data_point_ids)
    else:
        print("Provide either a list of geocodes or geolevels")
        return None
    response = s.scan()
    results = pd.DataFrame([h.to_dict() for h in response])
    if pivot:
        try:
            result_sp = results[["geocode", "datapointId", "total", "male", "female"]].\
                pivot(index=["geocode"], columns="datapointId", values=["total", "male", "female"])
            result_sp.columns = [' '.join(col).strip() for col in result_sp.columns.values]
            results = result_sp
        except Exception as error:
            print(error)
            print("Error! Unable to pivot.")
    return results


if __name__ == "__main__":
    print("===========get points===========")
    a = listing_lat_long(['6446e09d-f9c0-4e64-9279-08d691f0f2f6', '11111'])
    # a = listing_lat_long()
    print(type(a))
    print(a.columns)
    print(a.head(2))
    print(a.shape)

    # print("==============get boundaries==============")
    # b = point_boundaries([[-79.835696, 43.336962]])
    # print(type(b))
    # print(b.columns)
    # print(b.head(1))
    #
    # print("=============get area data of population survey=========")
    # c = area_data(['ada-35240056'], ['ada'], list(range(1, 3)))
    # print(c)
    # print(type(c))
    # print(c.head(1))
    # print(c.columns)

    # test = point_boundaries([[-79.835696, 43.336962], [-104.497882, 52.328702]])
    # print(test)
