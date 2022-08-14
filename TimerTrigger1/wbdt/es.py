import pandas as pd
import numpy as np
from .utils import functions_elasticsearch
# from utils import functions_elasticsearch
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
pd.set_option('display.width', 1000)
pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.max_colwidth', None)


def find_geocodes(listings=[]):
    """Get geography codes.

    Parameters
    ----------
    listings: list, default=[]
        Such as ['6446e09d-f9c0-4e64-9279-08d691f0f2f6', '68da7a2c-250b-9892-8737-0036719daf71'] or not specified.

    Returns
    -------
    result: DataFame
        Columns such as ['listingId', 'pointStr', 'level_1', 'geocode']
        Contents such as:
        listingId: 68da7a2c-250b-9892-8737-0036719daf71
        pointStr: [-104.497882, 52.328702]
        level_1: level_1
        geocode: ['ada-47140007', 'cd-4714', 'csd-4714028', 'da-47140222', 'fed-47014', 'fsa-S0K', 'ntl-01', 'pr-47']
    """
    result = None
    df_points = functions_elasticsearch.listing_lat_long(listings)
    if df_points is not None:
        df_points = df_points.loc[:, ['listingId', 'point']]
        df_points = df_points.rename(columns={'point': 'pointStr'})
        list_points = list(df_points['pointStr'])
        if np.nan in list_points:
            list_points.remove(np.nan)
        # list_points = list_points[2096:2103]
        df_geocodes = functions_elasticsearch.point_boundaries(list_points)
        if df_geocodes is not None:
            df_relevant_geocodes = df_geocodes.loc[:, ['geocode']]
            relevant_geocodes = df_relevant_geocodes.reset_index()
            df2 = relevant_geocodes.groupby(['pointStr']).agg({'level_1': list, 'geocode': list})
            df3 = df2.reset_index()
            df3['pointStr'] = df3['pointStr'].apply(str)
            df_points['pointStr'] = df_points['pointStr'].apply(str)
            result = pd.merge(df_points, df3, how='left', on='pointStr')
    return result


def get_datapoints(geocodes, geo_levels, data_point_ids):
    """Get data info through geography code and data_point_ids.

    Parameters
    ----------
    geocodes: list
        Such as ['ada-35240056', 'cd-3524', 'cmaca-537', 'csd-3524002', 'da-35240387', 'fed-35015', 'fsa-L7P']
    geo_levels: list.
        Such as ['ada', 'pr'].
    data_point_ids: list
        Such as [743, 751, 52, 51, 87, 68, 1149, 1141, 1142, 37].
    Returns
    -------
    results: DataFame
        Columns such as ['total 1141', 'total 1142', 'total 1149', 'total 37', 'total 51'].
        Index geocode such as ada-35240056,cd-3524,cmaca-537,csd-3524002,da-35240387,fed-35015,fsa-L7P.
    """
    result = functions_elasticsearch.area_data(geocodes, geo_levels, data_point_ids)
    return result


if __name__ == "__main__":
    # a = ['ab0b2653-ff3d-70a4-61dd-02ad6771d329', '23170145-5885-b9a3-dc84-046c6aff64e4',
    #      'fcb72880-6d99-4587-09cf-0f5165259df2', '6492d1b4-7062-2ea8-1c2d-0fdb4c64df1b',
    #      'fcd4bc05-e8a7-66e1-7ce7-10e01b5db3e0', '1a321409-7e76-7074-e828-163291dd0d52',
    #      '8889caa1-8e6d-071e-fe98-168bb93433fa', 'bc115cbf-4620-fa29-b8c2-16c98e12740d']
    geocode = find_geocodes(['e9be6025-5695-5ad8-3e64-b06629b6a287', '5fd801bbaeac9f000fbaa0e8'])
    # geocode = find_geocodes()
    print(geocode)
    # print("Completed successfully.")
    # geocode.to_csv('d:/geocode.csv')

    # geocode = ['ada-35240056', 'cd-3524', 'cmaca-537', 'csd-3524002', 'da-35240387', 'fed-35015', 'fsa-L7P']
    # datapoint_kids = [743, 751, 52, 51, 87, 68, 1149, 1141, 1142, 37]
    # df_datapoint = get_datapoints(geocode, ['ada', 'pr'], datapoint_kids)
    # print(df_datapoint)


