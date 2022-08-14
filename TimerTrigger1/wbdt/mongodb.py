from .utils import functions_mongo, constants


def download(database='*', download=True, path='', charity_mode='mostrecent') -> dict:
    """Pull data from WayBase database. Extract data from combined columns, such as 'tags' and 'locations'.

    Parameters
    ----------
    database : str, default='*'
    download : bool, default=True
        True means we need to save files, False means we not need to save it.
    path : str, default=''
        If path='', program will let download_path as 'd:/' or '/tmp/' in path_save_csv().
    charity_mode: str, default="mostrecent"
        The query mode should be one of the 'mostrecent' or 'recent' or 'other'.

    Returns
    -------
    dict_coll: dict

    """
    path = functions_mongo.path_save_csv(path)
    if database == '*':
        database = constants.databases
    elif isinstance(database, str):
        database = [database]
    elif isinstance(database, list):
        pass
    else:
        print("Invalid input type")
        return None
    if set(database).issubset(set(constants.databases)):
        client = functions_mongo.create_mongo_client()
        dict_coll = {}
        for database_name in database:
            for collection_name in constants.databases[database_name]:
                if database_name == 'charity' and collection_name == 'charities':
                    df_coll = functions_mongo.download_collection(client, database_name, collection_name,
                                                                  'aggregate', {}, charity_mode)
                else:
                    df_coll = functions_mongo.download_collection(client, database_name, collection_name, 'find', {})
                if df_coll is None:
                    print(f"Result is empty or meet errors. cllection_name is {collection_name}")
                else:
                    df_coll_proc = functions_mongo.process_collection(df_coll, collection_name, download, path)
                    dict_coll[database_name + "." + collection_name] = df_coll_proc
                    if download:
                        df_coll_proc.to_csv(path + database_name + "_" + collection_name + ".csv")
        return dict_coll
    else:
        print("Not a valid set of databases.")
        return None


if __name__ == "__main__":
    dict_result = download("charity", True)
    print(f"The len of dict_result is : {len(dict_result)}")
    print("Data downloaded")
