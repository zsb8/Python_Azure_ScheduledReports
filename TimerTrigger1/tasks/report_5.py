from .utils import functions
import pandas as pd
import logging


def get_two_df():
    df_mongo = functions.get_df_from_mongo('listing', 'listing.listings', ['_id', 'people', 'email'])
    df_mongo.rename(columns={'_id': 'listingid'}, inplace=True)
    df_m_people = df_mongo.copy()
    df_m_company = df_mongo.copy()
    # 这里只处理people 邮箱。
    df_m_people.drop(index=df_m_people.loc[df_m_people['people'].str.len() == 0].index, axis=0, inplace=True)
    df_m_people.drop(index=df_m_people.loc[df_m_people['people'] == ''].index, axis=0, inplace=True)   # 这个好像没啥用，没有''空的
    # 将people这列里面乱七八糟的邮箱，全部提纯出来，形成干净的邮件列表
    df_m_people['people_email'] = df_m_people['people'].apply(lambda x: functions.split_email(x))
    df_m_people.drop(['people', 'email'], axis=1, inplace=True)
    # 利用explode来将列表邮箱，全部展开为多行
    df_m_people.drop(index=df_m_people.loc[df_m_people['people_email'].str.len() == 0].index, axis=0, inplace=True)
    df_m_people = df_m_people.explode('people_email')
    df_m_people.rename(columns={'people_email': 'email'}, inplace=True)
    df_m_people = df_m_people.fillna('')
    df_m_people.drop(index=df_m_people.loc[df_m_people['email'] == ''].index, axis=0, inplace=True)
    # 这里只处理企业邮箱。仅对企业邮箱处理，这里删除掉空的没有企业邮箱的记录，等下要合并人民邮箱和企业邮箱
    df_m_company.drop('people', axis=1, inplace=True)
    df_m_company.drop(index=df_m_company.loc[df_m_company['email'] == ''].index, axis=0, inplace=True)
    print(f'企业邮箱有{df_m_company.shape[0]}行')  # 26387行
    # 人民邮箱和企业邮箱合并去重，发现的确不少重复的。从51420去重后得到41689个邮箱记录。成为新的芒果邮箱
    df_mongo = pd.concat([df_m_people, df_m_company], axis=0)
    df_mongo = df_mongo.drop_duplicates()
    print(f"合并后有{df_mongo.shape[0]}行")  # 39498行
    # 开始抓取ZOHO的数据
    df_zoho_email = functions.get_mails_zoho().loc[:, ['email', 'listingId']]
    df_zoho_email.rename(columns={'listingId': 'listingid'}, inplace=True)
    return df_mongo, df_zoho_email


def listings_people_recon():
    df_mongo, df_zoho = get_two_df()
    df = pd.merge(df_mongo, df_zoho, how='left', suffixes=('_mongo', '_zoho'), on=['email'])
    df['email_judge'] = df.isnull()['listingid_zoho'].apply(lambda x: 'only_mongodb' if x else 'both')
    df_email_only_mongodb = df.loc[df["email_judge"] == 'only_mongodb'].loc[:, ['email', 'listingid_mongo']]
    df_email_only_mongodb['diff'] = 'email in Mongo but not in Zoho'
    df_email_only_mongodb['listingid_zoho'] = ''

    df_listingid_zoho_empty = df.loc[df["email_judge"] == 'both'].loc[df["listingid_zoho"] == ''].loc[:, ['email', 'listingid_mongo', 'listingid_zoho']]
    df_listingid_zoho_empty['diff'] = 'email in Zoho but not in Mongo'

    df_both_email = df.loc[df["email_judge"] == 'both'].loc[:, ['email', 'listingid_mongo', 'listingid_zoho']]
    df_both_email['listingid_zoho'] = df_both_email['listingid_zoho'].str.lower()
    df_both_email_group = functions.group_by_df(df_both_email, ['listingid_mongo', 'listingid_zoho'])
    df_both_email_group['listing_diff'] = list(map(lambda x, y: y <= x,
                                                   df_both_email_group['listingid_mongo'],
                                                   df_both_email_group['listingid_zoho']))
    df_both_email_diff = df_both_email_group.loc[df_both_email_group["listing_diff"] == False]
    df_both_email_diff = functions.format_table(df_both_email_diff)
    df_both_email_diff['diff'] = 'email connected to listing in Mongo but not in Zoho'
    df_result = pd.concat([df_email_only_mongodb, df_listingid_zoho_empty, df_both_email_diff], axis=0)
    df_result = df_result[['email', 'diff', 'listingid_mongo', 'listingid_zoho']]
    df_result.reset_index(drop=True, inplace=True)
    return df_result


if __name__ == "__main__":
    listings_people_recon()
