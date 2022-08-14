import datetime
import logging
import azure.functions as func
from .tasks import reports
from azure.storage.fileshare import ShareFileClient
import inspect
import platform
import pandas as pd
import time
from .gdrive import gdrive_functions as gf
from io import BytesIO
import traceback


def main(mytimer: func.TimerRequest) -> None:
    utc_timestamp = datetime.datetime.utcnow().replace(tzinfo=datetime.timezone.utc).isoformat()
    logging.info('version is 2022-06-22-04')
    if mytimer.past_due:
        logging.info('!!!!!!!!! The timer is past due!')
    logging.info('!!!!!!!!! Python timer trigger function ran at %s', utc_timestamp)
    compare_reports()


def compare_reports():
    try:
        connection_string = "DefaultEndpointsProtocol=https;EndpointSuffix=core.windows.net;AccountName=waybaseda;AccountKey=V02b46DB+w6MojVqBAWIrTlfa5GUbROvjVUSc2PA93zQbFgiZ2RTBy8p8SUenL3SHtHRzZJ/vwwT+AStTqbjEQ=="
        b = BytesIO()
        df_report0 = pd.DataFrame({'A1111111111': [4, 5, 6], 'B1111111111111': [4, 1, 1]})
        df_report0.to_csv(b)
        file_client0 = ShareFileClient.from_connection_string(conn_str=connection_string, share_name="compare-reports",
                                                              file_path="Begin_to_run.xls")
        file_client0.upload_file(b.getvalue())
        b.close()

        begin_time = int(time.time())
        df_report1 = reports.report1()
        df_report2 = reports.report2()
        df_report3 = reports.report3()
        df_report4 = reports.report4()
        df_report5 = reports.report5()
        end_time = int(time.time())
        logging.info(f"!!! This 5 tasks spend ： {round((end_time - begin_time) / 60, 2)} minutes.")  # 10 minutes

        # df_report1 = pd.DataFrame({'A1111111111': [4, 5, 6], 'B1111111111111': [4, 1, 1]})
        # df_report2 = pd.DataFrame({'A2222222222': [4, 5, 6], 'B2222222222222': [4, 1, 1]})
        # df_report3 = pd.DataFrame({'A3333333333': [4, 5, 6], 'B3333333333333': [4, 1, 1]})
        # df_report4 = pd.DataFrame({'A3333333333': [4, 5, 6], 'B3333333333333': [4, 1, 1]})
        # df_report5 = pd.DataFrame({'A3333333333': [4, 5, 6], 'B3333333333333': [4, 1, 1]})
        logging.info("!!! ================= Begin to save to the Azure.")
        b = BytesIO()
        df_report1.to_csv(b)
        file_client1 = ShareFileClient.from_connection_string(conn_str=connection_string, share_name="compare-reports",
                                                              file_path="report1_AccountRecon.csv")
        file_client1.upload_file(b.getvalue())
        b.close()
        b = BytesIO()
        df_report2.to_csv(b)
        file_client2 = ShareFileClient.from_connection_string(conn_str=connection_string, share_name="compare-reports",
                                                              file_path="report2_AccountUserRecon.csv")
        file_client2.upload_file(b.getvalue())
        b.close()
        b = BytesIO()
        df_report3.to_csv(b)
        file_client3 = ShareFileClient.from_connection_string(conn_str=connection_string, share_name="compare-reports",
                                                              file_path="report3_OrgListingRecon.csv")
        file_client3.upload_file(b.getvalue())
        b.close()
        b = BytesIO()
        df_report4.to_csv(b)
        file_client4 = ShareFileClient.from_connection_string(conn_str=connection_string, share_name="compare-reports",
                                                              file_path="report4_UserRecon.csv")
        file_client4.upload_file(b.getvalue())
        b.close()
        b = BytesIO()
        df_report5.to_csv(b)
        file_client5 = ShareFileClient.from_connection_string(conn_str=connection_string, share_name="compare-reports",
                                                              file_path="report5_ListingsPeopleRecon.csv")
        file_client5.upload_file(b.getvalue())
        b.close()
        logging.info("!!! ================= Save to Azure successfully!!!.")
        logging.info('!!!=================Begin save to Goolge Drive')
        df_dict = {"report1_AccountRecon": df_report1,
                   "report2_AccountUserRecon": df_report2,
                   "report3_OrgListingRecon": df_report3,
                   "report4_UserRecon": df_report4,
                   "report5_ListingsPeopleRecon": df_report5
                   }
        if gf.update_all_files(df_dict):
            logging.info("!!! =================  Upload Google Drive successful")
        else:
            logging.info("!!!=================  Upload Google Drive failed")
        logging.info("!!! save successfully! ")
    except Exception as error:
        logging.info(f'!!!=================error !! {error}')
        logging.info('!!!=================error info '.center(30, '-'))
        logging.info(traceback.format_exc())
        b = BytesIO()
        txt_error = str(traceback.format_exc())
        b.write(txt_error.encode("utf-8"))
        connection_string = "DefaultEndpointsProtocol=https;EndpointSuffix=core.windows.net;AccountName=waybaseda;AccountKey=V02b46DB+w6MojVqBAWIrTlfa5GUbROvjVUSc2PA93zQbFgiZ2RTBy8p8SUenL3SHtHRzZJ/vwwT+AStTqbjEQ=="
        file_client = ShareFileClient.from_connection_string(conn_str=connection_string, share_name="compare-reports",
                                                              file_path="compare_task_error.txt")
        file_client.upload_file(b.getvalue())
        b.close()
        logging.info('!!!=================save error into Azure file share!')