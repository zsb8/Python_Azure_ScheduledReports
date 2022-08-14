""" This module contains the functions related to Google Workspace interaction.
Connection is made using a secret credentials file for Google Service Account.
The App Service has access to the shared folder called Internal Reports.    
The function requires an existing file to already exist somewhere in the sub-folder.
The reports json contains a set of reports and their corresponding Google file id's.
Using these id's the function replaces the data in the existing file.

To create more reports:
1. Create a template file in the Google Drive folder and find the file's id.
2. Add to reports.json a key containing the name of the report and the file id as value

pip install google-api-python-client
pip install google-auth
"""
from io import BytesIO
import json
import pandas as pd
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload
from googleapiclient import errors
import inspect
import logging


def update_all_files(df_dict):
	""""
	This is the main program.
	:param (dict) df_dict: Such as {"compare_task1": my_df1, "compare_task2": my_df2, "compare_task3": my_df3}.
	:return: True - success, or False - fail.
	"""
	current_path = inspect.getfile(inspect.currentframe())
	files_json = current_path.replace("gdrive_functions.py", "") + "reports.json"
	try:
		with open(files_json) as reports_file:
			dict_reports = json.load(reports_file)
		for key_report_name, value_file_id in dict_reports.items():
			update_google_file(df_dict[key_report_name], value_file_id)
		result = True
	except Exception as error:
		logging.info(f"error:{error}")
		result = False
	return result


def update_google_file(df, file_id):
	"""
	Update an existing file's content with a pandas dataframe
	:param (dataFarme) df:  the data set which will be upload into Google Drive
	:param (str) file_id: ID of the file to update. Such as "11kzzvBLwKP32vRMxrpRoVqlMsiGLoK4u" in Google file link
	Args:
	file_id: ID of the file to update.
	:return: Updated file metadata if successful, None otherwise.
	"""
	def _set_service():
		"""
		Create a service of Google API, Drive API service instance.
		:return (obj) google_service: service: service to access Google Drive
		"""
		current_path = inspect.getfile(inspect.currentframe())
		SERVICE_ACCOUNT_FILE = current_path.replace("gdrive_functions.py", "") + "credentials.json"
		# TODO: Test if credentials can be part of function secret instead of json file
		# 'https://www.googleapis.com/auth/drive.file'] # this is the preferred scope since it is specific, but it doesn't work
		SCOPES = ['https://www.googleapis.com/auth/drive']  # this is too permissive and gives access to all
		creds = Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)
		google_service = build('drive', 'v3', credentials=creds)
		return google_service
	try:
		service = _set_service()
		# Convert dataframe with new data into a bytestream for upload as csv
		b = BytesIO()
		df.to_csv(b)
		media_body = MediaIoBaseUpload(b, mimetype='text/csv', resumable=True)
		# Update specific file with new data
		updated_file = service.files().update(fileId=file_id, media_body=media_body).execute()
		return updated_file
	except errors.HttpError as error:
		logging.info(f"error:{error}")
	return None


if __name__ == "__main__":
	my_df1 = pd.DataFrame({'A1111111111': [4, 5, 6], 'B1111111111111': [4, 1, 1]})
	my_df2 = pd.DataFrame({'A2222222222': [4, 5, 6], 'B2222222222222': [4, 1, 1]})
	my_df3 = pd.DataFrame({'A3333333333': [4, 5, 6], 'B3333333333333': [4, 1, 1]})
	my_df_dict = {"compare_task1": my_df1, "compare_task2": my_df2, "compare_task3": my_df3}
	update_all_files(my_df_dict)
