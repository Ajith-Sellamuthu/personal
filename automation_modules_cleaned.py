
pip install PyMuPDF

import xlwt
import numpy as np
import pandas as pd
from pyathena import connect
from imbox import Imbox
import pytz
import gspread
import openpyxl
import gspread_dataframe
from gspread_dataframe import set_with_dataframe, get_as_dataframe
from gspread_formatting import set_column_width, cellFormat, color
from pathlib import Path
import os
from gspread_formatting import *

try:
    import trino
except:
    !pip install trino
finally:
    import trino

from unicodedata import name
from datetime import datetime
from datetime import date
from datetime import timedelta
from dateutil.relativedelta import relativedelta
import re
import calendar
import sys, requests, json
import signal
import time
import gspread
from google.oauth2 import service_account
from googleapiclient.discovery import build
from typing import List, Tuple ,Dict, Optional
from googleapiclient.http import MediaIoBaseDownload

#_*_Modules for email automation _*_#
import smtplib
import email, ssl
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
from email.utils import formataddr
from email import encoders
from email.mime.base import MIMEBase
from email.mime.image import MIMEImage
from bs4 import BeautifulSoup

import base64
import mimetypes

import pydrive2
from pydrive2.auth import GoogleAuth
from pydrive2.drive import GoogleDrive
from oauth2client.service_account import ServiceAccountCredentials
from typing import List, Tuple ,Dict, Optional
from email.utils import parsedate_to_datetime


import matplotlib.pyplot as plt
from matplotlib.colors import LinearSegmentedColormap
import urllib
import typing
from typing import List, Tuple ,Dict, Optional
from tableau_api_lib import TableauServerConnection
from tableau_api_lib.utils import querying , flatten_dict_column
import pandas as pd
from PIL import Image
import io
import json
import os
import sys
import warnings
import textwrap
import bs4
import fitz
warnings.filterwarnings("ignore")

trino_hostname = dbutils.secrets.get(scope = "trino-conf", key = "hostname")
def get_sql_df(q):
    connection = trino.dbapi.connect(
        host=trino_hostname,
        port=443,
        user="databricks",
        catalog="awsdatacatalog",
        schema="default",
        http_scheme="https",
        client_tags=["databricks"]
    )

    cursor = connection.cursor()

    # Set Trino session property
    cursor.execute("SET SESSION join_distribution_type = 'PARTITIONED'")

    # Execute actual query
    cursor.execute(q)
    data_temp = cursor.fetchall()
    columns = [col[0] for col in cursor.description]
    df = pd.DataFrame(data_temp, columns=columns)

    connection.close()
    return df

# def get_sql_df(q):
#     connection = connect(aws_access_key_id = dbutils.secrets.get('corp_analytics_secrets',key='connect_to_athena_key'),
#                  aws_secret_access_key = dbutils.secrets.get('corp_analytics_secrets',key='connect_to_athena_secret'),
#                  s3_staging_dir="s3://query-results-athena-platform/databrick-pyathena/",
#                  region_name="ap-south-1",
#                  work_group = "databrick-pyathena")
#     cursor = connection.cursor()
#     sql_command = q
#     cursor.execute(sql_command)
#     data_temp = cursor.fetchall()
#     columns = [col[0] for col in cursor.description]
#     df = pd.DataFrame(data_temp, columns= columns)
#     connection.close()
#     return df

class GoogleDocsReader:
    def __init__(self, creds):
        self.creds = creds
        self.service = self.build_service()

    def build_service(self):
        scopes = ['https://www.googleapis.com/auth/documents']
        from google.oauth2 import service_account as sa
        creds_obj = sa.Credentials.from_service_account_info(self.creds, scopes=scopes)
        service = build('docs', 'v1', credentials=creds_obj)
        return service

    def read_document_metadata(self, document_id):
        document = self.service.documents().get(documentId=document_id).execute()
        return document

    def read_doc_content(self, document_id):
      document_content = self.read_document_metadata(document_id)
      tst = []
      try:
          for i in document_content.get('body').get('content'):
              j = i.get('paragraph')
              if j != None:
                  k = j.get('elements')
                  tst.append(k[0].get('textRun').get('content'))
      except:
          pass
      content = ' '.join(tst)
      return content

class GoogleSheetReader:
    def __init__(self, gsheet_url, service_account):
        self.gsheet_key = gsheet_url.split('/')[5]
        if isinstance(service_account, str):
            service_account = json.loads(service_account)
        self.service_account = service_account
        self.gc = gspread.service_account_from_dict(service_account)


    def get_gsheet(self):
        return self.gc.open_by_key(self.gsheet_key)


    def get_sheet(self, worksheet_name):
        try:
            worksheet = self.get_gsheet().worksheet(worksheet_name)
        except gspread.exceptions.WorksheetNotFound:
            print('Worksheet not found')
            worksheet = self.get_gsheet().add_worksheet(title=worksheet_name, rows="200", cols="20")
        return worksheet


    def get_df_from_sheets(self, worksheet_name, range_=None):
        worksheet = self.get_sheet(worksheet_name)
        if range_ is None:
            df = pd.DataFrame(worksheet.get_all_records())
        else:
            df = pd.DataFrame(worksheet.get_values(range_))
            df.columns = df.iloc[0]
            df = df[1:]
        return df


    def set_df_in_sheets(self, worksheet_name, df, include_headers = True, row=1, col=1):
        try:
            worksheet = self.get_sheet(worksheet_name)
            if worksheet is None:
                worksheet = self.get_gsheet().add_worksheet(title=worksheet_name, rows="100", cols="20")
            set_with_dataframe(worksheet, df, row, col, include_column_header=include_headers)
            print(f"Data has been set to a {worksheet_name} sheet...")
            return True
        except Exception as e:
            print(e)
            return False


    def append_df_in_sheets(self, worksheet_name, extra_df):
        df = self.get_df_from_sheets(worksheet_name)
        df = pd.concat([df, extra_df])
        self.set_df_in_sheets(worksheet_name, df, include_headers = True, row=1, col=1)


    def get_excel_cell(self, row_num, col_num):
        col_str = ""
        div = col_num
        while div:
            modulo = (div - 1) % 26
            col_str = chr(65 + modulo) + col_str
            div = (div - 1) // 26
        return col_str + str(row_num)


    def clear_sheets(self, sheetname, start_row, start_column, end_row=None, end_column=None):
        sheet = self.get_sheet(sheetname)
        data_range = sheet.get_all_values()
        if end_row is None:
            num_rows = len(data_range)
        num_cols = None
        if end_column is None:
            num_cols = len(data_range[0]) 
        # num_rows = len(data_range)
        # num_cols = len(data_range[0])
        range_ = '{start}:{end}'.format(start=self.get_excel_cell(start_row, start_column),
                                        end=self.get_excel_cell(num_rows, num_cols))
        print(range_)
        sheet.batch_clear([range_])

    def download_google_sheet_as_image(self, destination_folder, sheet_name=None, options=None):
        """
        Download a Google Sheet as a PDF and convert it into an image using PyMuPDF.
        """
        # serv_acc = json.loads(self.service_account)
        from google.oauth2 import service_account as sa
        SCOPES = ["https://www.googleapis.com/auth/drive"]
        creds = sa.Credentials.from_service_account_info(service_account, scopes=SCOPES)

        base_url = f"https://docs.google.com/spreadsheets/d/{self.gsheet_key}/export?format=pdf"
        
        default_options = {
            "portrait": False,
            "gridlines": False,
            "printtitle": False,
            "scale": 4,
        }

        if options:
            default_options.update(options)

        params = {
            "gid": "",
            "portrait": str(default_options["portrait"]).lower(),
            "gridlines": str(default_options["gridlines"]).lower(),
            "printtitle": str(default_options["printtitle"]).lower(),
            "scale": str(default_options["scale"]),
        }

        if sheet_name:
            service = build("sheets", "v4", credentials=creds)
            sheet_metadata = service.spreadsheets().get(spreadsheetId=self.gsheet_key).execute()
            sheets = sheet_metadata.get("sheets", [])
            for sheet in sheets:
                if sheet["properties"]["title"] == sheet_name:
                    params["gid"] = str(sheet["properties"]["sheetId"])
                    break

        export_url = base_url + "&" + "&".join(f"{k}={v}" for k, v in params.items())
        response = requests.get(export_url, headers={"Authorization": f"Bearer {creds.token}"})

        if response.status_code == 200:
            pdf_filename = os.path.join(destination_folder, f"{sheet_name or 'Google_Sheet'}.pdf")
            with open(pdf_filename, "wb") as pdf_file:
                pdf_file.write(response.content)
            print(f"PDF saved: {pdf_filename}")
        else:
            print(f"Failed to download PDF: {response.status_code} - {response.text}")
            return

        # Convert PDF to Image using PyMuPDF
        doc = fitz.open(pdf_filename)
        for page_num in range(len(doc)):
            scale_factor = 2  # Increase this for higher quality
            pix = doc[page_num].get_pixmap(matrix=fitz.Matrix(scale_factor, scale_factor))
            img = Image.frombytes("RGB", [pix.width, pix.height], pix.samples)
            
            # Convert to grayscale and find bounding box to crop whitespace
            gray = img.convert("L")
            bbox = gray.getbbox()
            if bbox:
                img = img.crop(bbox)
            
            image_path = pdf_filename.replace(".pdf", f"_page{page_num + 1}.png")
            img.save(image_path, "PNG")
            print(f"Image saved: {image_path}")

            os.remove(pdf_filename)

            return image_path

    """

    Attributes:

    Methods:

        Parameters:
        - message (str): The message content of the notification.
        - user_mentions (List[str], optional): List of user IDs to mention in the notification message. Defaults to None.
        - title (str, optional): The title of the notification message. Defaults to None.

        Returns:
        - None

        Example Usage:


          Parameters:
          - text (str): The text content of the message.
          - file_list (List[str], optional): List of file paths of images to embed in the message. Defaults to None.

          Returns:
          - None

          Example Usage:


        Parameters:
        - file_path (str): The path to the file to be uploaded.
        - title (str): The title of the file.
        - user_mentions (List[str], optional): List of user IDs to mention in the message. Defaults to None.
        - message (str, optional): The message content to accompany the file. Defaults to None.

        Returns:
        - None

        Example Usage:






        Parameters:
        - timestamp (str): The timestamp of the message to be deleted.

        Returns:
        - None

        Example Usage:

    """




        try:
            mention_text = 'CC: ' + ' '.join([f'<@{user}>' for user in user_mentions]) if user_mentions else ''
            if title:
                message = f"\n*{title}*\n{message}"
            response = self.client.chat_postMessage(
                channel=channel,
                text=f"{message}\n{mention_text}".strip()
            )
            print("Notification sent successfully!")
            print(f"Error sending notification: {e.response['error']}")
        except Exception as e:
            print(f'Encountered Error: {e}')





        try:
            mention_text = 'CC: ' + ' '.join([f'<@{user}>' for user in user_mentions]) if user_mentions else ''
            if message:
                message = f"{message}\n"

            file_type, _ = mimetypes.guess_type(file_path)

            if file_type == 'text/csv':
                with open(file_path, 'r') as f:
                    csv_content = f.read()
                response = self.client.files_upload(
                    channels=channel,
                    content=csv_content,
                    title=title,
                    filetype='csv',
                    filename=title,
                    initial_comment=f"{message}\n{mention_text}".strip() if mention_text or message else None
                )
            elif file_type == 'application/pdf':
                with open(file_path, 'rb') as f:
                    pdf_content = f.read()

                response = self.client.files_upload(
                    channels=channel,
                    content=pdf_content,
                    title=title,
                    filetype=file_type,
                    filename=title,
                    initial_comment=f"{message}\n{mention_text}".strip() if mention_text or message else None
                )
            elif file_type.startswith('image'):
                with open(file_path, 'rb') as f:
                    image_content = f.read()

                response = self.client.files_upload(
                    channels=channel,
                    content=image_content ,
                    title=title,
                    filetype=file_type,
                    filename=title,
                    initial_comment=f"{message}\n{mention_text}".strip() if mention_text or message else None
                )
            else:
                print("Unsupported file type")
                return
            print("File uploaded successfully!")
            print(f"Error uploading file: {e.response['error']}")
        except Exception as e:
            print(f'Encountered Error: {e}')



        # Set up the API endpoint and headers
        headers = {
            'Content-Type': 'application/json'
        }

        # Set up the payload with the channel ID and timestamp
        payload = {
            'channel': channel_id,
            'ts': timestamp
        }

        # Make the API request
        response = requests.post(url, headers=headers, json=payload)

        # Check the response
        if response.ok and response.json().get('ok'):
            print('Message deleted successfully')
        else:
            print(f'Failed to delete message: {response.json()}')


    def get_channel_messages(self,channel_id):
        response = requests.post(
            json={'channel': channel_id}
        ).json()

        if response['ok']:
            return response['messages']
        else:
            print(f"Error: {response['error']}")
            return []

class Gmail:
    """
    A class to manage sending and receiving emails via Gmail using SMTP and IMAP.

    This class provides methods for sending emails with optional attachments and inline images,
    as well as fetching emails with specific filters, such as unread status, sender, recipient, subject, and date range.

    Attributes:
    - email (str): The email address of the sender.
    - app_passwd (str): The app password or OAuth token for the sender's Gmail account.

    Methods:
    - send_email(sender_name, receiver_email, cc_recipient, subject, body, attachments=None, add_image_as_body=None, is_html=False):
        Sends an email with the specified details, including support for plain text or HTML body, attachments, and embedding images within the email body.

        Parameters:
        - sender_name (str): The name of the sender (displayed in the "From" field).
        - receiver_email (str): The email address of the main recipient.
        - cc_recipient (Optional[str]): A list of email addresses for CC recipients. Defaults to None.
        - subject (str): The subject of the email.
        - body (str): The body of the email (plain text or HTML).
        - attachments (Optional[List[str]]): A list of file paths to attach to the email. Defaults to None.
        - add_image_as_body (Optional[List[str]]): A list of image file paths to embed in the email body as inline images. Defaults to None.
        - is_html (Optional[bool]): Indicates if the email body should be treated as HTML. Defaults to False (plain text).

        Example Usage:
        - Initialize the email sender:
            sender = Gmail('sender@example.com', 'app_password')
        - Send an email with attachments and embedded images:
            sender.send_email(
                sender_name='Sender Name',
                receiver_email='recipient@example.com',
                cc_recipient=['cc1@example.com'],
                subject='Test Email',
                body='This is a test email.',
                attachments=['path_to_file1.pdf'],
                add_image_as_body=['path_to_image1.jpg'],
                is_html=True
            )

    - fetch_emails(unread=None, sent_from=None, sent_to=None, subject=None, date_lt=None, date_gt=None, date_on=None, save_attachments_path=None):
        Fetches emails from the Gmail inbox based on provided filters such as unread status, sender, recipient, subject, and date range.
        Inorder to get all read messages, first fetch all messages then fetch all unread messages. Then remove unread messages from all mails using 'message_id' column.

        Parameters:
        - unread (Optional[bool]): If True, fetches unread emails. If None, fetches all emails. Defaults to None.
        - sent_from (Optional[str]): Filters emails by sender's email address. Defaults to None.
        - sent_to (Optional[str]): Filters emails by recipient's email address. Defaults to None.
        - subject (Optional[str]): Filters emails by subject. Defaults to None.
        - date_lt (Optional[datetime.date]): Fetch emails received before this date. Defaults to None.
        - date_gt (Optional[datetime.date]): Fetch emails received after this date. Defaults to None.
        - date_on (Optional[datetime.date]): Fetch emails received on this specific date. Defaults to None.
        - save_attachments_path (Optional[str]): The directory path to save attachments. Defaults to 'data'.

        Returns:
        - emails_df (pandas.DataFrame): A DataFrame containing the email data with the following columns:
            - 'id': The email's unique identifier.
            - 'sent_from': The sender's email address.
            - 'sent_to': The list of recipient email addresses.
            - 'subject': The subject of the email.
            - 'date': The date the email was sent.
            - 'body_plain': The plain text version of the email body (if available).
            - 'body_html': The HTML version of the email body (if available).
            - 'attachments': A list of file paths to the saved attachments (if any).

        Example Usage:
        - Fetch all unread emails:
            emails = sender.fetch_emails(unread=True)
        - Fetch emails sent by a specific person:
            emails = sender.fetch_emails(sent_from='sender@example.com')
    """


    def __init__(self, email: str, app_passwd: str):
        self.email = email
        self.app_passwd = app_passwd

    def send_email(self,
                   sender_name,
                   receiver_email: str,
                   cc_recipient: Optional[str],
                   subject: str,
                   body: str,
                   attachments: Optional[List[str]] = None,
                   add_image_as_body: Optional[List[str]] = None ,
                   is_html: Optional[bool] = None):

        # Set up the email message
        msg = MIMEMultipart()
        msg['From'] = f"{sender_name} <{self.email}>"
        msg['To'] = receiver_email
        if cc_recipient:
            msg['Cc'] = cc_recipient
        msg['Subject'] = subject
        if is_html:
            msg.attach(MIMEText(body, 'html'))
        else:
            msg.attach(MIMEText(body))


        # Attach body
        if add_image_as_body:
            for filename in add_image_as_body:
                image_name = filename.split('''/''')[-1].split('.')[0]
                text = MIMEText('<img src="cid:image1">', 'html')
                msg.attach(text)
                image = MIMEImage(open(filename, 'rb').read())
                image.add_header('Content-ID', "<image1>")
                msg.attach(image)


        # Attachments
        if attachments:
            for filename in attachments:
                with open(filename, "rb") as attachment:
                    part = MIMEApplication(attachment.read(), Name=filename)
                    part['Content-Disposition'] = f'attachment; filename="{filename.split("/")[-1]}"'
                    msg.attach(part)



        # Connect to the SMTP server
        with smtplib.SMTP('142.250.4.108', 587) as server:
            server.starttls()
            server.login(self.email, self.app_passwd)
            server.send_message(msg)
            server.quit()



    def fetch_emails(self,
                    unread: Optional[bool] = None,
                    sent_from: Optional[str] = None,
                    sent_to: Optional[str] = None,
                    subject: Optional[str] = None,
                    date_lt: Optional[datetime.date] = None,
                    date_gt: Optional[datetime.date] = None,
                    date_on: Optional[datetime.date] = None):


        if not os.path.exists('data'):
            os.makedirs('data')
        save_attachments_path = 'data'

        emails = []
        with Imbox('imap.gmail.com',
                   username=self.email,
                   password=self.app_passwd,
                   ssl=True,
                   ssl_context=None,
                   starttls=False) as imbox:

            # Fetch messages based on filters
            messages = imbox.messages(unread=unread,
                                       sent_from=sent_from,
                                       sent_to=sent_to,
                                       subject=subject,
                                       date__lt=date_lt,
                                       date__gt=date_gt,
                                       date__on=date_on)

            for uid, message in messages:
                sent_to = [x['email'] for x in message.sent_to]
                email_data = {
                    'id':message.message_id,
                    "sent_from": message.sent_from[0]['email'],
                    "sent_to": sent_to,
                    "subject": message.subject,
                    "date": message.date,
                    "body_plain": message.body['plain'][0] if message.body['plain'] else None,
                    "body_html": message.body['html'][0] if message.body['html'] else None,
                    "attachments": []
                }

                # Save attachments if specified
                if save_attachments_path and message.attachments:
                    for attachment in message.attachments:
                        attachment_path = os.path.join(save_attachments_path, attachment['filename'])
                        with open(attachment_path, "wb") as f:
                            f.write(attachment.get('content').read())
                        email_data["attachments"].append(attachment_path)

                emails.append(email_data)
        emails_df = pd.DataFrame(emails)

        return emails_df


class GoogleDriveOps:
    def __init__(self, creds, scope: Optional[List[str]] = None):
        self.creds = creds
        self.scope = scope or ['https://www.googleapis.com/auth/drive.appdata',
                               'https://www.googleapis.com/auth/drive']
        self.gauth = self.get_credentials()
        self.drive = GoogleDrive(self.gauth)


    def get_credentials(self):
        gauth = GoogleAuth()
        gauth.credentials = ServiceAccountCredentials.from_json_keyfile_dict(self.creds, self.scope)
        return gauth


    def get_folder_dict(self, folder_id):

        """
        Returns: List of folder dictionaries
        - Get all the files within a specific parent folder.
        Parameters:
        - parent_folder_id [str]: The ID of the parent folder you want to list folders within.
        """

        query = "'{}' in parents and trashed=false".format(folder_id)
        file_list = self.drive.ListFile({'q': query}).GetList()
        folder_dict = {}
        for file in file_list:
            folder_dict[file['title']] = file['id']
        return folder_dict



    def get_all_folders_in_folder(self, parent_folder_id):

        """
        Returns: List of folder dictionaries
        - Get all the folders within a specific parent folder.
        Parameters:
        - parent_folder_id [str]: The ID of the parent folder you want to list folders within.
        """


        query = "'{}' in parents and mimeType = 'application/vnd.google-apps.folder' and trashed = false"
        query = query.format(parent_folder_id)
        file_list = self.drive.ListFile({'q': query}).GetList()

        folder_list = []
        for file in file_list:
            folder_info = {
                'title': file['title'],
                'id': file['id']
            }
            folder_list.append(folder_info)

        return folder_list

    def create_folder(self , folder_name , parent_folder_id):
      """

      Returns: Dictionary
      Get all the folder Id's as dictionary.
      Authorize G-Drive Credentials before using Create_folder function. Use the following method to authorize credentials.

        {gauth = GoogleAuth()
        gauth.credentials = ServiceAccountCredentials.from_json_keyfile_dict(service_account_key, SCOPES)
        drive = GoogleDrive(gauth)}

      """
      files_present = self.get_folder_dict(parent_folder_id)
      if folder_name not in files_present.keys():

        folder = self.drive.CreateFile({'parents':[{'id': parent_folder_id}],'title':folder_name, 'mimeType': 'application/vnd.google-apps.folder'})
        folder.Upload()
        folder_created =  self.get_folder_dict(parent_folder_id)
      else:
        print('Folder with same name already exists. Please try with another name')
        folder_created =  self.get_folder_dict(parent_folder_id)
      return folder_created




    def create_file(self, folder_id, title):
      """
      Create a Google Sheets spreadsheet in a Google Drive folder.

      Parameters:
      - folder_id: The ID of the Google Drive folder where you want to create the spreadsheet.
      - title: The title (name) for the new spreadsheet.

      Note: The 'drive' variable and required modules should be imported and available in the same scope where this function is called.
      """

      # Create a new Google Sheets file in the specified folder
      spreadsheet = self.drive.CreateFile(
          {
              "parents": [{"id": folder_id}],
              "title": title,
              "mimeType": "application/vnd.google-apps.spreadsheet",
          }
      )

      # Upload the empty spreadsheet
      spreadsheet.Upload()

      print(f"Created Google Sheets file: '{title}' in folder with ID '{folder_id}'")


    def convert_to_pdf(self, template_id: str, folder_id: str, title: str) -> bool:
        '''
        Converts excel template into pdf and uploads it to the given G-Drive folder.
        Parameters:
        - template_id: The ID of the Google Sheets template file.
        - folder_id: The ID of the Google Drive folder where the PDF will be stored.
        - title: The title (name) for the PDF file.
        '''
        file_obj = self.drive.CreateFile({'id': template_id})
        local_pdf_path = os.path.join(os.getcwd(), f'{title}.pdf')
        file_obj.GetContentFile(local_pdf_path, mimetype='application/pdf')

        gfile = self.drive.CreateFile({'parents': [{'id': folder_id}], 'title': f'{title}.pdf'})
        gfile.SetContentFile(local_pdf_path)
        gfile.Upload()

        os.remove(local_pdf_path)
        return True


    def upload_file_to_drive(self, folder_id: str, title: str , local_file_path : str) -> bool:
        local_path = os.path.join(local_file_path , title)
        gfile = self.drive.CreateFile({'parents': [{'id': folder_id}], 'title': f'{title}'})
        gfile.SetContentFile(local_path)
        gfile.Upload()
        return True


    def download_file(self, file_id, file_name):
        try:
            file = self.drive.CreateFile({'id': file_id})
            file.GetContentFile(file_name)  # Download and save the file with the given file name
            print(f"File '{file_name}' downloaded successfully.")
            return True
        except Exception as e:
            print(f"An error occurred: {e}")
            return False


class Tableau_report_generator():
    """
    Download csv , images and pdf and view backend data of Tableu views.
    Create Dataframes from Tableau Workbooks.

    Functions and Methods:

    1. get_view_csv(self,
                    view_name : str ,
                    workbook_name : str,
                    path : Optional[str] = None)

        Download csv of the backend data of tableau view.

        Parameters:
        - view_name (str): The name of the view to retrieve.
        - workbook_name (str): The name of the workbook containing the view.
        - path (str, optional): The path to save the downloaded csv.

        Returns:
        - str: The path to the downloaded image.


    2. download_view_image(self,
                        view_name : str,
                        workbook_name : str,
                        path : Optional[str] = None,
                        filters: Optional[Dict[str, List[str]]] = None
                        ):

     Retrieve an image of a specific view.

        Parameters:
        - view_name (str): The name of the view to retrieve.
        - workbook_name (str): The name of the workbook containing the view.
        - path (str, optional): The path to save the downloaded image.
                                If not provided, the default path will be used.
        - filters (dict, optional): The name of the filter and values to apply to the view.

        Returns:
        - str: The path to the downloaded image.


    3. download_view_pdf(self,
                        view_name : str,
                        workbook_name : str,
                        path : Optional[str] = None,
                        filters: Optional[Dict[str, List[str]]] = None
                        ):


        Retrieve an image of a specific view.

        Parameters:
        - view_name (str): The name of the view to retrieve.
        - workbook_name (str): The name of the workbook containing the view.
        - path (str, optional): The path to save the downloaded image.
                                If not provided, the default path will be used.
        - filters (dict, optional): The name of the filter and values to apply to the view.

        Returns:
        - str: The path to the downloaded image.

    """

    def __init__(self):

        tableau_config = {
        'tableau_prod': {
        'server': "https://tableau.dp.navi-tech.in/" ,
        'api_version': '3.15',
        'username': tableau_username,
        'password': tableau_password,
        'site_name': '',
        'site_url': '',
                    }
        }

        # create Tableau Server connection object
        self.conn = TableauServerConnection(tableau_config, env='tableau_prod')
        # sign in to the Tableau Server connection
        self.conn.sign_in()
        print('Connected to https://tableau.dp.navi-tech.in/')

    # @property
    def get_view_id(self, view_name, workbook_name):

        try:
            views_df = querying.get_views_dataframe(self.conn)
            wb_unnest_df = flatten_dict_column(df = views_df,keys=['name','id'], col_name='workbook')
            result_df = wb_unnest_df.loc[(wb_unnest_df['name']== view_name) & (wb_unnest_df['workbook_name']==workbook_name)]
            if len(result_df) == 0:
                print('View not found. Verify the workbook name and view name')
            else:
                view_id = result_df.id.values[0]
        except Exception as e:
            print('Exception Error: ', e)
        return view_id

    def get_all_views(self, workbook_name):

        try:
            views_df = querying.get_views_dataframe(self.conn)
            wb_unnest_df = flatten_dict_column(df = views_df,keys=['name','id'], col_name='workbook')
            result_df = wb_unnest_df.loc[(wb_unnest_df['workbook_name']==workbook_name)]
            if len(result_df) == 0:
                print('View not found. Verify the workbook name and view name')
            else:
                view_dct = {}
                for row in result_df.iterrows():
                    view_dct[row[1][5]] = row[1][4]
        except Exception as e:
            print('Exception Error: ', e)
        return view_dct

    # @property
    def get_view_csv(self, view_name : str ,workbook_name : str, path : Optional[str] = None):
        """
        Download csv of the backend data of tableau view.

        Parameters:
        - view_name (str): The name of the view to retrieve.
        - workbook_name (str): The name of the workbook containing the view.
        - path (str, optional): The path to save the downloaded csv.

        Returns:
        - str: The path to the downloaded image.
        """
        downloads_folder = Path.home() / "Downloads"
        view_id = self.get_view_id(view_name , workbook_name)
        if not path:
            download_path = downloads_folder / f'{view_name}.csv'
        else:
            download_path = Path(path) / f'{view_name}.csv'
        view_response = self.conn.query_view_data(view_id)
        with open(download_path , 'wb') as file:
            file.write(view_response.content)
        return download_path

    # @property
    def get_view_df(self, view_name ,workbook_name):
        download_path = self.get_view_csv(view_name ,workbook_name)
        df = pd.read_csv(download_path)
        os.remove(download_path)
        if 'Measure Names' in df.columns:
            df = df.pivot(columns='Measure Names', values='Measure Values', index = list(set(df.columns) - set(['Measure Values', 'Measure Names']))).reset_index()
        return df


    def _apply_custom_filters(self, filters: Dict[str, List[str]]) -> Dict[str, str]:
        """
        Apply custom filters to be used in querying a view image.

        Parameters:
        - filters (Dict[str, List[str]]): A dictionary where each key is the filter name
                                          and the corresponding value is a list of filter values.

        Returns:
        - Dict[str, str]: A dictionary where each key is the filter name (with necessary encoding)
                          and the corresponding value is the encoded filter values concatenated as a string.
        """
        encoded_filters = {}
        for filter_name, filter_values in filters.items():
            # Encode filter name
            encoded_filter_name = urllib.parse.quote(filter_name)
            # Encode filter values and join them with ","
            # encoded_filter_values = ",".join(urllib.parse.quote(value) for value in filter_values)
            encoded_filter_values = ",".join(urllib.parse.quote(str(value).lower() if isinstance(value, bool) else str(value)) for value in filter_values)

            # Construct filter expression
            filter_expression = f"vf_{encoded_filter_name}={encoded_filter_values}"
            # Add to encoded_filters dictionary
            encoded_filters[filter_name] = filter_expression
        return encoded_filters


    # @property
    def download_view_image(self,
                        view_name : str,
                        workbook_name : str,
                        path = None,
                        filters: Optional[Dict[str, List[str]]] = None
                        ):

        """
        Retrieve an image of a specific view.

        Parameters:
        - view_name (str): The name of the view to retrieve.
        - workbook_name (str): The name of the workbook containing the view.
        - path (str, optional): The path to save the downloaded image.
                                If not provided, the default path will be used.
        - filters (dict, optional): The name of the filter and values to apply to the view.

        Returns:
        - str: The path to the downloaded image.
        """
        if filters:
            encoded_filters = self._apply_custom_filters(filters)
        else:
            encoded_filters = None
        view_id = self.get_view_id(view_name, workbook_name)
        downloads_folder = Path.home() / "Downloads"
        if not path:
            download_path = downloads_folder / f'{view_name}.png'
        else:
            download_path = f'{path}'

        view_img_response = self.conn.query_view_image(view_id, parameter_dict=encoded_filters)
        with open(download_path , 'wb') as file:
            file.write(view_img_response.content)
        print(f'Image saved: {download_path}')
        with open(download_path, "rb") as f:
            Image.open(io.BytesIO(f.read()))
        return str(download_path)

    def download_view_pdf(self,
                        view_name : str,
                        workbook_name : str,
                        path : Optional[str] = None,
                        filters: Optional[Dict[str, List[str]]] = None
                        ):

        """
        Retrieve an image of a specific view.

        Parameters:
        - view_name (str): The name of the view to retrieve.
        - workbook_name (str): The name of the workbook containing the view.
        - path (str, optional): The path to save the downloaded image.
                                If not provided, the default path will be used.
        - filters (dict, optional): The name of the filter and values to apply to the view.

        Returns:
        - str: The path to the downloaded image.
        """
        if filters:
            encoded_filters = self._apply_custom_filters(filters)
        else:
            encoded_filters = None
        view_id = self.get_view_id(view_name , workbook_name)
        downloads_folder = Path.home() / "Downloads"
        if not path:
            download_path = downloads_folder / f'{view_name}.pdf'
        else:
            download_path = f'{path}/{view_name}.pdf'
        view_img_response = self.conn.query_view_pdf(view_id, parameter_dict=encoded_filters)
        with open(download_path , 'wb') as file:
            file.write(view_img_response.content)
        print(f'Image saved: {download_path}')

from pathlib import Path

def is_full_path(output_path):
    return len(Path(output_path).parents) > 1


def save_df_as_image(df, output_path, conditional_formatting=None, max_col_width=30, min_col_width=10, base_font_size=16, image_width=6, image_height=5):
    """
    Save a pandas DataFrame as an image with adjustable formatting options.

    Parameters:
    - df (pd.DataFrame): The DataFrame to be saved as an image.
    - output_path (str): Path to save the output image (e.g., 'output.png').
    - conditional_formatting (function, optional): A function to dynamically format cell background colors.
      It should accept (cell_value, row, col, column_name, df) as arguments and return a valid matplotlib color.
    - max_col_width (int, optional): Maximum width of each column in characters. Default is 30.
    - min_col_width (int, optional): Minimum width of each column in characters. Default is 10.
    - base_font_size (int, optional): Base font size for the table. Default is 16.
    - image_width (int, optional): Width of the output image in inches. Default is 6.
    - image_height (int, optional): Height of the output image in inches. Default is 5.

    Returns:
    - str: The output path where the image is saved.
    """
    print('Save IMAGE')

    if not is_full_path(output_path):
        output_path = f'/dbfs/FileStore/chaitanya_yadav/Playground/{output_path}'

    # Calculate number of rows and columns
    num_rows, num_cols = df.shape

    # Set fixed image size (width and height)
    fixed_width = image_width
    fixed_height = image_height

    # Estimate space needed for table based on number of rows and columns
    table_width = num_cols * 0.25
    table_height = num_rows * 0.2

    # Calculate scaling factors for font size, row height, and column width
    scale_x = fixed_width / table_width
    scale_y = fixed_height / table_height

    # Use the smaller scaling factor to avoid overcrowding
    scale = min(scale_x, scale_y)

    # Dynamically adjust font size
    font_size = base_font_size * scale

    # Create the figure with fixed dimensions
    fig, ax = plt.subplots(figsize=(fixed_width, fixed_height))

    # Hide axis and grid lines
    ax.axis('off')
    ax.grid(False)

    # Prepare DataFrame for display
    temp_df = df.fillna('')

    # Create the table
    table = ax.table(cellText=temp_df.values, colLabels=temp_df.columns, cellLoc='center', loc='upper left')

    # Apply dynamic font size
    table.auto_set_font_size(False)
    for key, cell in table.get_celld().items():
        cell.set_fontsize(font_size)

    # Style the header
    for col_index in range(len(df.columns)):
        header_cell = table[(0, col_index)]
        header_cell.set_facecolor('#cfe2f3')
        header_cell.set_text_props(weight='bold')

    # Wrap text and apply conditional formatting
    max_lines_per_row = [0] * len(df)

    for col_index, column in enumerate(df.columns):
        column_width = max(df[column].astype(str).apply(len).max(), len(column)) + 1
        column_width = min(max(column_width, min_col_width), max_col_width)

        for row in range(len(df)):
            cell_value = df.iloc[row, col_index]
            wrapped_text = textwrap.fill(str(cell_value), width=column_width)
            table[(row + 1, col_index)].set_text_props(text=wrapped_text)

            # Apply conditional formatting if a function is provided
            if callable(conditional_formatting):
                color = conditional_formatting(cell_value, row, col_index, column, df)
                if color:
                    table[(row + 1, col_index)].set_facecolor(color)

            # Track max lines for height adjustment
            num_lines = wrapped_text.count('\n') + 1
            max_lines_per_row[row] = max(max_lines_per_row[row], num_lines)

        table.auto_set_column_width(col=col_index)

    # Adjust row height
    max_lines_per_row = [1] + max_lines_per_row
    for row, num_lines in enumerate(max_lines_per_row):
        cell_height = 0.17 * num_lines * scale
        for col in range(len(df.columns)):
            table[(row, col)].set_height(cell_height)

    # Save the image
    plt.savefig(output_path, bbox_inches='tight', dpi=50)
    plt.close()

    return output_path

        cursor = None  # Start with no cursor for the first page
        try:
            while True:
                if cursor:
                    response = client.users_list(cursor=cursor)  # Fetch with cursor for pagination
                else:
                    response = client.users_list()  # First call without cursor

                users = response['members']
                for user in users:
                    if not user.get('deleted') and not user.get('is_bot'):  # Skip bots and deleted users
                        email = user.get('name') + '@navi.com'  # Example of constructing email
                        user_id = user.get('id')

                        if email == email_to_check:  # Map only if email exists
                            return user_id

                cursor = response.get('response_metadata', {}).get('next_cursor')
                if not cursor:
                    break  # No more pages, break the loop

            print(f"Error fetching user list: {e.response['error']}")

        return 'NOT_FOUND'


tableau_username = "tableauadmin"
tableau_password = "Tableau@prod2022"
OPENAI_KEY = "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"
automation_email = 'automation-analytics@navi.com'
automation_app_password = 'gqfv aque xryl lqjb'
service_account = {
  "type": "service_account",
      "project_id": "automation-project-352212",
      "private_key_id": "abe8a1b978144d7c2b253a2f6b820a88a0f76204",
      "private_key": "-----BEGIN PRIVATE KEY-----\nMIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQDDryiajZjqLUst\n2JLACngs1Dkf7vw9zs1vCAKkLvuHtBmkJwNPnSm5wPf0bvbhs3WWq5iICnhdRZI9\nLf7aaMmYEkq1LIpnRcmM/x5tOYm6ZaN7IWkkkyO0ZC9ZJCHnfrqG11wp2jTggk1C\nU11KgoRGX5rfm80xwflBdXYAHzjSSFNnE6F6B/smLi4cnMdTTOwD2H76A8wlJUW7\nfbcYsGlhAWbbuhH+DuDjsr6Kz3s3G97hIZajLmjY3+hidprnh2vBUCL+LA9Zi2Og\nky/xQytIBbT4aXUzI1gZKrZbQdqS0RXKqPsCrcXQeAwFKEYd66BuiioE/7LoVtY+\n3q32CpOBAgMBAAECggEAH8K1N2iWEiQp2JYXM34/hR8o6mrym+GN7GCDdTx+7Map\nhsAiRHDAzfppBE5iWb2z7zGIv0Pbx1+8XZuyiqaJWdpWL9O73wkvrmfV+2pkVVGb\n2CdTFnGfUO8V1EUOD7G8+pfHyK82992ORp6bPLW0XGu2CmcSReoy15IGBf0LTu3P\nIpSVzBx7ozqk9NPr6dfV/of+6BcKm28zqQzDcXtzCScmTYfHL4nxJZgheYrX1suN\n13DAHISV03b+O+g5jen7IuZ3DHVyBe7ILkUK34XIW9CrxZPuUh3faQSb9i+LWvWG\neO2A7GiiitSCS+GRJ31kuXjb1K8ZZIAuvKVi75es8QKBgQD64iFavRVvzTCxE2B5\n9yfedh5c9r/2yp+v2Pahz+oehNte93oWn2nmdyEkBLe8WEfEfSuYQPi6aG31GCzD\nqLaCmVQHVT9wNks9CUujTAIrDMansZWbslkKbVe3upoqkVycEW9rfax6eGkNP/gG\nXG0ElWrmhrhWaBfpwH5N5yJO/QKBgQDHrNUAeE1CNxK2A+VISZhcjnkwkjlMpFb3\nLnLa5FiI49hzvkU2Fdzq1wKYoWCDy3ggxLIclMgYq2Ovl6DnZBU2JjB6mxLsskLR\nxx6DB3q3Xc5ClDW7d2ZEvrddxOM9e6t1Qp+h0AIdH9Aa8XmUhKoLYKcvuKSL8k8p\ny9hPkqi31QKBgQCCxrFATWQJPVpuU31s1krwCX5WU0FJceG/lkcpnemaMwLvA+2e\nMUbQnbSmw9bK1PHEGMNKwENnV9xJcGqVKzLH7QkCMYu6AHNDw76rxokyy5wZ+dXU\nrSkA6HJbEWgfXFp7+BKuY3ou9Ok9wOSW3ELvqrtooz0fvW8JqA+uqBcYOQKBgGyp\nmK0NyvWa5Praud8R9fGAs2EEYlxcHtubknOeyrOMUxIB3MZnNXczlT0crVpr0y75\n+/fj29TZCIHZYX4rEAQM2lsOo0jeZdClrOwfmN/LE+FVqLwhaS1GIKHN8OBXueIx\nZI/5k/zngCf5e3GglK1mZvVoZVunfP9A6LG9HUYVAoGAZB5+yppstBKCzVrMczRU\nt4Cs02PYF2DY3fMa68b6ogDE1cp1E12rzwrNuZ3GVoLi9d3tCa5qgVKFDvfR2Tj0\nU68c3xdsT2PbM9C31ARfycJFlWlC4JAJVZGZ5GCNAS8jMXk2aMZ9X5VJb0zl4LON\nuF1/2VdVlCrQQcM1wPtenOQ=\n-----END PRIVATE KEY-----\n",
      "client_email": "automation-service-account@automation-project-352212.iam.gserviceaccount.com",
      "client_id": "105333839471178074488",
      "auth_uri": "https://accounts.google.com/o/oauth2/auth",
      "token_uri": "https://oauth2.googleapis.com/token",
      "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
      "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/automation-service-account%40automation-project-352212.iam.gserviceaccount.com",
      "universe_domain": "googleapis.com"
}