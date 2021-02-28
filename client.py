import pickle
import ssl
import httplib2
import socket
from functools import wraps
from typing import BinaryIO, Optional, Callable, Tuple, List
import threading

from pathlib import Path
from googleapiclient.discovery import build
import googleapiclient.errors
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.http import MediaIoBaseDownload, MediaFileUpload
from google.auth.exceptions import RefreshError, TransportError

from CloudStorageFileSystem.logger import LOGGER
from .const import AF


# def is_connected(func):
#     @wraps(func)
#     def wrapped(self, *args, **kwargs):
#         try:
#             # Quick check
#             if self.connected:
#                 return func(self, *args, **kwargs)
#             else:
#                 raise ConnectionError("Unable to reach www.googleapis.com")
#
#         # Handle errors while executing function
#         except (ConnectionError, httplib2.ServerNotFoundError) as err:
#             # LOGGER.debug(err)
#             raise ConnectionError(err)
#
#         except TransportError as err:
#             # LOGGER.debug(err)
#             raise ConnectionError("Unable to reach server, max retries exceeded")
#
#         except socket.timeout as err:
#             # LOGGER.error(err)
#             raise ConnectionError("Connection timed out")
#
#         except socket.gaierror as err:
#             # LOGGER.error(err, type(err).__name__)
#             raise ConnectionError("Connection failed")
#
#     return wrapped


class DriveFile:
    def __init__(self, **kwargs):
        self._data = kwargs

    def __getitem__(self, item):
        return self._data[item]

    def __setitem__(self, key, value):
        self._data[key] = value

    def __repr__(self):
        return str(self._data)

    def keys(self):
        return list(self._data.keys())

    def get(self, key, value):
        return self._data.get(key, value)


class DriveClient:
    credentials: Credentials
    _service = {}
    _scopes = ["https://www.googleapis.com/auth/drive"]

    def __init__(self, client_id: str, client_secret: str):
        self.client_config = {
            "installed": {
                "client_id": client_id,
                "client_secret": client_secret,
                "auth_uri": "https://accounts.google.com/o/oauth2/auth",
                "token_uri": "https://oauth2.googleapis.com/token",
                "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
            }
        }

    @property
    def flow(self):
        return InstalledAppFlow.from_client_config(client_config=self.client_config, scopes=self._scopes)

    @property
    def service(self):
        """For every thread new service is created"""
        try:
            return self._service[threading.get_ident()]
        except KeyError:
            self._service[threading.get_ident()] = build("drive", "v3", credentials=self.credentials)
            return self.service

    @property
    def start_page_token(self):
        """https://developers.google.com/drive/api/v3/reference/changes/getStartPageToken"""
        return self.service.changes().getStartPageToken().execute()["startPageToken"]

    # @is_connected
    def auth(self) -> str:
        """
        Launches local authentication server and opens browser
        :return: json credentials as str
        """
        LOGGER.info("Authenticating")
        self.credentials = self.flow.run_local_server(host="localhost", port=8080)
        return self.credentials.to_json()

    # @is_connected
    def load_credentials(self, credentials: dict) -> bool:
        """Load credentials from dict"""
        credentials = Credentials.from_authorized_user_info(credentials, self._scopes)

        if not credentials.valid:
            if credentials.expired and credentials.refresh_token:
                LOGGER.info("Refreshing credentials")
                try:
                    credentials.refresh(Request())
                    self.credentials = credentials
                    return True
                except RefreshError:
                    LOGGER.warn("Error refreshing credentials")
                    return False

            else:
                LOGGER.info("Credentials loaded successfully")
                self.credentials = credentials
                return True

        else:
            LOGGER.info("Credentials loaded successfully")
            self.credentials = credentials
            return True

    def get_root_id(self) -> str:
        """Update root id 'root' to actual root id"""
        return self.get_by_id(id=AF.ROOT_ID)["id"]

    def about(self):
        """https://developers.google.com/drive/api/v3/reference/about"""
        response = self.service.about().get(fields="*").execute(num_retries=1)
        return response

    # @is_connected
    # @lock
    def changes(self, page_token: str, fields: Optional[Tuple[str]] = AF.DEFAULT_FIELDS) -> dict:
        """
        https://developers.google.com/drive/api/v3/reference/changes
        :param fields: Pass None to get all file fields, for custom fields refer to https://developers.google.com/drive/api/v3/reference/files
        """
        if fields is None:
            fields = "nextPageToken,newStartPageToken,changes(removed,fileId,file(*))"
        else:
            fields = f"nextPageToken,newStartPageToken,changes(removed,fileId,file({','.join(fields)}))"

        response = self.service.changes().list(pageToken=page_token, pageSize=1000, spaces="drive", fields=fields).execute(num_retries=1)
        for i, change in enumerate(response["changes"]):
            if "file" in response["changes"][i].keys():
                response["changes"][i]["file"] = DriveFile(**response["changes"][i]["file"])
        return response

    # @is_connected
    # @lock
    def list_files(self, q: str,
                   next_page_token: Optional[str] = None,
                   fields: Optional[Tuple[str]] = AF.DEFAULT_FIELDS) -> Tuple[List[DriveFile], Optional[str]]:
        """
        https://developers.google.com/drive/api/v3/reference/files/list
        :param q:
        :param next_page_token:
        :param fields: Pass None to get all file fields, for custom fields refer to https://developers.google.com/drive/api/v3/reference/files
        :return: DriveFile objects and next page token
        """
        kwargs = {"q": q,
                  "fields": "*" if fields is None else f"nextPageToken,files({','.join(fields)})",
                  "pageSize": 1000,
                  "spaces": "drive"}
        if next_page_token is not None:
            kwargs["pageToken"] = next_page_token

        response = self.service.files().list(**kwargs).execute(num_retries=1)
        next_page_token = response["nextPageToken"] if "nextPageToken" in response.keys() else None

        return [DriveFile(**file) for file in response["files"]], next_page_token

    # @is_connected
    # @lock
    def get_by_id(self, id: str, fields: Optional[Tuple[str]] = AF.DEFAULT_FIELDS) -> DriveFile:
        """
        https://developers.google.com/drive/api/v3/reference/files/get
        :param id:
        :param fields: Pass None to get all file fields, for custom fields refer to https://developers.google.com/drive/api/v3/reference/files
        :return:
        """
        file = self.service.files().get(fileId=id,
                                        fields="*" if fields is None else ','.join(fields)).execute(num_retries=1)
        return DriveFile(**file)

    # @is_connected
    # @lock
    def create_folder(self, parent_id: str, name: str, fields: Optional[Tuple[str]] = AF.DEFAULT_FIELDS) -> DriveFile:
        """

        :param parent_id: New Folder's parent
        :param name: New Folder's name
        :param fields: Fields to return after creation, pass None to get all file fields, for custom fields refer to https://developers.google.com/drive/api/v3/reference/files
        :return: DriveFile of newly created folder
        """
        file_metadata = {
            "name": name,
            "mimeType": AF.FOLDER_MIME_TYPE,
            "parents": [parent_id]
        }
        file = self.service.files().create(body=file_metadata,
                                           fields="*" if fields is None else ','.join(fields)).execute(num_retries=1)
        return DriveFile(**file)

    # @is_connected
    # @lock
    def create_shortcut(self, parent_id: str, name: str, target_id: str,
                        fields: Optional[Tuple[str]] = AF.DEFAULT_FIELDS) -> DriveFile:
        """
        :param parent_id: Shortcuts's parent
        :param name: Shortcut's name
        :param target_id: ID of a file, shortcut is referring to
        :param fields: Fields to return after creation, pass None to get all file fields, for custom fields refer to https://developers.google.com/drive/api/v3/reference/files
        :return: DriveFile of newly created shortcut
        """
        file_metadata = {
            "name": name,
            "mimeType": AF.LINK_MIME_TYPE,
            "parents": [parent_id],
            "shortcutDetails": {"targetId": target_id}
        }
        file = self.service.files().create(body=file_metadata,
                                           fields="*" if fields is None else ','.join(fields)).execute(num_retries=1)
        return DriveFile(**file)

    # @is_connected
    # @lock
    def trash_file(self, id: str):
        """https://developers.google.com/drive/api/v3/reference/files/update"""
        self.service.files().update(fileId=id, body={"trashed": True}).execute(num_retries=1)

    # @is_connected
    # @lock
    def untrash_file(self, id: str):
        """https://developers.google.com/drive/api/v3/reference/files/update"""
        self.service.files().update(fileId=id, body={"trashed": False}).execute(num_retries=1)

    # @is_connected
    # @lock
    def rename_file(self, id: str, name: str, fields: Optional[Tuple[str]] = AF.DEFAULT_FIELDS) -> DriveFile:
        """
        https://developers.google.com/drive/api/v3/reference/files/update
        :param id: ID of a file to rename
        :param name: New name
        :param fields: Fields to return after update, pass None to get all file fields, for custom fields refer to https://developers.google.com/drive/api/v3/reference/files
        :return: DriveFile of updated file
        """
        file = self.service.files().update(fileId=id,
                                           body={"name": name},
                                           fields="*" if fields is None else ','.join(fields)).execute(num_retries=1)
        return DriveFile(**file)

    # @is_connected
    # @lock
    def move_file(self, file_id: str, old_parent_id: str, new_parent_id: str, fields: Optional[Tuple[str]] = AF.DEFAULT_FIELDS) -> DriveFile:
        """
        https://developers.google.com/drive/api/v3/reference/files/update
        :param file_id: ID of a file to move
        :param old_parent_id:
        :param new_parent_id:
        :param fields: Fields to return after update, pass None to get all file fields, for custom fields refer to https://developers.google.com/drive/api/v3/reference/files
        :return: DriveFile of updated file
        """
        file = self.service.files().update(fileId=file_id,
                                           addParents=new_parent_id,
                                           removeParents=old_parent_id,
                                           fields="*" if fields is None else ','.join(fields)).execute(num_retries=1)
        return DriveFile(**file)

    # @is_connected
    def download(self, file_id: str, output_buffer: BinaryIO, mime_type: Optional[str] = None, update_func: Optional[Callable] = None):
        """
        https://developers.google.com/drive/api/v3/manage-downloads
        :param file_id: ID of a file do download
        :param output_buffer: Buffer to write to
        :param mime_type: Optional, required for Google Apps to export them
        :param update_func: Function to run at every new data chunk, 1 float value is passed
        """
        if mime_type is not None:
            request = self.service.files().export_media(fileId=file_id, mimeType=mime_type)
        else:
            request = self.service.files().get_media(fileId=file_id)

        downloader = MediaIoBaseDownload(output_buffer, request)
        done = False
        while not done:
            status, done = downloader.next_chunk(num_retries=1)
            if update_func is not None:
                update_func(status.progress())

    def upload(self, name: str, parent_id: str, filename: Optional[str] = None, fields: Optional[Tuple[str]] = AF.DEFAULT_FIELDS) -> DriveFile:
        kwargs = {
            "body": {
                "name": name,
                "parents": [parent_id]
            },
            "fields": "*" if fields is None else ','.join(fields)
        }
        if filename is not None:
            kwargs["media_body"] = MediaFileUpload(filename)

        file = self.service.files().create(**kwargs).execute(num_retries=1)
        return DriveFile(**file)
