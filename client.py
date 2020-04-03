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
from googleapiclient.http import MediaIoBaseDownload
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
        try:
            return self._service[threading.get_ident()]
        except KeyError:
            self._service[threading.get_ident()] = build("drive", "v3", credentials=self.credentials)
            return self.service

    @property
    def start_page_token(self):
        return self.service.changes().getStartPageToken().execute()["startPageToken"]

    # @is_connected
    def auth(self) -> str:
        LOGGER.info("Authenticating")
        self.credentials = self.flow.run_local_server(host="localhost", port=8080)
        return self.credentials.to_json()

    # @is_connected
    def load_credentials(self, credentials: dict) -> bool:
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

    # @is_connected
    # @lock
    def changes(self, page_token: str, fields: Optional[Tuple[str]] = AF.DEFAULT_FIELDS) -> dict:
        if fields is None:
            fields = "nextPageToken,newStartPageToken,changes(removed,file(*))"
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

        kwargs = {"q": q,
                  "fields": "*" if fields is None else f"nextPageToken,files({','.join(fields)})",
                  "pageSize": 1000}
        if next_page_token is not None:
            kwargs["pageToken"] = next_page_token

        response = self.service.files().list(**kwargs).execute(num_retries=1)
        next_page_token = response["nextPageToken"] if "nextPageToken" in response.keys() else None

        return [DriveFile(**file) for file in response["files"]], next_page_token

    # @is_connected
    # @lock
    def get_by_id(self, id: str, fields: Optional[Tuple[str]] = AF.DEFAULT_FIELDS) -> DriveFile:
        file = self.service.files().get(fileId=id,
                                        fields="*" if fields is None else ','.join(fields)).execute(num_retries=1)
        return DriveFile(**file)

    # @is_connected
    # @lock
    def create_folder(self, parent_id: str, name: str, fields: Optional[Tuple[str]] = AF.DEFAULT_FIELDS) -> DriveFile:
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
    def trash_file(self, id: str):
        self.service.files().update(fileId=id, body={"trashed": True}).execute(num_retries=1)

    # @is_connected
    # @lock
    def untrash_file(self, id: str):
        self.service.files().update(fileId=id, body={"trashed": False}).execute(num_retries=1)

    # @is_connected
    # @lock
    def rename_file(self, id: str, name: str, fields: Optional[Tuple[str]] = AF.DEFAULT_FIELDS) -> DriveFile:
        file = self.service.files().update(fileId=id,
                                           body={"name": name},
                                           fields="*" if fields is None else ','.join(fields)).execute(num_retries=1)
        return DriveFile(**file)

    # @is_connected
    # @lock
    def move_file(self, file_id: str, old_parent_id: str, new_parent_id: str, fields: Optional[Tuple[str]] = AF.DEFAULT_FIELDS) -> DriveFile:
        file = self.service.files().update(fileId=file_id,
                                           addParents=new_parent_id,
                                           removeParents=old_parent_id,
                                           fields="*" if fields is None else ','.join(fields)).execute(num_retries=1)
        return DriveFile(**file)

    # @is_connected
    def download(self, file_id: str, output_buffer: BinaryIO, mime_type: Optional[str] = None, update_func: Optional[Callable] = None):
        if mime_type is not None:
            request = self.service.files().export_media(fileId=file_id, mimeType=mime_type)
        else:
            request = self.service.files().get_media(fileId=file_id)

        downloader = MediaIoBaseDownload(output_buffer, request)
        eof = False
        while not eof:
            status, eof = downloader.next_chunk(num_retries=1)
            if update_func is not None:
                update_func(status.progress())
