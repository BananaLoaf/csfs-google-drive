import errno
from typing import Union, Tuple, List, Optional
import time
from datetime import datetime

from refuse.high import FuseOSError
from pathlib import Path

from CloudStorageFileSystem.utils.filesystem import FileSystem, Stat
from CloudStorageFileSystem.logger import LOGGER
from .client import DriveClient, DriveFile
from .database import DriveDatabase, DatabaseFile
from .const import DF, AF, FF


FS_PROC_NAME = "Filesystem"


def google_datetime_to_timestamp(datetime_str: str) -> int:
    return int(datetime.strptime(datetime_str, "%Y-%m-%dT%H:%M:%S.%fZ").timestamp())


class DriveFileSystem(FileSystem):
    def __init__(self, db: DriveDatabase, client: DriveClient):
        self.db = db
        self.client = client
        self.trashed = False

    ################################################################
    # Helpers
    def exists(self, path: str) -> bool:
        path = Path(path)
        return path.name not in FF.IGNORED_FILES

    def get_db_file(self, path: Path) -> Optional[DatabaseFile]:
        if path == "/":
            db_file = self.db.get_file(**{DF.FILENAME: str(path)})
        else:
            path = Path(path)
            db_file = self.db.get_file(**{DF.PATH: str(path.parent), DF.FILENAME: str(path.name), DF.TRASHED: self.trashed})

        return db_file

    def db_file2stat(self, db_file: DatabaseFile) -> Stat:
        st = Stat(is_dir=db_file[DF.MIME_TYPE] == AF.FOLDER_MIME_TYPE,
                  atime=db_file[DF.ATIME] - time.timezone,
                  mtime=db_file[DF.MTIME] - time.timezone,
                  ctime=db_file[DF.CTIME] - time.timezone,
                  size=0 if db_file[DF.MIME_TYPE] == AF.FOLDER_MIME_TYPE else db_file[DF.FILE_SIZE])  # or 4096
        return st

    def drive2db(self, file: DriveFile) -> DatabaseFile:
        # path
        try:
            db_file = self.db.get_file(**{DF.ID: file["parents"][0]})
            if db_file[DF.PATH]:
                path = str(Path(db_file[DF.PATH], db_file[DF.FILENAME]))
            else:
                path = db_file[DF.FILENAME]
        except KeyError:
            path = None
        except TypeError:
            path = "/"

        # parent_id
        try:
            file["parents"][0]
        except KeyError:
            file["parents"] = [None]

        # name
        if file["id"] == AF.ROOT_ID:
            file["name"] = "/"

        # file_size
        try:
            file["size"]
        except KeyError:
            file["size"] = 0

        # atime UTC
        try:
            file["viewedByMeTime"] = file["viewedByMeTime"] if isinstance(file["viewedByMeTime"], int) else google_datetime_to_timestamp(file["viewedByMeTime"])
        except KeyError:
            file["viewedByMeTime"] = 0

        # ctime UTC
        file["createdTime"] = file["createdTime"] if isinstance(file["createdTime"], int) else google_datetime_to_timestamp(file["createdTime"])

        # mtime UTC
        file["modifiedTime"] = file["modifiedTime"] if isinstance(file["modifiedTime"], int) else google_datetime_to_timestamp(file["modifiedTime"])

        # md5
        try:
            file["md5Checksum"]
        except KeyError:
            file["md5Checksum"] = None

        db_file = DatabaseFile.from_kwargs(**{
            DF.ID: file["id"],
            DF.PARENT_ID: file["parents"][0],
            DF.PATH: path,
            DF.FILENAME: file["name"],
            DF.FILE_SIZE: file["size"],
            DF.ATIME: file["viewedByMeTime"],
            DF.CTIME: file["createdTime"],
            DF.MTIME: file["modifiedTime"],
            DF.MIME_TYPE: file["mimeType"],
            DF.TRASHED: file["trashed"],
            DF.MD5: file["md5Checksum"]
        })
        return db_file

    def _exec_query(self, q: str) -> List[DriveFile]:
        LOGGER.debug(f"[{FS_PROC_NAME}] q='{q}'")

        drive_files, next_page_token = self.client.list_files(q=q)
        LOGGER.debug(f"[{FS_PROC_NAME}] Received {len(drive_files)} DriveFiles")
        while next_page_token is not None:
            drive_files_next, next_page_token = self.client.list_files(q=q, next_page_token=next_page_token)
            drive_files += drive_files_next
            LOGGER.debug(f"[{FS_PROC_NAME}] Received {len(drive_files)} DriveFiles")

        return drive_files

    def _recursive_list_root(self):
        q = f"'me' in owners and trashed={str(self.trashed).lower()}"  # and '{AF.ROOT_ID}' in parents
        drive_files = self._exec_query(q=q)

        # Get parent dir and add to db
        drive_file = self.client.get_by_id(id=AF.ROOT_ID)
        self.db.new_file(self.drive2db(drive_file))
        added_ids = [drive_file[DF.ID]]

        # Split folders and files into different lists
        folders_drive_files = list(filter(lambda drive_file: drive_file["mimeType"] == AF.FOLDER_MIME_TYPE, drive_files))
        file_drive_files = [drive_file for drive_file in drive_files if drive_file not in folders_drive_files]

        # Add folder files
        while len(folders_drive_files) > 0:
            # Filter drive files with added parents
            next_drive_files = list(filter(
                lambda drive_file: drive_file["parents"][0] in added_ids,
                folders_drive_files
            ))
            folders_drive_files = [drive_file for drive_file in folders_drive_files if
                                   drive_file not in next_drive_files]

            # Add them to db and add them to the list of added parents
            self.db.new_files([self.drive2db(drive_file) for drive_file in next_drive_files])
            added_ids += [drive_file[DF.ID] for drive_file in next_drive_files]

        # Add file files
        self.db.new_files([self.drive2db(drive_file) for drive_file in file_drive_files])

    ################################################################
    # FS ops
    def init(self, path: str):
        LOGGER.info(f"[{FS_PROC_NAME}] Initiating filesystem")
        self._recursive_list_root()

    def destroy(self, path: str):
        pass

    def statfs(self, path: str) -> dict:
        return {}

    ################################################################
    # Permissions
    def access(self, path: str, amode) -> int:
        return 0

    ################################################################
    # Dir ops
    def getattr(self, path: str, fh) -> Stat:
        if self.exists(path):
            db_file = self.get_db_file(Path(path))
            return self.db_file2stat(db_file)

        else:
            LOGGER.error(f"[{FS_PROC_NAME}] '{path}' does not exist")
            raise FuseOSError(errno.ENOENT)

    def readdir(self, path: str, fh) -> Union[List[str], List[Tuple[str, Stat, int]]]:
        if self.exists(path):
            parent_id = AF.ROOT_ID if path == "/" else self.get_db_file(Path(path))[DF.ID]
            db_files = self.db.get_files(**{DF.PARENT_ID: parent_id, DF.TRASHED: self.trashed})

            for db_file in db_files:
                yield db_file[DF.FILENAME], self.db_file2stat(db_file), 0

        else:
            LOGGER.error(f"[{FS_PROC_NAME}] Unable to list '{path}', does not exist")
            raise FuseOSError(errno.ENOENT)

    def rename(self, old: str, new: str):
        raise FuseOSError(errno.EROFS)

    def mkdir(self, path: str, mode):
        raise FuseOSError(errno.EROFS)

    def rmdir(self, path: str):
        raise FuseOSError(errno.EROFS)

    def unlink(self, path: str):
        raise FuseOSError(errno.EROFS)
