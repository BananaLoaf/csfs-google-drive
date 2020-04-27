import errno
from typing import Union, Tuple, List, Optional
import time
from datetime import datetime

from refuse.high import FuseOSError
from pathlib import Path

from CloudStorageFileSystem.utils.operations import CustomOperations, Stat
from CloudStorageFileSystem.logger import LOGGER
from .client import DriveClient, DriveFile
from .database import DriveDatabase, DatabaseFile
from .const import DF, AF, FF


FS_PROC_NAME = "Filesystem"


def google_datetime_to_timestamp(datetime_str: str) -> int:
    return int(datetime.strptime(datetime_str, "%Y-%m-%dT%H:%M:%S.%fZ").timestamp())


class DriveFileSystem(CustomOperations):
    _statfs: dict

    def __init__(self, db: DriveDatabase, client: DriveClient):
        self.db = db
        self.client = client
        self.trashed = False

    ################################################################
    # Helpers
    def try2ignore(self, path: str):
        """Ignore linux specific or file manager specific files and folders"""
        path = Path(path)
        if path.name in FF.IGNORED_FILES:
            raise FuseOSError(errno.EIO)

    def get_db_file(self, path: str) -> Optional[DatabaseFile]:
        if path == "/":
            db_file = self.db.get_file(**{DF.PATH: path})
        else:
            db_file = self.db.get_file(**{DF.PATH: path, DF.TRASHED: self.trashed})
        return db_file

    def db_file2stat(self, db_file: DatabaseFile) -> Stat:
        st = Stat(is_dir=db_file[DF.MIME_TYPE] == AF.FOLDER_MIME_TYPE,
                  atime=db_file[DF.ATIME] - time.timezone,
                  mtime=db_file[DF.MTIME] - time.timezone,
                  ctime=db_file[DF.CTIME] - time.timezone,
                  size=0 if db_file[DF.MIME_TYPE] == AF.FOLDER_MIME_TYPE else db_file[DF.FILE_SIZE])  # or 4096
        return st

    def drive_file2db_file(self, file: DriveFile) -> DatabaseFile:
        id = file["id"]
        parent_id = file["parents"][0] if "parents" in file.keys() else None

        if file["id"] == AF.ROOT_ID:
            path = "/"
        else:
            parent_db_file = self.db.get_file(**{DF.ID: parent_id, DF.TRASHED: self.trashed})
            path = str(Path(parent_db_file[DF.PATH], file["name"]))

        file_size = file["size"] if "size" in file.keys() else 0
        atime = google_datetime_to_timestamp(file["viewedByMeTime"]) if "viewedByMeTime" in file.keys() else 0
        ctime = google_datetime_to_timestamp(file["createdTime"])
        mtime = google_datetime_to_timestamp(file["modifiedTime"])
        mime_type = file["mimeType"]
        trashed = file["trashed"]
        md5 = file["md5Checksum"] if "md5Checksum" in file.keys() else None

        db_file = DatabaseFile.from_kwargs(**{
            DF.ID: id,
            DF.PARENT_ID: parent_id,
            DF.PATH: path,
            DF.FILE_SIZE: file_size,
            DF.ATIME: atime,
            DF.CTIME: ctime,
            DF.MTIME: mtime,
            DF.MIME_TYPE: mime_type,
            DF.TRASHED: trashed,
            DF.MD5: md5
        })
        return db_file

    def exec_query(self, q: str) -> List[DriveFile]:
        LOGGER.debug(f"[{FS_PROC_NAME}] q='{q}'")

        drive_files, next_page_token = self.client.list_files(q=q)
        LOGGER.info(f"[{FS_PROC_NAME}] Received {len(drive_files)} DriveFiles")
        while next_page_token is not None:
            drive_files_next, next_page_token = self.client.list_files(q=q, next_page_token=next_page_token)
            drive_files += drive_files_next
            LOGGER.info(f"[{FS_PROC_NAME}] Received {len(drive_files)} DriveFiles")

        return drive_files

    def recursive_listdir(self, path: str):
        LOGGER.debug(f"[{FS_PROC_NAME}] Recursively listing '{path}'")
        if path == "/":
            self._recursive_list_root()
        else:
            parent_id = self.get_db_file(path=path)[DF.ID]
            self._recursive_list_any(parent_id=parent_id)

    def _recursive_list_root(self):
        q = f"'me' in owners and trashed={str(self.trashed).lower()}"
        drive_files = self.exec_query(q=q)

        # Get parent dir and add to db
        db_file = self.drive_file2db_file(self.client.get_by_id(id=AF.ROOT_ID))
        self.db.new_file(db_file)
        added_ids = [db_file[DF.ID]]

        # Split folders and files into different lists
        folders_drive_files = list(filter(lambda drive_file: drive_file["mimeType"] == AF.FOLDER_MIME_TYPE, drive_files))
        file_drive_files = list(filter(lambda drive_file: drive_file["mimeType"] != AF.FOLDER_MIME_TYPE, drive_files))

        # Add folder files
        while len(folders_drive_files) > 0:
            # Add drive files with added parents and add them to the list of added parents
            folder_drive_files_next = list(filter(
                lambda drive_file: drive_file["parents"][0] in added_ids,
                folders_drive_files
            ))
            self.db.new_files([self.drive_file2db_file(drive_file) for drive_file in folder_drive_files_next])
            added_ids += [drive_file[DF.ID] for drive_file in folder_drive_files_next]

            # Filter not added files
            folders_drive_files = list(filter(
                lambda drive_file: drive_file not in folder_drive_files_next,
                folders_drive_files
            ))

        # Add file files
        self.db.new_files([self.drive_file2db_file(drive_file) for drive_file in file_drive_files])

    def _recursive_list_any(self, parent_id: str):
        q = f"'{parent_id}' in parents and trashed={str(self.trashed).lower()}"
        drive_files = self.exec_query(q=q)

        drive_file = self.client.get_by_id(id=parent_id)
        self.db.new_file(self.drive_file2db_file(drive_file))

        while len(drive_files) > 0:
            db_files = [self.drive_file2db_file(drive_file) for drive_file in drive_files]
            self.db.new_files(db_files)

            # Filter folders and extract their ids
            folders_drive_files = filter(lambda db_file: db_file[DF.MIME_TYPE] == AF.FOLDER_MIME_TYPE, db_files)
            parent_ids = [drive_file["id"] for drive_file in folders_drive_files]

            # Break in chunks and exec query
            drive_files = []
            CHUNK_SIZE = 50
            for i in range(0, len(parent_ids), CHUNK_SIZE):
                ids_chunk = parent_ids[i:i + CHUNK_SIZE]
                q = " or ".join([f"'{id}' in parents" for id in ids_chunk]) + f" and trashed={str(self.trashed).lower()}"
                drive_files += self.exec_query(q=q)

    def listdir(self, path: str):
        LOGGER.debug(f"[{FS_PROC_NAME}] Listing '{path}'")
        parent_id = AF.ROOT_ID if path == "/" else self.get_db_file(path=path)[DF.ID]
        try:
            LOGGER.debug(f"[{FS_PROC_NAME}] Updating files inside '{path}' - '{parent_id}'")
            q = f"'{parent_id}' in parents and trashed={str(self.trashed).lower()}"
            drive_files = self.exec_query(q=q)
            drive_files.append(self.client.get_by_id(id=parent_id))

            db_files = [self.drive_file2db_file(drive_file) for drive_file in drive_files]
            self.db.new_files(db_files)

        except ConnectionError as err:
            LOGGER.error(f"[{FS_PROC_NAME}] {err}")

    def _remove(self, path: str) -> str:
        db_file = self.get_db_file(path)
        if db_file[DF.TRASHED]:
            self.client.untrash_file(id=db_file[DF.ID])
        else:
            self.client.trash_file(id=db_file[DF.ID])
        return db_file[DF.ID]

    ################################################################
    # FS ops
    def init(self, path: str):
        """Called on filesystem initialization. Path is always /"""
        LOGGER.info(f"[{FS_PROC_NAME}] Initiating filesystem")

        drive_info = self.client.about()
        total_space = int(drive_info["storageQuota"]["limit"])
        used_space = int(drive_info["storageQuota"]["usage"])
        self._statfs = {
            "f_bsize": 512,  # Filesystem block size
            "f_frsize": 512,  # Fragment size
            "f_blocks": int(total_space / 512),  # Size of fs in f_frsize units
            "f_bfree": int((total_space - used_space) / 512),  # Number of free blocks
            "f_bavail": int((total_space - used_space) / 512),  # Number of free blocks for unprivileged users
            "f_files": 0,  # Number of inodes
            "f_ffree": 0,  # Number of free inodes
            "f_favail": 0,  # Number of free inodes for unprivileged users
            # "f_fsid": 0,  # Filesystem ID
            # "f_flag": 0,  # Mount flags
            # "f_namemax": 0  # Maximum filename length
        }

        self.recursive_listdir(path)

        LOGGER.info(f"[{FS_PROC_NAME}] Filesystem initiated successfully")

    def destroy(self, path: str):
        """Called on filesystem destruction. Path is always /"""
        LOGGER.info(f"[{FS_PROC_NAME}] Destroying filesystem")

    def statfs(self, path: str) -> dict:
        """
        Returns a dictionary with keys identical to the statvfs C structure of
        statvfs(3).

        On Mac OS X f_bsize and f_frsize must be a power of 2
        (minimum 512).
        """

        LOGGER.debug(f"[{FS_PROC_NAME}] Statfs '{path}'")
        return self._statfs

    # def ioctl(self, path, cmd, arg, fip, flags, data):
    #     raise FuseOSError(errno.ENOTTY)

    ################################################################
    # Permissions
    # def access(self, path: str, amode) -> int:
    #     return 0
    #
    # def chmod(self, path: str, mode):
    #     raise FuseOSError(errno.EROFS)
    #
    # def chown(self, path: str, uid, gid):
    #     raise FuseOSError(errno.EROFS)

    ################################################################
    # Main ops
    def getattr(self, path: str, fh: Optional[int] = None) -> Stat:
        self.try2ignore(path)

        db_file = self.get_db_file(path)
        if db_file is not None:
            # LOGGER.debug(f"[{FS_PROC_NAME}] Getting attributes of '{path}'")
            return self.db_file2stat(db_file)

        else:
            LOGGER.error(f"[{FS_PROC_NAME}] '{path}' does not exist")
            raise FuseOSError(errno.ENOENT)

    def readdir(self, path: str, fh) -> Union[str, Tuple[str, Stat, int]]:
        self.try2ignore(path)

        yield "."
        yield ".."

        db_file = self.get_db_file(path)
        if db_file is not None:
            LOGGER.info(f"[{FS_PROC_NAME}] Listing '{path}'")
            db_files = self.db.get_files(**{DF.PARENT_ID: db_file[DF.ID], DF.TRASHED: self.trashed})

            for db_file in db_files:
                yield Path(db_file[DF.PATH]).name, self.db_file2stat(db_file), 0

        else:
            LOGGER.error(f"[{FS_PROC_NAME}] Unable to list '{path}', does not exist")
            raise FuseOSError(errno.ENOENT)

    def rename(self, old: str, new: str):
        old_db_file = self.get_db_file(path=old)

        old = Path(old)
        new = Path(new)
        # Renaming
        if old.parent == new.parent:
            LOGGER.info(f"[{FS_PROC_NAME}] Renaming '{old}' into '{new}'")

            drive_file = self.client.rename_file(id=old_db_file[DF.ID], name=new.name)

            new_db_file = self.drive_file2db_file(drive_file)
            self.db.new_file(new_db_file)

        # Moving
        else:
            LOGGER.info(f"[{FS_PROC_NAME}] Moving '{old}' to '{new}'")

            old_parent_id = self.get_db_file(path=str(old.parent))[DF.ID]
            new_parent_id = self.get_db_file(path=str(new.parent))[DF.ID]

            drive_file = self.client.move_file(file_id=old_db_file[DF.ID],
                                               old_parent_id=old_parent_id, new_parent_id=new_parent_id)
            new_db_file = self.drive_file2db_file(drive_file)
            self.db.new_file(new_db_file)

    def mkdir(self, path: str, mode):
        self.try2ignore(path)

        LOGGER.info(f"[{FS_PROC_NAME}] Creating directory '{path}'")

        path = Path(path)
        parent_path = path.parent

        parent_id = self.get_db_file(path=str(parent_path))[DF.ID]
        drive_file = self.client.create_folder(parent_id=parent_id, name=path.name)

        new_db_file = self.drive_file2db_file(drive_file)
        self.db.new_file(new_db_file)

    def rmdir(self, path: str):
        LOGGER.info(f"[{FS_PROC_NAME}] Removing directory '{path}'")

        id = self._remove(path)
        # self.db.delete_file_children(id=id)
        self.db.delete_file(id=id)

    def unlink(self, path: str):
        LOGGER.info(f"[{FS_PROC_NAME}] Removing file '{path}'")

        id = self._remove(path)
        self.db.delete_file(id=id)

    ################################################################
    # Other ops
    # def utimens(self, path: str, times: Optional[Tuple] = None) -> int:
    #     """Times is a (atime, mtime) tuple. If None use current time."""
    #     return 0
    #
    # def link(self, target: str, source: str):
    #     """Creates a hard link `target -> source` (e.g. ln source target)"""
    #     raise FuseOSError(errno.EROFS)
    #
    # def symlink(self, target: str, source: str):
    #     """Creates a symlink `target -> source` (e.g. ln -s source target)"""
    #     raise FuseOSError(errno.EROFS)
    #
    # def readlink(self, path: str):
    #     raise FuseOSError(errno.ENOENT)
    #
    # def opendir(self, path: str) -> int:
    #     """Returns a numerical file handle."""
    #     return 0
    #
    # def releasedir(self, path: str, fh) -> int:
    #     return 0
    #
    # def fsyncdir(self, path: str, datasync, fh) -> int:
    #     return 0

    ################################################################
    # File ops
    # def create(self, path: str, mode, fi: Optional[int] = None) -> int:
    #     """Create should return a numerical file handle."""
    #     raise FuseOSError(errno.EROFS)
    #     return 0
    #
    # def open(self, path: str, flags) -> int:
    #     """Open should return a numerical file handle."""
    #     return 0
    #
    # def read(self, path: str, size: int, offset: int, fh) -> bytes:
    #     """Returns a byte string containing the data requested."""
    #     raise FuseOSError(errno.EIO)
    #
    # def write(self, path: str, data: bytes, offset: int, fh):
    #     raise FuseOSError(errno.EROFS)
    #
    # def truncate(self, path: str, length: int, fh: Optional[int] = None):
    #     raise FuseOSError(errno.EROFS)
    #
    # def release(self, path: str, fh) -> int:
    #     return 0
    #
    # def flush(self, path: str, fh) -> int:
    #     return 0
    #
    # def fsync(self, path: str, datasync, fh) -> int:
    #     return 0

    ################################################################
    # Extended attributes
    # def setxattr(self, path: str, name, value, options, position: int = 0):
    #     raise FuseOSError(errno.ENOTSUP)
    #
    # def getxattr(self, path: str, name, position: int = 0):
    #     raise FuseOSError(errno.ENOTSUP)
    #
    # def removexattr(self, path: str, name):
    #     raise FuseOSError(errno.ENOTSUP)
    #
    # def listxattr(self, path: str) -> list:
    #     return []
