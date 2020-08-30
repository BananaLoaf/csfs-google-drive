import errno
from typing import Union, Tuple, List, Optional
import time
from datetime import datetime
import hashlib
import threading
import stat
import os

import pyfuse3
from pyfuse3 import Operations, FUSEError
from pathlib import Path

from CloudStorageFileSystem.utils.operations import flag2mode
from CloudStorageFileSystem.logger import LOGGER
from .inodemap import InodeMap
from .client import DriveClient, DriveFile
from .database import DriveDatabase, DatabaseFile
from .const import DF, AF, FF


def google_datetime_to_timestamp(datetime_str: str) -> int:
    return int(datetime.strptime(datetime_str, "%Y-%m-%dT%H:%M:%S.%fZ").timestamp())


class DriveFileSystem(Operations):
    _statfs: pyfuse3.StatvfsData

    download_lock = threading.Lock()

    inode_map = InodeMap()

    def __init__(self, db: DriveDatabase, client: DriveClient, trash: bool, cache_path: Path):
        super().__init__()

        self.db = db
        self.client = client
        self.trashed = trash

        self.cache_path = cache_path

    ################################################################
    # Helpers
    def try2ignore(self, path: str):
        """Ignore specific files and folders"""
        path = Path(path)
        if path.name in FF.IGNORED_FILES:
            raise FUSEError(errno.EIO)

    def get_db_file(self, path: str) -> Optional[DatabaseFile]:
        if path == "/":
            db_file = self.db.get_file(**{DF.PATH: path})
        else:
            db_file = self.db.get_file(**{DF.PATH: path, DF.TRASHED: self.trashed})
        return db_file

    def db2stat(self, db_file: DatabaseFile) -> pyfuse3.EntryAttributes:
        is_dir = db_file[DF.MIME_TYPE] == AF.FOLDER_MIME_TYPE

        st = pyfuse3.EntryAttributes()
        st.st_ino = self.inode_map.path2inode(db_file[DF.PATH])
        st.st_mode = stat.S_IFDIR | 0o755 if is_dir else stat.S_IFREG | 0o644
        st.st_nlink = 1
        st.st_size = 0 if is_dir else db_file[DF.FILE_SIZE]
        st.st_blocks = int((st.st_size + 511) / 512)
        st.st_atime_ns = float(db_file[DF.ATIME] - time.timezone)
        st.st_mtime_ns = float(db_file[DF.MTIME] - time.timezone)
        st.st_ctime_ns = float(db_file[DF.CTIME] - time.timezone)

        return st

    def drive2db(self, file: DriveFile) -> DatabaseFile:
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
        LOGGER.debug(f"q=\"{q}\"")

        drive_files, next_page_token = self.client.list_files(q=q)
        LOGGER.info(f"Received {len(drive_files)} DriveFiles")
        while next_page_token is not None:
            drive_files_next, next_page_token = self.client.list_files(q=q, next_page_token=next_page_token)
            drive_files += drive_files_next
            LOGGER.info(f"Received {len(drive_files)} DriveFiles")

        return drive_files

    # Listing
    def recursive_listdir(self, path: str):
        LOGGER.debug(f"Recursively listing '{path}'")
        if path == "/":
            self._recursive_list_root()
        else:
            parent_id = self.get_db_file(path=path)[DF.ID]
            self._recursive_list_any(parent_id=parent_id)

    def _recursive_list_root(self):
        q = f"'me' in owners and trashed={str(self.trashed).lower()}"
        drive_files = self.exec_query(q=q)
        drive_files = list(filter(lambda drive_file: "parents" in drive_file.keys(), drive_files))  # No orphans allowed

        # Get parent dir and add to db
        db_file = self.drive2db(self.client.get_by_id(id=AF.ROOT_ID))
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
            self.db.new_files([self.drive2db(drive_file) for drive_file in folder_drive_files_next])
            added_ids += [drive_file[DF.ID] for drive_file in folder_drive_files_next]

            # Filter not added files
            folders_drive_files = list(filter(
                lambda drive_file: drive_file not in folder_drive_files_next,
                folders_drive_files
            ))

        # Add file files
        self.db.new_files([self.drive2db(drive_file) for drive_file in file_drive_files])

    def _recursive_list_any(self, parent_id: str):
        q = f"'{parent_id}' in parents and trashed={str(self.trashed).lower()}"
        drive_files = self.exec_query(q=q)

        drive_file = self.client.get_by_id(id=parent_id)
        self.db.new_file(self.drive2db(drive_file))

        while len(drive_files) > 0:
            db_files = [self.drive2db(drive_file) for drive_file in drive_files]
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
        LOGGER.debug(f"Listing '{path}'")
        parent_id = AF.ROOT_ID if path == "/" else self.get_db_file(path=path)[DF.ID]
        try:
            LOGGER.debug(f"Updating files inside '{path}' - '{parent_id}'")
            q = f"'{parent_id}' in parents and trashed={str(self.trashed).lower()}"
            drive_files = self.exec_query(q=q)
            drive_files.append(self.client.get_by_id(id=parent_id))

            db_files = [self.drive2db(drive_file) for drive_file in drive_files]
            self.db.new_files(db_files)

        except ConnectionError as err:
            LOGGER.error(err)

    ################################################################
    # Cache handling
    def is_cached(self, md5: str):
        return self.cache_path.joinpath(md5).exists()

    def check_filesize(self, md5: str, dest_filesize: int):
        return self.cache_path.joinpath(md5).stat().st_size == dest_filesize

    @staticmethod
    def validate_cache_file(filename: Path, md5: str):
        with filename.open("rb") as file:
            return md5 == hashlib.md5(file.read()).hexdigest()

    def validate_all_cache_files(self):
        for filename in self.cache_path.glob("*"):
            if filename.is_file():
                if not self.validate_cache_file(filename, filename.name):
                    filename.unlink()

    ################################################################
    # Downloading and uploading related
    def download_file(self, db_file: DatabaseFile):
        output_file = Path(self.cache_path, db_file[DF.MD5]).with_suffix(".dpart")
        while True:
            LOGGER.info(f"Downloading '{db_file[DF.PATH]}'")
            with output_file.open("wb") as file:
                self.client.download(file_id=db_file[DF.ID], output_buffer=file)

            if self.validate_cache_file(output_file, db_file[DF.MD5]):
                break
            else:
                LOGGER.info(f"Invalid md5 hash of '{db_file[DF.PATH]}'")
                output_file.unlink(missing_ok=True)

        # Remove .dpart extension
        new_output_file = output_file.with_suffix("")
        output_file.rename(new_output_file)

        # Read only permissions
        new_output_file.chmod(stat.S_IREAD | stat.S_IRGRP)  # stat.S_IROTH

    ################################################################
    # FS ops
    def init(self):
        LOGGER.info(f"Initiating filesystem")

        drive_info = self.client.about()
        total_space = int(drive_info["storageQuota"]["limit"])
        used_space = int(drive_info["storageQuota"]["usageInDrive"])

        self._statfs = pyfuse3.StatvfsData()
        self._statfs.f_bsize = 512  # Filesystem block size
        self._statfs.f_frsize = 512  # Fragment size
        self._statfs.f_blocks = int(total_space / 512)  # Size of fs in f_frsize units
        self._statfs.f_bfree = int((total_space - used_space) / 512)  # Number of free blocks
        self._statfs.f_bavail = int((total_space - used_space) / 512)  # Number of free blocks for unprivileged users
        # self._statfs.f_files = 0,  # Number of inodes
        # self._statfs.f_ffree = 0,  # Number of free inodes
        # self._statfs.f_favail = 0,  # Number of free inodes for unprivileged users
        # self._statfs.f_fsid = 0,  # Filesystem ID
        # self._statfs.f_flag = 0,  # Mount flags
        self._statfs.f_namemax = 32767  # Maximum filename length

        self.recursive_listdir("/")

        LOGGER.info(f"Filesystem initiated successfully")

    async def statfs(self, ctx) -> pyfuse3.StatvfsData:
        LOGGER.debug(f"Statfs")
        return self._statfs

    ################################################################
    # Permissions
    async def access(self, inode, mode, ctx) -> int:
        return True

    ################################################################
    # Main ops
    async def lookup(self, inode_p: int, name: bytes, ctx: Optional[pyfuse3.RequestContext] = None):
        try:
            try:
                parent_path = self.inode_map[inode_p]
            except KeyError:
                raise FileNotFoundError(f"Parent inode ({inode_p}) does not exist")

            path = Path(parent_path).joinpath(os.fsdecode(name))
            self.try2ignore(path)

            db_file = self.get_db_file(str(path))
            if db_file is not None:
                return self.db2stat(db_file)
            else:
                raise FileNotFoundError(f"'{path}' does not exist")

        except FileNotFoundError as err:
            LOGGER.error(err)
            raise FUSEError(errno.ENOENT)

    async def forget(self, inode_list: list):  # Called on sleep, hibernation
        for inode in inode_list:
            # Root stays
            if inode[0] == pyfuse3.ROOT_INODE:
                continue

            try:
                path = self.inode_map.pop(inode[0])
                LOGGER.info(f"Forgetting '{path}' ({inode[0]})")
            except KeyError:
                LOGGER.warning(f"Nothing to forget, inode ({inode[0]})")

    # async def setattr(self, inode, attr, fields, fh, ctx):
    #     pass

    async def getattr(self, inode: int, ctx: Optional[pyfuse3.RequestContext]) -> pyfuse3.EntryAttributes:
        try:
            try:
                path = self.inode_map[inode]
            except KeyError:
                raise FileNotFoundError(f"Inode ({inode}) does not exist")
            self.try2ignore(path)

            db_file = self.get_db_file(path)
            if db_file is None:
                raise FileNotFoundError(f"'{path}' does not exist")

            return self.db2stat(db_file)

        except FileNotFoundError as err:
            LOGGER.error(err)
            raise FUSEError(errno.ENOENT)

    async def opendir(self, inode: int, ctx: Optional[pyfuse3.RequestContext]):
        return inode

    # async def releasedir(self, fh: int):
    #     pass

    async def readdir(self, inode: int, start_id: int, token):
        try:
            try:
                path = self.inode_map[inode]
            except KeyError:
                raise FileNotFoundError(f"Inode ({inode}) does not exist")

            db_file = self.get_db_file(path)
            if db_file is None:
                raise FileNotFoundError(f"'{path}' does not exist")

            LOGGER.info(f"Listing '{path}'")
            db_files = self.db.get_files(**{DF.PARENT_ID: db_file[DF.ID], DF.TRASHED: self.trashed})

            # List all entries and sort them by inode
            entries = []
            for db_file in db_files:
                name = os.fsencode(Path(db_file[DF.PATH]).name)
                stat = self.db2stat(db_file)
                inode = stat.st_ino
                entries.append((name, stat, inode))

            # Sort entries by inode
            entries = list(sorted(entries, key=lambda e: e[2]))

            # Take only new entries if start_id > 0, new entries are guaranteed to be at the end
            for args in entries[start_id - 1 if start_id > 0 else start_id:]:
                if not pyfuse3.readdir_reply(token, *args):
                    break

        except FileNotFoundError as err:
            LOGGER.error(err)
            raise FUSEError(errno.ENOENT)

    async def rename(self, inode_p: int, name: bytes, inode_p_new: int, name_new: bytes, flags: int, ctx):
        try:
            # Renaming
            if inode_p == inode_p_new:
                try:
                    parent_path = self.inode_map[inode_p]
                except KeyError:
                    raise FileNotFoundError(f"Parent inode ({inode_p}) does not exist")

                path = Path(parent_path).joinpath(os.fsdecode(name))
                path_new = Path(parent_path).joinpath(os.fsdecode(name_new))
                LOGGER.info(f"Renaming '{path}' into '{path_new}'")

                # Check existence and non existence
                db_file = self.get_db_file(str(path))
                if db_file is None:
                    raise FileNotFoundError(f"'{path}' does not exist")
                db_file_new = self.get_db_file(str(path_new))
                if db_file_new is not None:
                    raise FileExistsError(f"'{path_new}' already exist")

                # Create and add to db
                drive_file = self.client.rename_file(id=db_file[DF.ID], name=path_new.name)
                db_file_new = self.drive2db(drive_file)
                self.db.new_file(db_file_new)

                # Pop old inode from inode_map and append new one
                self.inode_map.pop(self.inode_map[str(path)])
                self.inode_map.append(db_file_new[DF.PATH])

            # Moving
            else:
                try:
                    path = Path(self.inode_map[inode_p]).joinpath(os.fsdecode(name))
                except KeyError:
                    raise FileNotFoundError(f"Parent inode ({inode_p}) does not exist")
                try:
                    path_new = Path(self.inode_map[inode_p_new]).joinpath(os.fsdecode(name_new))
                except KeyError:
                    raise FileNotFoundError(f"New parent inode ({inode_p_new}) does not exist")
                LOGGER.info(f"Moving '{path}' to '{path_new}'")

                # Check existence and non existence
                db_file = self.get_db_file(str(path))
                if db_file is None:
                    raise FileNotFoundError(f"'{path}' does not exist")
                db_file_new = self.get_db_file(str(path_new))
                if db_file_new is not None:
                    raise FileExistsError(f"'{path_new}' already exist")

                parent_db_file_new = self.get_db_file(path=str(path_new.parent))

                drive_file = self.client.move_file(file_id=db_file[DF.ID],
                                                   old_parent_id=db_file[DF.PARENT_ID],
                                                   new_parent_id=parent_db_file_new[DF.ID])
                db_file_new = self.drive2db(drive_file)
                self.db.new_file(db_file_new)

                # Pop old inode from inode_map and append new one
                self.inode_map.pop(self.inode_map[str(path)])
                self.inode_map.append(db_file_new[DF.PATH])

        except FileNotFoundError as err:
            LOGGER.error(err)
            raise FUSEError(errno.ENOENT)
        except FileExistsError as err:
            LOGGER.error(err)
            raise FUSEError(errno.EEXIST)

    # async def mknod(self, inode_p, name, mode, rdev, ctx):
    #     raise FUSEError(errno.EIO)

    async def mkdir(self, inode_p: int, name: bytes, mode: int, ctx) -> pyfuse3.EntryAttributes:
        try:
            try:
                parent_path = self.inode_map[inode_p]
            except KeyError:
                raise FileNotFoundError(f"Parent inode ({inode_p}) does not exist")

            path = Path(parent_path).joinpath(os.fsdecode(name))
            self.try2ignore(path)

            db_file = self.get_db_file(str(path))
            if db_file is not None:
                raise FileExistsError(f"'{path}' already exists")

            LOGGER.info(f"Creating directory '{path}'")
            parent_id = self.get_db_file(path=str(parent_path))[DF.ID]
            drive_file = self.client.create_folder(parent_id=parent_id, name=path.name)
            db_file = self.drive2db(drive_file)
            self.db.new_file(db_file)

            return self.db2stat(db_file)

        except FileNotFoundError as err:
            LOGGER.error(err)
            raise FUSEError(errno.ENOENT)
        except FileExistsError as err:
            LOGGER.error(err)
            raise FUSEError(errno.EEXIST)

    def _remove(self, path: str) -> str:
        db_file = self.get_db_file(path)
        if db_file[DF.TRASHED]:
            self.client.untrash_file(id=db_file[DF.ID])
        else:
            self.client.trash_file(id=db_file[DF.ID])
        return db_file[DF.ID]

    async def rmdir(self, parent_inode, name, ctx):
        raise FUSEError(errno.EIO)
        LOGGER.info(f"Removing directory '{path}'")

        id = self._remove(path)
        self.db.delete_file_children(id=id)
        self.db.delete_file(id=id)

    async def unlink(self, parent_inode, name, ctx):
        raise FUSEError(errno.EIO)
        LOGGER.info(f"Removing file '{path}'")

        id = self._remove(path)
        self.db.delete_file(id=id)

    ################################################################
    # File ops
    async def create(self, parent_inode, name, mode, flags, ctx) -> int:
        raise FUSEError(errno.EROFS)
        return 0

    async def open(self, inode, flags, ctx) -> int:
        raise FUSEError(errno.EIO)
        mode = flag2mode(flags)
        LOGGER.info(f"Opening '{path}' in '{mode}' mode ({flags})")

        # Download
        if "r" in mode or "a" in mode:
            db_file = self.get_db_file(path)

            # Files
            if db_file[DF.MIME_TYPE] not in AF.GOOGLE_APP_MIME_TYPES:
                try:
                    assert self.is_cached(db_file[DF.MD5]), f"No cache found for '{path}'"
                    assert self.check_filesize(md5=db_file[DF.MD5], dest_filesize=db_file[DF.FILE_SIZE]), f"Cache filesize mismatch for '{path}'"
                    # TODO Validate cache if config says so
                    return 0

                except AssertionError as err:
                    LOGGER.debug(err)

                    self.download_lock.acquire()
                    self.download_file(db_file)
                    self.download_lock.release()
                    return 0

            # Google Apps
            else:
                return 0

        return 0

    async def read(self, fh, off, size) -> bytes:
        raise FUSEError(errno.EIO)
        db_file = self.get_db_file(path)

        # Files
        if db_file[DF.MIME_TYPE] not in AF.GOOGLE_APP_MIME_TYPES:
            with Path(self.cache_path, db_file[DF.MD5]).open("rb") as file:
                file.seek(offset)
                return file.read(size)

        # Google Apps
        else:
            raise FuseOSError(errno.EIO)

    async def write(self, fh, off, buf):
        raise FUSEError(errno.EIO)

    async def release(self, fh) -> int:
        raise FUSEError(errno.EIO)
        return 0

    async def flush(self, fh) -> int:
        raise FUSEError(errno.EIO)
        pass

    async def fsync(self, fh, datasync) -> int:
        raise FUSEError(errno.EIO)
        return 0
