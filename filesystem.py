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
from CloudStorageFileSystem.utils.database import ROWID
from CloudStorageFileSystem.logger import LOGGER
from .client import DriveClient, DriveFile
from .database import DriveDatabase, DatabaseFile, DatabaseDJob
from .const import DF, AF, FF


def google_datetime_to_timestamp(datetime_str: str) -> int:
    return int(datetime.strptime(datetime_str, "%Y-%m-%dT%H:%M:%S.%fZ").timestamp())


################################################################
class FUSEErrorTemplate(Exception):
    en: int

    def __init__(self, msg: Optional[str] = None):
        if msg:
            LOGGER.error(msg)
        raise FUSEError(self.en)


class FUSEFileNotFoundError(FUSEErrorTemplate):
    en = errno.ENOENT


class FUSEFileExistsError(FUSEErrorTemplate):
    en = errno.EEXIST


class FUSECrossDeviceLink(FUSEErrorTemplate):
    en = errno.EXDEV


class FUSEIOError(FUSEErrorTemplate):
    en = errno.EIO


################################################################
class DriveFileSystem(Operations):
    def __init__(self, db: DriveDatabase, client: DriveClient, trash: bool, mountpoint: Path, cache_path: Path):
        super().__init__()

        self.db = db
        self.client = client
        self.trashed = trash

        self.mountpoint = mountpoint
        self.cache_path = cache_path

    ################################################################
    # Helpers
    def try2ignore(self, path: str):
        """Ignore specific files and folders"""
        path = Path(path)
        if path.name in FF.IGNORED_FILES:
            raise FUSEIOError

    def resolve_path(self, db_file: DatabaseFile) -> Path:
        parents = [db_file[DF.NAME], ]

        while db_file[DF.ID] != AF.ROOT_ID:
            rowid, db_file = self.db.get_file(**{DF.ID: db_file[DF.PARENT_ID]})
            parents.append(db_file[DF.NAME])

        return Path(*reversed(parents))

    def get_by_path(self, path: Path) -> Tuple[int, DatabaseFile]:
        inode, db_file = self.db.get_file(**{DF.ID: AF.ROOT_ID})
        for child_name in path.parts[1:]:
            inode, db_file = self.db.get_file(**{DF.PARENT_ID: db_file[DF.ID], DF.NAME: child_name})

        return inode, db_file

    def db2stat(self, inode: int, db_file: DatabaseFile) -> pyfuse3.EntryAttributes:
        is_dir = db_file[DF.MIME_TYPE] == AF.FOLDER_MIME_TYPE
        is_link = db_file[DF.MIME_TYPE] == AF.LINK_MIME_TYPE

        st = pyfuse3.EntryAttributes()
        if is_dir:
            st.st_mode = stat.S_IFDIR | 0o755
            st.st_size = 0
        elif is_link:
            st.st_mode = stat.S_IFLNK | 0o777
            st.st_size = 40
        else:
            st.st_mode = stat.S_IFREG | 0o644
            st.st_size = db_file[DF.FILE_SIZE]

        st.st_ino = inode
        st.st_nlink = 1
        st.st_blocks = int((st.st_size + 511) / 512)
        st.st_atime_ns = float(db_file[DF.ATIME] - time.timezone) * 10**9
        st.st_mtime_ns = float(db_file[DF.MTIME] - time.timezone) * 10**9
        st.st_ctime_ns = float(db_file[DF.MTIME] - time.timezone) * 10**9

        return st

    def get_file_info(self, fh: int) -> pyfuse3.FileInfo:
        fi = pyfuse3.FileInfo()
        fi.direct_io = True
        fi.fh = fh
        fi.keep_cache = True
        fi.nonseekable = False
        return fi

    def drive2db(self, file: DriveFile) -> DatabaseFile:
        id = file["id"]
        parent_id = file["parents"][0] if "parents" in file.keys() else None

        if file["id"] == AF.ROOT_ID:
            real_name = "/"
            name = real_name
        else:
            real_name = file["name"]
            name = file["name"]

            # while True:
            #     try:
            #         db_file = self.db.get_file(**{DF.PARENT_ID: parent_id, DF.NAME: name})
            #     except ValueError:
            #         pass

        file_size = file.get("size", 0)
        atime = google_datetime_to_timestamp(file["viewedByMeTime"]) if "viewedByMeTime" in file.keys() else 0
        ctime = google_datetime_to_timestamp(file["createdTime"])  # TODO ctime is not creation time
        mtime = google_datetime_to_timestamp(file["modifiedTime"])
        mime_type = file["mimeType"]
        target_id = file["shortcutDetails"]["targetId"] if "shortcutDetails" in file.keys() else None
        trashed = file["trashed"]
        md5 = file.get("md5Checksum", None)

        db_file = DatabaseFile.from_kwargs(**{
            DF.ID: id,
            DF.PARENT_ID: parent_id,
            DF.REAL_NAME: real_name,
            DF.NAME: name,
            DF.FILE_SIZE: file_size,
            DF.ATIME: atime,
            DF.CTIME: ctime,
            DF.MTIME: mtime,
            DF.MIME_TYPE: mime_type,
            DF.TARGET_ID: target_id,
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

    ################################################################
    # Listing
    # def recursive_listdir(self, path: str):
    #     LOGGER.debug(f"Recursively listing '{path}'")
    #     if path == "/":
    #         self._recursive_list_root()
    #     else:
    #         rowid, db_file = self.get_db_file(path=path)
    #         self._recursive_list_any(parent_id=db_file[DF.ID])

    def _recursive_list_root(self):
        db_file = self.drive2db(self.client.get_by_id(id=AF.ROOT_ID))
        self.db.new_file(db_file)

        q = f"'me' in owners and trashed={str(self.trashed).lower()}"
        drive_files = self.exec_query(q=q)

        # TODO files with invisible parents
        # drive_files = list(filter(lambda drive_file: "parents" in drive_file.keys(), drive_files))# No orphans allowed
        self.db.new_files([self.drive2db(drive_file) for drive_file in drive_files])

    def _recursive_list_any(self, parent_id: str):
        return 0
        """Asks every file and folder, adds all folders in the right order"""
        q = f"'me' in owners and '{parent_id}' in parents and trashed={str(self.trashed).lower()}"
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
            chunk_size = 50
            for i in range(0, len(parent_ids), chunk_size):
                ids_chunk = parent_ids[i:i + chunk_size]
                q = "'me' in owners" + \
                    " or ".join([f"'{id}' in parents" for id in ids_chunk]) + \
                    f" and trashed={str(self.trashed).lower()}"
                drive_files += self.exec_query(q=q)

    # def listdir(self, path: str):
    #     LOGGER.debug(f"Listing '{path}'")
    #     parent_id = AF.ROOT_ID if path == "/" else self.get_db_file(path=path)[DF.ID]
    #     try:
    #         LOGGER.debug(f"Updating files inside '{path}' - '{parent_id}'")
    #         q = f"'{parent_id}' in parents and trashed={str(self.trashed).lower()}"
    #         drive_files = self.exec_query(q=q)
    #         drive_files.append(self.client.get_by_id(id=parent_id))
    #
    #         db_files = [self.drive2db(drive_file) for drive_file in drive_files]
    #         self.db.new_files(db_files)
    #
    #     except ConnectionError as err:
    #         LOGGER.error(err)

    ################################################################
    # Cache handling
    def is_cached(self, db_file: DatabaseFile) -> bool:
        cache_file = self.cache_path.joinpath(db_file[DF.MD5])

        res = cache_file.exists()
        if res and db_file[DF.MIME_TYPE] not in AF.GOOGLE_APP_MIME_TYPES + [AF.FOLDER_MIME_TYPE, AF.LINK_MIME_TYPE]:
            res *= cache_file.stat().st_size == db_file[DF.FILE_SIZE]

        return res

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
    def download_loop(self, sleep_time: int = 0.5):
        LOGGER.info("Download queue started")

        while True:
            for djob in self.db.get_all_djobs():
                try:
                    rowid, db_file = self.db.get_file(**{DF.ID: djob[DF.ID], DF.TRASHED: self.trashed})
                except ValueError:  # No such file
                    self.db.delete_djob(djob[DF.ID])
                    continue

                if self.is_cached(db_file):  # Already cached
                    self.db.delete_djob(djob[DF.ID])
                    continue

                self.download_file(db_file)
                djob[DF.STATUS] = DF.COMPLETE
                self.db.new_djob(djob)

            time.sleep(sleep_time)

    def download_file(self, db_file: DatabaseFile):
        output_file = Path(self.cache_path, db_file[DF.MD5]).with_suffix(".dpart")
        output_file.unlink(missing_ok=True)

        while True:
            if db_file[DF.FILE_SIZE] == 0:
                file = output_file.open("wb")
                file.close()
                break

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

    def request_download(self, db_file: DatabaseFile) -> DatabaseDJob:
        djob = DatabaseDJob.from_kwargs(**{DF.ID: db_file[DF.ID], DF.STATUS: DF.WAITING})
        self.db.new_djob(djob)
        return djob

    def await_download(self, djob: DatabaseDJob):
        while True:
            try:
                if djob[DF.STATUS] == DF.COMPLETE:
                    continue
                elif djob[DF.STATUS] == DF.COMPLETE:
                    break
                elif djob[DF.STATUS] == DF.NETWORK_ERROR:
                    raise FUSEIOError

                rowid, djob = self.db.get_djob(**{DF.ID: djob[DF.ID]})
            finally:
                self.db.delete_djob(djob[DF.ID])

    ################################################################
    # FS ops
    _statfs: pyfuse3.StatvfsData = pyfuse3.StatvfsData()

    def init(self):
        LOGGER.info(f"Initiating filesystem")
        self.update_statfs()
        self._recursive_list_root()
        LOGGER.info(f"Filesystem initiated successfully")

    def update_statfs(self):
        drive_info = self.client.about()
        total_space = int(drive_info["storageQuota"]["limit"])
        used_space = int(drive_info["storageQuota"]["usageInDrive"])

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

    async def statfs(self, ctx) -> pyfuse3.StatvfsData:
        # LOGGER.debug(f"statfs")
        return self._statfs

    ################################################################
    # Permissions
    async def access(self, inode, mode, ctx) -> int:
        return True

    ################################################################
    # Main ops
    async def lookup(self, inode_p: int, name: bytes, ctx: pyfuse3.RequestContext):
        name = os.fsdecode(name)
        self.try2ignore(name)

        try:
            inode_p, db_file_p = self.db.get_file(**{ROWID: inode_p})
        except ValueError:
            raise FUSEFileNotFoundError(f"[lookup] (inode_p {inode_p}) does not exist")

        try:
            inode, db_file = self.db.get_file(**{DF.PARENT_ID: db_file_p[DF.ID], DF.NAME: name})
            return self.db2stat(inode, db_file)
        except ValueError:
            raise FUSEFileNotFoundError(f"[lookup] (inode_p {inode_p})/'{name}' does not exist")

    async def getattr(self, inode: int, ctx: pyfuse3.RequestContext) -> pyfuse3.EntryAttributes:
        try:
            inode, db_file = self.db.get_file(**{ROWID: inode})
            self.try2ignore(db_file[DF.NAME])
            return self.db2stat(inode, db_file)
        except ValueError:
            raise FUSEFileNotFoundError(f"[getattr] (inode {inode}) does not exist")

    async def symlink(self, inode_p: int, name: bytes, target_path: bytes, ctx: pyfuse3.RequestContext):
        # Check if target_path is absolute
        target_path = Path(os.fsdecode(target_path))
        if not target_path.is_absolute():
            raise FUSEFileNotFoundError(f"[symlink] target path '{target_path}' is not absolute")

        # Check if under mountpoint
        if self.mountpoint not in list(target_path.parents):
            raise FUSECrossDeviceLink(f"[symlink] invalid target path '{target_path}', cross-device link")
        target_path = Path("/") / target_path.relative_to(self.mountpoint)

        try:
            inode_p, db_file_p = self.db.get_file(**{ROWID: inode_p})
        except ValueError:
            raise FUSEFileNotFoundError(f"[symlink] (inode_p {inode_p}) does not exist")

        # Check existence and non existence
        name = os.fsdecode(name)
        try:
            rowid, db_file_target = self.get_by_path(target_path)
        except ValueError:
            raise FUSEFileNotFoundError(f"[symlink] target '{target_path}' does not exists")
        try:
            self.db.get_file(**{DF.PARENT_ID: db_file_p[DF.ID], DF.NAME: name})
            raise FUSEFileExistsError(f"[symlink] link (inode_p {inode_p})/'{name}' already exists")
        except ValueError:
            pass

        # Create shortcut
        LOGGER.info(f"[symlink] (inode_p {inode_p})/'{name}' -> '{target_path}'")
        drive_file = self.client.create_shortcut(parent_id=db_file_p[DF.ID],
                                                 name=name,
                                                 target_id=db_file_target[DF.ID])
        db_file = self.drive2db(drive_file)
        inode = self.db.new_file(db_file)
        return self.db2stat(inode, db_file)

    async def readlink(self, inode: int, ctx: pyfuse3.RequestContext) -> bytes:
        try:
            inode, db_file = self.db.get_file(**{ROWID: inode})
        except ValueError:
            raise FUSEFileNotFoundError(f"[readlink] (inode {inode}) does not exist")

        # Handle link target
        try:
            inode_t, db_file_target = self.db.get_file(**{DF.ID: db_file[DF.TARGET_ID]})
            target_path = self.resolve_path(db_file_target)
        except ValueError:  # Invalid link
            target_path = self.resolve_path(db_file)

        # Replace target_path's root with mountpoint
        target_path = self.mountpoint / target_path.relative_to(target_path.root)
        return os.fsencode(target_path)

    async def opendir(self, inode: int, ctx: pyfuse3.RequestContext):
        return inode

    async def readdir(self, inode: int, start_id: int, token: pyfuse3.ReaddirToken):
        try:
            inode, db_file = self.db.get_file(**{ROWID: inode})
        except ValueError:
            raise FUSEFileNotFoundError(f"[readdir] (inode {inode}) does not exist")

        LOGGER.info(f"[readdir] (inode {inode}) '{db_file[DF.NAME]}'")
        db_files = self.db.get_files(**{DF.PARENT_ID: db_file[DF.ID], DF.TRASHED: self.trashed})

        # List all entries and sort them by inode
        entries = []
        for inode, db_file in db_files:
            name = os.fsencode(db_file[DF.NAME])
            stat = self.db2stat(inode, db_file)
            entries.append((name, stat))
        entries = sorted(entries, key=lambda row: row[1].st_ino)

        # Take only new entries if start_id > 0, new entries have to be at the end
        for name, stat in list(entries)[start_id - 1 if start_id > 0 else start_id:]:
            if not pyfuse3.readdir_reply(token, name, stat, stat.st_ino):
                break

    async def rename(self, inode_p_old: int, name: bytes, inode_p_new: int, name_new: bytes, flags: int, ctx: pyfuse3.RequestContext):
        try:
            inode_p_old, db_file_p_old = self.db.get_file(**{ROWID: inode_p_old})
        except ValueError:
            raise FUSEFileNotFoundError(f"[rename] (inode_p_old {inode_p_old}) does not exist")

        try:
            inode_p_new, db_file_p_new = self.db.get_file(**{ROWID: inode_p_new})
        except ValueError:
            raise FUSEFileNotFoundError(f"[rename] (inode_p_new {inode_p_new}) does not exist")

        # Check existence and non existence
        name = os.fsdecode(name)
        name_new = os.fsdecode(name_new)
        try:
            inode_old, db_file_old = self.db.get_file(**{DF.PARENT_ID: db_file_p_old[DF.ID], DF.NAME: name})
        except ValueError:
            raise FUSEFileNotFoundError(f"[rename] (inode_p_old {inode_p_old})/'{name}' does not exist")
        try:
            self.db.get_file(**{DF.PARENT_ID: db_file_p_new[DF.ID], DF.NAME: name_new})
            raise FUSEFileExistsError(f"[rename] (inode_p_new {inode_p_new})/'{name_new}' already exist")
        except ValueError:
            pass

        LOGGER.info(f"[rename] (inode_p_old {inode_p_old})/'{name}' -> (inode_p_new {inode_p_new})/'{name_new}'")
        # Renaming
        if inode_p_old == inode_p_new:
            # Rename and update in db
            drive_file = self.client.rename_file(id=db_file_old[DF.ID], name=name_new)
            self.db.new_file(self.drive2db(drive_file))

        # Moving
        else:
            drive_file = self.client.move_file(file_id=db_file_old[DF.ID],
                                               old_parent_id=db_file_old[DF.PARENT_ID],
                                               new_parent_id=db_file_p_new[DF.ID])
            self.db.new_file(self.drive2db(drive_file))

    async def mkdir(self, inode_p: int, name: bytes, mode: int, ctx: pyfuse3.RequestContext) -> pyfuse3.EntryAttributes:
        try:
            inode_p, db_file_p = self.db.get_file(**{ROWID: inode_p})
            self.try2ignore(db_file_p[DF.NAME])
        except ValueError:
            raise FUSEFileNotFoundError(f"[mkdir] (inode_p {inode_p}) does not exist")

        name = os.fsdecode(name)
        try:
            inode, db_file = self.db.get_file(**{DF.PARENT_ID: db_file_p[DF.ID], DF.NAME: name})
            raise FUSEFileExistsError(f"[mkdir] (inode {inode})/'{db_file[DF.NAME]}' already exists")
        except ValueError:
            pass

        LOGGER.info(f"[mkdir] (inode_p {inode_p})/'{name}'")
        drive_file = self.client.create_folder(parent_id=db_file_p[DF.ID], name=name)
        db_file = self.drive2db(drive_file)
        inode = self.db.new_file(db_file)

        return self.db2stat(inode, db_file)

    def _remove(self, db_file: DatabaseFile):
        if db_file[DF.TRASHED]:
            self.client.untrash_file(id=db_file[DF.ID])
        else:
            self.client.trash_file(id=db_file[DF.ID])

    async def rmdir(self, inode_p: int, name: bytes, ctx: pyfuse3.RequestContext):
        try:
            inode_p, db_file_p = self.db.get_file(**{ROWID: inode_p})
        except ValueError:
            raise FUSEFileNotFoundError(f"[rmdir] (inode_p {inode_p}) does not exist")

        name = os.fsdecode(name)
        try:
            inode, db_file = self.db.get_file(**{DF.PARENT_ID: db_file_p[DF.ID], DF.NAME: name})
        except ValueError:
            raise FUSEFileNotFoundError(f"[rmdir] (inode_p {inode_p})/'{name}' does not exist")

        LOGGER.info(f"[rmdir] (inode {inode}) '{name}'")
        self._remove(db_file)
        # self.db.delete_file_children(id=id)
        self.db.delete_file(id=db_file[DF.ID])

    async def unlink(self, inode_p: int, name: bytes, ctx: pyfuse3.RequestContext):
        try:
            inode_p, db_file_p = self.db.get_file(**{ROWID: inode_p})
        except ValueError:
            raise FUSEFileNotFoundError(f"[unlink] (inode_p {inode_p}) does not exist")

        name = os.fsdecode(name)
        try:
            inode, db_file = self.db.get_file(**{DF.PARENT_ID: db_file_p[DF.ID], DF.NAME: name})
        except ValueError:
            raise FUSEFileNotFoundError(f"[unlink] (inode_p {inode_p})/'{name}' does not exist")

        LOGGER.info(f"[unlink] (inode {inode}) '{name}'")
        self._remove(db_file)
        self.db.delete_file(id=db_file[DF.ID])

    ################################################################
    # File ops
    async def create(self, parent_inode, name, mode, flags, ctx) -> int:
        raise FUSEError(errno.EROFS)
        return 0

    async def open(self, inode: int, flags: int, ctx: pyfuse3.RequestContext) -> pyfuse3.FileInfo:
        try:
            rowid, db_file = self.db.get_file(**{ROWID: inode})
            path = Path(db_file[DF.PATH])
        except ValueError:
            raise FUSEFileNotFoundError(f"open, inode ({inode}) does not exist")

        mode = flag2mode(flags)
        LOGGER.info(f"open '{path}' - '{mode}' ({flags})")

        # Download
        if "r" in mode:
            # Files
            if db_file[DF.MIME_TYPE] not in AF.GOOGLE_APP_MIME_TYPES:
                if not self.is_cached(db_file):
                    LOGGER.debug(f"open, '{path}' no cache")
                    # Queue and wait
                    djob = self.request_download(db_file)
                    self.await_download(djob)

                return self.get_file_info(fh=rowid)

            # Google Apps
            else:
                raise FUSEIOError

        raise FUSEIOError

    async def read(self, inode: int, offset: int, size: int) -> bytes:
        try:
            rowid, db_file = self.db.get_file(**{ROWID: inode})
        except ValueError:
            raise FUSEFileNotFoundError(f"read, inode ({inode}) does not exist")

        # Files
        if db_file[DF.MIME_TYPE] not in AF.GOOGLE_APP_MIME_TYPES:
            with Path(self.cache_path, db_file[DF.MD5]).open("rb") as file:
                file.seek(offset)
                return file.read(size)

        # Google Apps
        else:
            raise FUSEError(errno.EIO)

    async def write(self, inode: int, offset: int, buffer: bytes):
        raise FUSEError(errno.EIO)

    async def release(self, inode: int) -> int:
        try:
            rowid, db_file = self.db.get_file(**{ROWID: inode})
            LOGGER.debug(f"release '{db_file[DF.PATH]}'")
            return 0

        except ValueError:
            raise FUSEFileNotFoundError(f"release, inode ({inode}) does not exist")

    async def flush(self, fh) -> int:
        raise FUSEError(errno.EIO)
        pass

    async def fsync(self, fh, datasync) -> int:
        raise FUSEError(errno.EIO)
        return 0
