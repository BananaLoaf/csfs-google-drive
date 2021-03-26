from typing import *
import os

from CloudStorageFileSystem.utils.database import Database, DatabaseItem, eval_kwargs, ROWID, lock, handle_exceptions
from .const import DF, AF


class DatabaseDriveFile(DatabaseItem):
    _columns = DF.DRIVE_FILES_COLUMNS


class DatabaseFile(DatabaseItem):
    _columns = DF.FILES_COLUMNS


class DatabaseRequest(DatabaseItem):
    _columns = DF.REQUEST_QUEUE_COLUMNS


# class DatabaseDJob(DatabaseItem):
#     _columns = DF.DJOBS_COLUMNS


class DriveDatabase(Database):
    drive_files_table = "drive_files"
    files_table = "files"
    bin_table = "bin"

    request_queue_table = "request_queue"
    # djobs_table = "djobs"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.create_table(self.drive_files_table, headers=DF.DRIVE_FILES_COLUMNS, reset=True)
        self.create_index(self.drive_files_table, DF.ID)
        self.create_index(self.drive_files_table, DF.PARENT_ID)
        self.create_index(self.drive_files_table, DF.NAME)

        self.create_table(self.files_table, headers=DF.FILES_COLUMNS, reset=True)
        self.create_table(self.bin_table, headers=DF.FILES_COLUMNS, reset=True)

        self.create_table(self.request_queue_table, headers=DF.REQUEST_QUEUE_COLUMNS)

        # self.create_table(self.djobs_table, headers=DF.DJOBS_COLUMNS, reset=True)
        # self.create_index(self.djobs_table, DF.ID)

    ################################################################
    # DFiles
    def new_dfile(self, dfile: DatabaseDriveFile):
        # Add drive file
        query = f"INSERT OR REPLACE INTO '{self.drive_files_table}' " \
                f"({', '.join(DF.DRIVE_FILES_COLUMNS.keys())}) " \
                f"VALUES ({','.join('?' * len(DF.DRIVE_FILES_COLUMNS.keys()))})"
        self._execute({query: dfile.values})

    def new_dfiles(self, dfiles: List[DatabaseDriveFile]):
        query = f"INSERT OR REPLACE INTO '{self.drive_files_table}' " \
                f"({', '.join(DF.DRIVE_FILES_COLUMNS.keys())}) " \
                f"VALUES ({', '.join('?' * len(DF.DRIVE_FILES_COLUMNS.keys()))})"
        values = [file.values for file in dfiles]
        self._executemany({query: values})

    # @eval_kwargs(DatabaseDriveFile)
    def get_dfile(self, **kwargs) -> Optional[DatabaseDriveFile]:
        query = f"SELECT rowid,* FROM '{self.drive_files_table}' WHERE {' AND '.join([f'{key}=?' for key in kwargs.keys()])}"
        values = list(kwargs.values())

        if item := self._execute_fetchone({query: values}):
            file = DatabaseDriveFile.from_list(item[1:])
            file.rowid = item[0]
            return file
        else:
            return item

    # @eval_kwargs(DatabaseFile)
    def get_dfiles(self, **kwargs) -> List[DatabaseDriveFile]:
        query = f"SELECT rowid,* FROM '{self.drive_files_table}' WHERE {' AND '.join([f'{key}=?' for key in kwargs.keys()])}"
        values = list(kwargs.values())
        items = self._execute_fetchall({query: values})

        files = []
        for item in items:
            file = DatabaseDriveFile.from_list(item[1:])
            file.rowid = item[0]
            files.append(file)

        return files

    ################################################################
    # Files
    @lock
    # @handle_exceptions
    def new_file_from_dfile(self, dfile: DatabaseDriveFile, bin: bool):
        cursor = self.conn.cursor()
        table_name = self.bin_table if bin else self.files_table

        if dfile[DF.ID] == AF.ROOT_ID:
            dirname = "/"
            basename = None
            path = "/"
        else:
            dirname_query = f"SELECT {DF.PATH} FROM '{table_name}' WHERE {DF.ID}=?"
            cursor.execute(dirname_query, (dfile[DF.PARENT_ID],))
            dirname = cursor.fetchone()[0]

            basename = dfile[DF.NAME]
            path = os.path.join(dirname, basename)

        is_dir = dfile[DF.MIME_TYPE] == AF.FOLDER_MIME_TYPE
        is_link = dfile[DF.MIME_TYPE] == AF.LINK_MIME_TYPE

        if dfile[DF.TARGET_ID] is not None:
            target_query = f"SELECT path FROM '{table_name}' WHERE {DF.ID}=?"
            cursor.execute(target_query, (dfile[DF.TARGET_ID],))
            target_path = cursor.fetchone()[0]
        else:
            target_path = None

        file_query = f"INSERT OR REPLACE INTO '{table_name}' " \
                     f"({', '.join(DF.FILES_COLUMNS.keys())}) " \
                     f"VALUES ({','.join('?' * len(DF.FILES_COLUMNS.keys()))})"
        file_values = (dfile[DF.ID], dfile[DF.PARENT_ID], dirname, basename, path,
                       dfile[DF.FILE_SIZE],
                       dfile[DF.ATIME], dfile[DF.MTIME], dfile[DF.CTIME],
                       is_dir, is_link, dfile[DF.TARGET_ID], target_path)

        cursor.execute(file_query, file_values)
        self.conn.commit()
        cursor.execute(f"SELECT last_insert_rowid() FROM '{table_name}'")

        return cursor.fetchone()[0], DatabaseFile.from_list(file_values)

    def new_file(self, file: DatabaseFile, bin: bool = False) -> int:
        # Add drive file
        table_name = self.bin_table if bin else self.files_table
        query = f"INSERT OR REPLACE INTO '{table_name}' " \
                f"({', '.join(DF.FILES_COLUMNS.keys())}) " \
                f"VALUES ({','.join('?' * len(DF.FILES_COLUMNS.keys()))})"
        return self._execute_fetchone({query: file.values, f"SELECT last_insert_rowid() FROM '{table_name}'": None})[0]

    # @eval_kwargs(DatabaseFile)
    def get_file(self, bin: bool = False, **kwargs) -> Optional[DatabaseFile]:
        query = f"SELECT rowid,* FROM '{self.bin_table if bin else self.files_table}' WHERE {' AND '.join([f'{key}=?' for key in kwargs.keys()])}"
        values = list(kwargs.values())

        if item := self._execute_fetchone({query: values}):
            file = DatabaseFile.from_list(item[1:])
            file.rowid = item[0]
            return file
        else:
            return item

    # @eval_kwargs(DatabaseFile)
    def get_files(self, bin: bool, **kwargs) -> List[DatabaseFile]:
        query = f"SELECT rowid,* FROM '{self.bin_table if bin else self.files_table}' WHERE {' AND '.join([f'{key}=?' for key in kwargs.keys()])}"
        values = list(kwargs.values())
        items = self._execute_fetchall({query: values})

        files = []
        for item in items:
            file = DatabaseFile.from_list(item[1:])
            file.rowid = item[0]
            files.append(file)

        return files

    def delete_file(self, id: str):
        query = f"DELETE FROM '{self.drive_files_table}' WHERE id='{id}'"
        self._execute({query: None})

    ################################################################
    # Queries
    def new_request(self, request: DatabaseRequest):
        query = f"INSERT OR REPLACE INTO '{self.request_queue_table}' " \
                f"({', '.join(DF.REQUEST_QUEUE_COLUMNS.keys())}) " \
                f"VALUES ({','.join('?' * len(DF.REQUEST_QUEUE_COLUMNS.keys()))})"
        self._execute({query: request.values})

    def delete_request(self, request: DatabaseRequest):
        query = f"DELETE FROM '{self.request_queue_table}' WHERE {DF.TYPE}=? AND {DF.PAYLOAD}=?"
        self._execute({query: request.values})

    def get_all_requests(self) -> List[DatabaseRequest]:
        q = f"SELECT * FROM '{self.request_queue_table}'"
        items = self._execute_fetchall({q: None})
        return [DatabaseRequest.from_list(row) for row in items]

    ################################################################
    # DJobs
    # def new_djob(self, djob: DatabaseDJob):
    #     query = f"INSERT OR REPLACE INTO '{self.djobs_table}' " \
    #             f"({', '.join(DF.DJOBS_COLUMNS.keys())}) " \
    #             f"VALUES ({', '.join('?' * len(DF.DJOBS_COLUMNS.keys()))})"
    #     headers = djob.headers
    #     self._execute(query, headers)
    #
    # @eval_kwargs(DatabaseDJob)
    # def get_djob(self, **kwargs) -> Tuple[int, DatabaseDJob]:
    #     query = f"SELECT rowid,* FROM '{self.djobs_table}' WHERE {' AND '.join([f'{key}=?' for key in kwargs.keys()])}"
    #     values = list(kwargs.values())
    #     item = self._execute_fetchone(query, values)
    #     if item is not None:
    #         return item[0], DatabaseDJob.from_list(item[1:])
    #     else:
    #         raise ValueError
    #
    # def get_all_djobs(self) -> List[DatabaseDJob]:
    #     query = f"SELECT * FROM '{self.djobs_table}'"
    #     items = self._execute_fetchall(query)
    #     return [DatabaseDJob.from_list(row) for row in items]
    #
    # def delete_djob(self, id: str):
    #     query = f"DELETE FROM '{self.djobs_table}' WHERE id='{id}'"
    #     self._execute(query)
