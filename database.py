from typing import *
import os

from CloudStorageFileSystem.utils.database import Database, DatabaseItem, eval_kwargs, ROWID, lock, handle_exceptions
from .const import DF, AF


class DatabaseDriveFile(DatabaseItem):
    _columns = DF.DRIVE_FILES_COLUMNS


class DatabaseFile(DatabaseItem):
    _columns = DF.FILES_COLUMNS


# class DatabaseDJob(DatabaseItem):
#     _columns = DF.DJOBS_COLUMNS


class DriveDatabase(Database):
    drive_files_table = "drive_files"
    files_table = "files"
    bin_table = "bin"
    # djobs_table = "djobs"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.create_table(self.drive_files_table, headers=DF.DRIVE_FILES_COLUMNS, reset=True)
        self.create_index(self.drive_files_table, DF.ID)
        self.create_index(self.drive_files_table, DF.PARENT_ID)
        self.create_index(self.drive_files_table, DF.NAME)

        self.create_table(self.files_table, headers=DF.FILES_COLUMNS, reset=True)
        self.create_table(self.bin_table, headers=DF.FILES_COLUMNS, reset=True)

        # self.create_table(self.djobs_table, headers=DF.DJOBS_COLUMNS, reset=True)
        # self.create_index(self.djobs_table, DF.ID)

    ################################################################
    # Files
    @lock
    @handle_exceptions
    def new_drive_file(self, drive_file: DatabaseDriveFile) -> int:
        cursor = self.conn.cursor()

        # Add drive file
        drive_file_query = f"INSERT OR REPLACE INTO '{self.drive_files_table}' " \
                           f"({', '.join(DF.DRIVE_FILES_COLUMNS.keys())}) " \
                           f"VALUES ({','.join('?' * len(DF.DRIVE_FILES_COLUMNS.keys()))})"
        drive_file_values = drive_file.values
        cursor.execute(drive_file_query, drive_file_values)

        # Add file
        if drive_file[DF.ID] == AF.ROOT_ID:
            self._new_file(cursor, drive_file, self.files_table)
            self._new_file(cursor, drive_file, self.bin_table)
            self.conn.commit()
            return 1

        else:
            self._new_file(cursor, drive_file, self.bin_table if drive_file[DF.TRASHED] else self.files_table)
            self.conn.commit()
            cursor.execute(f"SELECT last_insert_rowid() FROM '{self.files_table}'")
            return cursor.fetchone()[0]

    def _new_file(self, cursor, drive_file: DatabaseDriveFile, table_name: str):
        if drive_file[DF.NAME] == "/":
            dirname = "/"
            basename = None
            path = "/"
        else:
            dirname_query = f"SELECT {DF.PATH} FROM '{table_name}' WHERE {DF.ID}=?"
            cursor.execute(dirname_query, (drive_file[DF.PARENT_ID],))
            dirname = cursor.fetchone()[0]

            basename = drive_file[DF.NAME]
            path = os.path.join(dirname, basename)

        is_dir = drive_file[DF.MIME_TYPE] == AF.FOLDER_MIME_TYPE
        is_link = drive_file[DF.MIME_TYPE] == AF.LINK_MIME_TYPE
        is_file = not is_dir and not is_link

        file_query = f"INSERT OR REPLACE INTO '{table_name}' " \
                     f"({', '.join(DF.FILES_COLUMNS.keys())}) " \
                     f"VALUES ({','.join('?' * len(DF.FILES_COLUMNS.keys()))})"
        file_values = (drive_file[DF.ID], drive_file[DF.PARENT_ID], dirname, basename, path,
                       drive_file[DF.FILE_SIZE],
                       drive_file[DF.ATIME], drive_file[DF.MTIME], drive_file[DF.CTIME],
                       is_dir, is_link, is_file, drive_file[DF.TARGET_ID])

        cursor.execute(file_query, file_values)

    # def new_files(self, files: List[DatabaseDriveFile]):
    #     query = f"INSERT OR REPLACE INTO '{self.drive_files_table}' " \
    #             f"({', '.join(DF.DRIVE_FILES_COLUMNS.keys())}) " \
    #             f"VALUES ({', '.join('?' * len(DF.DRIVE_FILES_COLUMNS.keys()))})"
    #     values = [file.values for file in files]
    #     self._executemany(query, values)

    # @eval_kwargs(DatabaseFile)
    def get_file(self, bin: bool, **kwargs) -> Tuple[int, DatabaseFile]:
        query = f"SELECT rowid,* FROM '{self.bin_table if bin else self.files_table}' WHERE {' AND '.join([f'{key}=?' for key in kwargs.keys()])}"
        values = list(kwargs.values())
        item = self._execute_fetchone({query: values})
        if item is not None:
            return item[0], DatabaseFile.from_list(item[1:])
        else:
            raise ValueError

    # @eval_kwargs(DatabaseDriveFile)
    def get_drive_file(self, **kwargs) -> Tuple[int, DatabaseDriveFile]:
        query = f"SELECT rowid,* FROM '{self.drive_files_table}' WHERE {' AND '.join([f'{key}=?' for key in kwargs.keys()])}"
        values = list(kwargs.values())
        item = self._execute_fetchone({query: values})
        if item is not None:
            return item[0], DatabaseDriveFile.from_list(item[1:])
        else:
            raise ValueError

    # @eval_kwargs(DatabaseFile)
    def get_files(self, bin: bool, **kwargs) -> List[Tuple[int, DatabaseFile]]:
        query = f"SELECT rowid,* FROM '{self.bin_table if bin else self.files_table}' WHERE {' AND '.join([f'{key}=?' for key in kwargs.keys()])}"
        values = list(kwargs.values())
        items = self._execute_fetchall({query: values})
        return [(item[0], DatabaseFile.from_list(item[1:])) for item in items]

    # def delete_file_children(self, id: str):
    #     ids = [id]
    #     while ids:
    #         db_files = self.get_files(**{DF.PARENT_ID: ids[0]})
    #         ids.pop(0)
    #
    #         for rowid, db_file in db_files:
    #             ids.append(db_file[DF.ID])
    #             self.delete_file(id=db_file[DF.ID])

    def delete_file(self, id: str):
        query = f"DELETE FROM '{self.drive_files_table}' WHERE id='{id}'"
        self._execute(query)

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
