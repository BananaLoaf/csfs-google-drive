from typing import *
import os

from CloudStorageFileSystem.utils.database import Database, DatabaseItem, eval_kwargs, ROWID, lock, handle_exceptions
from .const import DF, AF


class DatabaseDriveFile(DatabaseItem):
    _columns = DF.DRIVE_FILES_COLUMNS


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

        # self.create_table(self.djobs_table, headers=DF.DJOBS_COLUMNS, reset=True)
        # self.create_index(self.djobs_table, DF.ID)

    ################################################################
    # DFiles
    @lock
    # @handle_exceptions
    def new_dfile(self, file: DatabaseDriveFile) -> Tuple[int, str]:
        # Add drive file
        cursor = self.conn.cursor()

        if file[DF.ID] == AF.ROOT_ID:
            file[DF.DIRNAME] = "/"
            file[DF.BASENAME] = ""
            file[DF.PATH] = "/"

        else:
            cursor.execute(f"SELECT rowid,* FROM '{self.drive_files_table}' WHERE {DF.ID}=?", (file[DF.PARENT_ID],))
            if item := cursor.fetchone():
                file_p = DatabaseDriveFile.from_list(item[1:])
                file_p.rowid = item[0]

                file[DF.DIRNAME] = file_p[DF.PATH]
                file[DF.BASENAME] = file[DF.NAME]
                file[DF.PATH] = os.path.join(file[DF.DIRNAME], file[DF.BASENAME])  # TODO filter doubles

            # Or
            else:
                pass  # What?

        query = f"INSERT OR REPLACE INTO '{self.drive_files_table}' " \
                f"({', '.join(DF.DRIVE_FILES_COLUMNS.keys())}) " \
                f"VALUES ({','.join('?' * len(DF.DRIVE_FILES_COLUMNS.keys()))})"
        cursor.execute(query, file.values)
        self.conn.commit()

        rowid = cursor.lastrowid
        return rowid, file[DF.PATH]

    @eval_kwargs(DatabaseDriveFile)
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
