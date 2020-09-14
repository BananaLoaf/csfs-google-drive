from typing import List, Optional, Tuple

from CloudStorageFileSystem.utils.database import Database, DatabaseItem, eval_kwargs
from .const import DF


class DatabaseFile(DatabaseItem):
    headers = DF.FILES_HEADERS


class DatabaseDJob(DatabaseItem):
    headers = DF.DJOBS_HEADERS


class DriveDatabase(Database):
    files_table = "files"
    djobs_table = "djobs"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.create_table(self.files_table, headers=DF.FILES_HEADERS, reset=True)
        self._execute(f"CREATE INDEX IF NOT EXISTS {DF.ID}_index ON '{self.files_table}' ({DF.ID})")
        self._execute(f"CREATE INDEX IF NOT EXISTS {DF.PARENT_ID}_index ON '{self.files_table}' ({DF.PARENT_ID})")
        self._execute(f"CREATE INDEX IF NOT EXISTS {DF.PATH}_index ON '{self.files_table}' ({DF.PATH})")

        self.create_table(self.djobs_table, headers=DF.DJOBS_HEADERS, reset=True)
        self._execute(f"CREATE INDEX IF NOT EXISTS {DF.ID}_index ON '{self.djobs_table}' ({DF.ID})")

    ################################################################
    # Files
    def new_file(self, file: DatabaseFile):
        query = f"INSERT OR REPLACE INTO '{self.files_table}' " \
                f"({', '.join(DF.FILES_HEADERS.keys())}) " \
                f"VALUES ({', '.join('?' * len(DF.FILES_HEADERS.keys()))})"
        values = file.tuple
        self._execute(query, values)

    def new_files(self, files: List[DatabaseFile]):
        query = f"INSERT OR REPLACE INTO '{self.files_table}' " \
                f"({', '.join(DF.FILES_HEADERS.keys())}) " \
                f"VALUES ({', '.join('?' * len(DF.FILES_HEADERS.keys()))})"
        values = [file.tuple for file in files]
        self._executemany(query, values)

    @eval_kwargs(DatabaseFile)
    def get_file(self, **kwargs) -> Tuple[int, DatabaseFile]:
        query = f"SELECT rowid,* FROM '{self.files_table}' WHERE {' AND '.join([f'{key}=?' for key in kwargs.keys()])}"
        values = list(kwargs.values())
        item = self._execute_fetchone(query, values)
        if item is not None:
            return item[0], DatabaseFile.from_list(item[1:])
        else:
            raise ValueError

    @eval_kwargs(DatabaseFile)
    def get_files(self, **kwargs) -> List[Tuple[int, DatabaseFile]]:
        query = f"SELECT rowid,* FROM '{self.files_table}' WHERE {' AND '.join([f'{key}=?' for key in kwargs.keys()])}"
        values = list(kwargs.values())
        items = self._execute_fetchall(query, values)
        return [(item[0], DatabaseFile.from_list(item[1:])) for item in items]

    def delete_file_children(self, id: str):
        ids = [id]
        while ids:
            db_files = self.get_files(**{DF.PARENT_ID: ids[0]})
            ids.pop(0)

            for rowid, db_file in db_files:
                ids.append(db_file[DF.ID])
                self.delete_file(id=db_file[DF.ID])

    def delete_file(self, id: str):
        query = f"DELETE FROM '{self.files_table}' WHERE id='{id}'"
        self._execute(query)

    ################################################################
    # DJobs
    def new_djob(self, djob: DatabaseDJob):
        query = f"INSERT OR REPLACE INTO '{self.djobs_table}' " \
                f"({', '.join(DF.DJOBS_HEADERS.keys())}) " \
                f"VALUES ({', '.join('?' * len(DF.DJOBS_HEADERS.keys()))})"
        values = djob.tuple
        self._execute(query, values)

    @eval_kwargs(DatabaseDJob)
    def get_djob(self, **kwargs) -> Tuple[int, DatabaseDJob]:
        query = f"SELECT rowid,* FROM '{self.djobs_table}' WHERE {' AND '.join([f'{key}=?' for key in kwargs.keys()])}"
        values = list(kwargs.values())
        item = self._execute_fetchone(query, values)
        if item is not None:
            return item[0], DatabaseDJob.from_list(item[1:])
        else:
            raise ValueError

    def get_all_djobs(self) -> List[DatabaseDJob]:
        query = f"SELECT * FROM '{self.djobs_table}'"
        items = self._execute_fetchall(query)
        return [DatabaseDJob.from_list(row) for row in items]

    def delete_djob(self, id: str):
        query = f"DELETE FROM '{self.djobs_table}' WHERE id='{id}'"
        self._execute(query)
