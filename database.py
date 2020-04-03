from typing import List, Optional

from CloudStorageFileSystem.utils.database import Database, DatabaseItem, eval_kwargs
from .const import DF


class DatabaseFile(DatabaseItem):
    _headers = DF.FILES_HEADERS


class DriveDatabase(Database):
    files_table = "files"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.create_table(self.files_table, headers=DF.FILES_HEADERS, reset=True)

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

    @eval_kwargs(DF.FILES_HEADERS)
    def get_file(self, **kwargs) -> Optional[DatabaseFile]:
        query = f"SELECT * FROM '{self.files_table}' WHERE {' AND '.join([f'{key}=?' for key in kwargs.keys()])}"
        values = list(kwargs.values())
        item = self._execute_fetchone(query, values)
        return DatabaseFile.from_list(item) if item else None

    @eval_kwargs(DF.FILES_HEADERS)
    def get_files(self, **kwargs) -> Optional[List[DatabaseFile]]:
        query = f"SELECT * FROM '{self.files_table}' WHERE {' AND '.join([f'{key}=?' for key in kwargs.keys()])}"
        values = list(kwargs.values())
        items = self._execute_fetchall(query, values)
        return [DatabaseFile.from_list(row) for row in items]

    # def delete_file_children(self, id: str):
    #     ids = [id]
    #     while ids:
    #         db_files = self.get_files(**{DF.PARENT_ID: ids[0]})
    #         ids.pop(0)
    #
    #         for db_file in db_files:
    #             ids.append(db_file[DF.ID])
    #             self.delete_file(id=db_file[DF.ID])

    def delete_file(self, id: str):
        query = f"DELETE FROM '{self.files_table}' WHERE id='{id}'"
        self._execute(query)
