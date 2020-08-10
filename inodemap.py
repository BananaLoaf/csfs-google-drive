from threading import Lock
from typing import Union

import pyfuse3


class InodeMap:
    def __init__(self):
        self._lock = Lock()
        self._map = {pyfuse3.ROOT_INODE: "/"}

    def __len__(self):
        return len(self._map)

    def __getitem__(self, item: Union[int, str]):
        self._lock.acquire()
        try:
            # inode2path
            if isinstance(item, int):
                return self._map[item]

            # path2inode
            elif isinstance(item, str):
                paths = list(self._map.values())
                i = paths.index(item)
                return list(self._map.keys())[i]

            else:
                raise NotImplementedError

        finally:
            self._lock.release()

    def path2inode(self, path: str):
        try:
            return self[path]
        except ValueError:
            return self.append(path)

    def _remap(self):
        self._map = dict(
            zip(
                range(1, len(self) + 1),
                self._map.values()
            )
        )

    def append(self, path: str) -> int:
        self._lock.acquire()
        try:
            return self._append(path)
        finally:
            self._lock.release()

    def _append(self, path: str):
        inode = len(self) + 1
        self._map[inode] = path
        return inode

    def pop(self, path: str):
        self._lock.acquire()
        try:
            self._pop(path)
        finally:
            self._lock.release()

    def _pop(self, path: str):
        del self._map[path]
        self._remap()

    def update(self, path: str):
        self._lock.acquire()
        try:
            self._pop(path)
            self._append(path)
        finally:
            self._lock.release()
