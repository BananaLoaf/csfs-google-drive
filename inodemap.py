from threading import Lock
from typing import Union

import pyfuse3


class InodeMap:
    def __init__(self):
        self._lock = Lock()
        self._map = {pyfuse3.ROOT_INODE: "/"}

    def __len__(self):
        """
        :return: Dict size
        """
        return len(self._map)

    def __getitem__(self, item: Union[int, str]):
        """
        :return: Path if item is inode, inode if item is path
        """
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

    def get_or_add(self, path: str):
        """
        :return: Inode for path, if inode does not exist, it is added
        """
        try:
            return self[path]
        except ValueError:
            return self.append(path)

    def _remap(self):
        """Fills in the gaps in inode mapping, e.g. (1, 2, 4, 5) -> (1, 2, 3, 4)"""
        self._map = dict(
            zip(
                range(1, len(self) + 1),
                self._map.values()
            )
        )

    def append(self, path: str) -> int:
        """
        Add path to inode map
        :return: Inode for given path
        """
        self._lock.acquire()
        try:
            return self._append(path)
        finally:
            self._lock.release()

    def _append(self, path: str):
        inode = len(self) + 1
        self._map[inode] = path
        return inode

    def pop(self, inode: int) -> str:
        """
        Remove inode from inode map, remap it
        :param inode: Inode to remove
        :return: Path of removed inode
        """
        # Skip root
        if inode == pyfuse3.ROOT_INODE:
            return self[inode]

        self._lock.acquire()
        try:
            return self._pop(inode)
        finally:
            self._lock.release()

    def _pop(self, inode: int) -> str:
        try:
            return self._map[inode]
        finally:
            del self._map[inode]
            self._remap()

    def update(self, path: str):
        """Pop path from inode map and append back"""
        self._lock.acquire()
        try:
            self._pop(self[path])
            self._append(path)
        finally:
            self._lock.release()
