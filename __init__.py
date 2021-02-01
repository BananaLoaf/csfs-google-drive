import json
from typing import Tuple, List
from threading import Thread

from pathlib import Path
import keyring
from keyring import errors
import pyfuse3

from CloudStorageFileSystem.utils.profile import Profile, ThreadHandler
from CloudStorageFileSystem.utils.exceptions import *
from .client import DriveClient
from .database import DriveDatabase
from .filesystem import DriveFileSystem
from .const import CF, FF, AF


class GoogleDriveProfile(Profile):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.client = DriveClient(client_id="831763421443-sjd4r53rp7a8ifbfsk6tpk4v50jpp467.apps.googleusercontent.com",
                                  client_secret="giRscT5FI4hmmdsVtV8BTzAe")  # TODO new ones

    ################################################################
    @property
    def service_name(self) -> str:
        return "google-drive"

    @property
    def service_label(self) -> str:
        return "Gooogle Drive"

    @property
    def version(self) -> str:
        return "1.0"

    ################################################################
    @property
    def schema(self) -> dict:
        return {
            CF.MOUNT_SECTION: {
                CF.MOUNTPOINT: "str()",
                CF.TRASH: "bool()",
                CF.GOOGLE_APP_MODE: f"enum({', '.join(FF.GOOGLE_APP_MODES)})"
            }
        }

    @property
    def default_config(self) -> dict:
        return {
            CF.MOUNT_SECTION: {
                CF.MOUNTPOINT: str(Path.home().joinpath("Google Drive")),
                CF.TRASH: False,
                CF.GOOGLE_APP_MODE: FF.WEB
            }
        }

    ################################################################
    def _create(self):
        credentials = self.client.auth()
        keyring.set_password(self.service_name, self.profile_name, credentials)

    def _remove(self):
        try:
            keyring.delete_password(self.service_name, self.profile_name)
        except keyring.errors.PasswordDeleteError:
            pass

    def _start(self) -> Tuple[pyfuse3.Operations, Path, List[ThreadHandler]]:
        # Load credentials
        credentials = keyring.get_password(self.service_name, self.profile_name)
        if credentials is not None:
            res = self.client.load_credentials(json.loads(credentials))
            if not res:
                raise ProfileStartingError("Error loading credentials, invalid profile")
        else:
            raise ProfileStartingError("No credentials found, invalid profile")

        self.client.update_root_id()
        db = DriveDatabase(self.profile_path.joinpath("data.db"))
        ops = DriveFileSystem(db=db,
                              client=self.client,
                              trash=self.config[CF.MOUNT_SECTION][CF.TRASH],
                              mountpoint=Path(self.config[CF.MOUNT_SECTION][CF.MOUNTPOINT]),
                              cache_path=self.cache_path)

        mountpoint = Path(self.config[CF.MOUNT_SECTION][CF.MOUNTPOINT])

        ths = [
            ThreadHandler(
                t=Thread(target=lambda: ops.download_loop()),
                join=False)
        ]

        return ops, mountpoint, ths
