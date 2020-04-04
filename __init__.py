import json
from typing import Tuple, List

from pathlib import Path
import keyring

from CloudStorageFileSystem.utils.servicesupervisor import ServiceSupervisor, ThreadHandler
from CloudStorageFileSystem.utils.operations import CustomOperations
from CloudStorageFileSystem.utils.exceptions import *
from .client import DriveClient
from .database import DriveDatabase
from .filesystem import DriveFileSystem
from .const import CF, FF, AF


class GoogleDriveSS(ServiceSupervisor):
    SERVICE_NAME: str = "google-drive"
    SERVICE_LABEL: str = "Gooogle Drive"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.client = DriveClient(client_id="831763421443-sjd4r53rp7a8ifbfsk6tpk4v50jpp467.apps.googleusercontent.com",
                                  client_secret="giRscT5FI4hmmdsVtV8BTzAe")  # TODO new ones

    @property
    def schema(self) -> dict:
        return {
            "type": dict,
            "required": [
                CF.MOUNT_SECTION,
            ],
            "properties": {
                CF.MOUNT_SECTION: {
                    "type": dict,
                    "required": [
                        CF.MOUNTPOINT,
                        CF.TRASH,
                        CF.GOOGLE_APP_MODE
                    ],
                    "properties": {
                        CF.MOUNTPOINT: {"type": str},
                        CF.TRASH: {"type": bool},
                        CF.GOOGLE_APP_MODE: {"type": str, "enum": FF.GOOGLE_APP_MODES}
                    }
                }
            }
        }

    @property
    def default_config(self) -> dict:
        return {
            CF.MOUNT_SECTION: {
                CF.MOUNTPOINT: str(Path.home().joinpath("GoogleDrive")),
                CF.TRASH: False,
                CF.GOOGLE_APP_MODE: FF.DESKTOP
            }
        }

    def _create_profile(self):
        credentials = self.client.auth()
        keyring.set_password(self.SERVICE_NAME, self.profile_name, credentials)

    def _start(self) -> Tuple[CustomOperations, Path, List[ThreadHandler]]:
        # Load credentials
        credentials = keyring.get_password(self.SERVICE_NAME, self.profile_name)
        if credentials is not None:
            res = self.client.load_credentials(json.loads(credentials))
            if not res:
                raise ProfileStartingError("Error loading credentials, invalid profile")
        else:
            raise ProfileStartingError("No credentials found, invalid profile")

        self.client.update_root_id()
        db = DriveDatabase(self.profile_path.joinpath("data.db"))

        fs = DriveFileSystem(db=db, client=self.client)
        mountpoint = Path(self.config[CF.MOUNT_SECTION][CF.MOUNTPOINT])

        return fs, mountpoint, []
