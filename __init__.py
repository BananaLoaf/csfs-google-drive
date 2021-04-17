import json
from typing import Tuple, List
from threading import Thread, Event

from pathlib import Path
import pyfuse3
import oauthlib.oauth2

from CloudStorageFileSystem.utils.profile import Profile, ThreadHandler
from CloudStorageFileSystem.utils.exceptions import *
from .client import DriveClient
from .database import DriveDatabase
from .filesystem import DriveFileSystem
from .const import CF, FF, AF


class GoogleDriveProfile(Profile):
    service_name = "google-drive"
    service_label = "Gooogle Drive"
    version = "1.0"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.client = DriveClient(client_id="831763421443-sjd4r53rp7a8ifbfsk6tpk4v50jpp467.apps.googleusercontent.com",
                                  client_secret="giRscT5FI4hmmdsVtV8BTzAe")  # TODO new ones

    ################################################################
    @property
    def schema(self) -> dict:
        return {
            CF.MOUNT_SECTION: {
                CF.MOUNTPOINT: "str()",
                CF.TRASH: "bool()",
                CF.GOOGLE_APP_MODE: "enum('{}', '{}', '{}')".format(*FF.GOOGLE_APP_MODES)
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
        try:
            credentials = self.client.auth()
            with self.profile_path.joinpath("credentials.json").open("w") as file:
                file.write(credentials)

        except oauthlib.oauth2.rfc6749.errors.AccessDeniedError:
            raise ProfileCreationError("Access denied")

    def _remove(self):
        pass

    def _start(self, stop_event: Event) -> Tuple[pyfuse3.Operations, Path, List[ThreadHandler]]:
        # Load credentials
        with self.profile_path.joinpath("credentials.json").open("r") as file:
            credentials = file.read()
        if credentials is not None:
            res = self.client.load_credentials(json.loads(credentials))
            if not res:
                raise ProfileStartingError("Error loading credentials, invalid profile")
        else:
            raise ProfileStartingError("No credentials found, invalid profile")

        AF.ROOT_ID = self.client.get_root_id()
        db = DriveDatabase(self.profile_path.joinpath("data.db"))
        ops = DriveFileSystem(db=db,
                              client=self.client,
                              bin=self.config[CF.MOUNT_SECTION][CF.TRASH],
                              mountpoint=Path(self.config[CF.MOUNT_SECTION][CF.MOUNTPOINT]),
                              cache_path=self.cache_path,
                              stop_event=stop_event)

        mountpoint = Path(self.config[CF.MOUNT_SECTION][CF.MOUNTPOINT])

        ths = [
            # ThreadHandler(
            #     t=Thread(target=lambda: ops.download_loop()),
            #     join=False),
            ThreadHandler(
                t=Thread(target=lambda: ops.update_statfs()),
                join=False),
        ]

        return ops, mountpoint, ths
