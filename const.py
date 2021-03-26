class CF:  # Config Fields
    MOUNT_SECTION = "MOUNT"
    MOUNTPOINT = "MOUNTPOINT"
    TRASH = "TRASH"
    GOOGLE_APP_MODE = "GOOGLE_APP_MODE"


class FF:  # File System Fields
    IGNORED_FILES = [".Trash", ".Trash-1000", "BDMV", ".xdg-volume-info", "autorun.inf", ".hidden", ".comments", ".directory"]

    WEB = "WEB"
    CONVERT = "CONVERT"
    IGNORE = "IGNORE"
    GOOGLE_APP_MODES = [WEB, CONVERT, IGNORE]


class DF:  # Database Fields
    ID = "id"
    PARENT_ID = "parent_id"
    NAME = "name"
    FILE_SIZE = "file_size"
    ATIME = "atime"
    CTIME = "ctime"
    MTIME = "mtime"
    MIME_TYPE = "mime_type"
    TARGET_ID = "target_id"
    TRASHED = "trashed"
    MD5 = "md5"
    DRIVE_FILES_COLUMNS = {ID: "TEXT NOT NULL UNIQUE",
                           PARENT_ID: "TEXT",
                           NAME: "TEXT NOT NULL",
                           FILE_SIZE: "INTEGER NOT NULL",
                           MD5: "TEXT",
                           MIME_TYPE: "TEXT NOT NULL",
                           TARGET_ID: "TEXT",
                           ATIME: "INTEGER NOT NULL",
                           CTIME: "INTEGER NOT NULL",
                           MTIME: "INTEGER NOT NULL",
                           TRASHED: "BOOLEAN NOT NULL"}

    DIRNAME = "dirname"
    BASENAME = "basename"
    PATH = "path"
    IS_DIR = "is_dir"
    IS_LINK = "is_link"
    TARGET_PATH = "target_path"
    FILES_COLUMNS = {ID: "TEXT",
                     PARENT_ID: "TEXT",
                     DIRNAME: "TEXT",
                     BASENAME: "TEXT",
                     PATH: "TEXT UNIQUE",
                     FILE_SIZE: "INTEGER",
                     ATIME: "INTEGER NOT NULL",
                     CTIME: "INTEGER NOT NULL",
                     MTIME: "INTEGER NOT NULL",
                     IS_DIR: "BOOLEAN NOT NULL",
                     IS_LINK: "BOOLEAN NOT NULL",
                     TARGET_ID: "TEXT",
                     TARGET_PATH: "TEXT"}

    TYPE = "type"
    PAYLOAD = "payload"
    REQUEST_QUEUE_COLUMNS = {TYPE: "TEXT",
                             PAYLOAD: "TEXT"}

    # STATUS = "status"
    # DJOBS_COLUMNS = {ID: "TEXT NOT NULL UNIQUE",
    #                  # MIME_TYPE: "TEXT NOT NULL",
    #                  # MD5: "TEXT",
    #                  STATUS: "INTEGER NOT NULL"}
    #
    # # DJob Statuses
    # WAITING = 0
    # COMPLETE = 1
    # NETWORK_ERROR = 2


class AF:  # Drive API Fields
    DEFAULT_FIELDS = ("id", "parents", "name", "size", "viewedByMeTime", "createdTime", "modifiedTime", "mimeType", "trashed", "md5Checksum", "shortcutDetails")
    ROOT_ID = "root"
    FOLDER_MIME_TYPE = "application/vnd.google-apps.folder"
    LINK_MIME_TYPE = "application/vnd.google-apps.shortcut"

    GOOGLE_APP_MIME_TYPES = [
        "application/vnd.google-apps.document",
        "application/vnd.google-apps.spreadsheet",
        "application/vnd.google-apps.drawing",
        "application/vnd.google-apps.presentation",
        "application/vnd.google-apps.form",
        "application/vnd.google-apps.fusiontable",
        "application/vnd.google-apps.map",
        "application/vnd.google-apps.script",
        "application/vnd.google-apps.site"
    ]

    DEFAULT_ICON = "google"
    GOOGLE_APP_DESKTOP = {
        "application/vnd.google-apps.document": [".gdoc", "x-office-document"],
        "application/vnd.google-apps.spreadsheet": [".gsheet", "x-office-spreadsheet"],
        "application/vnd.google-apps.drawing": ["gdraw", "image-x-generic"],
        "application/vnd.google-apps.presentation": [".gpres", "x-office-presentation"],
        "application/vnd.google-apps.form": [".gfrom", DEFAULT_ICON],
        "application/vnd.google-apps.fusiontable": [".gfustab", DEFAULT_ICON],
        "application/vnd.google-apps.map": [".gmap", DEFAULT_ICON],
        "application/vnd.google-apps.script": [".gscript", DEFAULT_ICON],
        "application/vnd.google-apps.site": [".gsite", DEFAULT_ICON]
    }

    # These are the only apps that can be converted
    GOOGLE_APP_CONVERT = {
        "application/vnd.google-apps.document": ["application/pdf", ".pdf"],
        "application/vnd.google-apps.spreadsheet": ["application/pdf", ".pdf"],
        "application/vnd.google-apps.drawing": ["image/png", ".png"],
        "application/vnd.google-apps.presentation": ["application/pdf", ".pdf"],
    }
