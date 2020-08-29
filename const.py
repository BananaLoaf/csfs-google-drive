class CF:  # Config Fields
    MOUNT_SECTION = "MOUNT"
    MOUNTPOINT = "MOUNTPOINT"
    TRASH = "TRASH"
    GOOGLE_APP_MODE = "GOOGLE_APP_MODE"


class FF:  # File System Fields
    IGNORED_FILES = [".Trash", ".Trash-1000", "BDMV", ".xdg-volume-info", "autorun.inf", ".hidden", ".comments", ".directory"]

    DESKTOP: str = "DESKTOP"
    CONVERT: str = "CONVERT"
    IGNORE: str = "IGNORE"
    GOOGLE_APP_MODES: str = [DESKTOP, CONVERT, IGNORE]


class DF:  # Database Fields
    ID = "id"
    PARENT_ID = "parent_id"
    PATH = "path"
    FILE_SIZE = "file_size"
    ATIME = "atime"
    CTIME = "ctime"
    MTIME = "mtime"
    MIME_TYPE = "mime_type"
    TRASHED = "trashed"
    MD5 = "md5"
    FILES_HEADERS = {ID: "TEXT NOT NULL UNIQUE",
                     PARENT_ID: "TEXT",
                     PATH: "TEXT NOT NULL",
                     FILE_SIZE: "INTEGER NOT NULL",
                     ATIME: "INTEGER NOT NULL",
                     CTIME: "INTEGER NOT NULL",
                     MTIME: "INTEGER NOT NULL",
                     MIME_TYPE: "TEXT NOT NULL",
                     TRASHED: "BOOLEAN NOT NULL",
                     MD5: "TEXT"}


class AF:  # Drive API Fields
    DEFAULT_FIELDS = ("id", "parents", "name", "size", "viewedByMeTime", "createdTime", "modifiedTime", "mimeType", "trashed", "md5Checksum")
    ROOT_ID = "root"
    FOLDER_MIME_TYPE = "application/vnd.google-apps.folder"

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
