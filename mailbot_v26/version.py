"""Single source of truth for MailBot app version."""

__version__ = "27.2.0"
BUILD_METADATA = ""


def get_version() -> str:
    return __version__


__all__ = ["__version__", "BUILD_METADATA", "get_version"]
