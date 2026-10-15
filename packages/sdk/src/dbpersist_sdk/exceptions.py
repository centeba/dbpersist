class DBPersistError(Exception):
    """Base class for SDK errors."""

    def __init__(
        self, message: str, status: int | None = None, payload: dict | None = None
    ) -> None:
        super().__init__(message)
        self.status = status
        self.payload = payload or {}


class Unauthorized(DBPersistError):
    pass


class NotFound(DBPersistError):
    pass


class DuplicateKey(DBPersistError):
    pass


class VersionConflict(DBPersistError):
    @property
    def current_version(self) -> int | None:
        v = self.payload.get("current_version")
        return int(v) if v is not None else None


class InvalidData(DBPersistError):
    pass


class DeliveryError(DBPersistError):
    """Raised to Callbacks.on_error when a webhook cannot be processed."""
