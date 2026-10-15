"""Domain errors raised by services and mapped to HTTP responses in main.py."""


class NotFound(Exception):
    def __init__(self, what: str) -> None:
        super().__init__(what)
        self.what = what


class DuplicateKey(Exception):
    def __init__(self, type_name: str, key: str) -> None:
        super().__init__(f"{type_name} with key {key!r} already exists")
        self.type_name = type_name
        self.key = key


class VersionConflict(Exception):
    def __init__(self, expected: int, current: int) -> None:
        super().__init__(f"version conflict: expected {expected}, current {current}")
        self.expected = expected
        self.current = current


class InvalidData(Exception):
    def __init__(self, message: str) -> None:
        super().__init__(message)
        self.message = message
