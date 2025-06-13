"""Custom exceptions for the Control Freak eDIDIO client."""


class EDIDIOConnectionError(Exception):
    """Base exception for eDIDIO connection issues."""


class EDIDIOCommunicationError(EDIDIOConnectionError):
    """Exception for errors during sending/receiving data."""


class EDIDIOTimeoutError(EDIDIOCommunicationError):
    """Exception for operation timeouts."""


class EDIDIOInvalidMessageError(EDIDIOCommunicationError):
    """Exception for receiving an invalid or malformed message."""
