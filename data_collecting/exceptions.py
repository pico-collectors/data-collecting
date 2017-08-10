class UnrecoverableError(Exception):
    """
    Raised to indicate that a serious error occurred and that the application
    can not recover from it. This error should be handled by exiting the
    application immediately.
    """
    pass
