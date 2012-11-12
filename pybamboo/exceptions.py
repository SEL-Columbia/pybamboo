class PyBambooException(Exception):
    """
    This is the base class for all pybamboo-related
    exceptions.
    """
    pass


class BambooError(PyBambooException):
    """
    Receiving this error means that bamboo returned
    a failing status code (something other than those
    in connection.OK_STATUS_CODES).
    """
    pass


class ErrorParsingBambooData(PyBambooException, ValueError):
    """
    Signifies an error in parsing the response text
    (JSON) from bamboo.
    """
    pass
