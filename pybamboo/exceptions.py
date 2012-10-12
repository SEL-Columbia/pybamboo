class ErrorRetrievingBambooData(IOError):
    pass


class ErrorParsingBambooData(ValueError):
    pass


class ErrorCreatingBambooDataset(Exception):
    pass


class BambooDatasetDoesNotExist(Exception):
    pass
