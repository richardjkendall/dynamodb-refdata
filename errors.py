class MalformedTableData(Exception):
    """Error thrown for malformed table data"""
    def __init__(self, *args, **kwargs):
        Exception.__init__(self, *args, **kwargs)

class ProcessError(Exception):
    """Error thrown when processing does not work"""
    def __init__(self, *args, **kwargs):
        Exception.__init__(self, *args, **kwargs)