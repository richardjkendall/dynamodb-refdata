class MalformedTableData(Exception):
    """Error thrown for malformed table data"""
    def __init__(self, *args, **kwargs):
        Exception.__init__(self, *args, **kwargs)