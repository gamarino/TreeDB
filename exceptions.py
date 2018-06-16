# Base exceptions

class DBException(Exception):
    def __init__(self, code=None, msg=''):
        self.code = code
        self.message = msg



