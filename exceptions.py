import os, sys


class DbException(Exception):
    pass


class DbNoResultException(DbException):
    pass
