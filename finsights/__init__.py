from .db.connection import get_conn
__all__ = ["get_conn"]
# when we do from finsights import * it will import 
# all the functions and classes from the db.connection module
# unless we specify __all__ which contains the names of the functions
# and classes that we want to import when we do from finsights import *