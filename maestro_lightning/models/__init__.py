__all__ = [ "get_context"]

from typing import Dict

class Context:
    def __init__(self, path : str="", extra_params : Dict={}):
        self.tasks = {}
        self.datasets = {}
        self.images = {}
        self.path = path
        self.extra_params = extra_params
        
    def __getitem__(self , name : str):
        return self.extra_params[name]
    
    def __setitem__(self , name : str, value):
        self.extra_params[name] = value
        
    def clear(self):
        self.tasks = {}
        self.datasets = {}
        self.images = {}

__context__ = Context()

def get_context(clear : bool=False):
    global __context__
    if clear:
        __context__.clear()
    return __context__
     

from . import status
__all__.extend( status.__all__ )
from .status import *

from . import dataset
__all__.extend( dataset.__all__ )
from .dataset import *

from . import image
__all__.extend( image.__all__ )
from .image import *

from . import job 
__all__.extend( job.__all__ )
from .job import *

from . import task 
__all__.extend( task.__all__ )
from .task import *