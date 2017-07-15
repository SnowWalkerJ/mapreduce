from .client import MRClient
from .common.settings import CONFIG

__all__ = ['MRClient', 'CONFIG', 'get_service']


__master = None

def get_service():
    global __master
    if __master is None:
        __master = MRClient(CONFIG.CORES)
    return __master