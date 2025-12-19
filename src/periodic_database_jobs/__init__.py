
import importlib.metadata 
import logging
periodic_db_logger = logging.getLogger(__name__)

__version__ =  importlib.metadata.version('periodic_database_jobs') 

from periodic_database_jobs.scheduled import run_jobs