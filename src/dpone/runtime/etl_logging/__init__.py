"""Own public logging exports without modifying another module's namespace.

Import the implementation before binding the singleton whose name also identifies
its submodule. Python's initial submodule binding then cannot overwrite the public
singleton. The top-level dpone facade remains lazy.
"""

from dpone.runtime.etl_logging.etl_logger import ETLLogger, etl_logger

__all__ = ["ETLLogger", "etl_logger"]
