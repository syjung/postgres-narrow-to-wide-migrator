"""
Thread-aware logging utilities for ship-specific processing
"""
import threading
from typing import Optional
from loguru import logger


class ThreadLogger:
    """Thread-aware logger that includes ship ID and thread ID in all log messages"""
    
    def __init__(self, ship_id: Optional[str] = None):
        self.ship_id = ship_id
        self.thread_id = threading.current_thread().ident
        self.thread_name = threading.current_thread().name
    
    def set_ship_id(self, ship_id: str):
        """Set ship ID for this logger instance"""
        self.ship_id = ship_id
    
    def _format_message(self, message: str) -> str:
        """Format message with ship and thread information"""
        if self.ship_id:
            return f"[{self.ship_id}:Thread-{self.thread_id}] {message}"
        else:
            return f"[Thread-{self.thread_id}] {message}"
    
    def info(self, message: str):
        """Log info message with ship and thread info"""
        logger.info(self._format_message(message))
    
    def debug(self, message: str):
        """Log debug message with ship and thread info"""
        logger.debug(self._format_message(message))
    
    def warning(self, message: str):
        """Log warning message with ship and thread info"""
        logger.warning(self._format_message(message))
    
    def error(self, message: str):
        """Log error message with ship and thread info"""
        logger.error(self._format_message(message))
    
    def success(self, message: str):
        """Log success message with ship and thread info"""
        logger.info(f"✅ {self._format_message(message)}")
    
    def fail(self, message: str):
        """Log failure message with ship and thread info"""
        logger.error(f"❌ {self._format_message(message)}")


def get_thread_logger(ship_id: Optional[str] = None) -> ThreadLogger:
    """Get a thread-aware logger for the current thread"""
    return ThreadLogger(ship_id)


def log_with_ship_thread(ship_id: str, message: str, level: str = "info"):
    """Quick logging function with ship and thread info"""
    thread_id = threading.current_thread().ident
    formatted_message = f"[{ship_id}:Thread-{thread_id}] {message}"
    
    if level == "info":
        logger.info(formatted_message)
    elif level == "debug":
        logger.debug(formatted_message)
    elif level == "warning":
        logger.warning(formatted_message)
    elif level == "error":
        logger.error(formatted_message)
    elif level == "success":
        logger.info(f"✅ {formatted_message}")
    elif level == "fail":
        logger.error(f"❌ {formatted_message}")
    else:
        logger.info(formatted_message)


# Global thread logger instances (one per thread)
_thread_loggers = {}


def get_ship_thread_logger(ship_id: str) -> ThreadLogger:
    """Get or create a thread logger for a specific ship"""
    thread_id = threading.current_thread().ident
    
    if thread_id not in _thread_loggers:
        _thread_loggers[thread_id] = ThreadLogger(ship_id)
    else:
        _thread_loggers[thread_id].set_ship_id(ship_id)
    
    return _thread_loggers[thread_id]


def clear_thread_logger():
    """Clear thread logger for current thread (cleanup)"""
    thread_id = threading.current_thread().ident
    if thread_id in _thread_loggers:
        del _thread_loggers[thread_id]
