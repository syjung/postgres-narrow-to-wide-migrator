"""
Thread-aware logging utilities for ship-specific processing
"""
import os
import threading
from typing import Optional
from loguru import logger


# Global flag to enable/disable ship-specific log files
ENABLE_SHIP_LOG_FILES = True

# Cache for ship log handlers to avoid duplicate additions
_ship_log_handlers = {}


class ThreadLogger:
    """Thread-aware logger that includes ship ID and thread ID in all log messages"""
    
    def __init__(self, ship_id: Optional[str] = None, mode: str = "unknown"):
        self.ship_id = ship_id
        self.mode = mode  # realtime, batch, unknown
        self.thread_id = threading.current_thread().ident
        self.thread_name = threading.current_thread().name
        
        # Add ship-specific log file if enabled
        if ship_id and ENABLE_SHIP_LOG_FILES:
            self._ensure_ship_log_file(ship_id, mode)
    
    def set_ship_id(self, ship_id: str, mode: str = None):
        """Set ship ID for this logger instance"""
        self.ship_id = ship_id
        if mode:
            self.mode = mode
        
        # Add ship-specific log file if not already added
        if ship_id and ENABLE_SHIP_LOG_FILES:
            self._ensure_ship_log_file(ship_id, self.mode)
    
    def _ensure_ship_log_file(self, ship_id: str, mode: str = "unknown"):
        """Ensure ship-specific log file exists (thread-safe)"""
        # Use global cache to avoid adding the same handler multiple times
        global _ship_log_handlers
        
        # Cache key includes both ship_id and mode
        cache_key = f"{ship_id}_{mode}"
        
        if cache_key in _ship_log_handlers:
            return  # Already added
        
        # Create logs directory if needed
        os.makedirs('logs', exist_ok=True)
        
        # Add ship-specific log file with mode suffix
        ship_log_file = f'logs/ship_{ship_id}_{mode}.log'
        
        try:
            handler_id = logger.add(
                ship_log_file,
                format='{time:YYYY-MM-DD HH:mm:ss} | {level} | {message}',
                level='INFO',
                rotation='10 MB',
                retention='30 days',
                compression='zip',
                filter=lambda record: ship_id in record['message']  # Only log messages for this ship
            )
            
            _ship_log_handlers[cache_key] = handler_id
            logger.debug(f"Added ship-specific log file: {ship_log_file}")
            
        except Exception as e:
            logger.warning(f"Failed to add ship-specific log file for {ship_id}: {e}")
    
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


def get_ship_thread_logger(ship_id: str, mode: str = "unknown") -> ThreadLogger:
    """Get or create a thread logger for a specific ship and mode"""
    thread_id = threading.current_thread().ident
    
    if thread_id not in _thread_loggers:
        _thread_loggers[thread_id] = ThreadLogger(ship_id, mode)
    else:
        _thread_loggers[thread_id].set_ship_id(ship_id, mode)
    
    return _thread_loggers[thread_id]


def get_current_thread_logger() -> Optional[ThreadLogger]:
    """Get the thread logger for current thread (if exists)"""
    thread_id = threading.current_thread().ident
    return _thread_loggers.get(thread_id)


def clear_thread_logger():
    """Clear thread logger for current thread (cleanup)"""
    thread_id = threading.current_thread().ident
    if thread_id in _thread_loggers:
        del _thread_loggers[thread_id]
