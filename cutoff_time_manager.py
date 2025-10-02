"""
Cutoff time management utility
"""
import os
from datetime import datetime
from typing import Optional
from loguru import logger
from config import migration_config


class CutoffTimeManager:
    """Manages cutoff time persistence for migration"""
    
    def __init__(self):
        self.cutoff_time_file = migration_config.cutoff_time_file
    
    def save_cutoff_time(self, cutoff_time: datetime) -> bool:
        """
        Save cutoff time to file
        
        Args:
            cutoff_time: Migration completion time
            
        Returns:
            True if successful, False otherwise
        """
        try:
            with open(self.cutoff_time_file, 'w') as f:
                f.write(cutoff_time.isoformat())
            
            logger.info(f"Cutoff time saved: {cutoff_time}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to save cutoff time: {e}")
            return False
    
    def load_cutoff_time(self) -> Optional[datetime]:
        """
        Load cutoff time from file
        
        Returns:
            Cutoff time if exists, None otherwise
        """
        try:
            if not os.path.exists(self.cutoff_time_file):
                logger.info("No cutoff time file found")
                return None
            
            with open(self.cutoff_time_file, 'r') as f:
                cutoff_str = f.read().strip()
            
            cutoff_time = datetime.fromisoformat(cutoff_str)
            logger.info(f"Cutoff time loaded: {cutoff_time}")
            return cutoff_time
            
        except Exception as e:
            logger.error(f"Failed to load cutoff time: {e}")
            return None
    
    def clear_cutoff_time(self) -> bool:
        """
        Clear cutoff time file
        
        Returns:
            True if successful, False otherwise
        """
        try:
            if os.path.exists(self.cutoff_time_file):
                os.remove(self.cutoff_time_file)
                logger.info("Cutoff time file cleared")
            return True
            
        except Exception as e:
            logger.error(f"Failed to clear cutoff time: {e}")
            return False
    
    def get_cutoff_time_status(self) -> dict:
        """
        Get cutoff time status information
        
        Returns:
            Dictionary with cutoff time status
        """
        cutoff_time = self.load_cutoff_time()
        
        return {
            'has_cutoff_time': cutoff_time is not None,
            'cutoff_time': cutoff_time.isoformat() if cutoff_time else None,
            'file_exists': os.path.exists(self.cutoff_time_file),
            'file_path': self.cutoff_time_file
        }


# Global cutoff time manager instance
cutoff_time_manager = CutoffTimeManager()
