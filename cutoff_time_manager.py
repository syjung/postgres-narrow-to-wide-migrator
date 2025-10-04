"""
Cutoff time management utility
"""
import os
from datetime import datetime
from typing import Optional, Dict, List
from loguru import logger
from config import migration_config


class CutoffTimeManager:
    """Manages cutoff time persistence for migration"""
    
    def __init__(self):
        self.cutoff_time_file = migration_config.cutoff_time_file
        self.cutoff_time_dir = "cutoff_times"  # 선박별 cutoff_time 파일 디렉토리
        
        # 선박별 cutoff_time 디렉토리 생성
        os.makedirs(self.cutoff_time_dir, exist_ok=True)
    
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
            
            # Check if file is empty or contains only whitespace
            if not cutoff_str:
                logger.info("Cutoff time file is empty")
                return None
            
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
    
    def save_ship_cutoff_time(self, ship_id: str, cutoff_time: datetime) -> bool:
        """
        Save cutoff time for a specific ship
        
        Args:
            ship_id: Ship ID
            cutoff_time: Migration completion time for this ship
            
        Returns:
            True if successful, False otherwise
        """
        try:
            ship_cutoff_file = os.path.join(self.cutoff_time_dir, f"{ship_id.lower()}_cutoff_time.txt")
            
            with open(ship_cutoff_file, 'w') as f:
                f.write(cutoff_time.isoformat())
            
            logger.debug(f"Ship cutoff time saved: {ship_id} -> {cutoff_time}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to save ship cutoff time for {ship_id}: {e}")
            return False
    
    def load_ship_cutoff_time(self, ship_id: str) -> Optional[datetime]:
        """
        Load cutoff time for a specific ship
        
        Args:
            ship_id: Ship ID
            
        Returns:
            Cutoff time if exists, None otherwise
        """
        try:
            ship_cutoff_file = os.path.join(self.cutoff_time_dir, f"{ship_id.lower()}_cutoff_time.txt")
            
            if not os.path.exists(ship_cutoff_file):
                logger.debug(f"No cutoff time file found for ship: {ship_id}")
                return None
            
            with open(ship_cutoff_file, 'r') as f:
                cutoff_str = f.read().strip()
            
            if not cutoff_str:
                logger.debug(f"Cutoff time file is empty for ship: {ship_id}")
                return None
            
            cutoff_time = datetime.fromisoformat(cutoff_str)
            logger.debug(f"Ship cutoff time loaded: {ship_id} -> {cutoff_time}")
            return cutoff_time
            
        except Exception as e:
            logger.error(f"Failed to load ship cutoff time for {ship_id}: {e}")
            return None
    
    def get_all_ship_cutoff_times(self) -> Dict[str, Optional[datetime]]:
        """
        Get cutoff times for all ships
        
        Returns:
            Dictionary mapping ship_id to cutoff_time
        """
        cutoff_times = {}
        
        try:
            # Get all ship cutoff time files
            if not os.path.exists(self.cutoff_time_dir):
                return cutoff_times
            
            for filename in os.listdir(self.cutoff_time_dir):
                if filename.endswith('_cutoff_time.txt'):
                    ship_id = filename.replace('_cutoff_time.txt', '').upper()
                    cutoff_time = self.load_ship_cutoff_time(ship_id)
                    cutoff_times[ship_id] = cutoff_time
            
            logger.debug(f"Loaded cutoff times for {len(cutoff_times)} ships")
            return cutoff_times
            
        except Exception as e:
            logger.error(f"Failed to get all ship cutoff times: {e}")
            return cutoff_times
    
    def get_global_cutoff_time(self) -> Optional[datetime]:
        """
        Get the global cutoff time (earliest among all ships)
        
        Returns:
            Earliest cutoff time among all ships, None if no ships have cutoff times
        """
        try:
            all_cutoff_times = self.get_all_ship_cutoff_times()
            
            # Filter out None values and get the earliest
            valid_cutoff_times = [ct for ct in all_cutoff_times.values() if ct is not None]
            
            if not valid_cutoff_times:
                logger.debug("No valid cutoff times found for any ship")
                return None
            
            global_cutoff = min(valid_cutoff_times)
            logger.debug(f"Global cutoff time: {global_cutoff} (from {len(valid_cutoff_times)} ships)")
            return global_cutoff
            
        except Exception as e:
            logger.error(f"Failed to get global cutoff time: {e}")
            return None
    
    def clear_ship_cutoff_time(self, ship_id: str) -> bool:
        """
        Clear cutoff time for a specific ship
        
        Args:
            ship_id: Ship ID
            
        Returns:
            True if successful, False otherwise
        """
        try:
            ship_cutoff_file = os.path.join(self.cutoff_time_dir, f"{ship_id.lower()}_cutoff_time.txt")
            
            if os.path.exists(ship_cutoff_file):
                os.remove(ship_cutoff_file)
                logger.debug(f"Ship cutoff time cleared: {ship_id}")
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to clear ship cutoff time for {ship_id}: {e}")
            return False
    
    def clear_all_ship_cutoff_times(self) -> bool:
        """
        Clear all ship cutoff times
        
        Returns:
            True if successful, False otherwise
        """
        try:
            if not os.path.exists(self.cutoff_time_dir):
                return True
            
            for filename in os.listdir(self.cutoff_time_dir):
                if filename.endswith('_cutoff_time.txt'):
                    file_path = os.path.join(self.cutoff_time_dir, filename)
                    os.remove(file_path)
            
            logger.info("All ship cutoff times cleared")
            return True
            
        except Exception as e:
            logger.error(f"Failed to clear all ship cutoff times: {e}")
            return False


# Global cutoff time manager instance
cutoff_time_manager = CutoffTimeManager()
