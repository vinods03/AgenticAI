import pandas as pd
import os
import logging
from typing import Dict
from datetime import datetime
from pathlib import Path

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class CSVHandler:
    def __init__(self, file_path: str):
        self.file_path = file_path
        self.path = Path(file_path)

    def read_csv(self):
        try:
            if not self.path.exists():
                raise FileNotFoundError(f"CSV file not found: {self.file_path}")
            return pd.read_csv(self.file_path)
        except Exception as e:
            logger.error(f"Error reading csv file {self.file_path}: {str(e)}")
            raise

    def get_last_modified_time(self):
        return datetime.fromtimestamp(os.path.getmtime(self.file_path))

    def get_file_stats(self):
        try:
            df = self.read_csv()
            return {
                "row_count": len(df),
                "col_count": len(df.columns),
                "file_size": os.path.getsize(self.file_path),
                "last_modified": self.get_last_modified_time().isoformat()
            }
        except Exception as e:
            return {
                "error": str(e)
            }