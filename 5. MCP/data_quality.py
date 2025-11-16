import pandas as pd
import numpy as np
from typing import Dict
from datetime import datetime, timedelta

class DataQualityChecker:
    def __init__(self, df: pd.DataFrame, threshold_settings: Dict):
        self.df = df
        self.threshold_settings = threshold_settings
    
    def check_nulls(self):
        null_percentages = self.df.isnull().mean()
        max_null_percentage = null_percentages.max()
        problematic_columns = null_percentages[null_percentages > self.threshold_settings["max_nulls_percentage"]].index.tolist()

        return {
            "null_percentages": null_percentages,
            "max_null_percentage": round(float(max_null_percentage * 100),2),
            "problematic_columns": problematic_columns
        }

    def check_duplicates(self):
        duplicate_count = self.df.duplicated().sum()
        duplicate_percentage = duplicate_count / len(self.df)

        return {
            'duplicate_count': int(duplicate_count),
            'duplicate_percentage': round(float(duplicate_percentage * 100),2)
        }

    def run_all_checks(self):
        return {
            'null_checks': self.check_nulls(),
            'duplicate_checks': self.check_duplicates()
        }