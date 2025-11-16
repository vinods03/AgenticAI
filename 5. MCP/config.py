from dataclasses import dataclass
from typing import List, Dict
import os

@dataclass
class DataSourceConfig:
    name: str
    file_path: str
    check_frequency: int
    threshold_settings: Dict

class Config:
    DATA_SOURCES = [
        DataSourceConfig(
            name="test",
            file_path=r"C:\Users\VINOD\projects\agents\6_mcp\my_project\test.csv",
            check_frequency=15,
            threshold_settings={
                "max_nulls_percentage":0.05,
                "freshness_threshold_minutes":60,
                "min_quality_score":0.9
            }
        ),
        DataSourceConfig(
            name="sales_data",
            file_path=r"C:\Users\VINOD\projects\agents\6_mcp\my_project\sales_data.csv",
            check_frequency=15,
            threshold_settings={
                "max_nulls_percentage":0.05,
                "freshness_threshold_minutes":60,
                "min_quality_score":0.9
            }
        ),
        DataSourceConfig(
            name="customer_data",
            file_path=r"C:\Users\VINOD\projects\agents\6_mcp\my_project\customer_data.csv",
            check_frequency=30,
            threshold_settings={
                "max_nulls_percentage":0.05,
                "freshness_threshold_minutes":60,
                "min_quality_score":0.9
            }
        )
    ]

