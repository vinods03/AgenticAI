from mcp.server.fastmcp import FastMCP
from typing import Dict, List
from datetime import datetime
from data_quality import DataQualityChecker
from csv_handler import CSVHandler
from config import Config
import pandas as pd
import json
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

mcp = FastMCP("pipeline_monitor")

monitoring_history = {}

@mcp.tool()
async def verify_data_quality(source_name: str):
# def verify_data_quality(source_name: str):
    try:
        source_config = next(
            (source for source in Config.DATA_SOURCES if source.name == source_name),
            None
        )

        if not source_config:
            raise ValueError(f"Unknown source: {source_name}")
        
        csv_handler = CSVHandler(source_config.file_path)
        df = csv_handler.read_csv()

        dq_checker = DataQualityChecker(df, source_config.threshold_settings)
        dq_results = dq_checker.run_all_checks()

        print("source: ", source_name, " dq_results: ", dq_results)

        return {
            "source": source_name,
            "dq_results": dq_results
        }
    
    except Exception as e:
        print("The exception in the pipeline monitor is: ", e)
        return {
            "source": source_name,
            "dq_results": str(e)
        }

if __name__ == "__main__":
    mcp.run(transport='stdio')
    # verify_data_quality('test')