#!/usr/bin/env python
import sys
import warnings

from datetime import datetime

from enhanced_automated_etl_pipeline.crew import EnhancedAutomatedEtlPipeline

warnings.filterwarnings("ignore", category=SyntaxWarning, module="pysbd")

mapping_doc = 'employee_transformation.xlsx'
pipeline_name = 'employee_etl'
source = 'Kinesis Data Stream'
destination = 'Redshift'

def run():
    """
    Run the crew.
    """
    inputs = {
        'mapping_doc': mapping_doc,
        'pipeline_name': pipeline_name,
        'source': source,
        'destination': destination
    }
    
    try:
        EnhancedAutomatedEtlPipeline().crew().kickoff(inputs=inputs)
    except Exception as e:
        raise Exception(f"An error occurred while running the crew: {e}")

