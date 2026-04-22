# Databricks notebook source
# DBTITLE 1,Logging Header
# Utils — Structured Logging & Metrics
#
# Production-grade logging framework for pipeline observability.
# Replaces print statements with structured logging, metrics collection, and execution tracking.
#
# Features:
#   Structured JSON logging for easy parsing
#   Execution time tracking
#   Metrics collection (row counts, data volumes)
#   Error context preservation
#   Integration with Databricks monitoring

# COMMAND ----------

# DBTITLE 1,Logger Implementation
import logging
import json
from datetime import datetime
from typing import Dict, Any, Optional
from contextlib import contextmanager
import time
from pyspark.sql import DataFrame


class PipelineLogger:
    """Structured logger for ELT pipeline operations."""
    
    def __init__(self, notebook_name: str, run_id: Optional[str] = None):
        self.notebook_name = notebook_name
        self.run_id = run_id or datetime.utcnow().isoformat()
        self.metrics = {}
        
        # Configure Python logging
        self.logger = logging.getLogger(notebook_name)
        self.logger.setLevel(logging.INFO)
        
        # Prevent duplicate handlers
        if not self.logger.handlers:
            handler = logging.StreamHandler()
            handler.setFormatter(
                logging.Formatter('%(asctime)s [%(levelname)s] %(message)s')
            )
            self.logger.addHandler(handler)
    
    def _log_structured(self, level: str, message: str, **kwargs):
        """Log with structured context."""
        log_entry = {
            "timestamp": datetime.utcnow().isoformat(),
            "notebook": self.notebook_name,
            "run_id": self.run_id,
            "level": level,
            "message": message,
            **kwargs
        }
        
        # Log to both structured JSON and readable format
        readable = f"{message}"
        if kwargs:
            readable += f" | {', '.join(f'{k}={v}' for k, v in kwargs.items())}"
        
        getattr(self.logger, level.lower())(readable)
        
        # Store metrics for later retrieval
        if 'metric' in kwargs:
            self.metrics[kwargs['metric']] = kwargs.get('value')
    
    def info(self, message: str, **kwargs):
        """Log info message."""
        self._log_structured("INFO", message, **kwargs)
    
    def warning(self, message: str, **kwargs):
        """Log warning message."""
        self._log_structured("WARNING", message, **kwargs)
    
    def error(self, message: str, error: Optional[Exception] = None, **kwargs):
        """Log error with optional exception context."""
        if error:
            kwargs['error_type'] = type(error).__name__
            kwargs['error_message'] = str(error)
        self._log_structured("ERROR", message, **kwargs)
    
    def log_dataframe_metrics(self, df: DataFrame, stage: str, table_name: str):
        """Log DataFrame row count and estimated size."""
        count = df.count()
        self.info(
            f"{stage} DataFrame metrics",
            metric=f"{stage.lower()}_row_count",
            value=count,
            table=table_name,
            rows=f"{count:,}"
        )
        return count
    
    @contextmanager
    def timed_operation(self, operation_name: str):
        """Context manager for timing operations."""
        start_time = time.time()
        self.info(f"Starting: {operation_name}")
        
        try:
            yield
            duration = time.time() - start_time
            self.info(
                f"Completed: {operation_name}",
                metric=f"{operation_name.lower().replace(' ', '_')}_duration_sec",
                value=round(duration, 2),
                duration=f"{duration:.2f}s"
            )
        except Exception as e:
            duration = time.time() - start_time
            self.error(
                f"Failed: {operation_name}",
                error=e,
                duration=f"{duration:.2f}s"
            )
            raise
    
    def get_metrics_summary(self) -> Dict[str, Any]:
        """Return collected metrics for the run."""
        return {
            "run_id": self.run_id,
            "notebook": self.notebook_name,
            "metrics": self.metrics,
            "timestamp": datetime.utcnow().isoformat()
        }


# Helper function to create logger
def get_logger(notebook_name: str) -> PipelineLogger:
    """Create a logger instance for a notebook."""
    return PipelineLogger(notebook_name)
