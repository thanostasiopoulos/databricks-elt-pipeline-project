# Databricks notebook source
# DBTITLE 1,Config Header
# Utils — Centralized Configuration
#
# Provides environment-aware configuration for all pipeline notebooks.
# Eliminate hardcoded catalog/schema names and centralize environment-specific settings.
#
# Usage:
#   %run ../utils/config
#   print(config.catalog)  # 'olist'
#   print(config.silver.dim_customer)  # 'olist.silver.dim_customer'

# COMMAND ----------

# DBTITLE 1,Configuration Class
from dataclasses import dataclass
from typing import Optional
import os


@dataclass
class LayerConfig:
    """Configuration for a specific layer (bronze, silver, gold)."""
    catalog: str
    schema: str
    
    def table(self, name: str) -> str:
        """Return fully qualified table name."""
        return f"{self.catalog}.{self.schema}.{name}"
    
    def __getattr__(self, name: str) -> str:
        """Allow dot notation: config.silver.dim_customer."""
        return self.table(name)


@dataclass
class PipelineConfig:
    """Main pipeline configuration."""
    catalog: str
    environment: str
    
    # Layer configs
    bronze: LayerConfig
    silver: LayerConfig
    gold: LayerConfig
    
    # Paths
    volume_path: str
    checkpoint_path: str
    
    # Data quality thresholds
    max_row_drop_pct: float = 10.0
    min_row_count: int = 1000
    
    @classmethod
    def from_environment(cls, env: Optional[str] = None) -> 'PipelineConfig':
        """
        Load configuration based on environment.
        
        Environment detection order:
        1. Explicit env parameter
        2. DATABRICKS_ENV environment variable
        3. Widget (if running in a job)
        4. Default to 'dev'
        """
        if env is None:
            env = os.getenv("DATABRICKS_ENV", "dev")
            try:
                env = dbutils.widgets.get("environment")
            except:
                pass
        
        # Environment-specific configs
        if env == "prod":
            catalog = "olist"
            volume_path = "/Volumes/olist/bronze/raw"
            checkpoint_path = "/Volumes/olist/_checkpoints"
        else:  # dev
            catalog = "olist"
            volume_path = "/Volumes/olist/bronze/raw"
            checkpoint_path = "/Volumes/olist/_checkpoints"
        
        return cls(
            catalog=catalog,
            environment=env,
            bronze=LayerConfig(catalog=catalog, schema="bronze"),
            silver=LayerConfig(catalog=catalog, schema="silver"),
            gold=LayerConfig(catalog=catalog, schema="gold"),
            volume_path=volume_path,
            checkpoint_path=checkpoint_path,
        )


# Global config instance
config = PipelineConfig.from_environment()

print(f"✓ Configuration loaded")
print(f"  Environment    : {config.environment}")
print(f"  Catalog        : {config.catalog}")
print(f"  Volume path    : {config.volume_path}")
