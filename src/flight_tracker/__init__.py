"""Pure-Python logic for the FlightPriceTracker pipeline.

Everything in this package is free of Spark and Databricks imports so it can be
unit tested locally with pytest. The Databricks notebooks call these functions
and handle reading and writing Delta tables.
"""

__version__ = "1.0.0"
