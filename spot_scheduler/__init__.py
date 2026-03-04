"""
spot-scheduler – A Dask scheduler plugin that profiles task runtimes,
computes the critical path / slack on a DAG, and assigns tasks to
spot vs. on-demand worker pools to minimise cost while meeting a deadline.
"""

__version__ = "0.1.0"
