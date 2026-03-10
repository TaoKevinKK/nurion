"""
Nurion Runtime - A Ray-based distributed streaming processing framework.

Features:
- Batch and streaming hybrid execution model
- Exactly-once checkpoint semantics
- Elastic scaling with Ray actors
- Dynamic load balancing and backpressure
- Remote state backend (S3/DFS)
- DAG-based task execution
"""

from pkgutil import extend_path

__path__ = extend_path(__path__, __name__)

from _internal.core.job import Job
from _internal.core.stage import Stage
from _internal.core.operator import Operator

__version__ = "0.2.0"
__all__ = ["Job", "Stage", "Operator"]
