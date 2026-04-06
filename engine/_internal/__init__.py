"""
Nurion Runtime - A Ray-based distributed streaming processing framework.

Features:
- Batch and streaming hybrid execution model
- At-least-once delivery with atomic ack-and-forward
- Elastic scaling with Ray actors
- Dynamic load balancing and backpressure
- DAG-based task execution
"""

from pkgutil import extend_path

__path__ = extend_path(__path__, __name__)

from _internal.core.job import Job
from _internal.core.stage import Stage
from _internal.core.operator import Operator

__version__ = "0.2.2"
__all__ = ["Job", "Stage", "Operator"]
