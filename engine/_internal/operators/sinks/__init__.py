"""Built-in sink operators."""

from _internal.operators.sinks.file import FileSink, FileSinkConfig
from _internal.operators.sinks.print import PrintSink, PrintSinkConfig
from _internal.utils.optional import optional_dependency_placeholder

# Lance sinks require optional [lance] extra (pylance)
try:
    from _internal.operators.sinks.lance import LanceSink, LanceSinkConfig
    from _internal.operators.sinks.lance_commit import LanceCommitPolicy, LanceSinkCommitter
except ImportError:
    LanceSink = optional_dependency_placeholder("LanceSink", "lance")  # type: ignore[assignment,misc]
    LanceSinkConfig = optional_dependency_placeholder("LanceSinkConfig", "lance")  # type: ignore[assignment,misc]
    LanceCommitPolicy = optional_dependency_placeholder("LanceCommitPolicy", "lance")  # type: ignore[assignment,misc]
    LanceSinkCommitter = optional_dependency_placeholder("LanceSinkCommitter", "lance")  # type: ignore[assignment,misc]

__all__ = [
    "FileSink",
    "FileSinkConfig",
    "LanceSink",
    "LanceSinkConfig",
    "LanceCommitPolicy",
    "LanceSinkCommitter",
    "PrintSink",
    "PrintSinkConfig",
]
