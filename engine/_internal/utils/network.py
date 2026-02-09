# Copyright 2025 nurion team
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Network utilities."""

from __future__ import annotations

import socket


def find_free_port() -> int:
    """Find an available port on the local machine.

    Returns:
        An available port number
    """
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("", 0))
        return s.getsockname()[1]


def get_node_ip() -> str:
    """Get the IP address of the current node.

    First tries Ray's utility, then falls back to socket-based detection.

    Returns:
        IP address string (e.g., "192.168.1.100")
    """
    # Try Ray first (works in Ray cluster)
    try:
        import ray

        return ray.util.get_node_ip_address()
    except Exception:
        pass

    # Fallback: connect to external address to get local IP
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
            s.connect(("8.8.8.8", 80))
            return s.getsockname()[0]
    except Exception:
        return "127.0.0.1"
