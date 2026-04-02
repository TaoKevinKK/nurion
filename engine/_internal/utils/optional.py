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

"""Helpers for optional dependency handling."""


def optional_dependency_placeholder(class_name: str, extra: str):
    """Return a placeholder class that raises ImportError on instantiation or subclassing.

    Used in __init__.py when an optional dependency is not installed, so users get
    a clear error message instead of 'NoneType is not callable'.

    Usage::

        try:
            from mymodule import MyClass
        except ImportError:
            MyClass = optional_dependency_placeholder("MyClass", "myextra")
    """
    msg = f"{class_name} requires the [{extra}] extra. Install with: pip install engine[{extra}]"

    class _Placeholder:
        def __init__(self, *args, **kwargs):
            raise ImportError(msg)

        def __init_subclass__(cls, **kwargs):
            raise ImportError(msg)

    _Placeholder.__name__ = class_name
    _Placeholder.__qualname__ = class_name
    return _Placeholder
