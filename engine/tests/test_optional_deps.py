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

"""Tests for optional dependency lazy import guards.

Verifies that:
1. `import nurion` works even when optional deps are missing
2. Using optional classes without installing the extra raises ImportError with a clear message
3. Subclassing optional classes also raises ImportError
"""

import sys
from contextlib import contextmanager
from unittest import mock

import pytest

from _internal.utils.optional import optional_dependency_placeholder


# ---------------------------------------------------------------------------
# Unit tests for the placeholder helper itself
# ---------------------------------------------------------------------------


class TestOptionalDependencyPlaceholder:
    def test_instantiation_raises_import_error(self):
        Cls = optional_dependency_placeholder("FooConfig", "bar")
        with pytest.raises(ImportError, match=r"FooConfig requires the \[bar\] extra"):
            Cls()

    def test_instantiation_with_args_raises_import_error(self):
        Cls = optional_dependency_placeholder("FooConfig", "bar")
        with pytest.raises(ImportError, match=r"pip install engine\[bar\]"):
            Cls(some_arg="value")

    def test_subclassing_raises_import_error(self):
        Cls = optional_dependency_placeholder("FooConfig", "bar")
        with pytest.raises(ImportError, match=r"FooConfig requires the \[bar\] extra"):
            class SubFoo(Cls):
                pass

    def test_class_name_is_set(self):
        Cls = optional_dependency_placeholder("MyClass", "myextra")
        assert Cls.__name__ == "MyClass"
        assert Cls.__qualname__ == "MyClass"

    def test_class_is_usable_as_type_check(self):
        """Placeholder should be a class (not None), so isinstance/isclass checks work."""
        Cls = optional_dependency_placeholder("Foo", "bar")
        assert isinstance(Cls, type)


# ---------------------------------------------------------------------------
# Integration tests: simulate missing optional deps at the __init__.py level
# ---------------------------------------------------------------------------


@contextmanager
def _hide_module(root_module: str):
    """Temporarily make a module (and all submodules) unimportable.

    Removes cached entries from sys.modules and patches __import__ to block
    fresh imports, so that importlib.reload() on consumer modules will hit
    the ImportError path.
    """
    import builtins

    # Save and remove all cached (sub)modules
    saved = {}
    to_remove = [
        key for key in sys.modules
        if key == root_module or key.startswith(root_module + ".")
    ]
    for key in to_remove:
        saved[key] = sys.modules.pop(key)

    real_import = builtins.__import__

    def guarded_import(name, *args, **kwargs):
        if name == root_module or name.startswith(root_module + "."):
            raise ImportError(f"Simulated: {name} not installed")
        return real_import(name, *args, **kwargs)

    with mock.patch("builtins.__import__", side_effect=guarded_import):
        try:
            yield
        finally:
            # Restore cached modules
            sys.modules.update(saved)


def _reload_sources():
    """Reload the sources __init__.py to pick up import guard changes."""
    # Also evict the specific submodule so it gets re-imported
    for key in list(sys.modules):
        if key.startswith("_internal.operators.sources"):
            del sys.modules[key]
    import _internal.operators.sources as mod
    return mod


def _reload_sinks():
    """Reload the sinks __init__.py to pick up import guard changes."""
    for key in list(sys.modules):
        if key.startswith("_internal.operators.sinks"):
            del sys.modules[key]
    import _internal.operators.sinks as mod
    return mod


class TestLanceMissing:
    """Verify behavior when pylance (lance) is not installed."""

    def test_sources_init_loads_without_lance(self):
        with _hide_module("lance"):
            mod = _reload_sources()
            # Should be a placeholder, not None
            assert mod.LanceTableSourceConfig is not None
            assert isinstance(mod.LanceTableSourceConfig, type)

    def test_lance_source_config_raises_on_use(self):
        with _hide_module("lance"):
            mod = _reload_sources()
            with pytest.raises(ImportError, match=r"LanceTableSourceConfig requires the \[lance\] extra"):
                mod.LanceTableSourceConfig(table_uri="s3://bucket/table")

    def test_sinks_init_loads_without_lance(self):
        with _hide_module("lance"):
            mod = _reload_sinks()
            assert mod.LanceSinkConfig is not None
            assert isinstance(mod.LanceSinkConfig, type)

    def test_lance_sink_config_raises_on_use(self):
        with _hide_module("lance"):
            mod = _reload_sinks()
            with pytest.raises(ImportError, match=r"LanceSinkConfig requires the \[lance\] extra"):
                mod.LanceSinkConfig(table_uri="s3://bucket/output")

    def test_lance_commit_policy_raises_on_use(self):
        with _hide_module("lance"):
            mod = _reload_sinks()
            with pytest.raises(ImportError, match=r"LanceCommitPolicy requires the \[lance\] extra"):
                mod.LanceCommitPolicy()


class TestIcebergMissing:
    """Verify behavior when pyiceberg is not installed."""

    def test_sources_init_loads_without_iceberg(self):
        with _hide_module("pyiceberg"):
            mod = _reload_sources()
            assert mod.IcebergSourceConfig is not None
            assert isinstance(mod.IcebergSourceConfig, type)

    def test_iceberg_source_config_raises_on_use(self):
        with _hide_module("pyiceberg"):
            mod = _reload_sources()
            with pytest.raises(ImportError, match=r"IcebergSourceConfig requires the \[iceberg\] extra"):
                mod.IcebergSourceConfig(catalog_name="default", table_id="db.table")


class TestDepsInstalled:
    """When optional deps ARE installed, everything works normally."""

    def test_lance_source_config_is_real_class(self):
        mod = _reload_sources()
        assert hasattr(mod.LanceTableSourceConfig, "__dataclass_fields__")

    def test_lance_sink_config_is_real_class(self):
        mod = _reload_sinks()
        assert hasattr(mod.LanceSinkConfig, "__dataclass_fields__")

    def test_iceberg_source_config_is_real_class(self):
        mod = _reload_sources()
        assert hasattr(mod.IcebergSourceConfig, "__dataclass_fields__")
