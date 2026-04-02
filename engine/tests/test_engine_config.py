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

"""Tests for the centralized engine configuration system."""

import os

import pytest

from _internal.config import EngineConfig, configure, get_config, reset_config


@pytest.fixture(autouse=True)
def _clean_config():
    """Reset config cache before/after each test."""
    reset_config()
    yield
    reset_config()


@pytest.fixture()
def _clean_env():
    """Remove all NURION_ env vars after test."""
    yield
    for key in list(os.environ):
        if key.startswith("NURION_"):
            del os.environ[key]


class TestEngineConfigDefaults:
    def test_default_values(self):
        cfg = EngineConfig()
        assert cfg.broker_claim_timeout_s == 60.0
        assert cfg.worker_idle_timeout_s == 300.0
        assert cfg.stage_no_progress_timeout_s == 600.0
        assert cfg.autoscaler_max_scale_step == 32
        assert cfg.flight_server_port == 18815

    def test_frozen(self):
        cfg = EngineConfig()
        with pytest.raises(AttributeError):
            cfg.broker_claim_timeout_s = 999  # type: ignore[misc]


class TestGetConfig:
    def test_returns_defaults_with_no_env(self):
        cfg = get_config()
        assert cfg.broker_claim_timeout_s == 60.0

    def test_reads_env_var(self, _clean_env):
        os.environ["NURION_BROKER_CLAIM_TIMEOUT_S"] = "120"
        reset_config()
        cfg = get_config()
        assert cfg.broker_claim_timeout_s == 120.0

    def test_caches_across_calls(self):
        cfg1 = get_config()
        cfg2 = get_config()
        assert cfg1 is cfg2

    def test_reset_clears_cache(self, _clean_env):
        cfg1 = get_config()
        os.environ["NURION_WORKER_IDLE_TIMEOUT_S"] = "999"
        reset_config()
        cfg2 = get_config()
        assert cfg2.worker_idle_timeout_s == 999.0
        assert cfg1 is not cfg2

    def test_int_env_var(self, _clean_env):
        os.environ["NURION_AUTOSCALER_MAX_SCALE_STEP"] = "64"
        reset_config()
        cfg = get_config()
        assert cfg.autoscaler_max_scale_step == 64

    def test_unknown_env_var_ignored(self, _clean_env):
        os.environ["NURION_THIS_DOES_NOT_EXIST"] = "42"
        reset_config()
        # Should not raise
        cfg = get_config()
        assert isinstance(cfg, EngineConfig)


class TestConfigure:
    def test_sets_value(self, _clean_env):
        cfg = configure(broker_claim_timeout_s=120)
        assert cfg.broker_claim_timeout_s == 120.0

    def test_sets_env_var(self, _clean_env):
        configure(worker_idle_timeout_s=999)
        assert os.environ["NURION_WORKER_IDLE_TIMEOUT_S"] == "999"

    def test_multiple_values(self, _clean_env):
        cfg = configure(
            broker_claim_timeout_s=120,
            autoscaler_max_scale_step=64,
        )
        assert cfg.broker_claim_timeout_s == 120.0
        assert cfg.autoscaler_max_scale_step == 64

    def test_unknown_key_raises(self):
        with pytest.raises(ValueError, match="Unknown config key"):
            configure(nonexistent_key=42)

    def test_subsequent_get_config_returns_updated(self, _clean_env):
        configure(s3_failure_threshold=10)
        cfg = get_config()
        assert cfg.s3_failure_threshold == 10


class TestEnvVars:
    def test_env_vars_returns_non_defaults(self, _clean_env):
        cfg = configure(broker_claim_timeout_s=120, worker_idle_timeout_s=999)
        env = cfg.env_vars()
        assert env == {
            "NURION_BROKER_CLAIM_TIMEOUT_S": "120.0",
            "NURION_WORKER_IDLE_TIMEOUT_S": "999.0",
        }

    def test_env_vars_empty_for_defaults(self):
        cfg = EngineConfig()
        assert cfg.env_vars() == {}

    def test_env_vars_roundtrip(self, _clean_env):
        """Values exported as env vars can be read back to produce the same config."""
        original = EngineConfig(
            broker_claim_timeout_s=120,
            autoscaler_max_scale_step=64,
            flight_server_port=19999,
        )
        for key, val in original.env_vars().items():
            os.environ[key] = val
        reset_config()
        restored = get_config()
        assert restored.broker_claim_timeout_s == 120.0
        assert restored.autoscaler_max_scale_step == 64
        assert restored.flight_server_port == 19999
