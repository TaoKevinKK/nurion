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

"""Utilities for accessing remote files (S3, etc.)."""

from __future__ import annotations

import configparser
import hashlib
import logging
import os
import tempfile
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Dict, Generator, Optional
from urllib.parse import urlparse

logger = logging.getLogger(__name__)

# Cache directory for downloaded files
_CACHE_DIR: Optional[Path] = None
_S3_CONFIG: Optional[Dict[str, Any]] = None


def _load_s3_config(
    rclone_remote: Optional[str] = None,
    aws_profile: str = "default",
) -> Dict[str, Any]:
    """Load S3 configuration once from env/aws/rclone."""
    if rclone_remote is None:
        rclone_remote = os.environ.get("NURION_S3_REMOTE", "s3")

    global _S3_CONFIG
    if _S3_CONFIG is not None:
        return _S3_CONFIG

    env_key = os.environ.get("AWS_ACCESS_KEY_ID", "")
    env_secret = os.environ.get("AWS_SECRET_ACCESS_KEY", "")
    env_endpoint = os.environ.get("AWS_ENDPOINT_URL") or os.environ.get(
        "FSSPEC_S3_ENDPOINT_URL", ""
    )
    env_region = os.environ.get("AWS_REGION") or os.environ.get("AWS_DEFAULT_REGION") or "us-east-1"

    if env_key and env_secret:
        _S3_CONFIG = {
            "key": env_key,
            "secret": env_secret,
            "endpoint_url": env_endpoint,
            "region_name": env_region,
            "source": "environment",
        }
        logger.info(
            "Loaded S3 config from environment variables: endpoint=%s, region=%s",
            _S3_CONFIG["endpoint_url"],
            _S3_CONFIG["region_name"],
        )
        return _S3_CONFIG

    key = ""
    secret = ""
    region = env_region
    endpoint = env_endpoint

    aws_creds_paths = [
        Path.home() / ".aws/credentials",
        Path("/root/.aws/credentials"),
    ]
    for creds_path in aws_creds_paths:
        try:
            if not creds_path.exists():
                continue
        except PermissionError:
            continue
        config = configparser.ConfigParser()
        config.read(creds_path)
        if aws_profile in config:
            section = config[aws_profile]
            key = section.get("aws_access_key_id", "")
            secret = section.get("aws_secret_access_key", "")
            if key and secret:
                logger.debug("Loaded AWS credentials from %s [%s]", creds_path, aws_profile)
                break

    aws_config_paths = [
        Path.home() / ".aws/config",
        Path("/root/.aws/config"),
    ]
    for config_path in aws_config_paths:
        try:
            if not config_path.exists():
                continue
        except PermissionError:
            continue
        config = configparser.ConfigParser()
        config.read(config_path)
        section_name = aws_profile if aws_profile == "default" else f"profile {aws_profile}"
        if section_name in config:
            section = config[section_name]
            region = section.get("region", region)
            endpoint = section.get("endpoint_url", endpoint)
            logger.debug("Loaded AWS config from %s [%s]", config_path, section_name)
            break

    if key and secret:
        _S3_CONFIG = {
            "key": key,
            "secret": secret,
            "endpoint_url": endpoint,
            "region_name": region,
            "source": f"aws_config:{aws_profile}",
        }
        logger.info(
            "Loaded S3 config from AWS config [%s]: endpoint=%s, region=%s",
            aws_profile,
            endpoint,
            region,
        )
        return _S3_CONFIG

    rclone_paths = [
        Path.home() / ".config/rclone/rclone.conf",
        Path("/root/.config/rclone/rclone.conf"),
    ]
    for rclone_config in rclone_paths:
        try:
            if not rclone_config.exists():
                continue
        except PermissionError:
            continue
        config = configparser.ConfigParser()
        config.read(rclone_config)
        if rclone_remote in config:
            section = config[rclone_remote]
            key = section.get("access_key_id", "")
            secret = section.get("secret_access_key", "")
            if key and secret:
                _S3_CONFIG = {
                    "key": key,
                    "secret": secret,
                    "endpoint_url": section.get("endpoint", ""),
                    "region_name": section.get("region", "us-east-1"),
                    "source": f"rclone:{rclone_remote}",
                }
                logger.info(
                    "Loaded S3 config from %s [%s]: endpoint=%s, region=%s",
                    rclone_config,
                    rclone_remote,
                    _S3_CONFIG["endpoint_url"],
                    _S3_CONFIG["region_name"],
                )
                return _S3_CONFIG

    logger.warning("No S3 configuration found from any source (env, aws, rclone)")
    _S3_CONFIG = {
        "key": "",
        "secret": "",
        "endpoint_url": "",
        "region_name": env_region,
        "source": "none",
    }
    return _S3_CONFIG


def get_s3_storage_options(
    rclone_remote: Optional[str] = None,
    aws_profile: str = "default",
) -> Dict[str, Any]:
    """Get S3 storage options for fsspec.

    Args:
        rclone_remote: Remote name in rclone config. If None, uses
                       NURION_S3_REMOTE env var or "s3" as default.
        aws_profile: Profile name in AWS config (default: "default")

    Returns:
        Dict of storage options for fsspec.open()
    """
    if rclone_remote is None:
        rclone_remote = os.environ.get("NURION_S3_REMOTE", "s3")
    config = _load_s3_config(rclone_remote, aws_profile)

    options: Dict[str, Any] = {
        "key": config["key"],
        "secret": config["secret"],
        # Use virtual-hosted style addressing for S3-compatible providers
        "config_kwargs": {
            "signature_version": "s3v4",
            "s3": {"addressing_style": "virtual"},
        },
    }

    if config["endpoint_url"]:
        options["endpoint_url"] = config["endpoint_url"]

    if config["region_name"]:
        options["client_kwargs"] = {"region_name": config["region_name"]}

    return options


def restore_s3_object(path: str, days: int = 2) -> bool:
    """Request a restore for an archived S3 object if needed.

    Returns True if a restore request was submitted, False otherwise.
    """
    if not path.startswith("s3://"):
        return False

    parsed = urlparse(path)
    if parsed.scheme != "s3" or not parsed.netloc:
        raise ValueError(f"Invalid S3 path: {path}")
    bucket = parsed.netloc
    key = parsed.path.lstrip("/")

    import boto3  # type: ignore[import-untyped]
    from botocore.config import Config  # type: ignore[import-untyped]

    endpoint_url = os.environ.get("AWS_ENDPOINT_URL") or os.environ.get("FSSPEC_S3_ENDPOINT_URL")
    region_name = os.environ.get("AWS_REGION") or os.environ.get("AWS_DEFAULT_REGION")
    if not region_name:
        options = get_s3_storage_options()
        region_name = options.get("client_kwargs", {}).get("region_name")

    client = boto3.client(
        "s3",
        region_name=region_name,
        endpoint_url=endpoint_url,
        config=Config(connect_timeout=3, read_timeout=5, retries={"max_attempts": 2}),
    )

    try:
        head = client.head_object(Bucket=bucket, Key=key)
    except Exception as e:
        logger.warning(f"Failed to head S3 object for restore: {path} ({e})")
        return False

    storage_class = head.get("StorageClass", "")
    archive_status = head.get("ArchiveStatus", "")
    restore_header = head.get("Restore", "") or ""

    is_intelligent_tiering_archive = archive_status in {
        "ARCHIVE_ACCESS",
        "DEEP_ARCHIVE_ACCESS",
    }
    is_glacier_archive = storage_class in {"GLACIER", "DEEP_ARCHIVE", "GLACIER_IR"}
    is_archived = is_glacier_archive or is_intelligent_tiering_archive

    if restore_header:
        # Restore already in progress or completed
        if 'ongoing-request="true"' in restore_header:
            return False
        if 'ongoing-request="false"' in restore_header:
            return False

    if not is_archived:
        return False

    try:
        if is_intelligent_tiering_archive:
            # Intelligent-Tiering archive tiers don't accept Days in restore requests.
            restore_request: Dict[str, Any] = {}
        else:
            restore_request = {
                "Days": days,
                "GlacierJobParameters": {"Tier": "Standard"},
            }

        client.restore_object(
            Bucket=bucket,
            Key=key,
            RestoreRequest=restore_request,
        )
        return True
    except Exception as e:
        logger.warning(f"Failed to request restore for {path}: {e}")
        return False


def get_lance_storage_options(
    bucket: str,
    rclone_remote: Optional[str] = None,
    aws_profile: str = "default",
) -> Dict[str, str]:
    """Get S3 storage options for Lance.

    Lance uses object_store crate which requires specific options format.
    For S3-compatible providers with custom endpoints, we need to use
    virtual-hosted style with bucket in the endpoint URL.

    Args:
        bucket: S3 bucket name (needed for virtual-hosted endpoint)
        rclone_remote: Remote name in rclone config
        aws_profile: Profile name in AWS config

    Returns:
        Dict of storage options for lance.write_dataset()
    """
    if rclone_remote is None:
        rclone_remote = os.environ.get("NURION_S3_REMOTE", "s3")
    config = _load_s3_config(rclone_remote, aws_profile)

    options: Dict[str, str] = {
        "aws_access_key_id": config["key"],
        "aws_secret_access_key": config["secret"],
        "aws_region": config["region_name"] or "us-east-1",
    }

    # For custom endpoints, use virtual-hosted style with bucket in endpoint
    if config["endpoint_url"]:
        endpoint = config["endpoint_url"]
        # Insert bucket name into endpoint for virtual-hosted style
        # https://endpoint.com -> https://bucket.endpoint.com
        if endpoint.startswith("https://"):
            options["aws_endpoint"] = f"https://{bucket}.{endpoint[8:]}"
        elif endpoint.startswith("http://"):
            options["aws_endpoint"] = f"http://{bucket}.{endpoint[7:]}"
        else:
            options["aws_endpoint"] = f"https://{bucket}.{endpoint}"
        options["aws_virtual_hosted_style_request"] = "true"

    return options


def is_remote_path(path: str) -> bool:
    """Check if a path is a remote URL (s3://, gs://, http://, etc.)."""
    if not path:
        return False
    return path.startswith(("s3://", "gs://", "http://", "https://", "az://"))


@contextmanager
def ensure_local_file(
    path: str,
    use_cache: bool = True,
) -> Generator[Path, None, None]:
    """Context manager that ensures a file is available locally.

    For local files, returns the path directly.
    For remote files, downloads to a temp/cache location.

    Args:
        path: Local path or remote URL.
        use_cache: If True, cache downloaded files for reuse.

    Yields:
        Path to the local file.
    """
    if not is_remote_path(path):
        local_path = Path(path)
        if not local_path.exists():
            raise FileNotFoundError(f"Local file not found: {path}")
        yield local_path
        return

    def _download(remote_url: str, target_path: Path) -> Path:
        if target_path.exists():
            try:
                if target_path.stat().st_size > 0:
                    logger.debug(f"Using cached file: {target_path}")
                    return target_path
                logger.warning(f"Cached file is empty, re-downloading: {target_path}")
                target_path.unlink()
            except OSError as e:
                logger.warning(f"Failed to stat cached file {target_path}: {e}")

        target_path.parent.mkdir(parents=True, exist_ok=True)
        logger.info(f"Downloading {remote_url} to {target_path}")

        if remote_url.startswith(("http://", "https://")):
            import requests

            with requests.get(remote_url, stream=True, timeout=300) as r:
                r.raise_for_status()
                with open(target_path, "wb") as local_file:
                    for chunk in r.iter_content(chunk_size=8 * 1024 * 1024):
                        if chunk:
                            local_file.write(chunk)
        else:
            import fsspec

            storage_options = get_s3_storage_options() if remote_url.startswith("s3://") else {}
            with fsspec.open(remote_url, "rb", **storage_options) as remote_file:
                with open(target_path, "wb") as local_file:
                    while True:
                        chunk = remote_file.read(8 * 1024 * 1024)
                        if not chunk:
                            break
                        local_file.write(chunk)

        logger.debug(f"Downloaded {remote_url} ({target_path.stat().st_size} bytes)")
        return target_path

    if use_cache:
        global _CACHE_DIR
        if _CACHE_DIR is None:
            cache_base = os.environ.get("NURION_CACHE_DIR", "/tmp/solstice_cache")
            _CACHE_DIR = Path(cache_base)
        _CACHE_DIR.mkdir(parents=True, exist_ok=True)
        url_hash = hashlib.md5(path.encode()).hexdigest()[:16]
        filename = Path(urlparse(path).path).name or "file"
        local_path = _CACHE_DIR / f"{url_hash}_{filename}"
        yield _download(path, local_path)
        return

    with tempfile.NamedTemporaryFile(
        suffix=Path(urlparse(path).path).suffix or ".tmp",
        delete=False,
    ) as tmp:
        tmp_path = Path(tmp.name)

    try:
        _download(path, tmp_path)
        yield tmp_path
    finally:
        if tmp_path.exists():
            tmp_path.unlink()


def clear_cache() -> None:
    """Clear the download cache."""
    import shutil

    cache_dir = Path(os.environ.get("NURION_CACHE_DIR", "/tmp/solstice_cache"))
    if cache_dir.exists():
        shutil.rmtree(cache_dir)
    cache_dir.mkdir(parents=True, exist_ok=True)
    logger.info(f"Cleared cache directory: {cache_dir}")
