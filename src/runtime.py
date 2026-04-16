import os
from pathlib import Path
from urllib.parse import urlsplit, urlparse


LOCAL_PROXY_BYPASS_HOSTS = {"127.0.0.1", "localhost", "::1"}


def bootstrap_dependency_path(base_dir: Path) -> None:
    deps_dir = base_dir / "deps"
    if not deps_dir.is_dir():
        return

    deps_path = str(deps_dir.resolve())
    current_entries = [entry for entry in os.environ.get("PATH", "").split(os.pathsep) if entry]
    normalized_entries = {os.path.normcase(os.path.normpath(entry)) for entry in current_entries}
    if os.path.normcase(os.path.normpath(deps_path)) in normalized_entries:
        return

    os.environ["PATH"] = os.pathsep.join([deps_path, *current_entries]) if current_entries else deps_path


def normalize_proxy(proxy: str | None) -> str | None:
    if not proxy:
        return None
    proxy = proxy.strip()
    return proxy or None


def http_proxy_kwargs(proxy: str | None) -> dict:
    resolved_proxy = normalize_proxy(proxy)
    return {"proxy": resolved_proxy} if resolved_proxy else {}


def validate_grpc_proxy(proxy: str | None) -> str | None:
    resolved_proxy = normalize_proxy(proxy)
    if not resolved_proxy:
        return None

    parsed = urlparse(resolved_proxy)
    if parsed.scheme != "http" or not parsed.hostname or not parsed.port:
        raise ValueError("instance.proxy must be an http://host:port proxy URI")
    return resolved_proxy


def configure_grpc_proxy_environment(target: str, proxy: str | None) -> str | None:
    resolved_proxy = validate_grpc_proxy(proxy)
    if resolved_proxy:
        os.environ["grpc_proxy"] = resolved_proxy

    host = urlsplit(f"//{target}").hostname
    if host and host.lower() in LOCAL_PROXY_BYPASS_HOSTS:
        _append_env_list("no_grpc_proxy", host)
        _append_env_list("no_proxy", host)

    return resolved_proxy


def _append_env_list(env_name: str, value: str) -> None:
    existing = [item.strip() for item in os.environ.get(env_name, "").split(",") if item.strip()]
    normalized_existing = {item.lower() for item in existing}
    if value.lower() in normalized_existing:
        return
    existing.append(value)
    os.environ[env_name] = ",".join(existing)
