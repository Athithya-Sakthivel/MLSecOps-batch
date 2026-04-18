#!/usr/bin/env python3
from __future__ import annotations

import hashlib
import os
import subprocess
import sys
from pathlib import Path
from typing import Any

import yaml

# ==========================================================
# Paths
# ==========================================================
ROOT = Path.cwd()
OUT_DIR = ROOT / "src" / "manifests" / "cloudflared"

# ==========================================================
# Environment
# ==========================================================
NAMESPACE = os.getenv("NAMESPACE", "inference")
IMAGE = os.getenv("IMAGE", "cloudflare/cloudflared:2026.3.0")
REPLICAS = int(os.getenv("REPLICAS", "2"))
METRICS_PORT = int(os.getenv("METRICS_PORT", "2000"))

DOMAIN = os.getenv("DOMAIN")
if not DOMAIN:
    print("ERROR: DOMAIN is required", file=sys.stderr)
    sys.exit(2)

TUNNEL_NAME = os.getenv("CLOUDFLARE_TUNNEL_NAME", "tabular-api-tunnel")
SECRET_NAME = os.getenv("CLOUDFLARE_SECRET_NAME", "cloudflared-token")
SECRET_KEY = os.getenv("CLOUDFLARE_SECRET_KEY", "token")

AUTH_UPSTREAM = os.getenv(
    "AUTH_UPSTREAM",
    "http://auth-svc.inference.svc.cluster.local:8000",
)

PREDICT_UPSTREAM = os.getenv(
    "PREDICT_UPSTREAM",
    "http://tabular-inference-serve-svc.inference.svc.cluster.local:8000",
)

TOKEN = os.getenv("CLOUDFLARE_TUNNEL_TOKEN")

WRITE = os.getenv("WRITE", "false").lower() in ("1", "true", "yes")
APPLY_SECRET = os.getenv("APPLY_SECRET", "true").lower() in ("1", "true", "yes")

# ==========================================================
# Helpers
# ==========================================================
def sha256_str(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def to_yaml(obj: Any) -> str:
    return yaml.safe_dump(
        obj,
        sort_keys=False,
        default_flow_style=False,
        width=120,
    )


def safe_write(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(content, encoding="utf-8")
    tmp.replace(path)


def run(cmd: list[str], stdin: str | None = None) -> None:
    subprocess.run(
        cmd,
        input=stdin,
        text=True,
        check=True,
    )


# ==========================================================
# Kubernetes Secret (direct apply)
# ==========================================================
def apply_secret(namespace: str, name: str, key: str, token: str) -> None:
    if not token:
        print("ERROR: CLOUDFLARE_TUNNEL_TOKEN required when APPLY_SECRET=true", file=sys.stderr)
        sys.exit(2)

    secret = {
        "apiVersion": "v1",
        "kind": "Secret",
        "metadata": {
            "name": name,
            "namespace": namespace,
            "labels": {
                "app.kubernetes.io/name": "cloudflared",
                "app.kubernetes.io/component": "tunnel-token",
            },
        },
        "type": "Opaque",
        "stringData": {
            key: token,
        },
    }

    print(f"INFO: applying secret {namespace}/{name}")
    run(["kubectl", "apply", "-f", "-"], stdin=to_yaml(secret))


# ==========================================================
# Manifest renderers
# ==========================================================
def render_namespace(namespace: str) -> dict[str, Any]:
    return {
        "apiVersion": "v1",
        "kind": "Namespace",
        "metadata": {
            "name": namespace,
            "labels": {
                "app.kubernetes.io/name": "cloudflared",
                "app.kubernetes.io/managed-by": "generator",
            },
        },
    }


def render_serviceaccount(namespace: str) -> dict[str, Any]:
    return {
        "apiVersion": "v1",
        "kind": "ServiceAccount",
        "metadata": {
            "name": "cloudflared-sa",
            "namespace": namespace,
            "labels": {
                "app.kubernetes.io/name": "cloudflared",
                "app.kubernetes.io/component": "tunnel",
            },
        },
    }


def render_deployment(
    namespace: str,
    image: str,
    replicas: int,
    metrics_port: int,
    secret_name: str,
    secret_key: str,
    routes_checksum: str,
) -> dict[str, Any]:
    return {
        "apiVersion": "apps/v1",
        "kind": "Deployment",
        "metadata": {
            "name": "cloudflared",
            "namespace": namespace,
            "labels": {
                "app.kubernetes.io/name": "cloudflared",
                "app.kubernetes.io/component": "tunnel",
            },
        },
        "spec": {
            "replicas": replicas,
            "selector": {
                "matchLabels": {
                    "app.kubernetes.io/name": "cloudflared",
                    "app.kubernetes.io/component": "tunnel",
                }
            },
            "template": {
                "metadata": {
                    "labels": {
                        "app.kubernetes.io/name": "cloudflared",
                        "app.kubernetes.io/component": "tunnel",
                    },
                    "annotations": {
                        "cloudflared/routes-checksum": routes_checksum
                    },
                },
                "spec": {
                    "serviceAccountName": "cloudflared-sa",
                    "terminationGracePeriodSeconds": 30,
                    "containers": [
                        {
                            "name": "cloudflared",
                            "image": image,
                            "imagePullPolicy": "IfNotPresent",
                            "command": ["cloudflared"],
                            "args": [
                                "tunnel",
                                "--no-autoupdate",
                                "--loglevel",
                                "info",
                                "--metrics",
                                f"0.0.0.0:{metrics_port}",
                                "run",
                            ],
                            "env": [
                                {
                                    "name": "TUNNEL_TOKEN",
                                    "valueFrom": {
                                        "secretKeyRef": {
                                            "name": secret_name,
                                            "key": secret_key,
                                        }
                                    },
                                }
                            ],
                            "ports": [
                                {
                                    "name": "metrics",
                                    "containerPort": metrics_port,
                                }
                            ],
                            "resources": {
                                "requests": {
                                    "cpu": "50m",
                                    "memory": "64Mi",
                                },
                                "limits": {
                                    "cpu": "200m",
                                    "memory": "256Mi",
                                },
                            },
                            "readinessProbe": {
                                "httpGet": {
                                    "path": "/ready",
                                    "port": metrics_port,
                                },
                                "initialDelaySeconds": 10,
                                "periodSeconds": 10,
                            },
                            "livenessProbe": {
                                "httpGet": {
                                    "path": "/ready",
                                    "port": metrics_port,
                                },
                                "initialDelaySeconds": 15,
                                "periodSeconds": 15,
                            },
                            "securityContext": {
                                "allowPrivilegeEscalation": False,
                                "readOnlyRootFilesystem": True,
                                "runAsNonRoot": True,
                                "runAsUser": 65532,
                                "runAsGroup": 65532,
                                "capabilities": {
                                    "drop": ["ALL"]
                                },
                            },
                        }
                    ],
                },
            },
        },
    }


def render_routes(
    tunnel_name: str,
    auth_host: str,
    predict_host: str,
    auth_upstream: str,
    predict_upstream: str,
) -> dict[str, Any]:
    return {
        "tunnel": tunnel_name,
        "ingress": [
            {
                "hostname": auth_host,
                "service": auth_upstream,
            },
            {
                "hostname": predict_host,
                "service": predict_upstream,
            },
            {
                "service": "http_status:404",
            },
        ],
    }


# ==========================================================
# Main
# ==========================================================
def main() -> None:
    auth_host = f"auth.api.{DOMAIN}"
    predict_host = f"predict.api.{DOMAIN}"

    routes = render_routes(
        TUNNEL_NAME,
        auth_host,
        predict_host,
        AUTH_UPSTREAM,
        PREDICT_UPSTREAM,
    )

    routes_yaml = to_yaml(routes)
    checksum = sha256_str(routes_yaml)

    namespace_doc = render_namespace(NAMESPACE)
    sa_doc = render_serviceaccount(NAMESPACE)

    deploy_doc = render_deployment(
        NAMESPACE,
        IMAGE,
        REPLICAS,
        METRICS_PORT,
        SECRET_NAME,
        SECRET_KEY,
        checksum,
    )

    namespace_yaml = to_yaml(namespace_doc)
    sa_yaml = to_yaml(sa_doc)
    deploy_yaml = to_yaml(deploy_doc)

    # ----------------------------------------------
    # Direct secret apply (no rendered secret file)
    # ----------------------------------------------
    if APPLY_SECRET:
        apply_secret(
            NAMESPACE,
            SECRET_NAME,
            SECRET_KEY,
            TOKEN or "",
        )

    # ----------------------------------------------
    # Output manifests
    # ----------------------------------------------
    if WRITE:
        safe_write(OUT_DIR / "00-namespace.yaml", namespace_yaml)
        safe_write(OUT_DIR / "01-serviceaccount.yaml", sa_yaml)
        safe_write(OUT_DIR / "02-deployment.yaml", deploy_yaml)
        safe_write(OUT_DIR / "03-routes-reference.yaml", routes_yaml)
        print(f"INFO: wrote manifests to {OUT_DIR}")
    else:
        print("---")
        print(namespace_yaml)
        print("---")
        print(sa_yaml)
        print("---")
        print(deploy_yaml)
        print("---")
        print(routes_yaml)


if __name__ == "__main__":
    main()