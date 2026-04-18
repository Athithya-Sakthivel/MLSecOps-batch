#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any

import yaml

ROOT = Path.cwd()
OUT_DIR = ROOT / "src" / "manifests" / "cloudflared"

NAMESPACE = os.getenv("NAMESPACE", "inference")
IMAGE = os.getenv("IMAGE", "cloudflare/cloudflared:2026.3.0")
REPLICAS = int(os.getenv("REPLICAS", "2"))
METRICS_PORT = int(os.getenv("METRICS_PORT", "2000"))
TUNNEL_PROTOCOL = os.getenv("TUNNEL_PROTOCOL", "http2").strip().lower()

DOMAIN = os.getenv("DOMAIN")
if not DOMAIN:
    print("ERROR: DOMAIN is required", file=sys.stderr)
    sys.exit(2)

if TUNNEL_PROTOCOL not in {"auto", "http2", "quic"}:
    print("ERROR: TUNNEL_PROTOCOL must be one of: auto, http2, quic", file=sys.stderr)
    sys.exit(2)

TUNNEL_NAME = os.getenv("CLOUDFLARE_TUNNEL_NAME", "tabular-api-tunnel")
SECRET_NAME = os.getenv("CLOUDFLARE_SECRET_NAME", "cloudflared-token")
SECRET_KEY = os.getenv("CLOUDFLARE_SECRET_KEY", "token")
TOKEN = os.getenv("CLOUDFLARE_TUNNEL_TOKEN")

AUTH_UPSTREAM = os.getenv("AUTH_UPSTREAM", "http://auth-svc.inference.svc.cluster.local:8000")
PREDICT_UPSTREAM = os.getenv(
    "PREDICT_UPSTREAM",
    "http://tabular-inference-serve-svc.inference.svc.cluster.local:8000",
)


def to_yaml(obj: Any) -> str:
    return yaml.safe_dump(obj, sort_keys=False, default_flow_style=False, width=120)


def sha256_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def sha256_obj(obj: Any) -> str:
    payload = json.dumps(obj, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def safe_write(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(content, encoding="utf-8")
    tmp.replace(path)


def run(cmd: list[str], stdin: str | None = None) -> None:
    subprocess.run(cmd, input=stdin, text=True, check=True)


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


def render_secret(namespace: str, name: str, key: str, token: str) -> dict[str, Any]:
    return {
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
        "stringData": {key: token},
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
            {"hostname": auth_host, "service": auth_upstream},
            {"hostname": predict_host, "service": predict_upstream},
            {"service": "http_status:404"},
        ],
    }


def render_deployment(
    namespace: str,
    image: str,
    replicas: int,
    metrics_port: int,
    secret_name: str,
    secret_key: str,
    checksum: str,
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
                        "cloudflared/config-checksum": checksum,
                    },
                },
                "spec": {
                    "serviceAccountName": "cloudflared-sa",
                    "terminationGracePeriodSeconds": 30,
                    "securityContext": {
                        "sysctls": [
                            {
                                "name": "net.ipv4.ping_group_range",
                                "value": "65532 65532",
                            }
                        ]
                    },
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
                                "--protocol",
                                TUNNEL_PROTOCOL,
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
                            "ports": [{"name": "metrics", "containerPort": metrics_port}],
                            "resources": {
                                "requests": {"cpu": "50m", "memory": "64Mi"},
                                "limits": {"cpu": "200m", "memory": "256Mi"},
                            },
                            "readinessProbe": {
                                "httpGet": {"path": "/ready", "port": metrics_port},
                                "initialDelaySeconds": 10,
                                "periodSeconds": 10,
                            },
                            "livenessProbe": {
                                "httpGet": {"path": "/ready", "port": metrics_port},
                                "initialDelaySeconds": 15,
                                "periodSeconds": 15,
                            },
                            "securityContext": {
                                "allowPrivilegeEscalation": False,
                                "readOnlyRootFilesystem": True,
                                "runAsNonRoot": True,
                                "runAsUser": 65532,
                                "runAsGroup": 65532,
                                "capabilities": {"drop": ["ALL"]},
                            },
                        }
                    ],
                },
            },
        },
    }


def build() -> tuple[list[dict[str, Any]], str]:
    auth_host = f"auth.api.{DOMAIN}"
    predict_host = f"predict.api.{DOMAIN}"

    routes = render_routes(
        TUNNEL_NAME,
        auth_host,
        predict_host,
        AUTH_UPSTREAM,
        PREDICT_UPSTREAM,
    )

    checksum_source = {
        "namespace": NAMESPACE,
        "image": IMAGE,
        "replicas": REPLICAS,
        "metrics_port": METRICS_PORT,
        "protocol": TUNNEL_PROTOCOL,
        "tunnel_name": TUNNEL_NAME,
        "secret_name": SECRET_NAME,
        "secret_key": SECRET_KEY,
        "token_hash": sha256_text(TOKEN or ""),
        "domain": DOMAIN,
        "auth_upstream": AUTH_UPSTREAM,
        "predict_upstream": PREDICT_UPSTREAM,
        "auth_host": auth_host,
        "predict_host": predict_host,
        "routes": routes,
        "icmp_sysctl": "net.ipv4.ping_group_range=65532 65532",
    }

    checksum = sha256_obj(checksum_source)

    docs = [
        render_namespace(NAMESPACE),
        render_serviceaccount(NAMESPACE),
        render_deployment(
            NAMESPACE,
            IMAGE,
            REPLICAS,
            METRICS_PORT,
            SECRET_NAME,
            SECRET_KEY,
            checksum,
        ),
        routes,
    ]
    rendered = "\n---\n".join(to_yaml(doc).rstrip() for doc in docs) + "\n"
    return docs, rendered


def write_manifests(docs: list[dict[str, Any]]) -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    safe_write(OUT_DIR / "00-namespace.yaml", to_yaml(docs[0]))
    safe_write(OUT_DIR / "01-serviceaccount.yaml", to_yaml(docs[1]))
    safe_write(OUT_DIR / "02-deployment.yaml", to_yaml(docs[2]))
    safe_write(OUT_DIR / "03-routes-reference.yaml", to_yaml(docs[3]))


def apply_rollout(docs: list[dict[str, Any]]) -> None:
    payload_docs = docs[:3]
    if TOKEN:
        payload_docs.insert(2, render_secret(NAMESPACE, SECRET_NAME, SECRET_KEY, TOKEN))
    payload = "\n---\n".join(to_yaml(doc).rstrip() for doc in payload_docs) + "\n"
    run(["kubectl", "apply", "-f", "-"], stdin=payload)


def destroy() -> None:
    run(
        [
            "kubectl",
            "delete",
            "deployment/cloudflared",
            "serviceaccount/cloudflared-sa",
            "-n",
            NAMESPACE,
            "--ignore-not-found=true",
        ]
    )
    if TOKEN:
        run(
            [
                "kubectl",
                "delete",
                "secret",
                SECRET_NAME,
                "-n",
                NAMESPACE,
                "--ignore-not-found=true",
            ]
        )


def main() -> None:
    parser = argparse.ArgumentParser()
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--rollout", action="store_true")
    group.add_argument("--destroy", action="store_true")
    args = parser.parse_args()

    docs, rendered = build()

    if not args.rollout and not args.destroy:
        sys.stdout.write(rendered)
        return

    if args.destroy:
        destroy()
        return

    write_manifests(docs)
    apply_rollout(docs)


if __name__ == "__main__":
    main()