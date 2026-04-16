#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import os
import sys
from pathlib import Path
from typing import Any

import yaml

ROOT = Path.cwd()
DEFAULT_OUT_DIR = ROOT / "src" / "manifests" / "cloudflared"

DEFAULT_NAMESPACE = "inference"
DEFAULT_IMAGE = "cloudflare/cloudflared:2026.2.0"
DEFAULT_REPLICAS = 2
DEFAULT_METRICS_PORT = 2000

DEFAULT_TUNNEL_NAME = "tabular-api-tunnel"
DEFAULT_AUTH_UPSTREAM = "http://auth-svc.inference.svc.cluster.local:8000"
DEFAULT_PREDICT_UPSTREAM = "http://tabular-inference-serve-svc.inference.svc.cluster.local:8000"

def die(msg: str) -> None:
    print(f"ERROR: {msg}", file=sys.stderr)
    sys.exit(2)


def info(msg: str) -> None:
    print(f"INFO: {msg}")


def env(name: str, default: str | None = None, required: bool = False) -> str:
    value = os.getenv(name, default or "").strip()
    if required and not value:
        die(f"{name} is required")
    return value


def sha256_str(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def safe_write(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(content, encoding="utf-8")
    tmp.replace(path)
    try:
        path.chmod(0o644)
    except Exception:
        pass


def to_yaml(obj: Any) -> str:
    return yaml.safe_dump(
        obj,
        sort_keys=False,
        default_flow_style=False,
        width=120,
    )


def render_namespace(namespace: str) -> dict[str, Any]:
    return {
        "apiVersion": "v1",
        "kind": "Namespace",
        "metadata": {
            "name": namespace,
            "labels": {
                "app.kubernetes.io/name": "cloudflared",
                "app.kubernetes.io/managed-by": "infrastructure-generator",
            },
        },
    }


def render_secret_token(namespace: str, token: str) -> dict[str, Any]:
    return {
        "apiVersion": "v1",
        "kind": "Secret",
        "metadata": {
            "name": "cloudflared-token",
            "namespace": namespace,
            "labels": {
                "app.kubernetes.io/name": "cloudflared",
                "app.kubernetes.io/component": "tunnel-token",
            },
        },
        "type": "Opaque",
        "stringData": {
            "token": token,
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
                    "annotations": {},
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
                                            "name": "cloudflared-token",
                                            "key": "token",
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
                                "timeoutSeconds": 3,
                                "failureThreshold": 3,
                            },
                            "livenessProbe": {
                                "httpGet": {
                                    "path": "/ready",
                                    "port": metrics_port,
                                },
                                "initialDelaySeconds": 15,
                                "periodSeconds": 15,
                                "timeoutSeconds": 3,
                                "failureThreshold": 3,
                            },
                            "securityContext": {
                                "allowPrivilegeEscalation": False,
                                "capabilities": {
                                    "drop": ["ALL"],
                                },
                                "readOnlyRootFilesystem": True,
                                "runAsNonRoot": True,
                                "runAsUser": 65532,
                                "runAsGroup": 65532,
                            },
                        }
                    ],
                },
            },
        },
    }


def render_routes_reference(
    tunnel_name: str,
    auth_hostname: str,
    predict_hostname: str,
    auth_upstream: str,
    predict_upstream: str,
) -> dict[str, Any]:
    # Reference-only route map.
    # The runtime tunnel is token-based; this file keeps the host/service
    # mapping versioned beside the Kubernetes manifests for review and sync.
    return {
        "tunnel": tunnel_name,
        "ingress": [
            {
                "hostname": auth_hostname,
                "service": auth_upstream,
            },
            {
                "hostname": predict_hostname,
                "service": predict_upstream,
            },
            {
                "service": "http_status:404",
            },
        ],
    }


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Generate Cloudflared Kubernetes manifests for the tabular MLOps backend."
    )
    parser.add_argument("--root", default=str(ROOT))
    parser.add_argument("--out-dir", default="")
    parser.add_argument("--namespace", default=DEFAULT_NAMESPACE)
    parser.add_argument("--image", default=DEFAULT_IMAGE)
    parser.add_argument("--replicas", type=int, default=DEFAULT_REPLICAS)
    parser.add_argument("--metrics-port", type=int, default=DEFAULT_METRICS_PORT)
    parser.add_argument("--domain", default=env("DOMAIN", required=True), help="Root domain, e.g. example.com")
    parser.add_argument("--tunnel-name", default=env("CLOUDFLARE_TUNNEL_NAME", default=DEFAULT_TUNNEL_NAME))
    parser.add_argument("--auth-upstream", default=env("AUTH_UPSTREAM", default=DEFAULT_AUTH_UPSTREAM))
    parser.add_argument("--predict-upstream", default=env("PREDICT_UPSTREAM", default=DEFAULT_PREDICT_UPSTREAM))
    parser.add_argument("--token", default=env("CLOUDFLARE_TUNNEL_TOKEN", required=True))
    parser.add_argument("--write", action="store_true", help="Write manifests to disk")
    args = parser.parse_args()

    root = Path(args.root).resolve()
    out_dir = Path(args.out_dir).resolve() if args.out_dir else (root / "src" / "manifests" / "cloudflared")

    auth_hostname = f"auth.api.{args.domain}"
    predict_hostname = f"predict.api.{args.domain}"

    namespace_doc = render_namespace(args.namespace)
    secret_doc = render_secret_token(args.namespace, args.token)
    sa_doc = render_serviceaccount(args.namespace)
    deploy_doc = render_deployment(
        namespace=args.namespace,
        image=args.image,
        replicas=args.replicas,
        metrics_port=args.metrics_port,
    )
    routes_doc = render_routes_reference(
        tunnel_name=args.tunnel_name,
        auth_hostname=auth_hostname,
        predict_hostname=predict_hostname,
        auth_upstream=args.auth_upstream,
        predict_upstream=args.predict_upstream,
    )

    namespace_yaml = to_yaml(namespace_doc)
    secret_yaml = to_yaml(secret_doc)
    sa_yaml = to_yaml(sa_doc)
    deploy_doc["spec"]["template"]["metadata"]["annotations"] = {
        "cloudflared/routes-checksum": sha256_str(to_yaml(routes_doc))
    }
    deploy_yaml = to_yaml(deploy_doc)
    routes_yaml = to_yaml(routes_doc)

    if args.write:
        safe_write(out_dir / "00-namespace.yaml", namespace_yaml)
        safe_write(out_dir / "01-secret-cloudflared-token.yaml", secret_yaml)
        safe_write(out_dir / "02-serviceaccount.yaml", sa_yaml)
        safe_write(out_dir / "03-deployment-cloudflared.yaml", deploy_yaml)
        safe_write(out_dir / "04-routes-reference.yaml", routes_yaml)
        info(f"Wrote manifests to {out_dir}")
        return

    print(f"# {out_dir / '00-namespace.yaml'}")
    print(namespace_yaml)
    print("---")
    print(f"# {out_dir / '01-secret-cloudflared-token.yaml'}")
    print(secret_yaml)
    print("---")
    print(f"# {out_dir / '02-serviceaccount.yaml'}")
    print(sa_yaml)
    print("---")
    print(f"# {out_dir / '03-deployment-cloudflared.yaml'}")
    print(deploy_yaml)
    print("---")
    print(f"# {out_dir / '04-routes-reference.yaml'}")
    print(routes_yaml)


if __name__ == "__main__":
    main()