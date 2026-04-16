
#!/usr/bin/env python3
from __future__ import annotations

import base64
import contextlib
import copy
import hashlib
import json
import os
import shutil
import subprocess
import sys
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import yaml

# -----------------------------------------------------------------------------
# Environment
# -----------------------------------------------------------------------------

MANIFEST_DIR = Path(os.environ.get("MANIFEST_DIR", "src/manifests/flyte"))
VALUES_FILE = Path(os.environ.get("VALUES_FILE", str(MANIFEST_DIR / "values.yaml")))
STATE_CONFIGMAP = os.environ.get("STATE_CONFIGMAP", "flyte-bootstrap-state")

TARGET_NS = os.environ.get("TARGET_NS", "flyte")
POSTGRES_NS = os.environ.get("POSTGRES_NS", "default")
CNPG_CLUSTER = os.environ.get("CNPG_CLUSTER", "postgres-cluster")
POOLER_SERVICE = os.environ.get("POOLER_SERVICE", "postgres-pooler")
DB_ACCESS_MODE = os.environ.get("DB_ACCESS_MODE", "rw").strip().lower()
DB_HOST = os.environ.get("DB_HOST", "").strip()
POOLER_PORT = int(os.environ.get("POOLER_PORT", "5432"))

DB_SECRET_NAME = os.environ.get("DB_SECRET_NAME", "db-pass")
AUTH_SECRET_NAME = os.environ.get("AUTH_SECRET_NAME", "flyte-secret-auth")
STORAGE_SECRET_NAME = os.environ.get("STORAGE_SECRET_NAME", "flyte-storage-config")
TASK_AWS_SECRET_NAME = os.environ.get("TASK_AWS_SECRET_NAME", "flyte-aws-credentials")
TASK_SERVICE_ACCOUNT = os.environ.get("TASK_SERVICE_ACCOUNT", "flyte-task").strip() or "flyte-task"

FLYTE_ADMIN_DB = os.environ.get("FLYTE_ADMIN_DB", "flyteadmin")
FLYTE_DATACATALOG_DB = os.environ.get("FLYTE_DATACATALOG_DB", "datacatalog")
FLYTE_OAUTH_CLIENT_ID = os.environ.get("FLYTE_OAUTH_CLIENT_ID", "flytepropeller")
FLYTE_OAUTH_CLIENT_SECRET = os.environ.get("FLYTE_OAUTH_CLIENT_SECRET", "flytepropeller-secret")

STORAGE_PROVIDER = os.environ.get("STORAGE_PROVIDER", "auto").strip().lower()
USE_IAM = os.environ.get("USE_IAM", "false").lower() in {"1", "true", "yes", "y", "on"}
K8S_CLUSTER = os.environ.get("K8S_CLUSTER", "auto").strip().lower()

AWS_REGION = os.environ.get("AWS_REGION", "ap-south-1")
S3_BUCKET = os.environ.get("S3_BUCKET", "s3-temp-bucket-mlsecops-681802563986")
S3_PREFIX = os.environ.get("S3_PREFIX", "").strip("/")
S3_ENDPOINT = os.environ.get("S3_ENDPOINT", "").strip()
AWS_ACCESS_KEY_ID = os.environ.get("AWS_ACCESS_KEY_ID", "").strip()
AWS_SECRET_ACCESS_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY", "").strip()
AWS_SESSION_TOKEN = os.environ.get("AWS_SESSION_TOKEN", "").strip()
AWS_ROLE_ARN = os.environ.get("AWS_ROLE_ARN", "").strip()
FLYTE_CONTROL_PLANE_ROLE_ARN = os.environ.get("FLYTE_CONTROL_PLANE_ROLE_ARN", "").strip()
FLYTE_TASK_ROLE_ARN = os.environ.get("FLYTE_TASK_ROLE_ARN", "").strip()

FLYTE_INGRESS_ENABLED = os.environ.get("FLYTE_INGRESS_ENABLED", "false")
FLYTE_INGRESS_CLASS_NAME = os.environ.get("FLYTE_INGRESS_CLASS_NAME", "")
FLYTE_SEPARATE_GRPC_INGRESS = os.environ.get("FLYTE_SEPARATE_GRPC_INGRESS", "false")

FLYTE_CLUSTER_RESOURCE_MANAGER_ENABLED = os.environ.get("FLYTE_CLUSTER_RESOURCE_MANAGER_ENABLED", "true")
FLYTE_WORKFLOW_SCHEDULER_ENABLED = os.environ.get("FLYTE_WORKFLOW_SCHEDULER_ENABLED", "false")
FLYTE_WORKFLOW_NOTIFICATIONS_ENABLED = os.environ.get("FLYTE_WORKFLOW_NOTIFICATIONS_ENABLED", "false")
FLYTE_EXTERNAL_EVENTS_ENABLED = os.environ.get("FLYTE_EXTERNAL_EVENTS_ENABLED", "false")
FLYTE_CLOUD_EVENTS_ENABLED = os.environ.get("FLYTE_CLOUD_EVENTS_ENABLED", "false")
FLYTE_CONNECTOR_ENABLED = os.environ.get("FLYTE_CONNECTOR_ENABLED", "false")
FLYTE_DASK_OPERATOR_ENABLED = os.environ.get("FLYTE_DASK_OPERATOR_ENABLED", "false")
FLYTE_SPARK_OPERATOR_ENABLED = os.environ.get("FLYTE_SPARK_OPERATOR_ENABLED", "true")
FLYTE_DATABRICKS_ENABLED = os.environ.get("FLYTE_DATABRICKS_ENABLED", "false")

CHART_REPO_NAME = os.environ.get("CHART_REPO_NAME", "flyte")
CHART_REPO_URL = os.environ.get("CHART_REPO_URL", "https://flyteorg.github.io/flyte")
CHART_NAME = os.environ.get("CHART_NAME", "flyte-core")
CHART_VERSION = os.environ.get("CHART_VERSION", "1.16.4")
RELEASE_NAME = os.environ.get("RELEASE_NAME", "flyte")

READY_TIMEOUT = int(os.environ.get("READY_TIMEOUT", "1200"))
ROLLOUT_TIMEOUT = os.environ.get("ROLLOUT_TIMEOUT", "1200s")
DNS_TIMEOUT = int(os.environ.get("DNS_TIMEOUT", "300"))
FLYTE_ATOMIC = os.environ.get("FLYTE_ATOMIC", "false").lower() in {"1", "true", "yes", "y", "on"}

DELETE_TARGET_NAMESPACE = os.environ.get("DELETE_TARGET_NAMESPACE", "true").lower() in {"1", "true", "yes", "y", "on"}
DELETE_TASK_NAMESPACES = os.environ.get("DELETE_TASK_NAMESPACES", "false").lower() in {"1", "true", "yes", "y", "on"}
DELETE_FLYTE_CRDS = os.environ.get("DELETE_FLYTE_CRDS", "true").lower() in {"1", "true", "yes", "y", "on"}
FORCE_FINALIZER_REMOVAL = os.environ.get("FORCE_FINALIZER_REMOVAL", "true").lower() in {"1", "true", "yes", "y", "on"}

APP_DB_USER = ""
APP_DB_PASSWORD = ""

MANAGED_LABEL_SELECTORS = [
    f"app.kubernetes.io/instance={RELEASE_NAME}",
    "app.kubernetes.io/part-of=flyte",
]

FLYTE_CR_RESOURCES = [
    "workflows.flyte.org",
    "launchplans.flyte.org",
    "tasks.flyte.org",
    "workflows.flyte.io",
    "launchplans.flyte.io",
    "tasks.flyte.io",
]

CLUSTER_MANAGED_RESOURCES = [
    "clusterroles",
    "clusterrolebindings",
    "mutatingwebhookconfigurations",
    "validatingwebhookconfigurations",
]

VALID_CLUSTER_MODES = {"auto", "kind", "eks"}
VALID_PROVIDERS = {"auto", "aws", "sandbox"}


def ts() -> str:
    return datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def log(msg: str) -> None:
    print(f"[{ts()}] [flyte] {msg}", file=sys.stderr, flush=True)


def fatal(msg: str) -> None:
    print(f"[{ts()}] [flyte][FATAL] {msg}", file=sys.stderr, flush=True)
    raise SystemExit(1)


def require_bin(name: str) -> None:
    if shutil.which(name) is None:
        fatal(f"{name} required in PATH")


def run(
    cmd: list[str],
    *,
    input_text: str | None = None,
    check: bool = True,
    env: dict[str, str] | None = None,
    cwd: str | Path | None = None,
) -> subprocess.CompletedProcess[str]:
    cp = subprocess.run(
        cmd,
        input=input_text,
        text=True,
        capture_output=True,
        check=False,
        env=env,
        cwd=cwd,
        close_fds=True,
    )
    if check and cp.returncode != 0:
        detail: list[str] = []
        if cp.stdout:
            detail.append(f"stdout:\n{cp.stdout.rstrip()}")
        if cp.stderr:
            detail.append(f"stderr:\n{cp.stderr.rstrip()}")
        raise RuntimeError(
            f"command failed ({cp.returncode}): {' '.join(cmd)}"
            + (f"\n{chr(10).join(detail)}" if detail else "")
        )
    return cp


def run_text(cmd: list[str], *, input_text: str | None = None, check: bool = True) -> str:
    return run(cmd, input_text=input_text, check=check).stdout.strip()


def yaml_bool(value: str | bool) -> bool:
    if isinstance(value, bool):
        return value
    return str(value).lower() in {"1", "true", "yes", "y", "on"}


def split_namespaces(value: str) -> list[str]:
    out: list[str] = []
    for part in value.replace(",", " ").replace(";", " ").split():
        part = part.strip()
        if part:
            out.append(part)
    return out


def join_uri_prefix(scheme: str, bucket_or_container: str, prefix: str = "") -> str:
    prefix = prefix.strip("/")
    if prefix:
        return f"{scheme}://{bucket_or_container}/{prefix}/"
    return f"{scheme}://{bucket_or_container}/"


def canonicalize(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(k): canonicalize(value[k]) for k in sorted(value)}
    if isinstance(value, list):
        return [canonicalize(v) for v in value]
    if isinstance(value, tuple):
        return [canonicalize(v) for v in value]
    return value


def stable_hash(obj: Any) -> str:
    data = json.dumps(canonicalize(obj), sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(data.encode("utf-8")).hexdigest()


def apply_manifest(manifest: dict[str, Any]) -> None:
    run(["kubectl", "apply", "-f", "-"], input_text=yaml.safe_dump(manifest, sort_keys=False))


def ensure_namespace(namespace: str) -> None:
    apply_manifest(
        {
            "apiVersion": "v1",
            "kind": "Namespace",
            "metadata": {"name": namespace, "labels": {"app.kubernetes.io/part-of": "flyte"}},
        }
    )


def ensure_default_serviceaccount(namespace: str) -> None:
    apply_manifest(
        {
            "apiVersion": "v1",
            "kind": "ServiceAccount",
            "metadata": {"name": "default", "namespace": namespace},
        }
    )


def patch_default_serviceaccount_annotation(namespace: str, key: str, value: str) -> None:
    if not key or not value:
        return
    ensure_default_serviceaccount(namespace)
    run(["kubectl", "-n", namespace, "annotate", "sa", "default", f"{key}={value}", "--overwrite"], check=True)


def ensure_serviceaccount(namespace: str, name: str, annotations: dict[str, str] | None = None) -> None:
    apply_manifest(
        {
            "apiVersion": "v1",
            "kind": "ServiceAccount",
            "metadata": {"name": name, "namespace": namespace, "annotations": annotations or {}},
        }
    )


def ensure_secret(namespace: str, name: str, string_data: dict[str, str]) -> None:
    apply_manifest(
        {
            "apiVersion": "v1",
            "kind": "Secret",
            "metadata": {"name": name, "namespace": namespace},
            "type": "Opaque",
            "stringData": string_data,
        }
    )


def ensure_configmap(
    namespace: str,
    name: str,
    data: dict[str, str],
    labels: dict[str, str] | None = None,
    annotations: dict[str, str] | None = None,
) -> None:
    apply_manifest(
        {
            "apiVersion": "v1",
            "kind": "ConfigMap",
            "metadata": {"name": name, "namespace": namespace, "labels": labels or {}, "annotations": annotations or {}},
            "data": data,
        }
    )


def secret_value(namespace: str, secret_name: str, key: str) -> str:
    cp = run(["kubectl", "-n", namespace, "get", "secret", secret_name, "-o", f"jsonpath={{.data.{key}}}"], check=False)
    if cp.returncode != 0 or not cp.stdout.strip():
        return ""
    try:
        return base64.b64decode(cp.stdout.strip()).decode("utf-8")
    except Exception:
        return ""


def _jsonpath_value(cmd: list[str]) -> str:
    cp = run(cmd, check=False)
    if cp.returncode != 0:
        return ""
    return cp.stdout.strip()


def _positive_int(text: str) -> bool:
    try:
        return int(text) > 0
    except Exception:
        return False


def deployment_available(namespace: str, name: str) -> bool:
    return _positive_int(
        _jsonpath_value(["kubectl", "-n", namespace, "get", "deployment", name, "-o", "jsonpath={.status.availableReplicas}"])
    )


def daemonset_ready(namespace: str, name: str) -> bool:
    ready = _jsonpath_value(["kubectl", "-n", namespace, "get", "daemonset", name, "-o", "jsonpath={.status.numberReady}"])
    return _positive_int(ready)


def service_has_endpoints(namespace: str, name: str) -> bool:
    return bool(_jsonpath_value(["kubectl", "-n", namespace, "get", "endpoints", name, "-o", "jsonpath={.subsets[*].addresses[*].ip}"]))


def wait_for_service_endpoints(namespace: str, svc_name: str, max_wait: int = 180) -> None:
    elapsed = 0
    while True:
        cp = run(
            [
                "kubectl",
                "-n",
                namespace,
                "get",
                "endpoints",
                svc_name,
                "-o",
                "jsonpath={.subsets[*].addresses[*].ip}",
            ],
            check=False,
        )
        if cp.returncode == 0 and cp.stdout.strip():
            return
        time.sleep(5)
        elapsed += 5
        if elapsed >= max_wait:
            fatal(f"Service '{svc_name}' has no endpoints after {max_wait}s")


def ensure_dns_ready() -> None:
    log("waiting for cluster DNS")
    deadline = time.monotonic() + DNS_TIMEOUT
    while time.monotonic() < deadline:
        for name in ("coredns", "kube-dns"):
            if deployment_available("kube-system", name) or daemonset_ready("kube-system", name):
                log(f"cluster DNS ready via {name}")
                return
        for name in ("kube-dns", "coredns", "dns"):
            if service_has_endpoints("kube-system", name):
                log(f"cluster DNS ready via service/{name} endpoints")
                return
        time.sleep(2)
    fatal(f"DNS not ready after {DNS_TIMEOUT}s")


def normalize_cluster_mode() -> str:
    if K8S_CLUSTER in VALID_CLUSTER_MODES and K8S_CLUSTER != "auto":
        if K8S_CLUSTER == "kind" and USE_IAM:
            fatal("USE_IAM=true is only valid on EKS, not kind")
        return K8S_CLUSTER
    if K8S_CLUSTER not in {"", "auto"}:
        fatal(f"unsupported K8S_CLUSTER={K8S_CLUSTER}")
    if USE_IAM or AWS_ROLE_ARN or FLYTE_CONTROL_PLANE_ROLE_ARN or FLYTE_TASK_ROLE_ARN:
        return "eks"
    return "kind"


CLUSTER_MODE = normalize_cluster_mode()
EKS = CLUSTER_MODE == "eks"
KIND = CLUSTER_MODE == "kind"
EFFECTIVE_USE_IRSA = EKS and USE_IAM

if EFFECTIVE_USE_IRSA:
    for _k in (
        "AWS_ACCESS_KEY_ID",
        "AWS_SECRET_ACCESS_KEY",
        "AWS_SESSION_TOKEN",
        "AWS_SECURITY_TOKEN",
        "AWS_SESSION_TOKEN",
    ):
        os.environ.pop(_k, None)
    AWS_ACCESS_KEY_ID = ""
    AWS_SECRET_ACCESS_KEY = ""
    AWS_SESSION_TOKEN = ""

GENERAL_NODE_SELECTOR = {"node-type": "general"} if EKS else {}
COMPUTE_NODE_SELECTOR = {"node-type": "compute"} if EKS else {}
GENERAL_TOLERATIONS = (
    [{"key": "node-type", "operator": "Equal", "value": "general", "effect": "NoSchedule"}] if EKS else []
)
COMPUTE_TOLERATIONS = (
    [{"key": "node-type", "operator": "Equal", "value": "compute", "effect": "NoSchedule"}] if EKS else []
)


def detect_storage_provider() -> str:
    if STORAGE_PROVIDER in {"aws", "sandbox"}:
        return STORAGE_PROVIDER
    if CLUSTER_MODE == "kind" and not (AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY):
        return "sandbox"
    return "aws"


def resolve_role_arn(*, task: bool) -> str:
    candidates = [
        FLYTE_TASK_ROLE_ARN if task else FLYTE_CONTROL_PLANE_ROLE_ARN,
        AWS_ROLE_ARN,
        FLYTE_CONTROL_PLANE_ROLE_ARN if task else FLYTE_TASK_ROLE_ARN,
    ]
    for candidate in candidates:
        if candidate:
            return candidate
    return ""


def detect_identity_annotation(provider: str) -> tuple[str, str]:
    if provider == "aws" and EFFECTIVE_USE_IRSA:
        role_arn = resolve_role_arn(task=False)
        if not role_arn:
            fatal("FLYTE_CONTROL_PLANE_ROLE_ARN, FLYTE_TASK_ROLE_ARN, or AWS_ROLE_ARN is required when USE_IAM=true on EKS")
        return "eks.amazonaws.com/role-arn", role_arn
    return "", ""


def build_storage_block(provider: str) -> dict[str, Any]:
    if provider == "aws":
        return {
            "secretName": "",
            "type": "s3",
            "bucketName": S3_BUCKET,
            "s3": {
                "endpoint": S3_ENDPOINT,
                "region": AWS_REGION,
                "authType": "iam" if EFFECTIVE_USE_IRSA else "accesskey",
                "accessKey": "" if EFFECTIVE_USE_IRSA else AWS_ACCESS_KEY_ID,
                "secretKey": "" if EFFECTIVE_USE_IRSA else AWS_SECRET_ACCESS_KEY,
            },
            "custom": {},
            "enableMultiContainer": False,
            "limits": {"maxDownloadMBs": 10},
            "cache": {"maxSizeMBs": 0, "targetGCPercent": 70},
        }
    if provider == "sandbox":
        return {
            "secretName": "",
            "type": "sandbox",
            "bucketName": "",
            "s3": {"endpoint": "", "region": AWS_REGION, "authType": "iam", "accessKey": "", "secretKey": ""},
            "custom": {},
            "enableMultiContainer": False,
            "limits": {"maxDownloadMBs": 10},
            "cache": {"maxSizeMBs": 0, "targetGCPercent": 70},
        }
    fatal(f"unsupported storage provider {provider}")
    raise AssertionError("unreachable")


def build_k8s_block(provider: str) -> dict[str, Any]:
    default_env_vars: dict[str, str] = {}
    default_env_from_secrets: list[str] = []

    if provider == "aws" and not EFFECTIVE_USE_IRSA:
        default_env_from_secrets = [TASK_AWS_SECRET_NAME]
        default_env_vars = {
            "AWS_ACCESS_KEY_ID": AWS_ACCESS_KEY_ID,
            "AWS_SECRET_ACCESS_KEY": AWS_SECRET_ACCESS_KEY,
            "AWS_DEFAULT_REGION": AWS_REGION,
            "AWS_REGION": AWS_REGION,
            "FLYTE_AWS_ACCESS_KEY_ID": AWS_ACCESS_KEY_ID,
            "FLYTE_AWS_SECRET_ACCESS_KEY": AWS_SECRET_ACCESS_KEY,
        }
        if AWS_SESSION_TOKEN:
            default_env_vars["AWS_SESSION_TOKEN"] = AWS_SESSION_TOKEN
        if S3_ENDPOINT:
            default_env_vars["FLYTE_AWS_ENDPOINT"] = S3_ENDPOINT

    return {
        "plugins": {
            "k8s": {
                "default-cpus": "100m",
                "default-memory": "200Mi",
                "default-env-from-configmaps": [],
                "default-env-from-secrets": default_env_from_secrets,
                "default-env-vars": default_env_vars,
            }
        }
    }


def build_task_resource_defaults() -> dict[str, Any]:
    return {"task_resources": {"defaults": {"cpu": "500m", "memory": "512Mi"}, "limits": {"cpu": "32", "memory": "32Gi"}}}


def component_block(
    *,
    enabled: bool = True,
    service_type: str = "ClusterIP",
    role: str = "general",
    service_account: dict[str, Any] | None = None,
) -> dict[str, Any]:
    block: dict[str, Any] = {"enabled": enabled, "service": {"type": service_type}}
    if service_account is not None:
        block["serviceAccount"] = service_account
    if EKS:
        if role == "general":
            block["nodeSelector"] = copy.deepcopy(GENERAL_NODE_SELECTOR)
            block["tolerations"] = copy.deepcopy(GENERAL_TOLERATIONS)
        elif role == "compute":
            block["nodeSelector"] = copy.deepcopy(COMPUTE_NODE_SELECTOR)
            block["tolerations"] = copy.deepcopy(COMPUTE_TOLERATIONS)
    return block


def build_values(provider: str) -> dict[str, Any]:
    identity_key, identity_value = detect_identity_annotation(provider)
    service_account: dict[str, Any] = {"create": True, "annotations": {}}
    if identity_key and identity_value:
        service_account["annotations"] = {identity_key: identity_value}

    if provider == "aws":
        raw_prefix = os.environ.get("FLYTE_RAWOUTPUT_PREFIX", join_uri_prefix("s3", S3_BUCKET, S3_PREFIX))
        remote_scheme = "s3"
    else:
        raw_prefix = os.environ.get("FLYTE_RAWOUTPUT_PREFIX", "")
        remote_scheme = "local"

    flyteadmin = component_block(service_account=copy.deepcopy(service_account), role="general")
    flytescheduler = component_block(service_account=copy.deepcopy(service_account), role="general")
    datacatalog = component_block(service_account=copy.deepcopy(service_account), role="general")
    flytepropeller = component_block(service_account=copy.deepcopy(service_account), role="general")
    flyteconsole = component_block(service_account={"create": True, "annotations": {}}, role="general")
    webhook = component_block(service_account={"create": True, "annotations": {}}, role="general")

    return {
        "deployRedoc": False,
        "flyteadmin": {
            **flyteadmin,
            "replicaCount": int(os.environ.get("FLYTEADMIN_REPLICAS", "1")),
            "podEnv": {},
        },
        "flytescheduler": {
            **flytescheduler,
            "runPrecheck": True,
            "service": {"enabled": False},
            "podEnv": {},
        },
        "datacatalog": {
            **datacatalog,
            "replicaCount": int(os.environ.get("DATACATALOG_REPLICAS", "1")),
            "podEnv": {},
        },
        "flyteconnector": {"enabled": yaml_bool(FLYTE_CONNECTOR_ENABLED)},
        "flytepropeller": {
            **flytepropeller,
            "manager": False,
            "createCRDs": True,
            "replicaCount": int(os.environ.get("FLYTEPROPELLER_REPLICAS", "1")),
            "podEnv": {},
        },
        "flyteconsole": {
            **flyteconsole,
            "replicaCount": int(os.environ.get("FLYTECONSOLE_REPLICAS", "1")),
            "podEnv": {},
        },
        "webhook": {
            **webhook,
            "podEnv": {},
        },
        "common": {
            "databaseSecret": {"name": DB_SECRET_NAME, "secretManifest": {}},
            "ingress": {
                "ingressClassName": FLYTE_INGRESS_CLASS_NAME,
                "enabled": yaml_bool(FLYTE_INGRESS_ENABLED),
                "webpackHMR": False,
                "separateGrpcIngress": yaml_bool(FLYTE_SEPARATE_GRPC_INGRESS),
                "separateGrpcIngressAnnotations": {},
                "annotations": {},
                "albSSLRedirect": False,
                "tls": {"enabled": False},
            },
            "flyteNamespaceTemplate": {"enabled": False},
        },
        "storage": build_storage_block(provider),
        "db": {
            "datacatalog": {
                "database": {
                    "port": POOLER_PORT,
                    "username": APP_DB_USER,
                    "host": DB_HOST,
                    "dbname": FLYTE_DATACATALOG_DB,
                    "passwordPath": "/etc/db/pass.txt",
                }
            },
            "admin": {
                "database": {
                    "port": POOLER_PORT,
                    "username": APP_DB_USER,
                    "host": DB_HOST,
                    "dbname": FLYTE_ADMIN_DB,
                    "passwordPath": "/etc/db/pass.txt",
                }
            },
        },
        "secrets": {
            "adminOauthClientCredentials": {
                "enabled": True,
                "clientSecret": FLYTE_OAUTH_CLIENT_SECRET,
                "clientId": FLYTE_OAUTH_CLIENT_ID,
                "secretName": AUTH_SECRET_NAME,
            }
        },
        "configmap": {
            "task_resource_defaults": build_task_resource_defaults(),
            "core": {
                "propeller": {
                    "rawoutput-prefix": raw_prefix,
                    "metadata-prefix": "metadata/propeller",
                    "workers": 4,
                    "max-workflow-retries": 30,
                    "workflow-reeval-duration": "30s",
                    "downstream-eval-duration": "30s",
                    "limit-namespace": "all",
                    "leader-election": {
                        "enabled": True,
                        "lock-config-map": {"name": "propeller-leader", "namespace": TARGET_NS},
                        "lease-duration": "15s",
                        "renew-deadline": "10s",
                        "retry-period": "2s",
                    },
                },
            },
            "enabled_plugins": {
                "tasks": {
                    "task-plugins": {
                        "enabled-plugins": ["container", "sidecar", "k8s-array", "connector-service", "echo", "spark"],
                        "default-for-task-types": {
                            "container": "container",
                            "sidecar": "sidecar",
                            "container_array": "k8s-array",
                            "spark": "spark",
                        },
                    }
                }
            },
            "k8s": build_k8s_block(provider),
            "remoteData": {"remoteData": {"region": AWS_REGION, "scheme": remote_scheme, "signedUrls": {"durationMinutes": 3}}},
            "resource_manager": {"propeller": {"resourcemanager": {"type": "noop"}}},
            "task_logs": {"plugins": {"logs": {"kubernetes-enabled": True, "cloudwatch-enabled": False}}},
        },
        "workflow_scheduler": {"enabled": yaml_bool(FLYTE_WORKFLOW_SCHEDULER_ENABLED)},
        "workflow_notifications": {"enabled": yaml_bool(FLYTE_WORKFLOW_NOTIFICATIONS_ENABLED)},
        "external_events": {"enable": yaml_bool(FLYTE_EXTERNAL_EVENTS_ENABLED)},
        "cloud_events": {"enable": yaml_bool(FLYTE_CLOUD_EVENTS_ENABLED)},
        "cluster_resource_manager": {"enabled": yaml_bool(FLYTE_CLUSTER_RESOURCE_MANAGER_ENABLED)},
        "sparkoperator": {
            "enabled": yaml_bool(FLYTE_SPARK_OPERATOR_ENABLED),
            "plugin_config": {
                "plugins": {
                    "spark": {
                        "spark-config-default": (
                            [
                                {"spark.hadoop.fs.s3a.aws.credentials.provider": "com.amazonaws.auth.DefaultAWSCredentialsProviderChain"},
                                {"spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version": "2"},
                                {"spark.kubernetes.allocation.batch.size": "50"},
                                {"spark.hadoop.fs.s3a.acl.default": "BucketOwnerFullControl"},
                                {"spark.hadoop.fs.s3n.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem"},
                                {"spark.hadoop.fs.AbstractFileSystem.s3n.impl": "org.apache.hadoop.fs.s3a.S3A"},
                                {"spark.hadoop.fs.s3.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem"},
                                {"spark.hadoop.fs.AbstractFileSystem.s3.impl": "org.apache.hadoop.fs.s3a.S3A"},
                                {"spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem"},
                                {"spark.hadoop.fs.AbstractFileSystem.s3a.impl": "org.apache.hadoop.fs.s3a.S3A"},
                                {"spark.hadoop.fs.s3a.multipart.threshold": "536870912"},
                                {"spark.blacklist.enabled": "true"},
                                {"spark.blacklist.timeout": "5m"},
                                {"spark.task.maxfailures": "8"},
                            ]
                            if provider == "aws"
                            else []
                        )
                    }
                }
            },
        },
        "daskoperator": {"enabled": yaml_bool(FLYTE_DASK_OPERATOR_ENABLED)},
        "databricks": {"enabled": yaml_bool(FLYTE_DATABRICKS_ENABLED)},
    }


def render_values_file(values: dict[str, Any]) -> None:
    MANIFEST_DIR.mkdir(parents=True, exist_ok=True)
    with VALUES_FILE.open("w", encoding="utf-8") as f:
        yaml.safe_dump(values, f, sort_keys=False, default_flow_style=False, width=120)


def validate_rendered_values() -> None:
    with VALUES_FILE.open("r", encoding="utf-8") as f:
        yaml.safe_load(f)
    run(
        [
            "helm",
            "template",
            RELEASE_NAME,
            f"{CHART_REPO_NAME}/{CHART_NAME}",
            "--version",
            CHART_VERSION,
            "-n",
            TARGET_NS,
            "-f",
            str(VALUES_FILE),
            "--skip-crds",
        ],
        check=True,
    )


def require_prereqs() -> None:
    require_bin("kubectl")
    require_bin("helm")
    require_bin("python3")
    run(["kubectl", "cluster-info"], check=True)


def find_app_secret_name() -> str:
    selector = f"cnpg.io/cluster={CNPG_CLUSTER},cnpg.io/userType=app"
    cp = run(
        ["kubectl", "-n", POSTGRES_NS, "get", "secret", "-l", selector, "-o", "jsonpath={.items[0].metadata.name}"],
        check=False,
    )
    if cp.returncode == 0 and cp.stdout.strip():
        return cp.stdout.strip()
    fallback = f"{CNPG_CLUSTER}-app"
    cp = run(["kubectl", "-n", POSTGRES_NS, "get", "secret", fallback], check=False)
    return fallback if cp.returncode == 0 else ""


def get_primary_pod() -> str:
    selector = f"cnpg.io/cluster={CNPG_CLUSTER},cnpg.io/instanceRole=primary"
    cp = run(["kubectl", "-n", POSTGRES_NS, "get", "pods", "-l", selector, "-o", "jsonpath={.items[0].metadata.name}"], check=False)
    return cp.stdout.strip() if cp.returncode == 0 else ""


def resolve_db_host() -> str:
    global DB_HOST
    if DB_HOST:
        return DB_HOST
    if DB_ACCESS_MODE == "rw":
        DB_HOST = f"{CNPG_CLUSTER}-rw.{POSTGRES_NS}.svc.cluster.local"
    elif DB_ACCESS_MODE == "pooler":
        DB_HOST = f"{POOLER_SERVICE}.{POSTGRES_NS}.svc.cluster.local"
    else:
        fatal(f"unsupported DB_ACCESS_MODE={DB_ACCESS_MODE}")
    return DB_HOST


def ensure_database(db_name: str) -> None:
    primary = get_primary_pod()
    if not primary:
        fatal(f"CNPG primary pod not found for {CNPG_CLUSTER}")

    exists = run(
        [
            "kubectl",
            "-n",
            POSTGRES_NS,
            "exec",
            primary,
            "--",
            "sh",
            "-lc",
            f'psql -U postgres -tAc "SELECT 1 FROM pg_database WHERE datname=\'{db_name}\';"',
        ],
        check=False,
    ).stdout.strip()

    if exists != "1":
        log(f"creating database {db_name}")
        run(
            [
                "kubectl",
                "-n",
                POSTGRES_NS,
                "exec",
                primary,
                "--",
                "sh",
                "-lc",
                f'psql -U postgres -v ON_ERROR_STOP=1 -c "CREATE DATABASE {db_name} OWNER \\\"{APP_DB_USER}\\\";"',
            ],
            check=False,
        )
    else:
        log(f"database {db_name} already exists")

    run(
        [
            "kubectl",
            "-n",
            POSTGRES_NS,
            "exec",
            primary,
            "--",
            "sh",
            "-lc",
            f'psql -U postgres -v ON_ERROR_STOP=1 -c "ALTER DATABASE {db_name} OWNER TO \\\"{APP_DB_USER}\\\";"',
        ],
        check=False,
    )
    run(
        [
            "kubectl",
            "-n",
            POSTGRES_NS,
            "exec",
            primary,
            "--",
            "sh",
            "-lc",
            f'psql -U postgres -d "{db_name}" -v ON_ERROR_STOP=1 -c "ALTER SCHEMA public OWNER TO \\\"{APP_DB_USER}\\\";"',
        ],
        check=False,
    )


def validate_static_aws_credentials(provider: str) -> None:
    if provider != "aws" or EFFECTIVE_USE_IRSA:
        return
    if not AWS_ACCESS_KEY_ID or not AWS_SECRET_ACCESS_KEY:
        fatal("AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY are required when STORAGE_PROVIDER=aws and USE_IAM=false")


def find_deployment_name(component: str) -> str:
    direct_candidates = [component, f"{RELEASE_NAME}-{component}", f"{RELEASE_NAME}_{component}"]
    for name in direct_candidates:
        cp = run(["kubectl", "-n", TARGET_NS, "get", "deploy", name], check=False)
        if cp.returncode == 0:
            return name

    cp = run(
        [
            "kubectl",
            "-n",
            TARGET_NS,
            "get",
            "deploy",
            "-l",
            f"app.kubernetes.io/instance={RELEASE_NAME}",
            "-o",
            "jsonpath={range .items[*]}{.metadata.name}{\"\\n\"}{end}",
        ],
        check=False,
    )
    if cp.returncode == 0 and cp.stdout.strip():
        names = [line.strip() for line in cp.stdout.splitlines() if line.strip()]
        for name in names:
            if component in name:
                return name
        if names:
            return names[0]
    return component


def patch_deployment_scheduling(namespace: str, deployment: str, *, role: str) -> None:
    if not EKS:
        return
    selector = GENERAL_NODE_SELECTOR if role == "general" else COMPUTE_NODE_SELECTOR
    tolerations = GENERAL_TOLERATIONS if role == "general" else COMPUTE_TOLERATIONS
    patch = {"spec": {"template": {"spec": {"nodeSelector": selector, "tolerations": tolerations}}}}
    run(["kubectl", "-n", namespace, "patch", "deployment", deployment, "--type=merge", "-p", json.dumps(patch)], check=False)


def apply_eks_runtime_patches() -> None:
    if not EKS:
        return
    for component in ("flyteadmin", "flytescheduler", "datacatalog", "flytepropeller", "flyteconsole", "webhook"):
        patch_deployment_scheduling(TARGET_NS, find_deployment_name(component), role="general")


def print_summary(provider: str, spec_hash: str) -> None:
    log("helm release status")
    run(["helm", "status", RELEASE_NAME, "-n", TARGET_NS], check=False)
    log("workloads")
    run(["kubectl", "-n", TARGET_NS, "get", "deploy", "-o", "wide"], check=False)
    run(["kubectl", "-n", TARGET_NS, "get", "svc", "-o", "wide"], check=False)
    print()
    print(f"Kubernetes cluster mode: {CLUSTER_MODE}")
    print(f"Flyte namespace: {TARGET_NS}")
    print(f"Postgres namespace: {POSTGRES_NS}")
    print(f"Database host: {DB_HOST}")
    print(f"Database access mode: {DB_ACCESS_MODE}")
    print(f"Storage provider: {provider}")
    print(f"Rendered values file: {VALUES_FILE}")
    print(f"Rollout hash: {spec_hash}")
    print(f"State configmap: {STATE_CONFIGMAP}")
    print()
    print("Local access:")
    print(f"kubectl -n {TARGET_NS} port-forward svc/{resolve_service_name('flyteadmin')} 30080:80")


def dump_diagnostics() -> None:
    log(f"diagnostics namespace={TARGET_NS}")
    run(["kubectl", "-n", TARGET_NS, "get", "pods", "-o", "wide"], check=False)
    run(["kubectl", "-n", TARGET_NS, "get", "svc", "-o", "wide"], check=False)
    run(["kubectl", "-n", TARGET_NS, "get", "events", "--sort-by=.lastTimestamp"], check=False)


def build_secret_fingerprint(provider: str) -> dict[str, str]:
    out = {
        "db": hashlib.sha256(APP_DB_PASSWORD.encode("utf-8")).hexdigest(),
        "auth": hashlib.sha256(FLYTE_OAUTH_CLIENT_SECRET.encode("utf-8")).hexdigest(),
    }
    if provider == "aws":
        if EFFECTIVE_USE_IRSA:
            out["storage"] = hashlib.sha256(
                "\n".join([AWS_REGION, S3_BUCKET, S3_PREFIX, S3_ENDPOINT, resolve_role_arn(task=False)]).encode("utf-8")
            ).hexdigest()
            out["task_aws"] = hashlib.sha256(
                "\n".join([AWS_REGION, S3_ENDPOINT, resolve_role_arn(task=True)]).encode("utf-8")
            ).hexdigest()
        else:
            out["storage"] = hashlib.sha256(
                "\n".join([AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION, S3_BUCKET, S3_PREFIX, S3_ENDPOINT]).encode("utf-8")
            ).hexdigest()
            out["task_aws"] = hashlib.sha256(
                "\n".join([AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION, S3_ENDPOINT, AWS_SESSION_TOKEN]).encode("utf-8")
            ).hexdigest()
    return out


def build_rollout_state(values: dict[str, Any], provider: str) -> dict[str, Any]:
    return {
        "chart": {"repo": CHART_REPO_NAME, "url": CHART_REPO_URL, "name": CHART_NAME, "version": CHART_VERSION, "release": RELEASE_NAME},
        "cluster": {
            "mode": CLUSTER_MODE,
            "target_ns": TARGET_NS,
            "postgres_ns": POSTGRES_NS,
            "cnpg_cluster": CNPG_CLUSTER,
            "db_access_mode": DB_ACCESS_MODE,
            "db_host": DB_HOST,
            "pooler_port": POOLER_PORT,
        },
        "provider": provider,
        "values": values,
        "secrets": build_secret_fingerprint(provider),
        "task_namespaces": split_namespaces(os.environ.get("FLYTE_TASK_NAMESPACES", "flytesnacks-development")),
    }


def compute_rollout_hash(values: dict[str, Any], provider: str) -> str:
    return stable_hash(build_rollout_state(values, provider))


def read_cluster_hash() -> str:
    cp = run(["kubectl", "-n", TARGET_NS, "get", "configmap", STATE_CONFIGMAP, "-o", "jsonpath={.data.spec-hash}"], check=False)
    return cp.stdout.strip() if cp.returncode == 0 else ""


def write_cluster_hash(spec_hash: str) -> None:
    ensure_configmap(
        TARGET_NS,
        STATE_CONFIGMAP,
        {"spec-hash": spec_hash, "updated-at": ts()},
        labels={"app.kubernetes.io/name": "flyte-bootstrap-state", "app.kubernetes.io/part-of": "flyte"},
        annotations={"flyte.dev/spec-hash": spec_hash},
    )


def release_exists() -> bool:
    cp = run(["helm", "list", "-n", TARGET_NS, "--filter", f"^{RELEASE_NAME}$", "-q"], check=False)
    return cp.returncode == 0 and bool(cp.stdout.strip())


def helm_repo_sync() -> None:
    run(["helm", "repo", "add", CHART_REPO_NAME, CHART_REPO_URL, "--force-update"], check=True)
    run(["helm", "repo", "update"], check=True)


def ensure_control_plane_secrets(_provider: str) -> None:
    ensure_secret(TARGET_NS, DB_SECRET_NAME, {"pass.txt": APP_DB_PASSWORD})


def task_role_annotation(provider: str) -> tuple[str, str]:
    if provider == "aws" and EFFECTIVE_USE_IRSA:
        role_arn = resolve_role_arn(task=True)
        if not role_arn:
            fatal("FLYTE_TASK_ROLE_ARN, FLYTE_CONTROL_PLANE_ROLE_ARN, or AWS_ROLE_ARN is required when USE_IAM=true on EKS")
        return "eks.amazonaws.com/role-arn", role_arn
    return "", ""


def ensure_task_namespace_ready(namespace: str, provider: str) -> None:
    ensure_namespace(namespace)
    ensure_default_serviceaccount(namespace)

    if provider == "aws":
        if EFFECTIVE_USE_IRSA:
            key, value = task_role_annotation(provider)
            if key and value:
                ensure_serviceaccount(namespace, TASK_SERVICE_ACCOUNT, {key: value})
                patch_default_serviceaccount_annotation(namespace, key, value)
                return
            fatal("IRSA is enabled but no task role annotation could be resolved")

        data = {
            "AWS_ACCESS_KEY_ID": AWS_ACCESS_KEY_ID,
            "AWS_SECRET_ACCESS_KEY": AWS_SECRET_ACCESS_KEY,
            "AWS_DEFAULT_REGION": AWS_REGION,
            "AWS_REGION": AWS_REGION,
        }
        if AWS_SESSION_TOKEN:
            data["AWS_SESSION_TOKEN"] = AWS_SESSION_TOKEN
        ensure_secret(namespace, TASK_AWS_SECRET_NAME, data)
        ensure_serviceaccount(namespace, TASK_SERVICE_ACCOUNT)
        return

    ensure_serviceaccount(namespace, TASK_SERVICE_ACCOUNT)


def ensure_task_namespaces_ready(provider: str) -> None:
    for namespace in split_namespaces(os.environ.get("FLYTE_TASK_NAMESPACES", "flytesnacks-development")):
        ensure_task_namespace_ready(namespace, provider)


def install_or_upgrade(provider: str, values: dict[str, Any], spec_hash: str) -> None:
    helm_repo_sync()
    render_values_file(values)
    validate_rendered_values()

    current_hash = read_cluster_hash()
    if current_hash == spec_hash and release_exists():
        log("rollout hash unchanged; skipping helm upgrade")
        return

    helm_args = [
        "helm",
        "upgrade",
        "--install",
        RELEASE_NAME,
        f"{CHART_REPO_NAME}/{CHART_NAME}",
        "--version",
        CHART_VERSION,
        "-n",
        TARGET_NS,
        "-f",
        str(VALUES_FILE),
        "--wait",
        "--timeout",
        f"{READY_TIMEOUT}s",
    ]
    if FLYTE_ATOMIC:
        helm_args.append("--atomic")

    run(helm_args, check=True)
    apply_eks_runtime_patches()
    wait_for_rollouts()
    write_cluster_hash(spec_hash)


def wait_for_rollouts() -> None:
    try:
        deploys = run_text(["kubectl", "-n", TARGET_NS, "get", "deploy", "-o", 'jsonpath={range .items[*]}{.metadata.name}{"\\n"}{end}'])
    except Exception:
        deploys = ""

    for dep in [line.strip() for line in deploys.splitlines() if line.strip()]:
        log(f"waiting for deployment {dep}")
        run(["kubectl", "-n", TARGET_NS, "rollout", "status", f"deployment/{dep}", f"--timeout={ROLLOUT_TIMEOUT}"], check=True)


def resolve_service_name(component: str) -> str:
    if component == "flyteadmin":
        candidates = ["flyteadmin", f"{RELEASE_NAME}-flyteadmin", RELEASE_NAME]
    elif component == "flyteconsole":
        candidates = ["flyteconsole", f"{RELEASE_NAME}-flyteconsole", RELEASE_NAME]
    elif component == "webhook":
        candidates = ["flyte-pod-webhook", f"{RELEASE_NAME}-pod-webhook", "webhook"]
    else:
        candidates = [component]

    for name in candidates:
        cp = run(["kubectl", "-n", TARGET_NS, "get", "svc", name], check=False)
        if cp.returncode == 0:
            return name

    cp = run(
        ["kubectl", "-n", TARGET_NS, "get", "svc", "-l", f"app.kubernetes.io/instance={RELEASE_NAME}", "-o", "jsonpath={.items[0].metadata.name}"],
        check=False,
    )
    return cp.stdout.strip() if cp.returncode == 0 and cp.stdout.strip() else component


def reconcile(provider: str) -> None:
    ensure_namespace(TARGET_NS)
    validate_static_aws_credentials(provider)
    resolve_db_host()

    app_secret = find_app_secret_name()
    if not app_secret:
        fatal(f"CNPG app secret not found for cluster {CNPG_CLUSTER}")

    app_user = secret_value(POSTGRES_NS, app_secret, "username")
    app_password = secret_value(POSTGRES_NS, app_secret, "password")
    app_port = secret_value(POSTGRES_NS, app_secret, "port")

    if not app_user:
        fatal(f"username missing from CNPG app secret {app_secret}")
    if not app_password:
        fatal(f"password missing from CNPG app secret {app_secret}")

    global APP_DB_USER, APP_DB_PASSWORD, POOLER_PORT
    APP_DB_USER = app_user
    APP_DB_PASSWORD = app_password
    if app_port:
        with contextlib.suppress(ValueError):
            POOLER_PORT = int(app_port)

    ensure_database(FLYTE_ADMIN_DB)
    ensure_database(FLYTE_DATACATALOG_DB)

    ensure_control_plane_secrets(provider)
    ensure_task_namespaces_ready(provider)

    values = build_values(provider)
    spec_hash = compute_rollout_hash(values, provider)

    install_or_upgrade(provider, values, spec_hash)

    wait_for_service_endpoints(TARGET_NS, resolve_service_name("flyteadmin"), 300)
    print_summary(provider, spec_hash)


def api_resources(*, namespaced: bool) -> list[str]:
    flag = "--namespaced=true" if namespaced else "--namespaced=false"
    cp = run(["kubectl", "api-resources", "--verbs=list", flag, "-o", "name"], check=True)
    out: list[str] = []
    for line in cp.stdout.splitlines():
        line = line.strip()
        if line:
            out.append(line)
    return out


def get_objects(resource: str, *, namespace: str | None = None, all_namespaces: bool = False) -> list[dict[str, Any]]:
    cmd = ["kubectl", "get", resource, "-o", "json"]
    if all_namespaces:
        cmd.insert(2, "-A")
    elif namespace:
        cmd[2:2] = ["-n", namespace]
    cp = run(cmd, check=False)
    if cp.returncode != 0 or not cp.stdout.strip():
        return []
    try:
        payload = json.loads(cp.stdout)
    except Exception:
        return []
    return payload.get("items", [])


def wait_for_objects_deleted(
    resource: str,
    *,
    namespace: str | None = None,
    all_namespaces: bool = False,
    timeout: int = 120,
) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if not get_objects(resource, namespace=namespace, all_namespaces=all_namespaces):
            return True
        time.sleep(2)
    return not get_objects(resource, namespace=namespace, all_namespaces=all_namespaces)


def patch_finalizers(resource: str, *, namespace: str | None = None, all_namespaces: bool = False) -> int:
    patched = 0
    for obj in get_objects(resource, namespace=namespace, all_namespaces=all_namespaces):
        meta = obj.get("metadata", {})
        if not meta.get("finalizers"):
            continue
        name = meta.get("name")
        ns = meta.get("namespace")
        if not name:
            continue
        cmd = ["kubectl", "patch", resource, name, "--type=merge", "-p", '{"metadata":{"finalizers":[]}}']
        if ns:
            cmd[2:2] = ["-n", ns]
        if not all_namespaces and namespace is None and ns:
            cmd[2:2] = ["-n", ns]
        cp = run(cmd, check=False)
        if cp.returncode == 0:
            patched += 1
    return patched


def delete_resource_type(resource: str, *, namespace: str | None = None, all_namespaces: bool = False, selector: str | None = None) -> None:
    cmd = ["kubectl", "delete", resource]
    if all_namespaces:
        cmd.append("-A")
    elif namespace:
        cmd.extend(["-n", namespace])
    if selector:
        cmd.extend(["-l", selector])
    cmd.extend(["--ignore-not-found", "--wait=true", f"--timeout={READY_TIMEOUT}s"])
    run(cmd, check=False)


def delete_selected_resources(resource_types: list[str], *, namespace: str | None = None, all_namespaces: bool = False, selector: str | None = None) -> None:
    for resource in resource_types:
        delete_resource_type(resource, namespace=namespace, all_namespaces=all_namespaces, selector=selector)


def delete_flyte_custom_resources() -> None:
    for resource in FLYTE_CR_RESOURCES:
        log(f"deleting Flyte CRs: {resource}")
        delete_resource_type(resource, all_namespaces=True)
        if not wait_for_objects_deleted(resource, all_namespaces=True, timeout=180) and FORCE_FINALIZER_REMOVAL:
            patched = patch_finalizers(resource, all_namespaces=True)
            if patched:
                log(f"patched finalizers on {patched} {resource} object(s)")
            delete_resource_type(resource, all_namespaces=True)
            wait_for_objects_deleted(resource, all_namespaces=True, timeout=180)


def delete_flyte_managed_namespaced_objects(namespace: str) -> None:
    resources = api_resources(namespaced=True)
    for selector in MANAGED_LABEL_SELECTORS:
        delete_selected_resources(resources, namespace=namespace, selector=selector)
    if FORCE_FINALIZER_REMOVAL:
        for resource in resources:
            patch_finalizers(resource, namespace=namespace)
            delete_resource_type(resource, namespace=namespace)


def delete_flyte_managed_cluster_objects() -> None:
    resources = CLUSTER_MANAGED_RESOURCES
    for selector in MANAGED_LABEL_SELECTORS:
        delete_selected_resources(resources, all_namespaces=False, selector=selector)
    if FORCE_FINALIZER_REMOVAL:
        for resource in resources:
            patch_finalizers(resource)
            delete_resource_type(resource)


def delete_flyte_crds() -> None:
    delete_flyte_custom_resources()
    for crd in FLYTE_CR_RESOURCES:
        run(["kubectl", "delete", "crd", crd, "--ignore-not-found", "--wait=true", f"--timeout={READY_TIMEOUT}s"], check=False)


def delete_namespace(namespace: str) -> None:
    if namespace == "default":
        return
    run(["kubectl", "delete", "namespace", namespace, "--ignore-not-found", "--wait=true", f"--timeout={READY_TIMEOUT}s"], check=False)
    deadline = time.monotonic() + 180
    while time.monotonic() < deadline:
        cp = run(["kubectl", "get", "namespace", namespace], check=False)
        if cp.returncode != 0:
            return
        time.sleep(2)
    if FORCE_FINALIZER_REMOVAL:
        run(["kubectl", "patch", "namespace", namespace, "--type=merge", "-p", '{"metadata":{"finalizers":[]}}'], check=False)
        run(["kubectl", "delete", "namespace", namespace, "--ignore-not-found", "--wait=true", f"--timeout={READY_TIMEOUT}s"], check=False)


def delete_task_namespace_bootstrap(namespace: str) -> None:
    for kind, name in (("secret", TASK_AWS_SECRET_NAME), ("serviceaccount", TASK_SERVICE_ACCOUNT)):
        run(["kubectl", "-n", namespace, "delete", kind, name, "--ignore-not-found", "--wait=true", f"--timeout={READY_TIMEOUT}s"], check=False)


def release_uninstall() -> None:
    run(
        [
            "helm",
            "uninstall",
            RELEASE_NAME,
            "-n",
            TARGET_NS,
            "--ignore-not-found",
            "--wait",
            "--timeout",
            f"{READY_TIMEOUT}s",
        ],
        check=False,
    )


def delete_all() -> None:
    task_namespaces = split_namespaces(os.environ.get("FLYTE_TASK_NAMESPACES", "flytesnacks-development"))

    delete_flyte_custom_resources()
    release_uninstall()

    delete_flyte_managed_namespaced_objects(TARGET_NS)
    for namespace in task_namespaces:
        delete_flyte_managed_namespaced_objects(namespace)

    delete_flyte_managed_cluster_objects()

    run(["kubectl", "-n", TARGET_NS, "delete", "secret", DB_SECRET_NAME, "--ignore-not-found", "--wait=true", f"--timeout={READY_TIMEOUT}s"], check=False)
    run(["kubectl", "-n", TARGET_NS, "delete", "secret", AUTH_SECRET_NAME, "--ignore-not-found", "--wait=true", f"--timeout={READY_TIMEOUT}s"], check=False)
    run(["kubectl", "-n", TARGET_NS, "delete", "secret", STORAGE_SECRET_NAME, "--ignore-not-found", "--wait=true", f"--timeout={READY_TIMEOUT}s"], check=False)
    run(["kubectl", "-n", TARGET_NS, "delete", "configmap", STATE_CONFIGMAP, "--ignore-not-found", "--wait=true", f"--timeout={READY_TIMEOUT}s"], check=False)

    if DELETE_TARGET_NAMESPACE:
        delete_namespace(TARGET_NS)

    for namespace in task_namespaces:
        delete_task_namespace_bootstrap(namespace)
        if DELETE_TASK_NAMESPACES:
            delete_namespace(namespace)

    if DELETE_FLYTE_CRDS:
        delete_flyte_crds()

    log("deleted Flyte release and bootstrap objects")


def main() -> None:
    require_prereqs()
    ensure_dns_ready()
    provider = detect_storage_provider()
    reconcile(provider)


def cli() -> int:
    try:
        if len(sys.argv) == 1 or sys.argv[1] == "--rollout":
            main()
            return 0
        if sys.argv[1] == "--delete":
            require_prereqs()
            delete_all()
            return 0
        if sys.argv[1] in {"--help", "-h"}:
            print(
                "Usage: flyte_setup.py [--rollout|--delete]\n\n"
                "Environment variables:\n"
                "  K8S_CLUSTER=auto|kind|eks\n"
                "  STORAGE_PROVIDER=auto|aws|sandbox\n"
                "  USE_IAM=true|false\n"
                "  FLYTE_CONTROL_PLANE_ROLE_ARN=<role-arn>\n"
                "  FLYTE_TASK_ROLE_ARN=<role-arn>\n"
                "  FLYTE_TASK_NAMESPACES=flytesnacks-development[,other-namespace]\n"
                "  TASK_SERVICE_ACCOUNT=flyte-task|<other-sa>\n"
                "  TASK_AWS_SECRET_NAME=flyte-aws-credentials\n"
                "  STORAGE_SECRET_NAME=flyte-storage-config\n"
                "  DB_ACCESS_MODE=rw|pooler\n"
                "  FLYTE_INGRESS_ENABLED=true|false\n"
                "  FLYTE_ATOMIC=true|false\n"
                "  DNS_TIMEOUT=seconds\n"
                "  FORCE_FINALIZER_REMOVAL=true|false\n"
            )
            return 0
        fatal(f"unknown option: {sys.argv[1]}")
    except subprocess.CalledProcessError as exc:
        print(f"[{ts()}] [flyte][FATAL] command failed: {exc.cmd}", file=sys.stderr, flush=True)
        try:
            dump_diagnostics()
        except Exception:
            pass
        return exc.returncode
    except SystemExit:
        raise
    except Exception as exc:
        print(f"[{ts()}] [flyte][FATAL] {exc}", file=sys.stderr, flush=True)
        try:
            dump_diagnostics()
        except Exception:
            pass
        return 1


if __name__ == "__main__":
    raise SystemExit(cli())
