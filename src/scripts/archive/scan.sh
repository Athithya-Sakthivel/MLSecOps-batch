docker run --rm \
  --entrypoint sh \
  -v "$PWD:/workspace" \
  -w /workspace \
  ghcr.io/athithya-sakthivel/trivy-0.69.3-gitleaks-8.30.1-opengrep-1.16.5@sha256:4aea8e288a282f061f1f872b4cc1482f35807cd80d35da3e2689cc8ff5c7a7ba \
  -c '
    set -euo pipefail

    echo "=== OpenGrep (SAST) ===. Scans current commit only"
    PYTHONWARNINGS=ignore opengrep scan \
      --config p/owasp-top-ten \
      --config p/python \
      --config p/dockerfile \
      --config p/secrets \
      --config p/docker-compose.yaml \
      --config p/kubernetes.yaml \
      --config src/opengrep/custom.yaml \
      --error \
      .

    echo "=== Gitleaks (Secrets from all commits) ==="
    gitleaks git \
      --log-opts="--all" \
      --no-banner \
      --redact \
      --exit-code 1

    echo "=== Trivy (Filesystem) ==="
    trivy fs \
      --scanners vuln,misconfig \
      --severity HIGH,CRITICAL \
      --ignore-unfixed \
      --skip-dirs .git \
      --skip-dirs src/opengrep \
      --exit-code 1 \
      .
  '