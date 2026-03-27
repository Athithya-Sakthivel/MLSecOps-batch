set -euo pipefail

docker run --rm \
  --entrypoint sh \
  -v "$PWD:/work" \
  -w /work \
  ghcr.io/athithya-sakthivel/trivy-0.69.3-gitleaks-8.30.1-opengrep-1.16.5@sha256:4aea8e288a282f061f1f872b4cc1482f35807cd80d35da3e2689cc8ff5c7a7ba \
  -c '
    set -euo pipefail

    mkdir -p src/opengrep

    opengrep scan --config p/owasp-top-ten --dump-config > src/opengrep/owasp-top-ten.yaml
    opengrep scan --config p/python        --dump-config > src/opengrep/python.yaml
    opengrep scan --config p/dockerfile    --dump-config > src/opengrep/dockerfile.yaml
    opengrep scan --config p/secrets       --dump-config > src/opengrep/secrets.yaml
  '