# syntax=docker/dockerfile:1.7

FROM ghcr.io/gitleaks/gitleaks:v8.30.1 AS gitleaks

FROM ghcr.io/athithya-sakthivel/trivy-safe-0.69.3@sha256:bcc376de8d77cfe086a917230e818dc9f8528e3c852f7b1aff648949b6258d1c

ARG OPENGREP_VERSION=v1.16.5

USER root

RUN apk add --no-cache \
      bash \
      ca-certificates \
      curl \
      git \
      openssh-client \
    && update-ca-certificates

COPY --from=gitleaks /usr/bin/gitleaks /usr/local/bin/gitleaks

RUN set -eux; \
    if ldd /bin/sh 2>/dev/null | grep -qi musl; then \
      dist="opengrep_musllinux_x86"; \
    else \
      dist="opengrep_manylinux_x86"; \
    fi; \
    curl -fsSL --retry 5 --retry-all-errors \
      -o /usr/local/bin/opengrep \
      "https://github.com/opengrep/opengrep/releases/download/${OPENGREP_VERSION}/${dist}"; \
    chmod 0755 /usr/local/bin/opengrep

RUN git config --global --add safe.directory '*' \
    && gitleaks version \
    && opengrep --version \
    && trivy --version

RUN useradd -m appuser
USER appuser

WORKDIR /workspace