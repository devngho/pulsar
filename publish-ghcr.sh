#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage:
  ./publish-ghcr.sh [options] <owner> <image-name> [tag] [pulsar-version]

Example:
  ./publish-ghcr.sh devngho pulsar-r2-offloader 4.0.10-r2fix 4.0.10
  ./publish-ghcr.sh --user devngho --token ghp_xxx devngho pulsar-r2-offloader
  ./publish-ghcr.sh --skip-build devngho pulsar 4.2.1-patched 4.2.1
  ./publish-ghcr.sh --platforms linux/amd64,linux/arm64 devngho pulsar 4.2.1-patched 4.2.1

Credential sources (in order):
  1) --user / --token flags
  2) GHCR_USER/GHCR_TOKEN or GITHUB_USER/GITHUB_TOKEN
  3) gh CLI auth (`gh auth token`, `gh api user`)
  4) Interactive prompt (TTY only)
  5) Existing docker login session (skip explicit login)

Options:
  --user <username>    GitHub username for docker login
  --token <token>      GitHub token (write:packages)
  --login              Force explicit docker login (fail if creds unavailable)
  --no-login           Never run docker login (use existing docker auth)
  --skip-build         Skip Gradle build; require existing .nar artifact
  --platforms <list>   Target platforms for buildx (default: linux/amd64,linux/arm64)
  --builder <name>     Optional buildx builder name

Notes:
  - This script builds tiered-storage jcloud artifact first.
  - Then it builds Dockerfile at repo root and pushes to ghcr.io.
EOF
}

GH_USER=""
GH_TOKEN=""
FORCE_LOGIN=false
SKIP_LOGIN=false
SKIP_BUILD=false
PLATFORMS="linux/amd64,linux/arm64"
BUILDER=""

log() {
  echo "[publish-ghcr] $*"
}

fail() {
  echo "ERROR: $*" >&2
  exit 1
}

warn() {
  echo "WARN: $*" >&2
}

require_cmd() {
  command -v "$1" >/dev/null 2>&1 || fail "Missing required command: $1"
}

is_docker_podman_emulation() {
  docker --version 2>/dev/null | grep -qi podman
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    -h|--help)
      usage
      exit 0
      ;;
    --user)
      GH_USER="${2:-}"
      shift 2
      ;;
    --token)
      GH_TOKEN="${2:-}"
      shift 2
      ;;
    --login)
      FORCE_LOGIN=true
      shift
      ;;
    --no-login)
      SKIP_LOGIN=true
      shift
      ;;
    --skip-build)
      SKIP_BUILD=true
      shift
      ;;
    --platforms)
      PLATFORMS="${2:-}"
      shift 2
      ;;
    --builder)
      BUILDER="${2:-}"
      shift 2
      ;;
    --)
      shift
      break
      ;;
    -* )
      echo "ERROR: Unknown option: $1" >&2
      usage
      exit 1
      ;;
    *)
      break
      ;;
  esac
done

OWNER="${1:-}"
IMAGE_NAME="${2:-}"
TAG="${3:-}"
PULSAR_VERSION="${4:-4.0.10}"

if [[ -z "${OWNER}" || -z "${IMAGE_NAME}" ]]; then
  usage
  exit 1
fi

if [[ "${FORCE_LOGIN}" == "true" && "${SKIP_LOGIN}" == "true" ]]; then
  fail "--login and --no-login cannot be used together"
fi

require_cmd docker
require_cmd git
docker buildx version >/dev/null 2>&1 || fail "buildx-compatible builder is required"

if [[ -z "${TAG}" ]]; then
  if git rev-parse --short HEAD >/dev/null 2>&1; then
    TAG="${PULSAR_VERSION}-$(git rev-parse --short HEAD)"
  else
    TAG="${PULSAR_VERSION}"
  fi
fi

if [[ -z "${GH_USER}" ]]; then
  GH_USER="${GHCR_USER:-${GITHUB_USER:-}}"
fi

if [[ -z "${GH_TOKEN}" ]]; then
  GH_TOKEN="${GHCR_TOKEN:-${GITHUB_TOKEN:-}}"
fi

if [[ -z "${GH_TOKEN}" ]] && command -v gh >/dev/null 2>&1; then
  if gh auth status >/dev/null 2>&1; then
    GH_TOKEN="$(gh auth token 2>/dev/null || true)"
    if [[ -z "${GH_USER}" ]]; then
      GH_USER="$(gh api user --jq .login 2>/dev/null || true)"
    fi
  fi
fi

if [[ -t 0 ]]; then
  if [[ -z "${GH_USER}" ]]; then
    read -r -p "GitHub username for GHCR (leave blank to skip explicit login): " GH_USER
  fi
  if [[ -n "${GH_USER}" && -z "${GH_TOKEN}" ]]; then
    read -r -s -p "GitHub token for GHCR (write:packages): " GH_TOKEN
    echo
  fi
fi

if [[ "${SKIP_BUILD}" == "true" ]]; then
  log "[1/4] Skipping Gradle build (--skip-build)"
else
  require_cmd ./gradlew
  log "[1/4] Building tiered-storage offloader artifacts"
  ./gradlew :tiered-storage:tiered-storage-jcloud:assemble :tiered-storage:tiered-storage-aws-sdk-s3:assemble
fi

if ! ls ./tiered-storage/aws-sdk-s3/build/libs/*.nar >/dev/null 2>&1; then
  fail "No AWS SDK S3 offloader NAR found at tiered-storage/aws-sdk-s3/build/libs/*.nar. Build failed or --skip-build used without artifact."
fi

IMAGE="ghcr.io/${OWNER}/${IMAGE_NAME}"
REVISION="$(git rev-parse --short HEAD 2>/dev/null || echo unknown)"
CREATED="$(date -u +%Y-%m-%dT%H:%M:%SZ)"

if [[ "${SKIP_LOGIN}" == "true" ]]; then
  log "[2/4] Skipping explicit GHCR login (--no-login)"
elif [[ -n "${GH_USER}" && -n "${GH_TOKEN}" ]]; then
  log "[2/4] Logging in to GHCR as ${GH_USER}"
  if is_docker_podman_emulation && command -v podman >/dev/null 2>&1; then
    printf '%s' "${GH_TOKEN}" | podman login ghcr.io -u "${GH_USER}" --password-stdin
  else
    printf '%s' "${GH_TOKEN}" | docker login ghcr.io -u "${GH_USER}" --password-stdin
  fi
elif [[ "${FORCE_LOGIN}" == "true" ]]; then
  fail "--login specified but credentials are missing. Provide --user/--token or env vars."
else
  log "[2/4] No explicit credentials found; using existing docker auth/session"
fi

if is_docker_podman_emulation && command -v podman >/dev/null 2>&1; then
  if [[ -n "${BUILDER}" ]]; then
    log "Ignoring --builder for Podman-compatible flow"
  fi
  LOCAL_MANIFEST="${OWNER}-${IMAGE_NAME}-${TAG}"
  LOCAL_MANIFEST="${LOCAL_MANIFEST//\//-}"
  LOCAL_MANIFEST="${LOCAL_MANIFEST//:/-}"

  log "[3/4] Building multi-platform manifest with podman (${PLATFORMS})"
  podman buildx build \
    --platform "${PLATFORMS}" \
    --manifest "${LOCAL_MANIFEST}" \
    --build-arg VERSION="${PULSAR_VERSION}" \
    --build-arg IMAGE_TITLE="${IMAGE_NAME}" \
    --build-arg IMAGE_SOURCE="$(git config --get remote.origin.url 2>/dev/null || echo unknown)" \
    --build-arg IMAGE_REVISION="${REVISION}" \
    --build-arg IMAGE_CREATED="${CREATED}" \
    -f ./Dockerfile .

  log "[4/4] Pushing manifest to registry"
  DIGEST_FILE="$(mktemp -t publish-ghcr-digest.XXXXXX)"
  if ! podman manifest push --all --digestfile "${DIGEST_FILE}" "${LOCAL_MANIFEST}" "docker://${IMAGE}:${TAG}"; then
    rm -f "${DIGEST_FILE}" || true
    fail "Push failed. Verify GHCR token scopes and package permissions for ${OWNER}/${IMAGE_NAME}."
  fi

  if [[ -s "${DIGEST_FILE}" ]]; then
    DIGEST="$(cat "${DIGEST_FILE}")"
    log "Pushed manifest digest: ${DIGEST}"
  else
    warn "Manifest digest file is empty; registry verification will be best-effort"
  fi
  rm -f "${DIGEST_FILE}" || true

  podman manifest rm "${LOCAL_MANIFEST}" >/dev/null 2>&1 || true

  if ! podman manifest inspect "docker://${IMAGE}:${TAG}" >/dev/null 2>&1; then
    warn "Registry manifest inspection failed for ${IMAGE}:${TAG}. This can be a Podman/GHCR auth visibility issue even when push succeeded."
  fi
else
  BUILDX_ARGS=()
  if [[ -n "${BUILDER}" ]]; then
    BUILDX_ARGS+=(--builder "${BUILDER}")
  fi

  log "[3/4] Building and pushing multi-platform image ${IMAGE}:${TAG} (${PLATFORMS})"
  docker buildx build \
    "${BUILDX_ARGS[@]}" \
    --platform "${PLATFORMS}" \
    --push \
    --build-arg VERSION="${PULSAR_VERSION}" \
    --build-arg IMAGE_TITLE="${IMAGE_NAME}" \
    --build-arg IMAGE_SOURCE="$(git config --get remote.origin.url 2>/dev/null || echo unknown)" \
    --build-arg IMAGE_REVISION="${REVISION}" \
    --build-arg IMAGE_CREATED="${CREATED}" \
    -t "${IMAGE}:${TAG}" \
    -f ./Dockerfile .

  log "[4/4] Verifying manifest in registry"
  if ! docker buildx imagetools inspect "${IMAGE}:${TAG}" >/dev/null 2>&1; then
    fail "Publish finished but manifest verification failed for ${IMAGE}:${TAG}"
  fi
fi

log "Done. Published ${IMAGE}:${TAG}"
