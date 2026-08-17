#!/usr/bin/env bash
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

set -euo pipefail
set +x

usage() {
  cat <<'EOF'
Usage: ./publish-ghcr.sh [options] <owner> <image> [tag] [pulsar-version]

Options:
  --user <name>        GHCR username (default: GHCR_USER or GITHUB_USER)
  --login              Require GHCR_USER and GHCR_TOKEN and log in
  --no-login           Use the existing container registry credentials
  --platforms <list>   Build platforms (default: linux/amd64,linux/arm64)
  -h, --help           Show this help

Authentication uses GHCR_TOKEN or GITHUB_TOKEN through --password-stdin.
The Pulsar version defaults to the checkout version without a -SNAPSHOT suffix.
EOF
}

die() {
  printf 'ERROR: %s\n' "$*" >&2
  exit 1
}

require_value() {
  [[ $# -ge 2 && -n "$2" && "$2" != -* ]] || die "$1 requires a value"
}

user="${GHCR_USER:-${GITHUB_USER:-}}"
token="${GHCR_TOKEN:-${GITHUB_TOKEN:-}}"
export -n token
unset GHCR_TOKEN GITHUB_TOKEN
platforms="linux/amd64,linux/arm64"
login=auto

while [[ $# -gt 0 ]]; do
  case "$1" in
    --user) require_value "$@"; user="$2"; shift 2 ;;
    --login) [[ "$login" != no ]] || die "--login and --no-login cannot be combined"; login=yes; shift ;;
    --no-login) [[ "$login" != yes ]] || die "--login and --no-login cannot be combined"; login=no; shift ;;
    --platforms) require_value "$@"; platforms="$2"; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    --) shift; break ;;
    -*) die "unknown option: $1" ;;
    *) break ;;
  esac
done

[[ $# -ge 2 && $# -le 4 ]] || { usage >&2; exit 1; }

command -v git >/dev/null || die "git is required"
command -v docker >/dev/null || die "docker is required"

owner="$1"
image_name="$2"
[[ -n "$owner" && -n "$image_name" ]] || die "owner and image must not be empty"

project_version="$(awk -F= '$1 == "version" { print $2; exit }' gradle.properties)"
[[ -n "$project_version" ]] || die "version is missing from gradle.properties"
default_pulsar_version="${project_version%-SNAPSHOT}"
pulsar_version="${4:-$default_pulsar_version}"
[[ "$pulsar_version" == "$default_pulsar_version" ]] \
  || die "Pulsar $pulsar_version does not match checkout $project_version"

revision="$(git rev-parse HEAD 2>/dev/null)" || die "cannot determine Git revision"
tag="${3:-}"
if [[ -z "$tag" ]]; then
  tag="$pulsar_version-${revision:0:12}"
fi
image="ghcr.io/$owner/$image_name:$tag"
source="${IMAGE_SOURCE:-${GITHUB_SERVER_URL:-https://github.com}/${GITHUB_REPOSITORY:-devngho/pulsar}}"

if [[ "$login" != no && -n "$user" && -n "$token" ]]; then
  printf '%s' "$token" | docker login ghcr.io --username "$user" --password-stdin
elif [[ "$login" == yes ]]; then
  die "--login requires a username and GHCR_TOKEN/GITHUB_TOKEN"
fi
unset token

[[ -x ./gradlew ]] || die "./gradlew is required"
./gradlew :tiered-storage:tiered-storage-jcloud:assemble
nar="tiered-storage/jcloud/build/libs/tiered-storage-jcloud-$project_version.nar"
[[ -f "$nar" ]] || die "expected jcloud NAR not found: $nar"

build_args=(
  --file Dockerfile
  --build-arg "VERSION=$pulsar_version"
  --build-arg "IMAGE_TITLE=$image_name"
  --build-arg "IMAGE_SOURCE=$source"
  --build-arg "IMAGE_REVISION=$revision"
  --build-arg "IMAGE_CREATED=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
)

context="$(mktemp -d)"
cleanup() {
  rm -rf "$context"
  docker manifest rm "$image" >/dev/null 2>&1 || true
}
trap cleanup EXIT
cp Dockerfile "$context/Dockerfile"
cp "$nar" "$context/jcloud.nar"

IFS=, read -r -a platform_list <<< "$platforms"
platform_images=()
for platform in "${platform_list[@]}"; do
  [[ -n "$platform" ]] || die "platforms must not contain empty values"
  platform_image="${image}-${platform//\//-}"
  printf 'Publishing %s for %s\n' "$platform_image" "$platform"
  docker build "${build_args[@]}" --platform "$platform" --tag "$platform_image" "$context"
  docker push "$platform_image"
  platform_images+=("$platform_image")
done

docker manifest rm "$image" >/dev/null 2>&1 || true
docker manifest create "$image" "${platform_images[@]}"
docker manifest push "$image"
printf 'Published %s\n' "$image"
