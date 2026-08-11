#!/usr/bin/env bash
# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# setup-nvswitch-pki.sh — site-scoped PKI for NVSwitch control-plane mTLS.
#
# Creates a dedicated CA (separate from the main Vault-backed nicoca PKI) so
# that NVSwitch mTLS certificates are isolated from the general NICo PKI:
#
#   ${SITE_NAME}-nvswitch-self-issuer   (self-signed bootstrap)
#   ${SITE_NAME}-nvswitch-ca            (10-year CA in cert-manager namespace)
#   ${SITE_NAME}-nvswitch-server-issuer         \
#   ${SITE_NAME}-nvswitch-rms-client-issuer      > purpose-specific issuers
#   ${SITE_NAME}-nvswitch-nico-client-issuer    /
#   nvswitch-nico-client-certificate    (NICo client cert, nico-system ns)
#
# Optionally applies CertificateRequestPolicies when approver-policy is installed.
#
# After issuing the NICo client cert the script upgrades the NICo Core helm
# release to mount it at /var/run/secrets/nvswitch-client/ in nico-api, then
# restarts the pod so it picks up the new mount and can read the [nvlink_config]
# cert paths from the site config TOML.
#
# Runs TWO ways, both idempotent:
#   1. from setup.sh:   ./setup.sh --with-nvswitch-pki ...
#   2. standalone:      helm-prereqs/nvswitch-pki/setup-nvswitch-pki.sh
#
# Environment:
#   SITE_NAME                   Site identifier. Default: siteName from helm-prereqs/values.yaml.
#   NVSWITCH_TLS_DOMAIN         DNS SAN on the NMX-C server cert; the name nico-api dials
#                               for nmx_c_tls_authority. Default: ${SITE_NAME}.forge
#   NVSWITCH_SPIFFE_TRUST_DOMAIN  SPIFFE trust domain for URI SANs. Default: forge.local
#   NICO_NS                     NICo Core namespace. Default: nico-system
#   NICO_RELEASE                NICo Core helm release name. Default: nico
#   CERT_MANAGER_NS             cert-manager namespace. Default: cert-manager
#   NICO_HELM_UPGRADE           true (default) = upgrade the NICo helm release to add the
#                               volume mount; false = print the upgrade command instead.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PREREQS_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

command -v kubectl  >/dev/null || { echo "ERROR: kubectl not found"  >&2; exit 1; }
command -v helm     >/dev/null || { echo "ERROR: helm not found"     >&2; exit 1; }
command -v envsubst >/dev/null || { echo "ERROR: envsubst not found (install gettext)" >&2; exit 1; }

# siteName may be bare, single- or double-quoted in YAML (same parse as setup.sh / install-observability.sh).
_VALUES_SITE="$(awk '/^siteName:/{v=$2; gsub(/["'"'"']/,"",v); print v}' "${PREREQS_DIR}/values.yaml" 2>/dev/null || true)"
export SITE_NAME="${SITE_NAME:-${_VALUES_SITE}}"
if [[ -z "${SITE_NAME}" ]]; then
    echo "ERROR: SITE_NAME is unset and siteName is empty in helm-prereqs/values.yaml" >&2
    exit 1
fi

export NVSWITCH_TLS_DOMAIN="${NVSWITCH_TLS_DOMAIN:-${SITE_NAME}.forge}"
export NVSWITCH_SPIFFE_TRUST_DOMAIN="${NVSWITCH_SPIFFE_TRUST_DOMAIN:-forge.local}"
export NICO_NS="${NICO_NS:-nico-system}"
export CERT_MANAGER_NS="${CERT_MANAGER_NS:-cert-manager}"
NICO_RELEASE="${NICO_RELEASE:-nico}"
NICO_HELM_UPGRADE="${NICO_HELM_UPGRADE:-true}"

echo "=== NVSwitch PKI (site '${SITE_NAME}') ==="
echo "    TLS domain  : ${NVSWITCH_TLS_DOMAIN}"
echo "    SPIFFE domain: ${NVSWITCH_SPIFFE_TRUST_DOMAIN}"
echo "    NICo ns     : ${NICO_NS}"
echo "    cert-manager: ${CERT_MANAGER_NS}"

# --- [1/5] CA + ClusterIssuers -----------------------------------------------
echo "--- [1/5] CA and ClusterIssuers"

# Idempotent: re-apply the self-issuer, CA Certificate, and purpose-specific
# ClusterIssuers. The CA Certificate uses rotationPolicy: Never so re-apply
# does not rotate the CA private key.
envsubst '${SITE_NAME} ${CERT_MANAGER_NS}' \
    < "${SCRIPT_DIR}/nvswitch-ca.yaml" | kubectl apply -f -

echo "Waiting for ${SITE_NAME}-nvswitch-ca to be issued..."
kubectl wait --for=condition=Ready \
    certificate/"${SITE_NAME}-nvswitch-ca" \
    -n "${CERT_MANAGER_NS}" --timeout=120s

# --- [2/5] CertificateRequestPolicies (optional) ------------------------------
echo "--- [2/5] CertificateRequestPolicies"
if kubectl get crd certificaterequestpolicies.policy.cert-manager.io &>/dev/null; then
    echo "    approver-policy detected — applying NVSwitch CertificateRequestPolicies"
    envsubst '${SITE_NAME} ${CERT_MANAGER_NS} ${NICO_NS} ${NVSWITCH_TLS_DOMAIN} ${NVSWITCH_SPIFFE_TRUST_DOMAIN}' \
        < "${SCRIPT_DIR}/nvswitch-certificate-policies.yaml" | kubectl apply -f -
else
    echo "    approver-policy not installed — skipping CertificateRequestPolicies (built-in approver auto-approves)"
fi

# --- [3/5] NICo client certificate -------------------------------------------
echo "--- [3/5] NICo client certificate (${NICO_NS}/nvswitch-nico-client-certificate)"
envsubst '${SITE_NAME} ${NICO_NS} ${NVSWITCH_SPIFFE_TRUST_DOMAIN}' \
    < "${SCRIPT_DIR}/nvswitch-nico-client-certificate.yaml" | kubectl apply -f -

echo "Waiting for nvswitch-nico-client-certificate to be issued..."
kubectl wait --for=condition=Ready \
    certificate/nvswitch-nico-client-certificate \
    -n "${NICO_NS}" --timeout=120s
echo "NICo client certificate ready"

# --- [4/5] Mount cert into nico-api (helm upgrade) ----------------------------
echo "--- [4/5] Mounting nvswitch-client-tls into nico-api"

_CHART_DIR="${PREREQS_DIR}/../helm"
_VALUES_OVERLAY="${SCRIPT_DIR}/values-nvswitch-nico-client.yaml"
# Display-only hint for deferred / NICO_HELM_UPGRADE=false paths.
_UPGRADE_HINT="helm upgrade ${NICO_RELEASE} ${_CHART_DIR} -n ${NICO_NS} --reuse-values -f ${_VALUES_OVERLAY}"

if ! helm status "${NICO_RELEASE}" -n "${NICO_NS}" >/dev/null 2>&1; then
    echo "    NICo release '${NICO_RELEASE}' not found in ${NICO_NS} — mount deferred."
    echo "    After installing NICo Core, run:"
    echo "      ${_UPGRADE_HINT}"
elif [[ "${NICO_HELM_UPGRADE}" == "true" ]]; then
    echo "    Upgrading NICo Core to add the nvswitch-client-tls volume mount..."
    helm upgrade "${NICO_RELEASE}" "${_CHART_DIR}" -n "${NICO_NS}" --reuse-values \
        -f "${_VALUES_OVERLAY}" --wait --timeout 300s
    echo "    NICo Core upgraded"
else
    echo "    NICO_HELM_UPGRADE=false — run manually to add the volume mount:"
    echo "      ${_UPGRADE_HINT}"
fi

# --- [5/5] Summary -----------------------------------------------------------
echo ""
echo "=== NVSwitch PKI install complete (site: ${SITE_NAME}) ==="
echo ""
echo "  CA issuer  : ${SITE_NAME}-nvswitch-self-issuer (ClusterIssuer)"
echo "  CA cert    : ${SITE_NAME}-nvswitch-ca (${CERT_MANAGER_NS}/secret: ${SITE_NAME}-nvswitch-ca)"
echo "  Issuers    : ${SITE_NAME}-nvswitch-{server,rms-client,nico-client}-issuer"
echo "  NICo cert  : ${NICO_NS}/nvswitch-nico-client-certificate"
echo "  Mount path : /var/run/secrets/nvswitch-client/{ca.crt,tls.crt,tls.key}"
echo ""
echo "  Add to your site config TOML ([nvlink_config] block):"
echo "    [nvlink_config]"
echo "    enabled = true"
echo "    allow_insecure = false"
echo "    nmx_c_tls_ca_cert_path    = \"/var/run/secrets/nvswitch-client/ca.crt\""
echo "    nmx_c_tls_client_cert_path = \"/var/run/secrets/nvswitch-client/tls.crt\""
echo "    nmx_c_tls_client_key_path  = \"/var/run/secrets/nvswitch-client/tls.key\""
echo "    nmx_c_tls_authority        = \"${NVSWITCH_TLS_DOMAIN}\""
echo ""
echo "  Server and RMS client certs (issued by rack-manager) use:"
echo "    server issuer : ${SITE_NAME}-nvswitch-server-issuer"
echo "    RMS issuer    : ${SITE_NAME}-nvswitch-rms-client-issuer"
echo "  See nvswitch-pki/README.md for the full rack-manager values."
