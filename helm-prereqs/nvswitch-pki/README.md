# NVSwitch PKI — site-scoped CA for NVSwitch control-plane mTLS

An optional, self-contained PKI for sites that use NVSwitches managed via the
NMX-C (NVSwitch controller) mTLS interface. Keeps NVSwitch certificates on a
dedicated CA separate from the main Vault-backed nicoca PKI so the NVSwitch
trust boundary is explicit and independently rotatable.

```text
 cert-manager ns
 ┌──────────────────────────────────────────────────────────────────────┐
 │  ${SITE}-nvswitch-self-issuer (ClusterIssuer, selfSigned)            │
 │        │                                                              │
 │        ▼                                                              │
 │  ${SITE}-nvswitch-ca  (Certificate, 10yr, ECDSA P-384, isCA=true)   │
 │        │                                                              │
 │   Secret: ${SITE}-nvswitch-ca  (referenced by three ClusterIssuers) │
 └──────────────────────────────────────────────────────────────────────┘
        │
        ├── ${SITE}-nvswitch-server-issuer      → NMX-C server cert
        ├── ${SITE}-nvswitch-rms-client-issuer  → RMS client cert
        └── ${SITE}-nvswitch-nico-client-issuer → NICo client cert
                                                    (mounted into nico-api)
```

## Quick start

**With a fresh install:**

```bash
./setup.sh --with-nvswitch-pki [your usual flags...]
```

**Standalone, on an existing cluster:**

```bash
helm-prereqs/nvswitch-pki/setup-nvswitch-pki.sh
```

Both are idempotent — re-run freely to reconcile.

## Configuration (env vars)

| Variable | Default | Effect |
|---|---|---|
| `SITE_NAME` | `siteName` from `helm-prereqs/values.yaml` | prefix for all resource names |
| `NVSWITCH_TLS_DOMAIN` | `${SITE_NAME}.forge` | DNS SAN on the NMX-C server cert; the name nico-api uses for `nmx_c_tls_authority` |
| `NVSWITCH_SPIFFE_TRUST_DOMAIN` | `forge.local` | SPIFFE trust domain for URI SANs |
| `NICO_NS` | `nico-system` | namespace where nico-api runs |
| `NICO_RELEASE` | `nico` | NICo Core helm release name |
| `CERT_MANAGER_NS` | `cert-manager` | cert-manager namespace |
| `NICO_HELM_UPGRADE` | `true` | `false` = print the upgrade command instead of running it |

## Site config TOML

After running the script, add the `[nvlink_config]` block to your site config
(the `nicoApiSiteConfig` literal in your `--core-values` file):

```toml
[nvlink_config]
enabled = true
allow_insecure = false
nmx_c_tls_ca_cert_path     = "/var/run/secrets/nvswitch-client/ca.crt"
nmx_c_tls_client_cert_path = "/var/run/secrets/nvswitch-client/tls.crt"
nmx_c_tls_client_key_path  = "/var/run/secrets/nvswitch-client/tls.key"
nmx_c_tls_authority        = "<SITE_NAME>.forge"

[nvlink_config.nmx_c_certificate_rotation]
enabled = true
```

The secret `nvswitch-nico-client-certificate` is mounted at
`/var/run/secrets/nvswitch-client/` by `values-nvswitch-nico-client.yaml`;
the script applies this overlay automatically (or prints the command when
`NICO_HELM_UPGRADE=false`).

> **Warning:** Helm replaces list values rather than merging them. This overlay
> sets `nico-api.extraVolumes` / `extraVolumeMounts` as complete lists. A later
> `helm upgrade --reuse-values -f …` that also sets those keys will overwrite
> the NVSwitch mounts (and the reverse). Keep all extra volumes/mounts in a
> single values file when combining overlays.

## Rack-manager server and RMS client certs

The NMX-C server cert and the RMS mTLS client cert are issued by rack-manager,
not by this script. Point your rack-manager chart values at the new site-scoped
issuers instead of `vault-forge-issuer`:

```yaml
# in your rack-manager values
apiServer:
  tls:
    # NMX-C server certificate
    issuerRef:
      name: <SITE_NAME>-nvswitch-server-issuer
      kind: ClusterIssuer
    dnsNames:
      - <SITE_NAME>.forge
    uris:
      - spiffe://forge.local/nico-system/sa/<SITE_NAME>-nmxc
    usages:
      - digital signature
      - server auth
      - client auth   # required by NVOS when binding via nv action update cluster apps nmx-controller

  # RMS mTLS client certificate
  clientCert:
    issuerRef:
      name: <SITE_NAME>-nvswitch-rms-client-issuer
      kind: ClusterIssuer
    uris:
      - spiffe://forge.local/nico-system/sa/<SITE_NAME>-rms
    usages:
      - digital signature
      - client auth
```

## approver-policy

If the cluster runs cert-manager approver-policy, the script automatically
applies `nvswitch-certificate-policies.yaml`, which enforces:

- **CA policy**: only the 10-year CA cert may use the self-signed issuer
- **Server policy**: DNS SAN must be `${NVSWITCH_TLS_DOMAIN}`; SPIFFE URI must match the nmxc service account; `client auth` usage is in the allowlist but cannot be enforced as required (approver-policy treats `allowed.usages` as a plain allowlist)
- **RMS client policy**: SPIFFE URI must match the rms service account; no DNS SAN allowed
- **NICo client policy**: SPIFFE URI must match the nico-nmxc service account

Without approver-policy the built-in cert-manager approver auto-approves all
requests and policies are not needed.

## Uninstall

`helm-prereqs/clean.sh` removes all NVSwitch PKI resources, or by hand:

```bash
# Certificates and issuers
kubectl delete certificate nvswitch-nico-client-certificate -n nico-system --ignore-not-found
kubectl delete certificate <SITE_NAME>-nvswitch-ca -n cert-manager --ignore-not-found
kubectl delete clusterissuer \
    <SITE_NAME>-nvswitch-self-issuer \
    <SITE_NAME>-nvswitch-server-issuer \
    <SITE_NAME>-nvswitch-rms-client-issuer \
    <SITE_NAME>-nvswitch-nico-client-issuer \
    --ignore-not-found

# Approver-policy (if installed)
kubectl delete certificaterequestpolicy \
    <SITE_NAME>-nvswitch-ca-policy \
    <SITE_NAME>-nvswitch-server-policy \
    <SITE_NAME>-nvswitch-rms-client-policy \
    <SITE_NAME>-nvswitch-nico-client-policy \
    --ignore-not-found
kubectl delete clusterrole clusterrolebinding \
    cert-manager-policy:<SITE_NAME>-nvswitch \
    --ignore-not-found

# Remove the volume mount from nico-api
helm upgrade nico <chart> -n nico-system --reuse-values \
    --set 'nico-api.extraVolumes=null' \
    --set 'nico-api.extraVolumeMounts=null'
```

## Files

| File | Role |
|---|---|
| `setup-nvswitch-pki.sh` | the installer (setup.sh phase AND standalone) |
| `nvswitch-ca.yaml` | envsubst template: self-signed issuer, CA cert, purpose-specific ClusterIssuers |
| `nvswitch-certificate-policies.yaml` | envsubst template: CertificateRequestPolicies + RBAC (approver-policy only) |
| `nvswitch-nico-client-certificate.yaml` | envsubst template: NICo client cert for nico-api |
| `values-nvswitch-nico-client.yaml` | helm values overlay: adds the nvswitch-client-tls volume/volumeMount to nico-api |
