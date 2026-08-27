/*
 * SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

//! Deterministic IPv6 ULA addressing for service VPCs (DPU block-storage
//! design §6.2).
//!
//! Each service VPC owns a /48 selected by hashing its VPC UUID into the
//! configured ULA root; each (attachment, DPU) endpoint gets a /127 selected
//! by hashing inside that /48. Nothing allocates: addresses are computed from
//! identifiers, and the database's overlap constraints turn the rare hash
//! collision into a retry (next probe value / regenerated attachment id).

use std::net::Ipv6Addr;

use carbide_uuid::extension_service::AttachmentId;
use carbide_uuid::machine::MachineId;
use carbide_uuid::vpc::{VpcId, VpcPrefixId};
use db::DatabaseError;
use ipnetwork::{IpNetwork, Ipv6Network};
use model::metadata::Metadata;
use model::vpc::Vpc;
use model::vpc_prefix::{NewVpcPrefix, VpcPrefix, VpcPrefixConfig};
use sha2::{Digest, Sha256};
use sqlx::PgConnection;

/// Label marking a VpcPrefix row as a derived service-VPC ULA /48. Such rows
/// are excluded from FNN linknet allocation and torn down with the last
/// referencing extension-service registration.
pub(crate) const SERVICE_VPC_ULA_LABEL: &str = "carbide.nvidia.com/service-vpc-ula";

/// The ULA root every service-VPC /48 is derived from: the RFC 4193
/// locally-assigned half of the ULA space. Fixed by design; seeded at startup
/// as an operator-managed SitePrefix.
pub(crate) fn service_vpc_ula_root() -> IpNetwork {
    "fd00::/8".parse().expect("fd00::/8 is a valid prefix")
}

/// Upper bound on /48 probe attempts before giving up. 40 hashed bits make
/// even one collision unlikely; hitting this bound indicates a saturated or
/// misconfigured ULA root.
const MAX_ULA_PROBES: u8 = 16;

/// Derived /48 length per RFC 4193 (ULA global-ID boundary).
const SERVICE_VPC_PREFIX_LEN: u8 = 48;

/// Derives the service VPC's candidate /48: `root` bits, then
/// `48 - root_len` bits taken from the low end of
/// SHA-256("service-vpc-ula/48:{vpc_id}:{probe}").
pub(crate) fn derive_service_vpc_ula_prefix(
    root: IpNetwork,
    vpc_id: VpcId,
    probe: u8,
) -> Result<Ipv6Network, DatabaseError> {
    let IpNetwork::V6(root) = root else {
        return Err(DatabaseError::InvalidArgument(format!(
            "service_vpc_ula_root `{root}` is not an IPv6 prefix"
        )));
    };
    if root.prefix() > SERVICE_VPC_PREFIX_LEN {
        return Err(DatabaseError::InvalidArgument(format!(
            "service_vpc_ula_root `{root}` is longer than /{SERVICE_VPC_PREFIX_LEN}"
        )));
    }

    let digest = Sha256::digest(format!("service-vpc-ula/48:{vpc_id}:{probe}"));
    let hash = u64::from_be_bytes(digest[0..8].try_into().expect("digest is 32 bytes"));
    let select_bits = u32::from(SERVICE_VPC_PREFIX_LEN - root.prefix());
    let selector = u128::from(hash) & ((1u128 << select_bits) - 1);

    let base = u128::from(root.network());
    let network = base | (selector << (128 - u32::from(SERVICE_VPC_PREFIX_LEN)));
    Ipv6Network::new(Ipv6Addr::from(network), SERVICE_VPC_PREFIX_LEN)
        .map_err(|e| DatabaseError::InvalidArgument(e.to_string()))
}

/// Derives the /127 endpoint link prefix for one (attachment, DPU) pair
/// inside the service VPC's /48: the low 79 bits of
/// SHA-256("service-vpc-endpoint/127:{attachment_id}:{dpu_id}"), shifted onto
/// an even boundary so the `::0` (HBN) / `::1` (client) host-bit convention
/// of instance PF/VF linknets holds.
pub(crate) fn derive_endpoint_prefix(
    service_prefix: Ipv6Network,
    attachment_id: AttachmentId,
    dpu_id: &MachineId,
) -> Result<Ipv6Network, DatabaseError> {
    debug_assert_eq!(service_prefix.prefix(), SERVICE_VPC_PREFIX_LEN);

    let digest = Sha256::digest(format!("service-vpc-endpoint/127:{attachment_id}:{dpu_id}"));
    let hash = u128::from_be_bytes(digest[0..16].try_into().expect("digest is 32 bytes"));
    // 128 - 48 = 80 host bits; the /127 index uses 79 of them, and the final
    // host bit stays 0 (the HBN end of the linknet).
    let selector = hash & ((1u128 << 79) - 1);

    let base = u128::from(service_prefix.network());
    let network = base | (selector << 1);
    Ipv6Network::new(Ipv6Addr::from(network), 127)
        .map_err(|e| DatabaseError::InvalidArgument(e.to_string()))
}

/// Returns the VPC's derived service-ULA /48, creating it on first use.
///
/// Must run inside the registration transaction, after `validate_service_vpc`
/// (i.e., with the VPC row lock held) so concurrent registrations against the
/// same VPC serialize. Probing: candidate /48s are checked with
/// `db::vpc_prefix::probe` and persisted; the global exclusion constraint on
/// `network_vpc_prefixes` backstops any probe/persist race, which is treated
/// as a collision and retried with the next probe value.
pub(crate) async fn ensure_service_vpc_ula_prefix(
    txn: &mut PgConnection,
    vpc: &Vpc,
) -> Result<VpcPrefix, DatabaseError> {
    let ula_root = service_vpc_ula_root();
    // Reuse: one /48 per VPC, shared by every service bound to it.
    if let Some(existing) = db::vpc_prefix::find_by_vpc(&mut *txn, vpc.id)
        .await?
        .into_iter()
        .find(|p| p.metadata.labels.contains_key(SERVICE_VPC_ULA_LABEL))
    {
        return Ok(existing);
    }

    // The seeded ULA-root SitePrefix is the parent of every derived /48.
    // Shared advisory lock so configured-prefix reconciliation cannot retire
    // the root underneath this attachment.
    db::site_prefix::lock_operator_managed_site_prefix_attachments(&mut *txn).await?;
    let parent = db::site_prefix::find_legacy_operator_managed_for_vpc_prefix_attachment(
        &mut *txn, ula_root,
    )
    .await?
    .into_iter()
    .find(|p| p.config.prefix == ula_root);
    let Some(parent) = parent else {
        return Err(DatabaseError::FailedPrecondition(format!(
            "service-VPC ULA root SitePrefix `{ula_root}` is not seeded; \
             startup reconciliation must run first"
        )));
    };

    for probe in 0..MAX_ULA_PROBES {
        let candidate = derive_service_vpc_ula_prefix(ula_root, vpc.id, probe)?;
        let candidate_net = IpNetwork::V6(candidate);
        // The same admission rules as operator-driven VpcPrefix creation; the
        // status code flattens to FailedPrecondition here, but the rules stay
        // single-sourced in the handler.
        crate::handlers::vpc_prefix::validate_site_prefix_attachment(&parent, vpc, candidate_net)
            .map_err(|e| DatabaseError::FailedPrecondition(e.to_string()))?;
        if !db::vpc_prefix::probe(candidate_net, &mut *txn)
            .await?
            .is_empty()
        {
            continue;
        }

        let new_prefix = NewVpcPrefix {
            id: VpcPrefixId::new(),
            site_prefix_id: Some(parent.id),
            vpc_id: vpc.id,
            config: VpcPrefixConfig {
                prefix: candidate_net,
            },
            metadata: Metadata {
                name: format!("service-vpc-ula-{}", vpc.id),
                description: format!(
                    "Derived service-VPC ULA /48 (probe {probe}); managed by the \
                     extension-service lifecycle"
                ),
                labels: [(SERVICE_VPC_ULA_LABEL.to_string(), String::new())]
                    .into_iter()
                    .collect(),
            },
        };
        match db::vpc_prefix::persist(new_prefix, vpc.version, &mut *txn).await {
            Ok(prefix) => return Ok(prefix),
            // The exclusion constraint fired between probe and persist: a
            // concurrent insert claimed overlapping space. Treat as collision.
            Err(DatabaseError::InvalidArgument(msg))
                if msg.contains("overlaps an existing or deleting VPC prefix") =>
            {
                continue;
            }
            Err(e) => return Err(e),
        }
    }

    Err(DatabaseError::FailedPrecondition(format!(
        "could not derive a free service-VPC /48 for VPC {} within {MAX_ULA_PROBES} probes; \
         the ULA root is saturated or misconfigured",
        vpc.id
    )))
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use super::*;

    fn vpc(id: &str) -> VpcId {
        VpcId::from_str(id).unwrap()
    }

    #[test]
    fn service_vpc_prefix_derivation_is_deterministic_and_contained() {
        // (root, vpc uuid) table: derivation must be a /48, inside the root,
        // and stable across calls.
        let cases = [
            ("fd00::/8", "9c5405fe-2b6f-4bd2-a0c1-3d0c1c1f6a01"),
            ("fd00::/8", "9c5405fe-2b6f-4bd2-a0c1-3d0c1c1f6a02"),
            ("fd12:3400::/24", "9c5405fe-2b6f-4bd2-a0c1-3d0c1c1f6a01"),
            (
                "fd12:3456:7800::/40",
                "9c5405fe-2b6f-4bd2-a0c1-3d0c1c1f6a01",
            ),
        ];
        for (root, vpc_id) in cases {
            let root: IpNetwork = root.parse().unwrap();
            let first = derive_service_vpc_ula_prefix(root, vpc(vpc_id), 0).unwrap();
            let again = derive_service_vpc_ula_prefix(root, vpc(vpc_id), 0).unwrap();
            assert_eq!(first, again, "derivation must be deterministic");
            assert_eq!(first.prefix(), 48);
            let IpNetwork::V6(root_v6) = root else {
                unreachable!()
            };
            assert!(
                root_v6.contains(first.network()),
                "{first} must lie within {root}"
            );
        }
    }

    #[test]
    fn service_vpc_prefix_probe_and_vpc_change_move_the_prefix() {
        let root: IpNetwork = "fd00::/8".parse().unwrap();
        let a = vpc("9c5405fe-2b6f-4bd2-a0c1-3d0c1c1f6a01");
        let b = vpc("9c5405fe-2b6f-4bd2-a0c1-3d0c1c1f6a02");
        let a0 = derive_service_vpc_ula_prefix(root, a, 0).unwrap();
        let a1 = derive_service_vpc_ula_prefix(root, a, 1).unwrap();
        let b0 = derive_service_vpc_ula_prefix(root, b, 0).unwrap();
        assert_ne!(a0, a1, "probe must move the candidate");
        assert_ne!(a0, b0, "different VPCs must derive different prefixes");
    }

    #[test]
    fn endpoint_prefix_follows_linknet_host_bit_convention() {
        let root: IpNetwork = "fd00::/8".parse().unwrap();
        let service_prefix =
            derive_service_vpc_ula_prefix(root, vpc("9c5405fe-2b6f-4bd2-a0c1-3d0c1c1f6a01"), 0)
                .unwrap();
        let attachment = AttachmentId::from_str("6e1a49a2-71d4-4f8e-9c25-58c0c1a4b001").unwrap();
        let dpu =
            MachineId::from_str("fm100hseddco33hvlofuqvg543p6p9aj60g76q5cq491g9m9tgtf2dk0530")
                .expect("fixture machine id must parse");

        let endpoint = derive_endpoint_prefix(service_prefix, attachment, &dpu).unwrap();
        let again = derive_endpoint_prefix(service_prefix, attachment, &dpu).unwrap();
        assert_eq!(endpoint, again, "derivation must be deterministic");
        assert_eq!(endpoint.prefix(), 127);
        assert!(
            service_prefix.contains(endpoint.network()),
            "{endpoint} must lie within {service_prefix}"
        );
        // ::0 (even) is the HBN end; the client takes ::1, matching
        // get_host_ip's /127 convention for instance PF/VF linknets.
        let network_bits = u128::from(endpoint.network());
        assert_eq!(network_bits & 1, 0, "network address must be the even end");
        let client = carbide_network::virtualization::get_host_ip(&IpNetwork::V6(endpoint))
            .expect("host ip derivable");
        assert_eq!(u128::from(endpoint.network()) + 1, {
            let std::net::IpAddr::V6(v6) = client else {
                panic!("expected v6")
            };
            u128::from(v6)
        });
    }
}
