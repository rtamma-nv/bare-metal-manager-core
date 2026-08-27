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

//! Service-VPC endpoint /127 reservations (DPU block-storage design §6.2).
//!
//! Prefixes are hash-derived from (attachment_id, dpu_id); the table's
//! exclusion constraint is the collision detector. A collision is surfaced as
//! [`ServiceVpcEndpointError::PrefixCollision`] so callers can regenerate the
//! attachment id and retry.

use carbide_uuid::extension_service::{AttachmentId, ExtensionServiceId};
use carbide_uuid::instance::InstanceId;
use model::service_vpc_endpoint::{NewServiceVpcEndpoint, ServiceVpcEndpoint};
use sqlx::PgConnection;

use crate::DatabaseError;

const PREFIX_EXCLUSION_CONSTRAINT: &str = "service_vpc_endpoints_prefix_excl";

/// Errors from endpoint persistence, separating the retryable derivation
/// collision from plain database failures.
#[derive(Debug, thiserror::Error)]
pub enum ServiceVpcEndpointError {
    /// A derived /127 overlaps an existing reservation: regenerate the
    /// attachment id and re-derive.
    #[error("derived service-VPC endpoint prefix collides with an existing reservation")]
    PrefixCollision,
    #[error(transparent)]
    Database(#[from] DatabaseError),
}

/// Inserts all endpoints of one attachment atomically. On a prefix-overlap
/// conflict returns [`ServiceVpcEndpointError::PrefixCollision`]; the caller
/// owns transaction/savepoint rollback before retrying.
pub async fn persist_all(
    txn: &mut PgConnection,
    endpoints: &[NewServiceVpcEndpoint],
) -> Result<Vec<ServiceVpcEndpoint>, ServiceVpcEndpointError> {
    let mut inserted = Vec::with_capacity(endpoints.len());
    let query = "INSERT INTO service_vpc_endpoints
            (attachment_id, dpu_machine_id, extension_service_id, instance_id,
             vpc_prefix_id, vpc_prefix, prefix)
            VALUES ($1, $2, $3, $4, $5, $6, $7)
            RETURNING attachment_id, dpu_machine_id, extension_service_id, instance_id,
                      vpc_prefix_id, vpc_prefix, prefix, created";
    for endpoint in endpoints {
        let row = sqlx::query_as::<_, ServiceVpcEndpoint>(query)
            .bind(endpoint.attachment_id)
            .bind(endpoint.dpu_machine_id)
            .bind(endpoint.extension_service_id)
            .bind(endpoint.instance_id)
            .bind(endpoint.vpc_prefix_id)
            .bind(endpoint.vpc_prefix)
            .bind(endpoint.prefix)
            .fetch_one(&mut *txn)
            .await;
        match row {
            Ok(row) => inserted.push(row),
            Err(sqlx::Error::Database(db_err))
                if db_err.constraint() == Some(PREFIX_EXCLUSION_CONSTRAINT) =>
            {
                return Err(ServiceVpcEndpointError::PrefixCollision);
            }
            Err(e) => return Err(DatabaseError::query(query, e).into()),
        }
    }
    Ok(inserted)
}

/// Removes every endpoint of one attachment. Returns the number removed.
pub async fn delete_by_attachment(
    txn: &mut PgConnection,
    attachment_id: AttachmentId,
) -> Result<u64, DatabaseError> {
    let query = "DELETE FROM service_vpc_endpoints WHERE attachment_id = $1";
    let result = sqlx::query(query)
        .bind(attachment_id)
        .execute(&mut *txn)
        .await
        .map_err(|e| DatabaseError::query(query, e))?;
    Ok(result.rows_affected())
}

/// Removes every endpoint of one instance (instance deletion/deallocation).
pub async fn delete_by_instance(
    txn: &mut PgConnection,
    instance_id: InstanceId,
) -> Result<u64, DatabaseError> {
    let query = "DELETE FROM service_vpc_endpoints WHERE instance_id = $1";
    let result = sqlx::query(query)
        .bind(instance_id)
        .execute(&mut *txn)
        .await
        .map_err(|e| DatabaseError::query(query, e))?;
    Ok(result.rows_affected())
}

/// Removes every endpoint of one extension service (registration deletion).
pub async fn delete_by_extension_service(
    txn: &mut PgConnection,
    extension_service_id: ExtensionServiceId,
) -> Result<u64, DatabaseError> {
    let query = "DELETE FROM service_vpc_endpoints WHERE extension_service_id = $1";
    let result = sqlx::query(query)
        .bind(extension_service_id)
        .execute(&mut *txn)
        .await
        .map_err(|e| DatabaseError::query(query, e))?;
    Ok(result.rows_affected())
}

/// Returns the endpoints of one (instance, extension service) binding,
/// ordered by DPU id. These rows are the source of truth for the binding's
/// attachment id and link prefixes; instance-config echoes are validated
/// against them.
pub async fn find_by_instance_and_service(
    txn: &mut PgConnection,
    instance_id: InstanceId,
    extension_service_id: ExtensionServiceId,
) -> Result<Vec<ServiceVpcEndpoint>, DatabaseError> {
    let query = "SELECT attachment_id, dpu_machine_id, extension_service_id, instance_id,
                        vpc_prefix_id, vpc_prefix, prefix, created
                 FROM service_vpc_endpoints
                 WHERE instance_id = $1 AND extension_service_id = $2
                 ORDER BY dpu_machine_id";
    sqlx::query_as::<_, ServiceVpcEndpoint>(query)
        .bind(instance_id)
        .bind(extension_service_id)
        .fetch_all(&mut *txn)
        .await
        .map_err(|e| DatabaseError::query(query, e))
}

/// Returns the endpoints of one attachment, ordered by DPU id for stable
/// comparison in callers and tests.
pub async fn find_by_attachment(
    txn: &mut PgConnection,
    attachment_id: AttachmentId,
) -> Result<Vec<ServiceVpcEndpoint>, DatabaseError> {
    let query = "SELECT attachment_id, dpu_machine_id, extension_service_id, instance_id,
                        vpc_prefix_id, vpc_prefix, prefix, created
                 FROM service_vpc_endpoints WHERE attachment_id = $1
                 ORDER BY dpu_machine_id";
    sqlx::query_as::<_, ServiceVpcEndpoint>(query)
        .bind(attachment_id)
        .fetch_all(&mut *txn)
        .await
        .map_err(|e| DatabaseError::query(query, e))
}
