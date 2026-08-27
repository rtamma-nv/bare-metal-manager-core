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

use carbide_uuid::extension_service::{AttachmentId, ExtensionServiceId};
use carbide_uuid::instance::InstanceId;
use carbide_uuid::machine::MachineId;
use carbide_uuid::vpc::VpcPrefixId;
use chrono::{DateTime, Utc};
use ipnetwork::IpNetwork;
use serde::{Deserialize, Serialize};

/// One service-VPC endpoint: the /127 link prefix reserved for one
/// (attachment, DPU) pair inside the service VPC's derived ULA /48.
/// The `::0` address is the HBN side, `::1` the client side — the same
/// convention as instance PF/VF linknets.
#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow)]
pub struct ServiceVpcEndpoint {
    pub attachment_id: AttachmentId,
    pub dpu_machine_id: MachineId,
    pub extension_service_id: ExtensionServiceId,
    pub instance_id: InstanceId,
    pub vpc_prefix_id: VpcPrefixId,
    pub vpc_prefix: IpNetwork,
    pub prefix: IpNetwork,
    pub created: DateTime<Utc>,
}

/// Insert form of [`ServiceVpcEndpoint`].
#[derive(Debug, Clone)]
pub struct NewServiceVpcEndpoint {
    pub attachment_id: AttachmentId,
    pub dpu_machine_id: MachineId,
    pub extension_service_id: ExtensionServiceId,
    pub instance_id: InstanceId,
    pub vpc_prefix_id: VpcPrefixId,
    pub vpc_prefix: IpNetwork,
    pub prefix: IpNetwork,
}
