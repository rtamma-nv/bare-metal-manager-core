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

use crate::typed_uuids::{TypedUuid, UuidSubtype};

/// Marker type for ExtensionServiceId.
pub struct ExtensionServiceIdMarker;

impl UuidSubtype for ExtensionServiceIdMarker {
    const TYPE_NAME: &'static str = "ExtensionServiceId";
}

/// ExtensionServiceId is a strongly typed UUID specific to an
/// extension service.
pub type ExtensionServiceId = TypedUuid<ExtensionServiceIdMarker>;

/// Marker type for AttachmentId.
pub struct AttachmentIdMarker;

impl UuidSubtype for AttachmentIdMarker {
    const TYPE_NAME: &'static str = "AttachmentId";
}

/// AttachmentId identifies one binding of a service-VPC-backed extension
/// service to one instance. Service-VPC endpoint /127 prefixes are derived
/// from (AttachmentId, DPU id); a derivation collision regenerates the
/// AttachmentId, so it is stable only for the lifetime of the binding.
pub type AttachmentId = TypedUuid<AttachmentIdMarker>;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::typed_uuid_tests;
    // Run all boilerplate TypedUuid tests for this type, also
    // ensuring TYPE_NAME and DB_COLUMN_NAME test correctly.
    typed_uuid_tests!(ExtensionServiceId, "ExtensionServiceId", "id");
}

#[cfg(test)]
mod attachment_tests {
    use super::*;
    use crate::typed_uuid_tests;
    typed_uuid_tests!(AttachmentId, "AttachmentId", "id");
}
