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

//! Service-VPC attachment delivery and gated cleanup through the DPF SDK.
//!
//! Covers the increment-3 machine-controller integration: the
//! `WaitingForExtensionServicesConfig` handler reconciles the attachment CRs
//! (NAD, DPUServiceInterface, DPUServiceChain) via `DpfOperations`, and CR
//! removal plus endpoint /127 release happen only after every DPU has
//! acknowledged the service as terminated.

use std::sync::{Arc, Mutex};
use std::time::SystemTime;

use ::rpc::forge as rpc;
use ::rpc::forge::forge_server::Forge;
use carbide_dpf::ServiceVpcAttachmentRequest;
use carbide_machine_controller::dpf::{DpfOperations, MockDpfOperations};
use carbide_uuid::machine::MachineId;
use model::machine::{InstanceState, ManagedHostState};
use tonic::Request;

use crate::tests::common::api_fixtures::instance::{
    default_os_config, default_tenant_config, single_interface_network_config,
};
use crate::tests::common::api_fixtures::{
    TestEnv, TestEnvOverrides, create_managed_host, create_test_env_with_overrides, get_config,
};

// Extension services are currently rejected on DPF-ingested hosts (see the
// used_for_ingestion guards in instance allocation and update), so this test
// runs against a regular managed host. The machine controller talks to the
// DPF cluster whenever a dpf_sdk is configured, independent of how the host
// was ingested.
const TEST_SERVICE_DATA: &str = "apiVersion: v1\nkind: Pod\nmetadata:\n  name: test\nspec:\n  containers:\n    - name: app\n      image: nginx:1.27";

/// Calls recorded by the mock, so the test can assert what the handler asked
/// DPF to do and when.
#[derive(Clone, Default)]
struct DpfCallLog {
    ensures: Arc<Mutex<Vec<ServiceVpcAttachmentRequest>>>,
    removes: Arc<Mutex<Vec<String>>>,
}

fn service_vpc_mock(log: &DpfCallLog) -> MockDpfOperations {
    let mut mock = MockDpfOperations::new();
    let ensures = log.ensures.clone();
    mock.expect_ensure_service_vpc_attachment()
        .returning(move |req| {
            ensures.lock().unwrap().push(req.clone());
            Ok(())
        });
    mock.expect_service_vpc_attachment_ready()
        .returning(|_, _| Ok(true));
    let removes = log.removes.clone();
    mock.expect_remove_service_vpc_attachments()
        .returning(move |attachment_id| {
            removes.lock().unwrap().push(attachment_id.to_string());
            Ok(())
        });
    mock
}

async fn set_assigned_state(env: &TestEnv, host_id: &MachineId, instance_state: InstanceState) {
    let mut txn = env.db_txn().await;
    db::machine::update_state(
        txn.as_mut(),
        host_id,
        &ManagedHostState::Assigned { instance_state },
    )
    .await
    .unwrap();
    txn.commit().await.unwrap();
}

async fn load_host_state(env: &TestEnv, host_id: &MachineId) -> ManagedHostState {
    db::machine::find_one(
        &env.pool,
        host_id,
        model::machine::machine_search_config::MachineSearchConfig::default(),
    )
    .await
    .unwrap()
    .expect("host should exist")
    .current_state()
    .clone()
}

/// Reports agent status for one DPU: the given extension-services config
/// version with one service in the given state.
#[allow(clippy::too_many_arguments)]
async fn report_extension_service_status(
    env: &TestEnv,
    dpu_machine_id: MachineId,
    config_version: &str,
    service_id: &str,
    service_version: &str,
    state: rpc::DpuExtensionServiceDeploymentStatus,
) {
    env.api
        .record_dpu_network_status(Request::new(rpc::DpuNetworkStatus {
            dpu_machine_id: Some(dpu_machine_id),
            dpu_agent_version: Some("1.0.0-test".to_string()),
            observed_at: Some(SystemTime::now().into()),
            dpu_health: Some(::rpc::health::HealthReport {
                source: "forge-dpu-agent".to_string(),
                triggered_by: None,
                observed_at: None,
                successes: vec![],
                alerts: vec![],
            }),
            network_config_version: None,
            instance_id: None,
            instance_config_version: None,
            instance_network_config_version: None,
            interfaces: vec![],
            network_config_error: None,
            client_certificate_expiry_unix_epoch_secs: None,
            fabric_interfaces: vec![],
            last_dhcp_requests: vec![],
            dpu_extension_service_version: Some(config_version.to_string()),
            dpu_extension_services: vec![rpc::DpuExtensionServiceStatusObservation {
                service_id: service_id.to_string(),
                service_type: rpc::DpuExtensionServiceType::KubernetesPod.into(),
                service_name: "svc-vpc-gated".to_string(),
                version: service_version.to_string(),
                removed: None,
                state: state.into(),
                components: vec![],
                message: String::new(),
            }],
            astra_config_status: None,
        }))
        .await
        .unwrap();
}

#[crate::sqlx_test]
async fn test_service_vpc_attachment_delivery_and_gated_cleanup(pool: sqlx::PgPool) {
    let log = DpfCallLog::default();
    let dpf_sdk: Arc<dyn DpfOperations> = Arc::new(service_vpc_mock(&log));

    let env = create_test_env_with_overrides(
        pool,
        TestEnvOverrides::with_config(get_config()).with_dpf_sdk(dpf_sdk),
    )
    .await;
    let segment_id = env.create_vpc_and_tenant_segment().await;
    let mh = create_managed_host(&env).await;

    // Owner tenant, service VPC, and the VPC-bound extension service.
    env.api
        .create_tenant(Request::new(rpc::CreateTenantRequest {
            organization_id: "best_org".to_string(),
            routing_profile_type: None,
            metadata: Some(rpc::Metadata {
                name: "best_org".to_string(),
                description: String::new(),
                labels: vec![],
            }),
        }))
        .await
        .unwrap();
    let vpc_id = env
        .api
        .create_vpc(Request::new(
            crate::tests::common::rpc_builder::VpcCreationRequest::builder("best_org")
                .metadata(rpc::Metadata {
                    name: "gated vpc".to_string(),
                    ..Default::default()
                })
                .rpc(),
        ))
        .await
        .unwrap()
        .into_inner()
        .id
        .expect("created VPC must have an id");
    let service = env
        .api
        .create_dpu_extension_service(Request::new(rpc::CreateDpuExtensionServiceRequest {
            service_id: None,
            service_name: "svc-vpc-gated".to_string(),
            description: None,
            tenant_organization_id: "best_org".to_string(),
            service_type: rpc::DpuExtensionServiceType::KubernetesPod.into(),
            data: TEST_SERVICE_DATA.to_string(),
            credential: None,
            observability: None,
            service_vpc_id: Some(vpc_id),
        }))
        .await
        .unwrap()
        .into_inner();
    let service_version = service
        .latest_version_info
        .as_ref()
        .unwrap()
        .version
        .clone();

    let initial_config = rpc::InstanceConfig {
        tenant: Some(default_tenant_config()),
        os: Some(default_os_config()),
        network: Some(single_interface_network_config(segment_id)),
        infiniband: None,
        network_security_group_id: None,
        dpu_extension_services: Some(rpc::InstanceDpuExtensionServicesConfig {
            service_configs: vec![rpc::InstanceDpuExtensionServiceConfig {
                service_id: service.service_id.clone(),
                version: service_version.clone(),
                ..Default::default()
            }],
        }),
        nvlink: None,
        spxconfig: None,
        power_profile: None,
    };
    let (tinstance, _) = mh
        .instance_builer(&env)
        .config(initial_config.clone())
        .build_and_return()
        .await;

    // The attachment and its endpoint /127 exist from allocation.
    let mut txn = env.db_txn().await;
    let snapshot = mh.snapshot(&mut txn).await;
    let instance = snapshot.instance.as_ref().unwrap();
    let binding = instance
        .config
        .extension_services
        .service_configs
        .iter()
        .find(|c| c.attachment_id.is_some())
        .expect("the bound service has an attachment")
        .clone();
    txn.commit().await.unwrap();
    let attachment_id = binding.attachment_id.unwrap();
    let service_vpc_index = binding.service_vpc_index.expect("index assigned");
    let dpu_id = mh.dpu().id;

    // 1. The WaitingForExtensionServicesConfig handler reconciles the CRs.
    //    (The instance fixture already simulated the agent's config ack during
    //    build, so the state also advances past the wait.)
    log.ensures.lock().unwrap().clear();
    set_assigned_state(
        &env,
        &mh.host().id,
        InstanceState::WaitingForExtensionServicesConfig,
    )
    .await;
    env.run_machine_state_controller_iteration().await;

    {
        let ensures = log.ensures.lock().unwrap();
        assert!(
            !ensures.is_empty(),
            "the handler must ensure the attachment CRs"
        );
        let req = &ensures[0];
        assert_eq!(req.attachment_id, attachment_id.to_string());
        assert_eq!(req.service_id, service.service_id);
        assert_eq!(req.service_vpc_index, service_vpc_index);
        assert_eq!(req.dpu_machine_id, dpu_id.to_string());
        assert!(!req.dpu_device_id.is_empty());
    }
    assert!(
        log.removes.lock().unwrap().is_empty(),
        "an active binding must never have its CRs removed"
    );
    assert!(matches!(
        load_host_state(&env, &mh.host().id).await,
        ManagedHostState::Assigned {
            instance_state: InstanceState::WaitingForRebootToReady,
        }
    ));

    // 3. Remove the service from the instance config. The reservation stays
    //    until the DPU acknowledges the termination.
    set_assigned_state(&env, &mh.host().id, InstanceState::Ready).await;
    let mut removed_config = initial_config.clone();
    removed_config.dpu_extension_services = None;
    env.api
        .update_instance_config(Request::new(rpc::InstanceConfigUpdateRequest {
            instance_id: Some(tinstance.id),
            if_version_match: None,
            config: Some(removed_config),
            metadata: Some(rpc::Metadata {
                name: "gated".to_string(),
                description: String::new(),
                labels: vec![],
            }),
        }))
        .await
        .unwrap();

    let mut txn = env.db_txn().await;
    let snapshot = mh.snapshot(&mut txn).await;
    let instance = snapshot.instance.as_ref().unwrap();
    let terminating = instance
        .config
        .extension_services
        .service_configs
        .iter()
        .find(|c| c.attachment_id == Some(attachment_id))
        .expect("the terminating service stays in the persisted config");
    assert!(terminating.removed.is_some());
    let config_version_v2 = instance.extension_services_config_version.version_string();
    let rows = db::service_vpc_endpoint::find_by_attachment(txn.as_mut(), attachment_id)
        .await
        .unwrap();
    assert_eq!(rows.len(), 1, "endpoint reservation survives the update");
    txn.commit().await.unwrap();

    // 4. Without a termination ack at the new version, cleanup must not run.
    set_assigned_state(
        &env,
        &mh.host().id,
        InstanceState::WaitingForExtensionServicesConfig,
    )
    .await;
    env.run_machine_state_controller_iteration().await;
    assert!(
        log.removes.lock().unwrap().is_empty(),
        "no DPU ack yet: the CRs must not be removed"
    );
    let mut txn = env.db_txn().await;
    let rows = db::service_vpc_endpoint::find_by_attachment(txn.as_mut(), attachment_id)
        .await
        .unwrap();
    assert_eq!(rows.len(), 1, "no DPU ack yet: the /127 must stay reserved");
    txn.commit().await.unwrap();

    // 5. The DPU acknowledges the termination; the CRs are removed and the
    //    endpoint reservation is released in the same pass that drops the
    //    service from the config.
    report_extension_service_status(
        &env,
        dpu_id,
        &config_version_v2,
        &service.service_id,
        &service_version,
        rpc::DpuExtensionServiceDeploymentStatus::DpuExtensionServiceTerminated,
    )
    .await;
    set_assigned_state(
        &env,
        &mh.host().id,
        InstanceState::WaitingForExtensionServicesConfig,
    )
    .await;
    env.run_machine_state_controller_iteration().await;

    assert_eq!(
        log.removes.lock().unwrap().clone(),
        vec![attachment_id.to_string()],
        "cleanup removes the attachment's CRs exactly once"
    );
    let mut txn = env.db_txn().await;
    let rows = db::service_vpc_endpoint::find_by_attachment(txn.as_mut(), attachment_id)
        .await
        .unwrap();
    assert!(rows.is_empty(), "the /127 reservation is released");
    let snapshot = mh.snapshot(&mut txn).await;
    assert!(
        snapshot
            .instance
            .as_ref()
            .unwrap()
            .config
            .extension_services
            .service_configs
            .is_empty(),
        "the terminated service is dropped from the persisted config"
    );
    txn.commit().await.unwrap();
}
