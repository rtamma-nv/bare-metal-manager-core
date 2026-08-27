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
use std::sync::atomic::Ordering;

use ::rpc::forge::dpu_extension_service_observability_config::Config;
use ::rpc::forge::forge_server::Forge;
use ::rpc::forge::{
    self as rpc, DpuExtensionServiceObservabilityConfig,
    DpuExtensionServiceObservabilityConfigLogging,
};
use carbide_instrument::testing::MetricsCapture;
use carbide_secrets::credentials::{CredentialKey, Credentials};
use config_version::ConfigVersion;
use tonic::Request;
use uuid::Uuid;

use crate::api::Api;
use crate::tests::common::api_fixtures::{TestEnv, create_managed_host, create_test_env};

const TEST_SERVICE_DATA: &str = "apiVersion: v1\nkind: Pod\nmetadata:\n  name: test\nspec:\n  containers:\n    - name: app\n      image: nginx:1.27";
const TEST_SERVICE_DATA_VERSION_2: &str = "apiVersion: v1\nkind: Pod\nmetadata:\n  name: version-2\nspec:\n  containers:\n    - name: app\n      image: nginx:1.27";
const TEST_SERVICE_DATA_VERSION_3: &str = "apiVersion: v1\nkind: Pod\nmetadata:\n  name: version-3\nspec:\n  containers:\n    - name: app\n      image: nginx:1.27";
const CREDENTIAL_CLEANUP_FAILURE_METRIC: &str =
    "carbide_extension_service_credential_cleanup_failures_total";

fn create_credential() -> rpc::DpuExtensionServiceCredential {
    rpc::DpuExtensionServiceCredential {
        registry_url: "https://registry.test.com".to_string(),
        r#type: Some(
            rpc::dpu_extension_service_credential::Type::UsernamePassword(rpc::UsernamePassword {
                username: "test-username".to_string(),
                password: "test-password".to_string(),
            }),
        ),
    }
}

fn create_observability() -> rpc::DpuExtensionServiceObservability {
    rpc::DpuExtensionServiceObservability {
        configs: vec![rpc::DpuExtensionServiceObservabilityConfig {
            name: Some("prom_config".to_string()),
            config: Some(
                rpc::dpu_extension_service_observability_config::Config::Prometheus(
                    rpc::DpuExtensionServiceObservabilityConfigPrometheus {
                        scrape_interval_seconds: 1,
                        endpoint: "localhost:7777".to_string(),
                    },
                ),
            ),
        }],
    }
}

async fn create_test_tenants(env: &TestEnv) -> Result<(), eyre::Report> {
    let _ = env
        .api
        .create_tenant(tonic::Request::new(rpc::CreateTenantRequest {
            organization_id: "best_org".to_string(),
            routing_profile_type: None,
            metadata: Some(rpc::Metadata {
                name: "best_org".to_string(),
                description: "".to_string(),
                labels: vec![],
            }),
        }))
        .await
        .unwrap();

    let _ = env
        .api
        .create_tenant(tonic::Request::new(rpc::CreateTenantRequest {
            organization_id: "another_org".to_string(),
            routing_profile_type: None,
            metadata: Some(rpc::Metadata {
                name: "another_org".to_string(),
                description: "".to_string(),
                labels: vec![],
            }),
        }))
        .await
        .unwrap();

    Ok(())
}

async fn create_test_extension_service_and_tenants(
    env: &TestEnv,
) -> Result<rpc::DpuExtensionService, eyre::Report> {
    create_test_tenants(env).await?;
    create_test_extension_service(&env.api, "test-service", None).await
}

async fn create_test_extension_service(
    api: &Api,
    name: &str,
    credential: Option<rpc::DpuExtensionServiceCredential>,
) -> Result<rpc::DpuExtensionService, eyre::Report> {
    let extension_service = rpc::CreateDpuExtensionServiceRequest {
        service_id: None,
        service_name: name.to_string(),
        description: Some("Test service".to_string()),
        tenant_organization_id: "best_org".to_string(),
        service_type: rpc::DpuExtensionServiceType::KubernetesPod.into(),
        data: TEST_SERVICE_DATA.to_string(),
        credential,
        observability: None,
        ..Default::default()
    };

    let create_resp = api
        .create_dpu_extension_service(Request::new(extension_service))
        .await;

    Ok(create_resp?.into_inner())
}

async fn create_test_extension_service_with_three_versions(
    env: &TestEnv,
) -> Result<rpc::DpuExtensionService, eyre::Report> {
    create_test_tenants(env).await?;
    let extension_service = rpc::CreateDpuExtensionServiceRequest {
        service_id: None,
        service_name: "test-service".to_string(),
        description: Some("Test service".to_string()),
        tenant_organization_id: "best_org".to_string(),
        service_type: rpc::DpuExtensionServiceType::KubernetesPod.into(),
        data: TEST_SERVICE_DATA.to_string(),
        credential: None,
        observability: None,
        ..Default::default()
    };

    let create_resp = env
        .api
        .create_dpu_extension_service(Request::new(extension_service))
        .await;
    assert!(create_resp.is_ok());

    let service_id = create_resp.unwrap().into_inner().service_id;

    let update_resp = env
        .api
        .update_dpu_extension_service(Request::new(rpc::UpdateDpuExtensionServiceRequest {
            service_id: service_id.clone(),
            service_name: None,
            description: None,
            data: TEST_SERVICE_DATA_VERSION_2.to_string(),
            credential: None,
            observability: None,

            if_version_ctr_match: Some(1),
        }))
        .await;
    assert!(update_resp.is_ok());

    let update_resp = env
        .api
        .update_dpu_extension_service(Request::new(rpc::UpdateDpuExtensionServiceRequest {
            service_id: service_id.clone(),
            service_name: None,
            description: None,
            data: TEST_SERVICE_DATA_VERSION_3.to_string(),
            credential: None,
            observability: None,

            if_version_ctr_match: Some(2),
        }))
        .await;
    assert!(update_resp.is_ok());

    Ok(update_resp.unwrap().into_inner())
}

async fn create_test_extension_service_with_ten_versions(
    env: &TestEnv,
) -> Result<String, eyre::Report> {
    create_test_tenants(env).await?;
    let extension_service = rpc::CreateDpuExtensionServiceRequest {
        service_id: None,
        service_name: "test-service".to_string(),
        description: Some("Test service".to_string()),
        tenant_organization_id: "best_org".to_string(),
        service_type: rpc::DpuExtensionServiceType::KubernetesPod.into(),
        data: TEST_SERVICE_DATA.to_string(),
        credential: None,
        observability: None,
        ..Default::default()
    };

    let create_resp = env
        .api
        .create_dpu_extension_service(Request::new(extension_service))
        .await;
    assert!(create_resp.is_ok());

    let service_id = create_resp.unwrap().into_inner().service_id;

    for _ in 0..5 {
        // Since the extension service data/credential cannot be unchanged, we need to update it to a different version
        let update_resp = env
            .api
            .update_dpu_extension_service(Request::new(rpc::UpdateDpuExtensionServiceRequest {
                service_id: service_id.clone(),
                service_name: None,
                description: None,
                data: TEST_SERVICE_DATA_VERSION_2.to_string(),
                credential: None,
                observability: None,

                if_version_ctr_match: None,
            }))
            .await;
        assert!(update_resp.is_ok());

        let update_resp = env
            .api
            .update_dpu_extension_service(Request::new(rpc::UpdateDpuExtensionServiceRequest {
                service_id: service_id.clone(),
                service_name: None,
                description: None,
                data: TEST_SERVICE_DATA.to_string(),
                credential: None,
                observability: None,

                if_version_ctr_match: None,
            }))
            .await;
        assert!(update_resp.is_ok());
    }

    Ok(service_id)
}

async fn get_credentials_for_extension_service(
    env: &TestEnv,
    extension_service: &rpc::DpuExtensionService,
) -> Result<Credentials, eyre::Report> {
    // Verify the credential is stored correctly in Vault
    let credential_key = carbide_secrets::credentials::CredentialKey::ExtensionService {
        service_id: extension_service.service_id.clone(),
        version: extension_service
            .latest_version_info
            .as_ref()
            .unwrap()
            .version
            .clone()
            .parse::<config_version::ConfigVersion>()
            .unwrap()
            .version_nr()
            .to_string(),
    };

    let stored_credential = env
        .api
        .credential_manager
        .get_credentials(&credential_key)
        .await?;
    stored_credential.ok_or_else(|| eyre::eyre!("could not find the credential"))
}

#[crate::sqlx_test]
async fn test_extension_service_creation(db_pool: sqlx::PgPool) -> Result<(), eyre::Report> {
    let env = create_test_env(db_pool).await;

    create_test_tenants(&env).await?;

    let extension_service = rpc::CreateDpuExtensionServiceRequest {
        service_id: None,
        service_name: "test-service".to_string(),
        description: Some("Test service".to_string()),
        tenant_organization_id: "best_org".to_string(),
        service_type: rpc::DpuExtensionServiceType::KubernetesPod.into(),
        data: TEST_SERVICE_DATA.to_string(),
        credential: Some(create_credential()),
        observability: Some(create_observability()),
        ..Default::default()
    };

    let create_resp: Result<tonic::Response<rpc::DpuExtensionService>, tonic::Status> = env
        .api
        .create_dpu_extension_service(Request::new(extension_service))
        .await;

    assert!(create_resp.is_ok());

    println!(
        "Extension service created: {:?}",
        create_resp.unwrap().into_inner()
    );

    Ok(())
}

#[crate::sqlx_test]
async fn test_extension_service_create_with_credential(
    db_pool: sqlx::PgPool,
) -> Result<(), eyre::Report> {
    let env = create_test_env(db_pool).await;

    create_test_tenants(&env).await?;

    let extension_service = rpc::CreateDpuExtensionServiceRequest {
        service_id: None,
        service_name: "test-service".to_string(),
        description: Some("Test service".to_string()),
        tenant_organization_id: "best_org".to_string(),
        service_type: rpc::DpuExtensionServiceType::KubernetesPod.into(),
        data: TEST_SERVICE_DATA.to_string(),
        credential: Some(create_credential()),
        observability: Some(create_observability()),
        ..Default::default()
    };

    let create_resp = env
        .api
        .create_dpu_extension_service(Request::new(extension_service))
        .await;
    assert!(create_resp.is_ok());

    let extension_service = create_resp.unwrap().into_inner();

    assert!(
        extension_service
            .latest_version_info
            .as_ref()
            .unwrap()
            .has_credential
    );

    // Verify the credential is stored correctly in Vault
    let Credentials::UsernamePassword { username, password } =
        get_credentials_for_extension_service(&env, &extension_service).await?;

    assert_eq!(
        username,
        "url: https://registry.test.com, username: test-username"
    );
    assert_eq!(password, "test-password");

    Ok(())
}

#[crate::sqlx_test]
async fn test_extension_service_create_failure(db_pool: sqlx::PgPool) -> Result<(), eyre::Report> {
    let env = create_test_env(db_pool).await;

    create_test_tenants(&env).await?;

    let requested_extension_service = rpc::CreateDpuExtensionServiceRequest {
        service_id: None,
        service_name: "test-service".to_string(),
        description: Some("Test service".to_string()),
        tenant_organization_id: "best_org".to_string(),
        service_type: rpc::DpuExtensionServiceType::KubernetesPod.into(),
        data: TEST_SERVICE_DATA.to_string(),
        credential: Some(create_credential()),
        observability: Some(create_observability()),
        ..Default::default()
    };

    let create_resp = env
        .api
        .create_dpu_extension_service(Request::new(requested_extension_service.clone()))
        .await;
    assert!(create_resp.is_ok());

    let extension_service = create_resp.unwrap().into_inner();
    let latest_version = extension_service.latest_version_info.unwrap();

    assert!(latest_version.has_credential);

    // Verify the credential is stored correctly in Vault
    let credential_key = carbide_secrets::credentials::CredentialKey::ExtensionService {
        service_id: extension_service.service_id.clone(),
        version: latest_version
            .version
            .parse::<config_version::ConfigVersion>()
            .unwrap()
            .version_nr()
            .to_string(),
    };

    let stored_credential = env
        .api
        .credential_manager
        .get_credentials(&credential_key)
        .await?
        .expect("creating an extension service should have created a credential");

    let metrics = MetricsCapture::start();
    env.test_credential_manager
        .set_delete_credentials_failure(true);

    // Try to create an identical extension service. The database rejects the
    // duplicate after the new credential was stored, and this fixture makes
    // the follow-up credential cleanup fail too.
    let create_resp_2 = env
        .api
        .create_dpu_extension_service(Request::new(requested_extension_service))
        .await;
    assert!(
        create_resp_2.is_err(),
        "creating a second identical extension service should have failed"
    );
    assert_eq!(
        metrics.counter_delta(
            CREDENTIAL_CLEANUP_FAILURE_METRIC,
            &[("operation", "create")],
        ),
        1.0,
    );

    let stored_credential_2 = env
        .api
        .credential_manager
        .get_credentials(&credential_key)
        .await?
        .expect("Failing to create a second extension should not delete existing credentials");

    assert_eq!(
        stored_credential, stored_credential_2,
        "Failing to create a second extension should not have changed the credentials"
    );
    Ok(())
}

#[crate::sqlx_test]
async fn test_extension_service_update_race_condition(
    db_pool: sqlx::PgPool,
) -> Result<(), eyre::Report> {
    let env = create_test_env(db_pool).await;

    create_test_tenants(&env).await?;

    let credential = {
        let mut c = create_credential();
        c.r#type = Some(
            rpc::dpu_extension_service_credential::Type::UsernamePassword(rpc::UsernamePassword {
                username: "test-username-1".to_string(),
                password: "test-password-1".to_string(),
            }),
        );
        c
    };

    // When updating we will try to set the credentials to this for both services.
    let updated_credential = {
        let mut c = create_credential();
        c.r#type = Some(
            rpc::dpu_extension_service_credential::Type::UsernamePassword(rpc::UsernamePassword {
                username: "test-username-updated".to_string(),
                password: "test-password-updated".to_string(),
            }),
        );
        c
    };

    // Create an original service, then create another, then update the second service to collide with the first one, which will fail.
    let service = create_test_extension_service(&env.api, "test-service-1", Some(credential))
        .await
        .unwrap();

    // Make the credential writes slow so that the database can conflict
    env.test_credential_manager
        .set_credentials_sleep_time_ms
        .store(1000, Ordering::SeqCst);

    // Make both requests at the same time
    let (update_1, update_2) = {
        let join_handle_1 = tokio::spawn({
            let api = env.api.clone();
            let request = Request::new(rpc::UpdateDpuExtensionServiceRequest {
                service_id: service.service_id.clone(),
                service_name: Some("test-service-updated".to_string()), // should cause collision
                description: Some(service.description.clone()),
                data: TEST_SERVICE_DATA.to_string(),
                credential: Some(updated_credential.clone()),
                if_version_ctr_match: None,
                observability: None,
            });
            async move { api.update_dpu_extension_service(request).await }
        });
        let join_handle_2 = tokio::spawn({
            let api = env.api.clone();
            let request = Request::new(rpc::UpdateDpuExtensionServiceRequest {
                service_id: service.service_id.clone(),
                service_name: Some("test-service-updated".to_string()), // should cause collision
                description: Some(service.description.clone()),
                data: TEST_SERVICE_DATA.to_string(),
                credential: Some(updated_credential.clone()),
                if_version_ctr_match: None,
                observability: None,
            });
            async move { api.update_dpu_extension_service(request).await }
        });
        (join_handle_1.await.unwrap(), join_handle_2.await.unwrap())
    };

    match (update_1, update_2) {
        (Ok(s), Err(_)) => {
            let Credentials::UsernamePassword {
                username: _,
                password,
            } = get_credentials_for_extension_service(&env, &s.into_inner())
                .await
                .unwrap();
            assert_eq!(
                password, "test-password-updated",
                "update_1 won the race but the password did not get updated: {password}"
            );

            let Credentials::UsernamePassword {
                username: _,
                password,
            } = get_credentials_for_extension_service(&env, &service)
                .await
                .unwrap();
            assert_eq!(
                password, "test-password-1",
                "update_2 lost the race but the password got updated: {password}"
            );
        }
        (Err(_), Ok(s)) => {
            let Credentials::UsernamePassword {
                username: _,
                password,
            } = get_credentials_for_extension_service(&env, &service)
                .await
                .unwrap();
            assert_eq!(
                password, "test-password-1",
                "update_1 lost the race but the password got updated: {password}"
            );

            let Credentials::UsernamePassword {
                username: _,
                password,
            } = get_credentials_for_extension_service(&env, &s.into_inner())
                .await
                .unwrap();
            assert_eq!(
                password, "test-password-updated",
                "update_2 won the race but the password did not get updated: {password}"
            );
        }
        (Err(e1), Err(e2)) => {
            panic!("Both services failed in updating, this should not happen: {e1:?}, {e2:?}")
        }
        (Ok(s1), Ok(s2)) => {
            panic!("Both services succeeded in updating, this should not happen: {s1:?}, {s2:?}")
        }
    };

    Ok(())
}

#[crate::sqlx_test]
async fn test_extension_service_update_failure(db_pool: sqlx::PgPool) -> Result<(), eyre::Report> {
    let env = create_test_env(db_pool).await;

    create_test_tenants(&env).await?;

    let credentials = create_credential();

    // Create 2 services, then create another, then try to update them both to the same new name at the same time
    let service_1 =
        create_test_extension_service(&env.api, "test-service-1", Some(credentials.clone()))
            .await
            .unwrap();
    let service_2 =
        create_test_extension_service(&env.api, "test-service-2", Some(credentials.clone()))
            .await
            .unwrap();

    let service_2_credentials_before =
        get_credentials_for_extension_service(&env, &service_2).await?;

    let updated_credentials = {
        let mut c = credentials.clone();
        c.r#type = Some(
            rpc::dpu_extension_service_credential::Type::UsernamePassword(rpc::UsernamePassword {
                username: "test-username-updated".to_string(),
                password: "test-password-updated".to_string(),
            }),
        );
        c
    };

    let metrics = MetricsCapture::start();
    env.test_credential_manager
        .set_delete_credentials_failure(true);

    let update_response = env
        .api
        .update_dpu_extension_service(Request::new(rpc::UpdateDpuExtensionServiceRequest {
            service_id: service_2.service_id.clone(),
            service_name: Some("test-service-1".to_string()), // should cause collision
            description: Some(service_1.description.clone()),
            data: TEST_SERVICE_DATA.to_string(),
            credential: Some(updated_credentials.clone()),
            if_version_ctr_match: None,
            observability: None,
        }))
        .await;

    assert!(
        update_response.is_err(),
        "update_dpu_extension_service should have failed, got: {update_response:?}"
    );
    assert_eq!(
        metrics.counter_delta(
            CREDENTIAL_CLEANUP_FAILURE_METRIC,
            &[("operation", "update")],
        ),
        1.0,
    );

    let service_2_credentials_after =
        get_credentials_for_extension_service(&env, &service_2).await?;
    assert_eq!(
        service_2_credentials_before, service_2_credentials_after,
        "Failing to update should not have changed the credentials in vault"
    );

    Ok(())
}

#[crate::sqlx_test]
async fn test_extension_service_create_race_condition(
    db_pool: sqlx::PgPool,
) -> Result<(), eyre::Report> {
    let env = create_test_env(db_pool).await;

    create_test_tenants(&env).await?;

    let credential_1 = {
        let mut c = create_credential();
        c.r#type = Some(
            rpc::dpu_extension_service_credential::Type::UsernamePassword(rpc::UsernamePassword {
                username: "test-username-1".to_string(),
                password: "test-password-1".to_string(),
            }),
        );
        c
    };
    let credential_2 = {
        let mut c = create_credential();
        c.r#type = Some(
            rpc::dpu_extension_service_credential::Type::UsernamePassword(rpc::UsernamePassword {
                username: "test-username-2".to_string(),
                password: "test-password-2".to_string(),
            }),
        );
        c
    };

    // Make the credential writes slow so that the database can conflict
    env.test_credential_manager
        .set_credentials_sleep_time_ms
        .store(1000, Ordering::SeqCst);

    // Make both requests at the same time
    let (service_1, service_2) = {
        let join_handle_1 = tokio::spawn({
            let api = env.api.clone();
            async move {
                create_test_extension_service(&api, "test-service-1", Some(credential_1.clone()))
                    .await
            }
        });
        let join_handle_2 = tokio::spawn({
            let api = env.api.clone();
            async move {
                create_test_extension_service(&api, "test-service-1", Some(credential_2.clone()))
                    .await
            }
        });
        (join_handle_1.await.unwrap(), join_handle_2.await.unwrap())
    };

    match (service_1, service_2) {
        (Ok(s), Err(_)) => {
            let Credentials::UsernamePassword {
                username: _,
                password,
            } = get_credentials_for_extension_service(&env, &s).await?;
            assert_eq!(
                password, "test-password-1",
                "service_1 won the race but the password does not match: {password}"
            );
        }
        (Err(_), Ok(s)) => {
            let Credentials::UsernamePassword {
                username: _,
                password,
            } = get_credentials_for_extension_service(&env, &s).await?;
            assert_eq!(
                password, "test-password-2",
                "service_2 won the race but the password does not match: {password}"
            );
        }
        (Err(e1), Err(e2)) => {
            panic!("Both services failed in creation, this should not happen: {e1:?}, {e2:?}")
        }
        (Ok(s1), Ok(s2)) => {
            panic!("Both services succeeded in creation, this should not happen: {s1:?}, {s2:?}")
        }
    };

    Ok(())
}

#[crate::sqlx_test]
async fn test_extension_service_creation_invalid_arg(
    db_pool: sqlx::PgPool,
) -> Result<(), eyre::Report> {
    let env = create_test_env(db_pool).await;

    create_test_tenants(&env).await?;

    // Test empty service name
    let extension_service = rpc::CreateDpuExtensionServiceRequest {
        service_id: None,
        service_name: "".to_string(),
        description: Some("Test service".to_string()),
        tenant_organization_id: "best_org".to_string(),
        service_type: rpc::DpuExtensionServiceType::KubernetesPod.into(),
        data: TEST_SERVICE_DATA.to_string(),
        credential: None,
        observability: None,
        ..Default::default()
    };

    let create_resp = env
        .api
        .create_dpu_extension_service(Request::new(extension_service))
        .await;
    assert!(create_resp.is_err());

    // Test empty data
    let extension_service = rpc::CreateDpuExtensionServiceRequest {
        service_id: None,
        service_name: "test-service".to_string(),
        description: Some("Test service".to_string()),
        tenant_organization_id: "best_org".to_string(),
        service_type: rpc::DpuExtensionServiceType::KubernetesPod.into(),
        data: "".to_string(),
        credential: None,
        observability: None,
        ..Default::default()
    };

    let create_resp = env
        .api
        .create_dpu_extension_service(Request::new(extension_service))
        .await;
    assert!(create_resp.is_err());

    // Test invalid data format (not YAML or JSON)
    let extension_service = rpc::CreateDpuExtensionServiceRequest {
        service_id: None,
        service_name: "test-service".to_string(),
        description: Some("Test service".to_string()),
        tenant_organization_id: "best_org".to_string(),
        service_type: rpc::DpuExtensionServiceType::KubernetesPod.into(),
        data: "apiVersion: v1\nkind: Pod\nmetadata:\n  name: test".to_string(),
        credential: None,
        observability: None,
        ..Default::default()
    };

    let create_resp = env
        .api
        .create_dpu_extension_service(Request::new(extension_service))
        .await;
    assert!(create_resp.is_err());

    // Test invalid credential type
    let extension_service = rpc::CreateDpuExtensionServiceRequest {
        service_id: None,
        service_name: "test-service".to_string(),
        description: Some("Test service".to_string()),
        tenant_organization_id: "best_org".to_string(),
        service_type: rpc::DpuExtensionServiceType::KubernetesPod.into(),
        data: TEST_SERVICE_DATA.to_string(),
        observability: None,
        credential: Some(rpc::DpuExtensionServiceCredential {
            registry_url: "".to_string(),
            r#type: Some(
                rpc::dpu_extension_service_credential::Type::UsernamePassword(
                    rpc::UsernamePassword {
                        username: "test-username".to_string(),
                        password: "test-password".to_string(),
                    },
                ),
            ),
        }),
        ..Default::default()
    };

    let create_resp = env
        .api
        .create_dpu_extension_service(Request::new(extension_service))
        .await;
    assert!(create_resp.is_err());

    // Test invalid observability config
    let extension_service = rpc::CreateDpuExtensionServiceRequest {
        service_id: None,
        service_name: "test-service".to_string(),
        description: Some("Test service".to_string()),
        tenant_organization_id: "best_org".to_string(),
        service_type: rpc::DpuExtensionServiceType::KubernetesPod.into(),
        data: TEST_SERVICE_DATA.to_string(),
        credential: Some(create_credential()),
        observability: Some(rpc::DpuExtensionServiceObservability {
            configs: vec![rpc::DpuExtensionServiceObservabilityConfig {
                name: Some("prom".to_string()),
                config: None,
            }],
        }),
        ..Default::default()
    };

    let create_resp = env
        .api
        .create_dpu_extension_service(Request::new(extension_service))
        .await;
    assert!(create_resp.is_err());

    // Test invalid observability config name
    let extension_service = rpc::CreateDpuExtensionServiceRequest {
        service_id: None,
        service_name: "test-service".to_string(),
        description: Some("Test service".to_string()),
        tenant_organization_id: "best_org".to_string(),
        service_type: rpc::DpuExtensionServiceType::KubernetesPod.into(),
        data: TEST_SERVICE_DATA.to_string(),
        credential: Some(create_credential()),
        observability: Some(rpc::DpuExtensionServiceObservability {
            configs: vec![rpc::DpuExtensionServiceObservabilityConfig {
                name: Some("x".to_string().repeat(65)),
                config: Some(
                    rpc::dpu_extension_service_observability_config::Config::Logging(
                        rpc::DpuExtensionServiceObservabilityConfigLogging {
                            path: "something".to_string(),
                        },
                    ),
                ),
            }],
        }),
        ..Default::default()
    };

    let create_resp = env
        .api
        .create_dpu_extension_service(Request::new(extension_service))
        .await;
    assert!(create_resp.is_err());

    // Fail to create an an extension with too many observability configs
    let extension_service = rpc::CreateDpuExtensionServiceRequest {
        service_id: None,
        service_name: "test-service".to_string(),
        description: Some("Test service".to_string()),
        tenant_organization_id: "best_org".to_string(),
        service_type: rpc::DpuExtensionServiceType::KubernetesPod.into(),
        data: TEST_SERVICE_DATA.to_string(),
        credential: None,
        observability: Some(rpc::DpuExtensionServiceObservability {
            configs: vec![
                DpuExtensionServiceObservabilityConfig {
                    name: None,
                    config: Some(Config::Logging(
                        DpuExtensionServiceObservabilityConfigLogging {
                            path: "/dev/null".to_string(),
                        }
                    )),
                };
                100
            ],
        }),
        ..Default::default()
    };

    let create_resp = env
        .api
        .create_dpu_extension_service(Request::new(extension_service))
        .await;
    assert!(create_resp.is_err());
    let status = create_resp.err().unwrap();
    assert!(status.message().contains("exceeds"));

    // Fail to create an an extension with just a basic bad observability config
    // that's missing the actual config.
    let extension_service = rpc::CreateDpuExtensionServiceRequest {
        service_id: None,
        service_name: "test-service".to_string(),
        description: Some("Test service".to_string()),
        tenant_organization_id: "best_org".to_string(),
        service_type: rpc::DpuExtensionServiceType::KubernetesPod.into(),
        data: TEST_SERVICE_DATA.to_string(),
        credential: None,
        observability: Some(rpc::DpuExtensionServiceObservability {
            configs: vec![DpuExtensionServiceObservabilityConfig {
                name: None,
                config: None,
            }],
        }),
        ..Default::default()
    };

    let create_resp = env
        .api
        .create_dpu_extension_service(Request::new(extension_service))
        .await;
    assert!(create_resp.is_err());

    Ok(())
}

// Two extension services should not have same case-insensitive name
#[crate::sqlx_test]
async fn test_extension_service_creation_with_same_name(
    db_pool: sqlx::PgPool,
) -> Result<(), eyre::Report> {
    let env = create_test_env(db_pool).await;

    create_test_tenants(&env).await?;

    let extension_service = rpc::CreateDpuExtensionServiceRequest {
        service_id: None,
        service_name: "test-service".to_string(),
        description: Some("Test service".to_string()),
        service_type: rpc::DpuExtensionServiceType::KubernetesPod.into(),
        data: TEST_SERVICE_DATA.to_string(),
        credential: None,
        observability: None,

        tenant_organization_id: "best_org".to_string(),
        ..Default::default()
    };

    let create_resp = env
        .api
        .create_dpu_extension_service(Request::new(extension_service))
        .await;

    assert!(create_resp.is_ok());

    println!(
        "Extension service created: {:?}",
        create_resp.unwrap().into_inner()
    );

    // Creating a new extension service with the same name and tenant organization ID should fail
    let duplicate_extension_service = rpc::CreateDpuExtensionServiceRequest {
        service_id: None,
        service_name: "Test-Service".to_string(),
        description: Some("Test service".to_string()),
        service_type: rpc::DpuExtensionServiceType::KubernetesPod.into(),
        data: TEST_SERVICE_DATA.to_string(),
        credential: None,
        observability: None,

        tenant_organization_id: "best_org".to_string(),
        ..Default::default()
    };

    let create_resp = env
        .api
        .create_dpu_extension_service(Request::new(duplicate_extension_service))
        .await;
    assert!(create_resp.is_err());
    let status = create_resp.unwrap_err();
    println!("Error: {:?}", status);
    assert!(status.message().contains("already exists"));

    // However, creating a new extension service with the same name but different tenant organization ID should be allowed
    let new_extension_service = rpc::CreateDpuExtensionServiceRequest {
        service_id: None,
        service_name: "test-service".to_string(),
        description: Some("Test service".to_string()),
        service_type: rpc::DpuExtensionServiceType::KubernetesPod.into(),
        data: TEST_SERVICE_DATA.to_string(),
        credential: None,
        observability: None,

        tenant_organization_id: "another_org".to_string(),
        ..Default::default()
    };

    let create_resp = env
        .api
        .create_dpu_extension_service(Request::new(new_extension_service))
        .await;
    assert!(create_resp.is_ok());

    Ok(())
}

#[crate::sqlx_test]
async fn test_extension_service_recreate_same_name_after_delete(
    db_pool: sqlx::PgPool,
) -> Result<(), eyre::Report> {
    let env = create_test_env(db_pool).await;

    create_test_tenants(&env).await?;

    let service_name = "test-service";
    let extension_service = create_test_extension_service(&env.api, service_name, None).await?;
    let service_id = extension_service.service_id.clone();

    let delete_resp = env
        .api
        .delete_dpu_extension_service(Request::new(rpc::DeleteDpuExtensionServiceRequest {
            service_id: service_id.clone(),
            versions: vec![],
        }))
        .await;
    assert!(delete_resp.is_ok());

    let find_resp = env
        .api
        .find_dpu_extension_services_by_ids(Request::new(rpc::DpuExtensionServicesByIdsRequest {
            service_ids: vec![service_id],
        }))
        .await;
    assert!(find_resp.is_ok());
    assert!(find_resp.unwrap().into_inner().services.is_empty());

    let recreate_resp = create_test_extension_service(&env.api, service_name, None).await;
    assert!(recreate_resp.is_ok());

    let recreated_service = recreate_resp.unwrap();
    assert_eq!(recreated_service.service_name, service_name);
    assert_ne!(recreated_service.service_id, extension_service.service_id);

    Ok(())
}

#[crate::sqlx_test]
async fn test_extension_service_update(db_pool: sqlx::PgPool) -> Result<(), eyre::Report> {
    let env = create_test_env(db_pool).await;
    let service_version = create_test_extension_service_and_tenants(&env).await?;
    let service_id = service_version.service_id;

    let update_resp = env
        .api
        .update_dpu_extension_service(Request::new(rpc::UpdateDpuExtensionServiceRequest {
            service_id: service_id.clone(),
            service_name: Some("updated-service".to_string()),
            description: Some("Updated service".to_string()),
            data: TEST_SERVICE_DATA_VERSION_2.to_string(),
            credential: Some(create_credential()),
            observability: Some(create_observability()),
            if_version_ctr_match: Some(1),
        }))
        .await;
    assert!(update_resp.is_ok());

    let extension_service = update_resp.unwrap().into_inner();
    assert_eq!(extension_service.service_name, "updated-service");
    assert_eq!(extension_service.description, "Updated service");
    assert_eq!(extension_service.version_ctr, 2);
    let latest_version = extension_service.latest_version_info.unwrap();
    assert_eq!(
        latest_version
            .version
            .parse::<config_version::ConfigVersion>()
            .unwrap()
            .version_nr(),
        2
    );
    // The versions are in descending order, so version2 will be placed first
    assert_eq!(extension_service.active_versions.len(), 2);
    assert_eq!(
        extension_service.active_versions[0]
            .parse::<config_version::ConfigVersion>()
            .unwrap()
            .version_nr(),
        2
    );
    assert_eq!(
        extension_service.active_versions[1]
            .parse::<config_version::ConfigVersion>()
            .unwrap()
            .version_nr(),
        1
    );

    // Update but with credential deleted
    let update_resp = env
        .api
        .update_dpu_extension_service(Request::new(rpc::UpdateDpuExtensionServiceRequest {
            service_id: service_id.clone(),
            service_name: None,
            description: None,
            data: TEST_SERVICE_DATA_VERSION_3.to_string(),
            credential: None,
            observability: None,

            if_version_ctr_match: Some(2),
        }))
        .await;
    assert!(update_resp.is_ok());

    let extension_service = update_resp.unwrap().into_inner();
    assert_eq!(extension_service.version_ctr, 3);
    assert!(
        !extension_service
            .latest_version_info
            .as_ref()
            .unwrap()
            .has_credential,
    );
    assert_eq!(
        extension_service
            .latest_version_info
            .as_ref()
            .unwrap()
            .version
            .parse::<config_version::ConfigVersion>()
            .unwrap()
            .version_nr(),
        3
    );
    assert_eq!(extension_service.active_versions.len(), 3);
    assert_eq!(
        extension_service.active_versions[0]
            .parse::<config_version::ConfigVersion>()
            .unwrap()
            .version_nr(),
        3
    );
    assert_eq!(
        extension_service.active_versions[1]
            .parse::<config_version::ConfigVersion>()
            .unwrap()
            .version_nr(),
        2
    );
    assert_eq!(
        extension_service.active_versions[2]
            .parse::<config_version::ConfigVersion>()
            .unwrap()
            .version_nr(),
        1
    );

    Ok(())
}

#[crate::sqlx_test]
async fn test_extension_service_update_invalid_arg(
    db_pool: sqlx::PgPool,
) -> Result<(), eyre::Report> {
    let env = create_test_env(db_pool).await;
    let service_version = create_test_extension_service_and_tenants(&env).await?;
    let service_id = service_version.service_id;

    // Create another extension service with a different name
    let other_extension_service = rpc::CreateDpuExtensionServiceRequest {
        service_id: None,
        service_name: "other-test-service".to_string(),
        description: Some("Other test service".to_string()),
        tenant_organization_id: "best_org".to_string(),
        service_type: rpc::DpuExtensionServiceType::KubernetesPod.into(),
        data: TEST_SERVICE_DATA.to_string(),
        credential: None,
        observability: None,
        ..Default::default()
    };
    let create_resp = env
        .api
        .create_dpu_extension_service(Request::new(other_extension_service))
        .await;
    assert!(create_resp.is_ok());

    // Update with empty service name
    let update_resp = env
        .api
        .update_dpu_extension_service(Request::new(rpc::UpdateDpuExtensionServiceRequest {
            service_id: service_id.clone(),
            service_name: Some("".to_string()),
            description: None,
            data: TEST_SERVICE_DATA_VERSION_3.to_string(),
            credential: None,
            observability: None,

            if_version_ctr_match: None,
        }))
        .await;
    assert!(update_resp.is_err());
    assert!(
        update_resp
            .unwrap_err()
            .message()
            .contains("service_name cannot be empty")
    );

    // Update with wrong version match number
    let update_resp = env
        .api
        .update_dpu_extension_service(Request::new(rpc::UpdateDpuExtensionServiceRequest {
            service_id: service_id.clone(),
            service_name: None,
            description: None,
            data: TEST_SERVICE_DATA_VERSION_3.to_string(),
            credential: None,
            observability: None,

            if_version_ctr_match: Some(2),
        }))
        .await;
    assert!(update_resp.is_err());

    // Update with same data and credential
    let update_resp = env
        .api
        .update_dpu_extension_service(Request::new(rpc::UpdateDpuExtensionServiceRequest {
            service_id: service_id.clone(),
            service_name: None,
            description: None,
            data: TEST_SERVICE_DATA.to_string(),
            credential: None,
            observability: Some(create_observability()),

            if_version_ctr_match: None,
        }))
        .await;
    assert!(update_resp.is_err());
    assert!(
        update_resp
            .unwrap_err()
            .message()
            .contains("no changes to data or credential from latest version")
    );

    // Update with empty data format
    let update_resp = env
        .api
        .update_dpu_extension_service(Request::new(rpc::UpdateDpuExtensionServiceRequest {
            service_id: service_id.clone(),
            service_name: None,
            description: None,
            data: "".to_string(),
            credential: None,
            observability: None,

            if_version_ctr_match: None,
        }))
        .await;
    assert!(update_resp.is_err());
    assert!(
        update_resp
            .unwrap_err()
            .message()
            .contains("invalid empty data for KubernetesPod service, need a valid pod manifest")
    );

    // Update with invalid data format
    let update_resp = env
        .api
        .update_dpu_extension_service(Request::new(rpc::UpdateDpuExtensionServiceRequest {
            service_id: service_id.clone(),
            service_name: None,
            description: None,
            data: "invalid data".to_string(),
            credential: None,
            observability: None,

            if_version_ctr_match: None,
        }))
        .await;
    assert!(update_resp.is_err());
    assert!(
        update_resp
            .unwrap_err()
            .message()
            .contains("pod manifest must be a valid mapping object")
    );

    // Update with wrong service id
    let update_resp = env
        .api
        .update_dpu_extension_service(Request::new(rpc::UpdateDpuExtensionServiceRequest {
            service_id: Uuid::new_v4().to_string(),
            service_name: None,
            description: None,
            data: TEST_SERVICE_DATA.to_string(),
            credential: None,
            observability: None,

            if_version_ctr_match: None,
        }))
        .await;
    assert!(update_resp.is_err());
    assert!(
        update_resp
            .unwrap_err()
            .message()
            .contains("extension_service not found:")
    );

    // Update to a name that's already taken
    let update_resp = env
        .api
        .update_dpu_extension_service(Request::new(rpc::UpdateDpuExtensionServiceRequest {
            service_id: service_id.clone(),
            service_name: Some("other-test-service".to_string()),
            description: None,
            data: TEST_SERVICE_DATA_VERSION_2.to_string(),
            credential: None,
            observability: None,

            if_version_ctr_match: Some(1),
        }))
        .await;
    assert!(update_resp.is_err());
    let status = update_resp.err().unwrap();
    assert!(status.message().contains("already exists"));

    // Update to set too many observability configs
    let update_resp = env
        .api
        .update_dpu_extension_service(Request::new(rpc::UpdateDpuExtensionServiceRequest {
            service_id: service_id.clone(),
            service_name: Some("other-test-service".to_string()),
            description: None,
            data: TEST_SERVICE_DATA_VERSION_2.to_string(),
            credential: None,
            observability: Some(rpc::DpuExtensionServiceObservability {
                configs: vec![
                    DpuExtensionServiceObservabilityConfig {
                        name: None,
                        config: Some(Config::Logging(
                            DpuExtensionServiceObservabilityConfigLogging {
                                path: "/dev/null".to_string(),
                            }
                        )),
                    };
                    100
                ],
            }),

            if_version_ctr_match: Some(1),
        }))
        .await;
    assert!(update_resp.is_err());
    let status = update_resp.err().unwrap();
    assert!(status.message().contains("exceeds"));

    Ok(())
}

#[crate::sqlx_test]
async fn test_extension_service_update_metadata(db_pool: sqlx::PgPool) -> Result<(), eyre::Report> {
    let env = create_test_env(db_pool).await;
    let service_version = create_test_extension_service_and_tenants(&env).await?;
    let service_id = service_version.service_id;

    // Create another extension service with a different name
    let other_extension_service = rpc::CreateDpuExtensionServiceRequest {
        service_id: None,
        service_name: "other-test-service".to_string(),
        description: Some("Other test service".to_string()),
        tenant_organization_id: "best_org".to_string(),
        service_type: rpc::DpuExtensionServiceType::KubernetesPod.into(),
        data: TEST_SERVICE_DATA.to_string(),
        credential: None,
        observability: None,
        ..Default::default()
    };
    let create_resp = env
        .api
        .create_dpu_extension_service(Request::new(other_extension_service))
        .await;
    assert!(create_resp.is_ok());

    // Update only name
    let update_resp = env
        .api
        .update_dpu_extension_service(Request::new(rpc::UpdateDpuExtensionServiceRequest {
            service_id: service_id.clone(),
            service_name: Some("updated-service".to_string()),
            description: None,
            data: "".to_string(),
            credential: None,
            observability: None,
            if_version_ctr_match: None,
        }))
        .await;
    assert!(update_resp.is_ok());

    let extension_service = update_resp.unwrap().into_inner();
    assert_eq!(extension_service.service_name, "updated-service");
    assert_eq!(extension_service.description, "Test service");
    assert_eq!(extension_service.version_ctr, 1);
    let latest_version = extension_service.latest_version_info.unwrap();
    // Expect no version increment
    assert_eq!(
        latest_version
            .version
            .parse::<config_version::ConfigVersion>()
            .unwrap()
            .version_nr(),
        1
    );

    // Normal update should still work
    let update_resp = env
        .api
        .update_dpu_extension_service(Request::new(rpc::UpdateDpuExtensionServiceRequest {
            service_id: service_id.clone(),
            service_name: None,
            description: None,
            data: TEST_SERVICE_DATA_VERSION_2.to_string(),
            credential: Some(create_credential()),
            observability: None,
            if_version_ctr_match: Some(1),
        }))
        .await;
    assert!(update_resp.is_ok());
    let extension_service = update_resp.unwrap().into_inner();
    assert_eq!(extension_service.service_name, "updated-service");
    assert_eq!(extension_service.description, "Test service");
    assert_eq!(extension_service.version_ctr, 2);
    let latest_version = extension_service.latest_version_info.unwrap();
    assert_eq!(
        latest_version
            .version
            .parse::<config_version::ConfigVersion>()
            .unwrap()
            .version_nr(),
        2
    );

    // Update both name and description
    let update_resp = env
        .api
        .update_dpu_extension_service(Request::new(rpc::UpdateDpuExtensionServiceRequest {
            service_id: service_id.clone(),
            service_name: Some("updated-service-2".to_string()),
            description: Some("Updated description".to_string()),
            data: "".to_string(),
            credential: None,
            observability: None,
            if_version_ctr_match: None,
        }))
        .await;
    assert!(update_resp.is_ok());

    let extension_service = update_resp.unwrap().into_inner();
    assert_eq!(extension_service.service_name, "updated-service-2");
    assert_eq!(extension_service.description, "Updated description");
    assert_eq!(extension_service.version_ctr, 2);
    let latest_version = extension_service.latest_version_info.unwrap();
    // Expect no version increment
    assert_eq!(
        latest_version
            .version
            .parse::<config_version::ConfigVersion>()
            .unwrap()
            .version_nr(),
        2
    );

    Ok(())
}

#[crate::sqlx_test]
async fn test_extension_service_find_ids(db_pool: sqlx::PgPool) -> Result<(), eyre::Report> {
    let env = create_test_env(db_pool).await;
    let service_version = create_test_extension_service_and_tenants(&env).await?;
    let service_id = service_version.service_id;

    // Find the extension service by name
    let find_resp = env
        .api
        .find_dpu_extension_service_ids(Request::new(rpc::DpuExtensionServiceSearchFilter {
            service_type: Some(rpc::DpuExtensionServiceType::KubernetesPod.into()),
            name: Some("test-service".to_string()),
            tenant_organization_id: Some("best_org".to_string()),
        }))
        .await;
    assert!(find_resp.is_ok());
    let service_ids = find_resp.unwrap().into_inner().service_ids;
    assert!(service_ids.contains(&service_id));

    // Find the extension service by service type
    let find_resp = env
        .api
        .find_dpu_extension_service_ids(Request::new(rpc::DpuExtensionServiceSearchFilter {
            service_type: Some(rpc::DpuExtensionServiceType::KubernetesPod.into()),
            name: None,
            tenant_organization_id: None,
        }))
        .await;
    assert!(find_resp.is_ok());
    let service_ids = find_resp.unwrap().into_inner().service_ids;
    assert!(service_ids.contains(&service_id));

    // Find the extension service by both service name and service type
    let find_resp = env
        .api
        .find_dpu_extension_service_ids(Request::new(rpc::DpuExtensionServiceSearchFilter {
            service_type: Some(rpc::DpuExtensionServiceType::KubernetesPod.into()),
            name: Some("test-service".to_string()),
            tenant_organization_id: None,
        }))
        .await;
    assert!(find_resp.is_ok());
    let service_ids = find_resp.unwrap().into_inner().service_ids;
    assert!(service_ids.contains(&service_id));

    Ok(())
}

#[crate::sqlx_test]
async fn test_extension_service_find_by_ids(db_pool: sqlx::PgPool) -> Result<(), eyre::Report> {
    let env = create_test_env(db_pool).await;
    let service_version = create_test_extension_service_and_tenants(&env).await?;
    let service_id = service_version.service_id;
    let service_ids = vec![
        service_id,
        Uuid::new_v4().to_string(),
        Uuid::new_v4().to_string(),
    ];

    let find_resp = env
        .api
        .find_dpu_extension_services_by_ids(Request::new(rpc::DpuExtensionServicesByIdsRequest {
            service_ids,
        }))
        .await;

    assert!(find_resp.is_ok());
    let services = find_resp.unwrap().into_inner().services;
    assert!(services.len() == 1);

    println!("Services found: {:?}", services);

    Ok(())
}

#[crate::sqlx_test]
async fn test_extension_service_find_by_ids_latest_version_numerical_ordering(
    db_pool: sqlx::PgPool,
) -> Result<(), eyre::Report> {
    let env = create_test_env(db_pool).await;
    let service_id = create_test_extension_service_with_ten_versions(&env).await?;

    let find_resp = env
        .api
        .find_dpu_extension_services_by_ids(Request::new(rpc::DpuExtensionServicesByIdsRequest {
            service_ids: vec![service_id.clone()],
        }))
        .await;
    assert!(find_resp.is_ok());
    let services = find_resp.unwrap().into_inner().services;
    assert!(services.len() == 1);
    assert!(
        services[0]
            .clone()
            .latest_version_info
            .unwrap()
            .version
            .parse::<config_version::ConfigVersion>()
            .unwrap()
            .version_nr()
            == 11
    );

    Ok(())
}

#[crate::sqlx_test]
async fn test_extension_service_delete(db_pool: sqlx::PgPool) -> Result<(), eyre::Report> {
    let env = create_test_env(db_pool).await;
    let service_version = create_test_extension_service_with_three_versions(&env).await?;
    let service_id = service_version.service_id;

    // The versions are in descending order, so version2 is the latest version
    println!("Active versions: {:?}", service_version.active_versions);
    let version3 = service_version.active_versions[0].clone();
    let version2 = service_version.active_versions[1].clone();
    let version1 = service_version.active_versions[2].clone();

    // Delete two versions, the service should still be found
    let delete_resp = env
        .api
        .delete_dpu_extension_service(Request::new(rpc::DeleteDpuExtensionServiceRequest {
            service_id: service_id.clone(),
            versions: vec![version1.to_string(), version2.to_string()],
        }))
        .await;
    assert!(delete_resp.is_ok());

    let find_resp = env
        .api
        .find_dpu_extension_services_by_ids(Request::new(rpc::DpuExtensionServicesByIdsRequest {
            service_ids: vec![service_id.clone()],
        }))
        .await;
    assert!(find_resp.is_ok());
    let services = find_resp.unwrap().into_inner().services;
    assert!(services.len() == 1);
    let service = services[0].clone();
    assert_eq!(service.version_ctr, 3);
    assert_eq!(
        service.latest_version_info.unwrap().version,
        version3.clone()
    );
    assert_eq!(service.active_versions.len(), 1);
    assert_eq!(
        service.active_versions[0]
            .parse::<config_version::ConfigVersion>()
            .unwrap()
            .version_nr(),
        3
    );

    // Try delete a version that does not exist, the request should pass and the service should still be found
    let fake_version = config_version::ConfigVersion::new(4);

    let delete_resp = env
        .api
        .delete_dpu_extension_service(Request::new(rpc::DeleteDpuExtensionServiceRequest {
            service_id: service_id.clone(),
            versions: vec![fake_version.to_string()],
        }))
        .await;
    assert!(delete_resp.is_ok());

    let find_resp = env
        .api
        .find_dpu_extension_services_by_ids(Request::new(rpc::DpuExtensionServicesByIdsRequest {
            service_ids: vec![service_id.clone()],
        }))
        .await;
    assert!(find_resp.is_ok());
    let services = find_resp.unwrap().into_inner().services;
    assert!(services.len() == 1);
    let service = services[0].clone();
    assert_eq!(service.version_ctr, 3);
    assert_eq!(
        service
            .latest_version_info
            .unwrap()
            .version
            .parse::<config_version::ConfigVersion>()
            .unwrap()
            .version_nr(),
        3
    );
    assert_eq!(service.active_versions.len(), 1);
    assert_eq!(
        service.active_versions[0]
            .parse::<config_version::ConfigVersion>()
            .unwrap()
            .version_nr(),
        3
    );

    // Try delete the service with some invalid version
    let delete_resp = env
        .api
        .delete_dpu_extension_service(Request::new(rpc::DeleteDpuExtensionServiceRequest {
            service_id: service_id.clone(),
            versions: vec!["V1-T?".to_string(), version3.to_string()],
        }))
        .await;
    assert!(delete_resp.is_err());
    assert!(
        delete_resp
            .err()
            .unwrap()
            .to_string()
            .contains("Failed to parse version")
    );

    // Now delete the last version, the service should now be fully deleted and cannot be found
    let delete_resp = env
        .api
        .delete_dpu_extension_service(Request::new(rpc::DeleteDpuExtensionServiceRequest {
            service_id: service_id.clone(),
            versions: vec![version3.to_string()],
        }))
        .await;
    assert!(delete_resp.is_ok());

    let find_resp = env
        .api
        .find_dpu_extension_services_by_ids(Request::new(rpc::DpuExtensionServicesByIdsRequest {
            service_ids: vec![service_id.clone()],
        }))
        .await;
    assert!(find_resp.is_ok());
    let services = find_resp.unwrap().into_inner().services;
    assert!(services.is_empty());

    Ok(())
}

#[crate::sqlx_test]
async fn test_extension_service_delete_in_use(db_pool: sqlx::PgPool) -> Result<(), eyre::Report> {
    let env = create_test_env(db_pool).await;
    let segment_id = env.create_vpc_and_tenant_segment().await;
    let mh = create_managed_host(&env).await;

    let service_version = create_test_extension_service_with_three_versions(&env).await?;
    let service_id = service_version.service_id;

    // The versions are in descending order, so version2 is the latest version
    println!("Active versions: {:?}", service_version.active_versions);
    let version3 = service_version.active_versions[0].clone();
    let version1 = service_version.active_versions[2].clone();

    // Create two instances that each use a different version of the extension service
    let (_, _) = mh
        .instance_builer(&env)
        .single_interface_network_config(segment_id)
        .extension_services(rpc::InstanceDpuExtensionServicesConfig {
            service_configs: vec![rpc::InstanceDpuExtensionServiceConfig {
                service_id: service_id.clone(),
                version: version3.clone(),
                ..Default::default()
            }],
        })
        .build_and_return()
        .await;

    // Now try to delete the extension service, this should fail since one of the versions is in use
    let delete_resp = env
        .api
        .delete_dpu_extension_service(Request::new(rpc::DeleteDpuExtensionServiceRequest {
            service_id: service_id.clone(),
            versions: vec![],
        }))
        .await;
    assert!(delete_resp.is_err());
    let status = delete_resp.err().unwrap();
    assert!(status.message().contains("in use"));

    // Now try to delete the extension service with the version that is in use, this shoud fail
    let delete_resp = env
        .api
        .delete_dpu_extension_service(Request::new(rpc::DeleteDpuExtensionServiceRequest {
            service_id: service_id.clone(),
            versions: vec![version3.clone()],
        }))
        .await;
    assert!(delete_resp.is_err());
    let status = delete_resp.err().unwrap();
    assert!(status.message().contains("in use"));

    // Now try to delete the extension service with the version that is not in use, this should succeed
    let delete_resp = env
        .api
        .delete_dpu_extension_service(Request::new(rpc::DeleteDpuExtensionServiceRequest {
            service_id: service_id.clone(),
            versions: vec![version1.clone()],
        }))
        .await;
    assert!(delete_resp.is_ok());

    let find_resp = env
        .api
        .find_dpu_extension_services_by_ids(Request::new(rpc::DpuExtensionServicesByIdsRequest {
            service_ids: vec![service_id.clone()],
        }))
        .await;

    assert!(find_resp.is_ok());
    let services = find_resp.unwrap().into_inner().services;
    assert!(services.len() == 1);
    assert_eq!(services[0].service_id, service_id);
    assert_eq!(services[0].active_versions.len(), 2);

    Ok(())
}

#[crate::sqlx_test]
async fn test_extension_service_delete_default(db_pool: sqlx::PgPool) -> Result<(), eyre::Report> {
    let env = create_test_env(db_pool).await;
    let service_version = create_test_extension_service_with_three_versions(&env).await?;
    let service_id = service_version.service_id;

    // Try delete the whole service by no providing the versions field
    // Delete two versions, the service should still be found
    let delete_resp = env
        .api
        .delete_dpu_extension_service(Request::new(rpc::DeleteDpuExtensionServiceRequest {
            service_id: service_id.clone(),
            versions: vec![],
        }))
        .await;
    assert!(delete_resp.is_ok());

    let find_resp = env
        .api
        .find_dpu_extension_services_by_ids(Request::new(rpc::DpuExtensionServicesByIdsRequest {
            service_ids: vec![service_id.clone()],
        }))
        .await;
    assert!(find_resp.is_ok());
    let services = find_resp.unwrap().into_inner().services;
    assert!(services.is_empty());

    Ok(())
}

#[crate::sqlx_test]
async fn test_extension_service_create_update_delete_credential(
    db_pool: sqlx::PgPool,
) -> Result<(), eyre::Report> {
    let env = create_test_env(db_pool).await;
    let service_version = create_test_extension_service_with_three_versions(&env).await?;
    let service_id = service_version.service_id;

    // The versions are in descending order, so version2 is the latest version
    println!("Active versions: {:?}", service_version.active_versions);
    let version3 = service_version.active_versions[0].clone();
    let version2 = service_version.active_versions[1].clone();
    let version1 = service_version.active_versions[2].clone();

    let delete_resp = env
        .api
        .delete_dpu_extension_service(Request::new(rpc::DeleteDpuExtensionServiceRequest {
            service_id: service_id.clone(),
            versions: vec![version1.to_string(), version2.to_string()],
        }))
        .await;
    assert!(delete_resp.is_ok());

    // Update the extension service with a credential
    let update_resp = env
        .api
        .update_dpu_extension_service(Request::new(rpc::UpdateDpuExtensionServiceRequest {
            service_id: service_id.clone(),
            credential: Some(create_credential()),
            observability: Some(create_observability()),
            if_version_ctr_match: None,
            service_name: None,
            description: None,
            data: TEST_SERVICE_DATA_VERSION_2.to_string(),
        }))
        .await;
    assert!(update_resp.is_ok());

    let extension_service = update_resp.unwrap().into_inner();
    assert!(
        extension_service
            .latest_version_info
            .clone()
            .unwrap()
            .has_credential,
    );
    let version4 = extension_service
        .latest_version_info
        .unwrap()
        .version
        .clone();

    // Delete the extension service
    let delete_resp = env
        .api
        .delete_dpu_extension_service(Request::new(rpc::DeleteDpuExtensionServiceRequest {
            service_id: service_id.clone(),
            versions: vec![version3.to_string()],
        }))
        .await;
    assert!(delete_resp.is_ok());

    // Find the extension service by name
    let find_resp = env
        .api
        .find_dpu_extension_services_by_ids(Request::new(rpc::DpuExtensionServicesByIdsRequest {
            service_ids: vec![service_id.clone()],
        }))
        .await;
    assert!(find_resp.is_ok());
    let services = find_resp.unwrap().into_inner().services;
    assert!(services.len() == 1);
    let service = services[0].clone();
    let latest_version = service.latest_version_info.unwrap();
    assert_eq!(service.version_ctr, 4);
    assert_eq!(service.active_versions, vec![version4.clone()]);
    assert_eq!(
        latest_version
            .version
            .parse::<config_version::ConfigVersion>()
            .unwrap()
            .version_nr(),
        4
    );
    assert!(latest_version.has_credential);

    // Verify the credential is stored correctly in Vault
    let credential_key = CredentialKey::ExtensionService {
        service_id: service_id.clone(),
        version: 4.to_string(),
    };

    let stored_credential = env
        .api
        .credential_manager
        .get_credentials(&credential_key)
        .await?;

    match stored_credential {
        Some(Credentials::UsernamePassword { username, password }) => {
            assert_eq!(
                username,
                "url: https://registry.test.com, username: test-username"
            );
            assert_eq!(password, "test-password");
        }
        _ => {
            return Err(eyre::eyre!("could not find the credential"));
        }
    }

    // Delete the version with credential
    let delete_resp = env
        .api
        .delete_dpu_extension_service(Request::new(rpc::DeleteDpuExtensionServiceRequest {
            service_id: service_id.clone(),
            versions: vec![version4.to_string()],
        }))
        .await;
    assert!(delete_resp.is_ok());

    // Expect the extension service is no longer found
    let find_resp = env
        .api
        .find_dpu_extension_services_by_ids(Request::new(rpc::DpuExtensionServicesByIdsRequest {
            service_ids: vec![service_id.clone()],
        }))
        .await;
    assert!(find_resp.is_ok());
    let services = find_resp.unwrap().into_inner().services;
    assert!(services.is_empty());

    // Expect the credential is deleted from Vault
    let credential_key = carbide_secrets::credentials::CredentialKey::ExtensionService {
        service_id: service_id.clone(),
        version: 4.to_string(),
    };
    let stored_credential = env
        .api
        .credential_manager
        .get_credentials(&credential_key)
        .await;
    assert!(stored_credential.is_ok());
    assert!(stored_credential.unwrap().is_none());

    Ok(())
}

#[crate::sqlx_test]
async fn test_extension_service_delete_credential_cleanup_failure(
    db_pool: sqlx::PgPool,
) -> Result<(), eyre::Report> {
    let env = create_test_env(db_pool).await;
    create_test_tenants(&env).await?;
    let first_service_version =
        create_test_extension_service(&env.api, "test-service", Some(create_credential())).await?;
    let service_id = first_service_version.service_id.clone();
    let first_version = first_service_version
        .latest_version_info
        .as_ref()
        .unwrap()
        .version
        .parse::<ConfigVersion>()?;

    let second_service_version = env
        .api
        .update_dpu_extension_service(Request::new(rpc::UpdateDpuExtensionServiceRequest {
            service_id: service_id.clone(),
            service_name: None,
            description: None,
            data: TEST_SERVICE_DATA_VERSION_2.to_string(),
            credential: Some(create_credential()),
            observability: None,
            if_version_ctr_match: Some(1),
        }))
        .await?
        .into_inner();
    let second_version = second_service_version
        .latest_version_info
        .as_ref()
        .unwrap()
        .version
        .parse::<ConfigVersion>()?;
    let first_credential_key = CredentialKey::ExtensionService {
        service_id: service_id.clone(),
        version: first_version.version_nr().to_string(),
    };
    let second_credential_key = CredentialKey::ExtensionService {
        service_id: service_id.clone(),
        version: second_version.version_nr().to_string(),
    };
    let first_credential_before = env
        .api
        .credential_manager
        .get_credentials(&first_credential_key)
        .await?;
    let second_credential_before = env
        .api
        .credential_manager
        .get_credentials(&second_credential_key)
        .await?;

    let metrics = MetricsCapture::start();
    env.test_credential_manager
        .set_delete_credentials_failure(true);

    // The service version is committed as deleted before credential cleanup
    // runs. A credential-store failure must be visible without turning the RPC
    // into an error that suggests the database deletion can be retried.
    let delete_response = env
        .api
        .delete_dpu_extension_service(Request::new(rpc::DeleteDpuExtensionServiceRequest {
            service_id: service_id.clone(),
            versions: vec![first_version.to_string(), second_version.to_string()],
        }))
        .await;

    assert!(delete_response.is_ok());
    assert_eq!(
        metrics.counter_delta(
            CREDENTIAL_CLEANUP_FAILURE_METRIC,
            &[("operation", "delete")],
        ),
        2.0,
    );

    let find_response = env
        .api
        .find_dpu_extension_services_by_ids(Request::new(rpc::DpuExtensionServicesByIdsRequest {
            service_ids: vec![service_id],
        }))
        .await?;
    assert!(find_response.into_inner().services.is_empty());

    let first_credential_after = env
        .api
        .credential_manager
        .get_credentials(&first_credential_key)
        .await?;
    let second_credential_after = env
        .api
        .credential_manager
        .get_credentials(&second_credential_key)
        .await?;
    assert_eq!(first_credential_after, first_credential_before);
    assert_eq!(second_credential_after, second_credential_before);

    Ok(())
}

#[crate::sqlx_test]
async fn test_extension_service_get_version_infos(
    db_pool: sqlx::PgPool,
) -> Result<(), eyre::Report> {
    let env = create_test_env(db_pool).await;
    let service = create_test_extension_service_with_three_versions(&env).await?;
    let service_id = service.service_id;

    println!("Active versions: {:?}", service.active_versions);
    let version3 = service.active_versions[0].clone();
    let version2 = service.active_versions[1].clone();
    let version1 = service.active_versions[2].clone();

    // Get all version infos without specifying versions filter
    let get_resp = env
        .api
        .get_dpu_extension_service_versions_info(Request::new(
            rpc::GetDpuExtensionServiceVersionsInfoRequest {
                service_id: service_id.clone(),
                versions: vec![],
            },
        ))
        .await;

    assert!(get_resp.is_ok());
    let version_infos = get_resp.unwrap().into_inner().version_infos;

    // Should return all 3 versions in descending order
    assert_eq!(version_infos.len(), 3);
    assert_eq!(
        version_infos[0]
            .version
            .parse::<config_version::ConfigVersion>()
            .unwrap()
            .version_nr(),
        3
    );
    assert_eq!(
        version_infos[1]
            .version
            .parse::<config_version::ConfigVersion>()
            .unwrap()
            .version_nr(),
        2
    );
    assert_eq!(
        version_infos[2]
            .version
            .parse::<config_version::ConfigVersion>()
            .unwrap()
            .version_nr(),
        1
    );

    // Verify each version has the correct data
    assert_eq!(version_infos[0].data, TEST_SERVICE_DATA_VERSION_3);
    assert_eq!(version_infos[1].data, TEST_SERVICE_DATA_VERSION_2);
    assert_eq!(version_infos[2].data, TEST_SERVICE_DATA);

    // All versions should not have credentials
    assert!(!version_infos[0].has_credential);
    assert!(!version_infos[1].has_credential);
    assert!(!version_infos[2].has_credential);

    // Get all version infos by specifying versions filter
    let get_resp = env
        .api
        .get_dpu_extension_service_versions_info(Request::new(
            rpc::GetDpuExtensionServiceVersionsInfoRequest {
                service_id: service_id.clone(),
                versions: vec![
                    version1.to_string(),
                    version2.to_string(),
                    version3.to_string(),
                ],
            },
        ))
        .await;

    assert!(get_resp.is_ok());
    let version_infos = get_resp.unwrap().into_inner().version_infos;

    // Should return all 3 versions in descending order
    assert_eq!(version_infos.len(), 3);
    assert_eq!(
        version_infos[0]
            .version
            .parse::<config_version::ConfigVersion>()
            .unwrap()
            .version_nr(),
        3
    );
    assert_eq!(
        version_infos[1]
            .version
            .parse::<config_version::ConfigVersion>()
            .unwrap()
            .version_nr(),
        2
    );
    assert_eq!(
        version_infos[2]
            .version
            .parse::<config_version::ConfigVersion>()
            .unwrap()
            .version_nr(),
        1
    );

    // Verify each version has the correct data
    assert_eq!(version_infos[0].data, TEST_SERVICE_DATA_VERSION_3);
    assert_eq!(version_infos[1].data, TEST_SERVICE_DATA_VERSION_2);
    assert_eq!(version_infos[2].data, TEST_SERVICE_DATA);

    // All versions should not have credentials
    assert!(!version_infos[0].has_credential);
    assert!(!version_infos[1].has_credential);
    assert!(!version_infos[2].has_credential);

    // Get all version infos by specifying versions filter
    let get_resp = env
        .api
        .get_dpu_extension_service_versions_info(Request::new(
            rpc::GetDpuExtensionServiceVersionsInfoRequest {
                service_id: service_id.clone(),
                versions: vec![version2.to_string()],
            },
        ))
        .await;

    assert!(get_resp.is_ok());
    let version_infos = get_resp.unwrap().into_inner().version_infos;

    // Should return 1 version
    assert_eq!(version_infos.len(), 1);
    assert_eq!(
        version_infos[0]
            .version
            .parse::<config_version::ConfigVersion>()
            .unwrap()
            .version_nr(),
        2
    );

    // Verify each version has the correct data
    assert_eq!(version_infos[0].data, TEST_SERVICE_DATA_VERSION_2);

    Ok(())
}

#[crate::sqlx_test]
async fn test_find_instances_by_extension_service(
    db_pool: sqlx::PgPool,
) -> Result<(), eyre::Report> {
    let env = create_test_env(db_pool).await;
    let segment_id = env.create_vpc_and_tenant_segment().await;
    let mh1 = create_managed_host(&env).await;
    let mh2 = create_managed_host(&env).await;

    // Create an extension service with two versions
    let service = create_test_extension_service_and_tenants(&env).await?;
    let service_id = service.service_id.clone();
    let version1 = service
        .latest_version_info
        .as_ref()
        .unwrap()
        .version
        .clone();

    // Update to create version 2
    let service = env
        .api
        .update_dpu_extension_service(Request::new(rpc::UpdateDpuExtensionServiceRequest {
            service_id: service_id.clone(),
            service_name: None,
            description: None,
            data: TEST_SERVICE_DATA_VERSION_2.to_string(),
            credential: None,
            observability: None,

            if_version_ctr_match: Some(1),
        }))
        .await?
        .into_inner();
    let version2 = service
        .latest_version_info
        .as_ref()
        .unwrap()
        .version
        .clone();

    // Create instance1 using version1
    let (instance1, _) = mh1
        .instance_builer(&env)
        .single_interface_network_config(segment_id)
        .extension_services(rpc::InstanceDpuExtensionServicesConfig {
            service_configs: vec![rpc::InstanceDpuExtensionServiceConfig {
                service_id: service_id.clone(),
                version: version1.clone(),
                ..Default::default()
            }],
        })
        .build_and_return()
        .await;

    // Create instance2 using version2
    let (instance2, _) = mh2
        .instance_builer(&env)
        .single_interface_network_config(segment_id)
        .extension_services(rpc::InstanceDpuExtensionServicesConfig {
            service_configs: vec![rpc::InstanceDpuExtensionServiceConfig {
                service_id: service_id.clone(),
                version: version2.clone(),
                ..Default::default()
            }],
        })
        .build_and_return()
        .await;

    // Find instances by service without version filter - should return both instances
    let find_resp = env
        .api
        .find_instances_by_dpu_extension_service(Request::new(
            rpc::FindInstancesByDpuExtensionServiceRequest {
                service_id: service_id.clone(),
                version: None,
            },
        ))
        .await?;

    let instances = find_resp.into_inner().instances;
    assert_eq!(instances.len(), 2);

    // Verify both instances are returned
    let instance_ids: Vec<String> = instances.iter().map(|i| i.instance_id.clone()).collect();
    assert!(instance_ids.contains(&instance1.id.to_string()));
    assert!(instance_ids.contains(&instance2.id.to_string()));

    // Verify version information
    let instance1_info = instances
        .iter()
        .find(|i| i.instance_id == instance1.id.to_string())
        .unwrap();
    assert_eq!(instance1_info.service_id, service_id);
    assert_eq!(instance1_info.version, version1);
    assert!(instance1_info.removed.is_none());

    let instance2_info = instances
        .iter()
        .find(|i| i.instance_id == instance2.id.to_string())
        .unwrap();
    assert_eq!(instance2_info.service_id, service_id);
    assert_eq!(instance2_info.version, version2);
    assert!(instance2_info.removed.is_none());

    // Find instances by service with version1 filter - should return only instance1
    let find_resp = env
        .api
        .find_instances_by_dpu_extension_service(Request::new(
            rpc::FindInstancesByDpuExtensionServiceRequest {
                service_id: service_id.clone(),
                version: Some(version1.clone()),
            },
        ))
        .await?;
    let instances = find_resp.into_inner().instances;
    assert_eq!(instances.len(), 1);
    assert_eq!(instances[0].instance_id, instance1.id.to_string());
    assert_eq!(instances[0].service_id, service_id);
    assert_eq!(instances[0].version, version1);
    assert!(instances[0].removed.is_none());

    // Find instance by service with version2 filter - should return only instance2
    let find_resp = env
        .api
        .find_instances_by_dpu_extension_service(Request::new(
            rpc::FindInstancesByDpuExtensionServiceRequest {
                service_id: service_id.clone(),
                version: Some(version2.clone()),
            },
        ))
        .await?;
    let instances = find_resp.into_inner().instances;
    assert_eq!(instances.len(), 1);
    assert_eq!(instances[0].instance_id, instance2.id.to_string());
    assert_eq!(instances[0].service_id, service_id);
    assert_eq!(instances[0].version, version2);
    assert!(instances[0].removed.is_none());

    // Find instance by a non-existent version - should return empty list
    let find_resp = env
        .api
        .find_instances_by_dpu_extension_service(Request::new(
            rpc::FindInstancesByDpuExtensionServiceRequest {
                service_id: service_id.clone(),
                version: Some(ConfigVersion::new(999).to_string()),
            },
        ))
        .await?;
    let instances = find_resp.into_inner().instances;
    assert_eq!(instances.len(), 0);

    // Find instance by a non-existent service -- should return error
    let find_resp = env
        .api
        .find_instances_by_dpu_extension_service(Request::new(
            rpc::FindInstancesByDpuExtensionServiceRequest {
                service_id: Uuid::new_v4().to_string(),
                version: None,
            },
        ))
        .await;
    assert!(find_resp.is_err());
    let status = find_resp.err().unwrap();
    assert!(status.message().contains("not found") || status.message().contains("NotFound"));

    Ok(())
}

#[crate::sqlx_test]
async fn test_find_instances_by_extension_service_multiple_services_per_instance(
    db_pool: sqlx::PgPool,
) -> Result<(), eyre::Report> {
    let env = create_test_env(db_pool).await;
    let segment_id = env.create_vpc_and_tenant_segment().await;
    let mh = create_managed_host(&env).await;

    // Create two extension services
    let service1 = create_test_extension_service_and_tenants(&env).await?;
    let service1_id = service1.service_id.clone();
    let service1_version = service1
        .latest_version_info
        .as_ref()
        .unwrap()
        .version
        .clone();

    let service2 = env
        .api
        .create_dpu_extension_service(Request::new(rpc::CreateDpuExtensionServiceRequest {
            service_id: None,
            service_name: "test-service-2".to_string(),
            description: Some("Second test service".to_string()),
            tenant_organization_id: "best_org".to_string(),
            service_type: rpc::DpuExtensionServiceType::KubernetesPod.into(),
            data: TEST_SERVICE_DATA_VERSION_2.to_string(),
            credential: None,
            observability: None,
            ..Default::default()
        }))
        .await?
        .into_inner();
    let service2_id = service2.service_id.clone();
    let service2_version = service2
        .latest_version_info
        .as_ref()
        .unwrap()
        .version
        .clone();

    // Create an instance using both services
    let (instance, _) = mh
        .instance_builer(&env)
        .single_interface_network_config(segment_id)
        .extension_services(rpc::InstanceDpuExtensionServicesConfig {
            service_configs: vec![
                rpc::InstanceDpuExtensionServiceConfig {
                    service_id: service1_id.clone(),
                    version: service1_version.clone(),
                    ..Default::default()
                },
                rpc::InstanceDpuExtensionServiceConfig {
                    service_id: service2_id.clone(),
                    version: service2_version.clone(),
                    ..Default::default()
                },
            ],
        })
        .build_and_return()
        .await;

    // Find instances by service1 - should return the instance
    let find_resp = env
        .api
        .find_instances_by_dpu_extension_service(Request::new(
            rpc::FindInstancesByDpuExtensionServiceRequest {
                service_id: service1_id.clone(),
                version: None,
            },
        ))
        .await?;

    let instances = find_resp.into_inner().instances;
    assert_eq!(instances.len(), 1);
    assert_eq!(instances[0].instance_id, instance.id.to_string());
    assert_eq!(instances[0].service_id, service1_id);
    assert_eq!(instances[0].version, service1_version);

    // Find instances by service2 - should also return the same instance
    let find_resp = env
        .api
        .find_instances_by_dpu_extension_service(Request::new(
            rpc::FindInstancesByDpuExtensionServiceRequest {
                service_id: service2_id.clone(),
                version: None,
            },
        ))
        .await?;

    let instances = find_resp.into_inner().instances;
    assert_eq!(instances.len(), 1);
    assert_eq!(instances[0].instance_id, instance.id.to_string());
    assert_eq!(instances[0].service_id, service2_id);
    assert_eq!(instances[0].version, service2_version);

    Ok(())
}

// ---------------------------------------------------------------------------
// Service VPC binding tests (DPU block-storage design)
// ---------------------------------------------------------------------------

/// Creates a VPC owned by `tenant_org` to serve as a service VPC.
async fn create_service_vpc(
    env: &TestEnv,
    tenant_org: &str,
    name: &str,
) -> carbide_uuid::vpc::VpcId {
    env.api
        .create_vpc(Request::new(
            crate::tests::common::rpc_builder::VpcCreationRequest::builder(tenant_org)
                .metadata(rpc::Metadata {
                    name: name.to_string(),
                    ..Default::default()
                })
                .rpc(),
        ))
        .await
        .unwrap()
        .into_inner()
        .id
        .expect("created VPC must have an id")
}

/// Creates an extension service owned by `best_org`, bound to `service_vpc_id`.
async fn create_test_extension_service_with_vpc(
    api: &Api,
    name: &str,
    service_vpc_id: carbide_uuid::vpc::VpcId,
) -> Result<rpc::DpuExtensionService, tonic::Status> {
    api.create_dpu_extension_service(Request::new(rpc::CreateDpuExtensionServiceRequest {
        service_id: None,
        service_name: name.to_string(),
        description: Some("Service with service VPC".to_string()),
        tenant_organization_id: "best_org".to_string(),
        service_type: rpc::DpuExtensionServiceType::KubernetesPod.into(),
        data: TEST_SERVICE_DATA.to_string(),
        credential: None,
        observability: None,
        service_vpc_id: Some(service_vpc_id),
    }))
    .await
    .map(|r| r.into_inner())
}

#[crate::sqlx_test]
async fn test_extension_service_create_with_service_vpc(
    db_pool: sqlx::PgPool,
) -> Result<(), eyre::Report> {
    let env = create_test_env(db_pool).await;
    create_test_tenants(&env).await?;

    let vpc_id = create_service_vpc(&env, "best_org", "storage service vpc").await;
    let service = create_test_extension_service_with_vpc(&env.api, "svc-vpc-service", vpc_id)
        .await
        .expect("create with service VPC should succeed");

    // The response echoes the binding.
    assert_eq!(service.service_vpc_id, Some(vpc_id));

    // The binding is also visible through find_by_ids (snapshot path).
    let found = env
        .api
        .find_dpu_extension_services_by_ids(Request::new(rpc::DpuExtensionServicesByIdsRequest {
            service_ids: vec![service.service_id.clone()],
        }))
        .await?
        .into_inner();
    assert_eq!(found.services.len(), 1);
    assert_eq!(found.services[0].service_vpc_id, Some(vpc_id));

    Ok(())
}

#[crate::sqlx_test]
async fn test_extension_service_create_with_missing_service_vpc(
    db_pool: sqlx::PgPool,
) -> Result<(), eyre::Report> {
    let env = create_test_env(db_pool).await;
    create_test_tenants(&env).await?;

    let bogus_vpc = carbide_uuid::vpc::VpcId::from(Uuid::new_v4());
    let err = create_test_extension_service_with_vpc(&env.api, "svc-no-vpc", bogus_vpc)
        .await
        .expect_err("create with nonexistent service VPC must fail");
    assert_eq!(err.code(), tonic::Code::NotFound);

    Ok(())
}

#[crate::sqlx_test]
async fn test_extension_service_create_with_foreign_service_vpc(
    db_pool: sqlx::PgPool,
) -> Result<(), eyre::Report> {
    let env = create_test_env(db_pool).await;
    create_test_tenants(&env).await?;

    // VPC owned by another_org; service owned by best_org.
    let vpc_id = create_service_vpc(&env, "another_org", "foreign vpc").await;
    let err = create_test_extension_service_with_vpc(&env.api, "svc-foreign-vpc", vpc_id)
        .await
        .expect_err("service and service VPC must share the owning tenant");
    assert_eq!(err.code(), tonic::Code::FailedPrecondition);

    Ok(())
}

#[crate::sqlx_test]
async fn test_vpc_delete_blocked_by_service_vpc_reference(
    db_pool: sqlx::PgPool,
) -> Result<(), eyre::Report> {
    let env = create_test_env(db_pool).await;
    create_test_tenants(&env).await?;

    let vpc_id = create_service_vpc(&env, "best_org", "referenced vpc").await;
    let service = create_test_extension_service_with_vpc(&env.api, "svc-blocks-vpc", vpc_id)
        .await
        .expect("create should succeed");

    // Deleting the VPC while referenced must fail.
    let err = env
        .api
        .delete_vpc(Request::new(rpc::VpcDeletionRequest { id: Some(vpc_id) }))
        .await
        .expect_err("VPC referenced as service VPC must not be deletable");
    assert_eq!(err.code(), tonic::Code::FailedPrecondition);

    // Delete the service (all versions), releasing the reference.
    env.api
        .delete_dpu_extension_service(Request::new(rpc::DeleteDpuExtensionServiceRequest {
            service_id: service.service_id.clone(),
            versions: vec![],
        }))
        .await?;

    // Now the VPC can be deleted.
    env.api
        .delete_vpc(Request::new(rpc::VpcDeletionRequest { id: Some(vpc_id) }))
        .await
        .expect("VPC deletion should succeed after the service is gone");

    Ok(())
}

#[crate::sqlx_test]
async fn test_service_vpc_index_assignment_and_reuse(
    db_pool: sqlx::PgPool,
) -> Result<(), eyre::Report> {
    use crate::tests::common::api_fixtures::instance::{
        default_os_config, default_tenant_config, single_interface_network_config,
    };

    let env = create_test_env(db_pool).await;
    let segment_id = env.create_vpc_and_tenant_segment().await;
    let mh1 = create_managed_host(&env).await;
    let mh2 = create_managed_host(&env).await;
    let mh3 = create_managed_host(&env).await;

    create_test_tenants(&env).await?;
    let vpc_a = create_service_vpc(&env, "best_org", "service vpc a").await;
    let vpc_b = create_service_vpc(&env, "best_org", "service vpc b").await;
    let service_a = create_test_extension_service_with_vpc(&env.api, "svc-a", vpc_a)
        .await
        .expect("create svc-a");
    let service_b = create_test_extension_service_with_vpc(&env.api, "svc-b", vpc_b)
        .await
        .expect("create svc-b");
    let version_a = service_a
        .latest_version_info
        .as_ref()
        .unwrap()
        .version
        .clone();
    let version_b = service_b
        .latest_version_info
        .as_ref()
        .unwrap()
        .version
        .clone();

    // Instance 1 attaches service A: first (tenant, vpc_a) binding -> index 0.
    let (_i1, _) = mh1
        .instance_builer(&env)
        .single_interface_network_config(segment_id)
        .extension_services(rpc::InstanceDpuExtensionServicesConfig {
            service_configs: vec![rpc::InstanceDpuExtensionServiceConfig {
                service_id: service_a.service_id.clone(),
                version: version_a.clone(),
                ..Default::default()
            }],
        })
        .build_and_return()
        .await;

    let mut txn = env.db_txn().await;
    let snapshot1 = mh1.snapshot(&mut txn).await;
    let configs1 = snapshot1
        .instance
        .unwrap()
        .config
        .extension_services
        .service_configs;
    assert_eq!(configs1.len(), 1);
    assert_eq!(configs1[0].service_vpc_id, Some(vpc_a));
    assert_eq!(configs1[0].service_vpc_index, Some(0));
    drop(txn);

    // Instance 2 attaches services A and B: A reuses index 0 (same tenant and
    // VPC as instance 1), B gets the next index, 1.
    let (_i2, _) = mh2
        .instance_builer(&env)
        .single_interface_network_config(segment_id)
        .extension_services(rpc::InstanceDpuExtensionServicesConfig {
            service_configs: vec![
                rpc::InstanceDpuExtensionServiceConfig {
                    service_id: service_a.service_id.clone(),
                    version: version_a.clone(),
                    ..Default::default()
                },
                rpc::InstanceDpuExtensionServiceConfig {
                    service_id: service_b.service_id.clone(),
                    version: version_b.clone(),
                    ..Default::default()
                },
            ],
        })
        .build_and_return()
        .await;

    let mut txn = env.db_txn().await;
    let snapshot2 = mh2.snapshot(&mut txn).await;
    let configs2 = snapshot2
        .instance
        .unwrap()
        .config
        .extension_services
        .service_configs;
    assert_eq!(configs2.len(), 2);
    let by_service: std::collections::HashMap<_, _> = configs2
        .iter()
        .map(|c| (c.service_id.to_string(), c))
        .collect();
    let a = by_service[&service_a.service_id];
    let b = by_service[&service_b.service_id];
    assert_eq!(a.service_vpc_id, Some(vpc_a));
    assert_eq!(a.service_vpc_index, Some(0));
    assert_eq!(b.service_vpc_id, Some(vpc_b));
    assert_eq!(b.service_vpc_index, Some(1));
    drop(txn);

    // A caller-supplied index that does not echo the system-assigned value is
    // rejected.
    let err = env
        .api
        .allocate_instance(Request::new(rpc::InstanceAllocationRequest {
            instance_id: None,
            machine_id: Some(mh3.host().id),
            instance_type_id: None,
            config: Some(rpc::InstanceConfig {
                tenant: Some(default_tenant_config()),
                os: Some(default_os_config()),
                network: Some(single_interface_network_config(segment_id)),
                infiniband: None,
                network_security_group_id: None,
                nvlink: None,
                spxconfig: None,
                power_profile: None,
                dpu_extension_services: Some(rpc::InstanceDpuExtensionServicesConfig {
                    service_configs: vec![rpc::InstanceDpuExtensionServiceConfig {
                        service_id: service_a.service_id.clone(),
                        version: version_a.clone(),
                        service_vpc_id: None,
                        service_vpc_index: Some(7),
                        ..Default::default()
                    }],
                }),
            }),
            metadata: None,
            allow_unhealthy_machine: false,
        }))
        .await
        .expect_err("caller-supplied service_vpc_index must be rejected");
    assert_eq!(err.code(), tonic::Code::InvalidArgument);

    // A fabricated attachment_id on first attach must be rejected: bindings
    // are system-assigned, and trusting one verbatim would let a caller plant
    // endpoints that bypass the reservation table's collision checks.
    let err = env
        .api
        .allocate_instance(Request::new(rpc::InstanceAllocationRequest {
            instance_id: None,
            machine_id: Some(mh3.host().id),
            instance_type_id: None,
            config: Some(rpc::InstanceConfig {
                tenant: Some(default_tenant_config()),
                os: Some(default_os_config()),
                network: Some(single_interface_network_config(segment_id)),
                infiniband: None,
                network_security_group_id: None,
                nvlink: None,
                spxconfig: None,
                power_profile: None,
                dpu_extension_services: Some(rpc::InstanceDpuExtensionServicesConfig {
                    service_configs: vec![rpc::InstanceDpuExtensionServiceConfig {
                        service_id: service_a.service_id.clone(),
                        version: version_a.clone(),
                        attachment_id: Some("6e1a49a2-71d4-4f8e-9c25-58c0c1a4bfab".to_string()),
                        ..Default::default()
                    }],
                }),
            }),
            metadata: None,
            allow_unhealthy_machine: false,
        }))
        .await
        .expect_err("fabricated attachment_id must be rejected");
    assert_eq!(err.code(), tonic::Code::InvalidArgument);

    Ok(())
}

// ---------------------------------------------------------------------------
// Service VPC ULA addressing tests (DPU block-storage design §6.2)
// ---------------------------------------------------------------------------

#[crate::sqlx_test]
async fn test_registration_derives_service_vpc_ula_prefix(
    db_pool: sqlx::PgPool,
) -> Result<(), eyre::Report> {
    let env = create_test_env(db_pool).await;
    create_test_tenants(&env).await?;

    let vpc_id = create_service_vpc(&env, "best_org", "ula vpc").await;
    create_test_extension_service_with_vpc(&env.api, "svc-ula-a", vpc_id)
        .await
        .expect("create should succeed");

    let root = crate::extension_service_ula::service_vpc_ula_root();
    let mut txn = env.db_txn().await;
    let prefixes = db::vpc_prefix::find_by_vpc(txn.as_mut(), vpc_id).await?;
    let derived: Vec<_> = prefixes
        .iter()
        .filter(|p| {
            p.metadata
                .labels
                .contains_key(crate::extension_service_ula::SERVICE_VPC_ULA_LABEL)
        })
        .collect();
    assert_eq!(derived.len(), 1, "exactly one derived /48 per service VPC");
    let prefix = derived[0];
    assert_eq!(prefix.config.prefix.prefix(), 48);
    assert!(
        root.contains(prefix.config.prefix.network()),
        "derived /48 must lie within the seeded ULA root"
    );
    assert!(
        prefix.site_prefix_id.is_some(),
        "derived /48 must be parented by the seeded ULA SitePrefix"
    );
    drop(txn);

    // A second service bound to the same VPC reuses the same /48.
    create_test_extension_service_with_vpc(&env.api, "svc-ula-b", vpc_id)
        .await
        .expect("second create should succeed");
    let mut txn = env.db_txn().await;
    let count = db::vpc_prefix::find_by_vpc(txn.as_mut(), vpc_id)
        .await?
        .into_iter()
        .filter(|p| {
            p.metadata
                .labels
                .contains_key(crate::extension_service_ula::SERVICE_VPC_ULA_LABEL)
        })
        .count();
    assert_eq!(count, 1, "the /48 is shared, not re-derived");

    Ok(())
}

#[crate::sqlx_test]
async fn test_registration_ula_collision_uses_next_probe(
    db_pool: sqlx::PgPool,
) -> Result<(), eyre::Report> {
    let env = create_test_env(db_pool).await;
    create_test_tenants(&env).await?;

    let vpc_id = create_service_vpc(&env, "best_org", "collision vpc").await;
    let blocker_vpc_id = create_service_vpc(&env, "best_org", "blocker vpc").await;

    let root = crate::extension_service_ula::service_vpc_ula_root();
    let probe0 =
        crate::extension_service_ula::derive_service_vpc_ula_prefix(root, vpc_id, 0).unwrap();
    let probe1 =
        crate::extension_service_ula::derive_service_vpc_ula_prefix(root, vpc_id, 1).unwrap();

    // Occupy the probe-0 candidate with an unrelated VPC prefix.
    let mut txn = env.db_txn().await;
    let blocker_vpc = db::vpc::find_by_with_lock(
        txn.as_mut(),
        db::ObjectColumnFilter::One(db::vpc::IdColumn, &blocker_vpc_id),
        db::vpc::VpcRowLock::None,
    )
    .await?
    .pop()
    .expect("blocker vpc exists");
    db::vpc_prefix::persist(
        model::vpc_prefix::NewVpcPrefix {
            id: carbide_uuid::vpc::VpcPrefixId::new(),
            site_prefix_id: None,
            vpc_id: blocker_vpc_id,
            config: model::vpc_prefix::VpcPrefixConfig {
                prefix: ipnetwork::IpNetwork::V6(probe0),
            },
            metadata: model::metadata::Metadata {
                name: "blocker".to_string(),
                description: String::new(),
                labels: Default::default(),
            },
        },
        blocker_vpc.version,
        txn.as_mut(),
    )
    .await?;
    txn.commit().await?;

    create_test_extension_service_with_vpc(&env.api, "svc-collision", vpc_id)
        .await
        .expect("create should succeed via the next probe");

    let mut txn = env.db_txn().await;
    let derived = db::vpc_prefix::find_by_vpc(txn.as_mut(), vpc_id)
        .await?
        .into_iter()
        .find(|p| {
            p.metadata
                .labels
                .contains_key(crate::extension_service_ula::SERVICE_VPC_ULA_LABEL)
        })
        .expect("derived /48 exists");
    assert_eq!(
        derived.config.prefix,
        ipnetwork::IpNetwork::V6(probe1),
        "probe-0 collision must fall through to probe 1"
    );

    Ok(())
}

#[crate::sqlx_test]
async fn test_service_delete_retires_ula_prefix_on_last_reference(
    db_pool: sqlx::PgPool,
) -> Result<(), eyre::Report> {
    let env = create_test_env(db_pool).await;
    create_test_tenants(&env).await?;

    let vpc_id = create_service_vpc(&env, "best_org", "retire vpc").await;
    let service_a = create_test_extension_service_with_vpc(&env.api, "svc-retire-a", vpc_id)
        .await
        .expect("create a");
    let service_b = create_test_extension_service_with_vpc(&env.api, "svc-retire-b", vpc_id)
        .await
        .expect("create b");

    async fn derived_count(
        txn: &mut sqlx::PgConnection,
        vpc_id: carbide_uuid::vpc::VpcId,
    ) -> Result<usize, eyre::Report> {
        Ok(db::vpc_prefix::find_by_vpc(txn, vpc_id)
            .await?
            .into_iter()
            .filter(|p| {
                p.metadata
                    .labels
                    .contains_key(crate::extension_service_ula::SERVICE_VPC_ULA_LABEL)
            })
            .count())
    }

    env.api
        .delete_dpu_extension_service(Request::new(rpc::DeleteDpuExtensionServiceRequest {
            service_id: service_a.service_id.clone(),
            versions: vec![],
        }))
        .await?;
    let mut txn = env.db_txn().await;
    assert_eq!(
        derived_count(txn.as_mut(), vpc_id).await?,
        1,
        "the /48 survives while another service references the VPC"
    );
    drop(txn);

    env.api
        .delete_dpu_extension_service(Request::new(rpc::DeleteDpuExtensionServiceRequest {
            service_id: service_b.service_id.clone(),
            versions: vec![],
        }))
        .await?;
    let mut txn = env.db_txn().await;
    assert_eq!(
        derived_count(txn.as_mut(), vpc_id).await?,
        0,
        "the /48 is retired with the last reference"
    );

    Ok(())
}

#[crate::sqlx_test]
async fn test_attach_creates_service_vpc_endpoints(
    db_pool: sqlx::PgPool,
) -> Result<(), eyre::Report> {
    let env = create_test_env(db_pool).await;
    let segment_id = env.create_vpc_and_tenant_segment().await;
    let mh = create_managed_host(&env).await;

    create_test_tenants(&env).await?;
    let vpc_id = create_service_vpc(&env, "best_org", "endpoint vpc").await;
    let service = create_test_extension_service_with_vpc(&env.api, "svc-endpoints", vpc_id)
        .await
        .expect("create service");
    let version = service
        .latest_version_info
        .as_ref()
        .unwrap()
        .version
        .clone();

    let (_instance, _) = mh
        .instance_builer(&env)
        .single_interface_network_config(segment_id)
        .extension_services(rpc::InstanceDpuExtensionServicesConfig {
            service_configs: vec![rpc::InstanceDpuExtensionServiceConfig {
                service_id: service.service_id.clone(),
                version,
                ..Default::default()
            }],
        })
        .build_and_return()
        .await;

    let mut txn = env.db_txn().await;
    let snapshot = mh.snapshot(&mut txn).await;
    let configs = snapshot
        .instance
        .as_ref()
        .unwrap()
        .config
        .extension_services
        .service_configs
        .clone();
    assert_eq!(configs.len(), 1);
    let binding = &configs[0];
    let attachment_id = binding.attachment_id.expect("attachment id assigned");
    let dpu_count = snapshot.dpu_snapshots.len();
    assert!(dpu_count > 0, "fixture host must have DPUs");
    assert_eq!(
        binding.endpoints.len(),
        dpu_count,
        "one endpoint per DPU of the instance"
    );

    let derived_48 = db::vpc_prefix::find_by_vpc(txn.as_mut(), vpc_id)
        .await?
        .into_iter()
        .find(|p| {
            p.metadata
                .labels
                .contains_key(crate::extension_service_ula::SERVICE_VPC_ULA_LABEL)
        })
        .expect("derived /48 exists");
    for endpoint in &binding.endpoints {
        assert_eq!(endpoint.link_prefix.prefix(), 127);
        assert!(
            derived_48
                .config
                .prefix
                .contains(endpoint.link_prefix.network()),
            "endpoint {} must lie within the derived /48 {}",
            endpoint.link_prefix,
            derived_48.config.prefix
        );
        let std::net::IpAddr::V6(network) = endpoint.link_prefix.network() else {
            panic!("expected an IPv6 endpoint prefix");
        };
        assert_eq!(
            u128::from(network) & 1,
            0,
            "the network address is the even (HBN) end"
        );
    }

    let rows = db::service_vpc_endpoint::find_by_attachment(txn.as_mut(), attachment_id).await?;
    assert_eq!(rows.len(), dpu_count, "one reservation row per endpoint");

    Ok(())
}

#[crate::sqlx_test]
async fn test_non_vpc_service_binding_tolerates_missing_dpu_list(
    db_pool: sqlx::PgPool,
) -> Result<(), eyre::Report> {
    // Regression: the DPU list is only needed to derive endpoints for NEW
    // service-VPC bindings. A service without a service VPC must bind even
    // when the DPU snapshot list is empty (e.g. a transient load gap).
    let env = create_test_env(db_pool).await;
    let service = create_test_extension_service_and_tenants(&env).await?;

    let mut config = model::instance::config::extension_services::InstanceExtensionServicesConfig {
        service_configs: vec![
            model::instance::config::extension_services::InstanceExtensionServiceConfig {
                service_id: service.service_id.parse().unwrap(),
                version: service
                    .latest_version_info
                    .as_ref()
                    .unwrap()
                    .version
                    .parse()
                    .unwrap(),
                removed: None,
                service_vpc_id: None,
                service_vpc_index: None,
                attachment_id: None,
                endpoints: Vec::new(),
            },
        ],
    };

    let mut txn = env.db_txn().await;
    crate::instance::assign_service_vpc_bindings(
        &mut config,
        "best_org",
        carbide_uuid::instance::InstanceId::new(),
        &[],
        8,
        txn.as_mut(),
    )
    .await
    .expect("non-VPC services must bind without a DPU list");
    assert!(config.service_configs[0].attachment_id.is_none());
    assert!(config.service_configs[0].endpoints.is_empty());

    Ok(())
}
