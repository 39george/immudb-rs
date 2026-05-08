use std::sync::Arc;
use std::time::Duration;

use bon::Builder;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tonic::{service::interceptor::InterceptedService, transport::Channel};

use crate::document::DocClient;
use crate::error::Error;
use crate::interceptor::SessionInterceptor;
use crate::schema::{DatabaseListRequestV2, DatabaseListResponseV2};
use crate::sql::SqlClient;

use super::Result;
use super::protocol::model::authorization_service_client::AuthorizationServiceClient;
use super::protocol::model::document_service_client::DocumentServiceClient;
use super::protocol::schema;
use super::protocol::schema::immu_service_client::ImmuServiceClient;

#[derive(Debug, Clone, Builder)]
#[builder(finish_fn(vis = "", name = build_internal))]
pub struct ConnectOptions {
    #[builder(into, default = String::from("immudb"))]
    pub username: String,

    #[builder(into, default = String::from("immudb"))]
    pub password: String,

    #[builder(into, default = String::from("defaultdb"))]
    pub database: String,

    #[builder(default = Duration::from_secs(5))]
    pub connect_timeout: Duration,

    #[builder(default = true)]
    pub keepalive_while_idle: bool,
}

impl<State: connect_options_builder::IsComplete> ConnectOptionsBuilder<State> {
    /// Uri example: "http://localhost:3322"
    pub async fn connect(self, uri: impl AsRef<str>) -> Result<ImmuDB> {
        let uri = uri.as_ref().parse()?;
        let opts = self.build_internal();

        // No TLS currently
        let endpoint = Channel::builder(uri)
            .connect_timeout(opts.connect_timeout)
            .keep_alive_while_idle(opts.keepalive_while_idle)
            // Little TCP keepalive, if enabled
            .tcp_keepalive(if opts.keepalive_while_idle {
                Some(Duration::from_secs(30))
            } else {
                None
            });

        let channel = endpoint.connect().await.map_err(Error::from)?;

        let schema::OpenSessionResponse {
            session_id,
            server_uuid,
        } = ImmuServiceClient::new(channel.clone())
            .open_session(schema::OpenSessionRequest {
                username: opts.username.clone().into_bytes(),
                password: opts.password.clone().into_bytes(),
                database_name: opts.database.clone(),
            })
            .await
            .map_err(Error::from)?
            .into_inner();

        let interceptor = SessionInterceptor::new(&session_id, &server_uuid);
        let service =
            InterceptedService::new(channel.clone(), interceptor.clone());

        let token = ImmuServiceClient::new(service.clone())
            .use_database(schema::Database {
                database_name: opts.database.clone(),
            })
            .await?
            .into_inner()
            .token;

        interceptor.set_token(token)?;

        let ka_cancel = CancellationToken::new();

        let db = ImmuDB {
            inner: Arc::new(Inner {
                service,
                channel,
                interceptor,
                cancel: ka_cancel.clone(),
                username: opts.username,
                password: opts.password,
                database: opts.database,
                reopen_lock: tokio::sync::Mutex::new(()),
            }),
        };

        spawn_keepalive(db.clone(), ka_cancel);

        Ok(db)
    }
}

#[derive(Clone)]
pub struct ImmuDB {
    inner: Arc<Inner>,
}

struct Inner {
    service: InterceptedService<Channel, SessionInterceptor>,
    channel: Channel,
    interceptor: SessionInterceptor,
    cancel: CancellationToken,
    username: String,
    password: String,
    database: String,
    reopen_lock: tokio::sync::Mutex<()>,
}

impl ImmuDB {
    pub fn builder() -> ConnectOptionsBuilder {
        ConnectOptions::builder()
    }
    pub(crate) fn raw_doc(
        &self,
    ) -> DocumentServiceClient<InterceptedService<Channel, SessionInterceptor>>
    {
        DocumentServiceClient::new(self.inner.service.clone())
    }
    pub(crate) fn raw_auth(
        &self,
    ) -> AuthorizationServiceClient<
        InterceptedService<Channel, SessionInterceptor>,
    > {
        AuthorizationServiceClient::new(self.inner.service.clone())
    }
    pub(crate) fn raw_main(
        &self,
    ) -> ImmuServiceClient<InterceptedService<Channel, SessionInterceptor>>
    {
        ImmuServiceClient::new(self.inner.service.clone())
    }
    pub fn sql(&self) -> SqlClient {
        SqlClient::new(&self)
    }
    pub fn doc(&self) -> DocClient {
        DocClient::new(&self)
    }
    pub async fn use_database(&self, database: &str) -> Result<()> {
        let mut cli = ImmuServiceClient::new(self.inner.service.clone());
        let resp = cli
            .use_database(schema::Database {
                database_name: database.to_string(),
            })
            .await?
            .into_inner();

        self.inner.interceptor.set_token(resp.token)?;
        Ok(())
    }
    pub async fn reopen_session(&self) -> Result<()> {
        let _guard = self.inner.reopen_lock.lock().await;

        let schema::OpenSessionResponse {
            session_id,
            server_uuid,
        } = ImmuServiceClient::new(self.inner.channel.clone())
            .open_session(schema::OpenSessionRequest {
                username: self.inner.username.clone().into_bytes(),
                password: self.inner.password.clone().into_bytes(),
                database_name: self.inner.database.clone(),
            })
            .await?
            .into_inner();

        self.inner
            .interceptor
            .set_session(session_id, server_uuid)?;

        let token = ImmuServiceClient::new(self.inner.service.clone())
            .use_database(schema::Database {
                database_name: self.inner.database.clone(),
            })
            .await?
            .into_inner()
            .token;

        self.inner.interceptor.set_token(token)?;

        Ok(())
    }
}

impl ImmuDB {
    pub async fn list_databases(&self) -> Result<Vec<schema::DatabaseInfo>> {
        let DatabaseListResponseV2 { databases } = self
            .raw_main()
            .database_list_v2(DatabaseListRequestV2 {})
            .await?
            .into_inner();
        Ok(databases)
    }
}

impl Drop for Inner {
    fn drop(&mut self) {
        self.cancel.cancel();
        let mut client = ImmuServiceClient::new(self.service.clone());
        let _ =
            std::thread::spawn(move || match tokio::runtime::Runtime::new() {
                Ok(rt) => {
                    rt.block_on(async {
                        if let Err(e) = client.close_session(()).await {
                            tracing::error!(
                                "failed to close immudb session: {e:?}"
                            );
                        }
                    });
                }
                Err(e) => {
                    tracing::error!("failed to spawn tokio runtime: {e}");
                }
            })
            .join();
    }
}

fn spawn_keepalive(db: ImmuDB, cancel: CancellationToken) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(Duration::from_secs(30));

        loop {
            tracing::trace!("keepalive tick");

            tokio::select! {
                _ = tick.tick() => {
                    let mut cli = db.raw_main();

                    match cli.keep_alive(()).await {
                        Ok(_) => {}

                        Err(e) if is_session_not_found(&e) => {
                            tracing::warn!(
                                %e,
                                "immudb session expired, reopening session"
                            );

                            if let Err(reopen_err) = db.reopen_session().await {
                                tracing::error!(
                                    ?reopen_err,
                                    "failed to reopen immudb session"
                                );
                            }
                        }

                        Err(e) => {
                            tracing::warn!(%e, "immudb keepalive failed");
                        }
                    }
                }

                _ = cancel.cancelled() => break,
            }
        }
    })
}

fn is_session_not_found(status: &tonic::Status) -> bool {
    status.code() == tonic::Code::PermissionDenied
        && status.message().contains("session not found")
}
