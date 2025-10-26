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
                username: opts.username.into_bytes(),
                password: opts.password.into_bytes(),
                database_name: opts.database.clone(),
            })
            .await
            .map_err(Error::from)?
            .into_inner();

        let interceptor = SessionInterceptor::new(&session_id, &server_uuid);
        let service =
            InterceptedService::new(channel.clone(), interceptor.clone());

        // 3) Выбираем БД и получаем token
        let token = ImmuServiceClient::new(service.clone())
            .use_database(schema::Database {
                database_name: opts.database.clone(),
            })
            .await?
            .into_inner()
            .token;

        // 4) Кладём token в интерсептор (теперь authorization будет на всех RPC)
        interceptor.set_token(token)?;

        // 5) Один keepalive-таск на весь клиент
        let (ka_cancel, _ka_handle) = spawn_keepalive(service.clone());

        Ok(ImmuDB {
            service,
            interceptor, // держим, чтобы можно было менять токен позже
            cancel_keep_alive: ka_cancel,
        })
    }
}

#[derive(Clone)]
pub struct ImmuDB {
    service: InterceptedService<Channel, SessionInterceptor>,
    interceptor: SessionInterceptor,
    cancel_keep_alive: CancellationToken,
}

impl ImmuDB {
    pub fn builder() -> ConnectOptionsBuilder {
        ConnectOptions::builder()
    }
    pub(crate) fn raw_doc(
        &self,
    ) -> DocumentServiceClient<InterceptedService<Channel, SessionInterceptor>>
    {
        DocumentServiceClient::new(self.service.clone())
    }
    pub(crate) fn raw_auth(
        &self,
    ) -> AuthorizationServiceClient<
        InterceptedService<Channel, SessionInterceptor>,
    > {
        AuthorizationServiceClient::new(self.service.clone())
    }
    pub(crate) fn raw_main(
        &self,
    ) -> ImmuServiceClient<InterceptedService<Channel, SessionInterceptor>>
    {
        ImmuServiceClient::new(self.service.clone())
    }
    pub fn sql(&self) -> SqlClient {
        SqlClient::new(&self)
    }
    pub fn doc(&self) -> DocClient {
        DocClient::new(&self)
    }
    pub async fn use_database(&self, database: &str) -> Result<()> {
        let mut cli = ImmuServiceClient::new(self.service.clone());
        let resp = cli
            .use_database(schema::Database {
                database_name: database.to_string(),
            })
            .await?
            .into_inner();

        self.interceptor.set_token(resp.token)?;
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

impl Drop for ImmuDB {
    fn drop(&mut self) {
        self.cancel_keep_alive.cancel();
        return;
        let mut client = self.raw_main();
        let _ =
            std::thread::spawn(move || match tokio::runtime::Runtime::new() {
                Ok(rt) => {
                    rt.block_on(async {
                        if let Err(e) = client.close_session(()).await {
                            eprintln!("failed to close immudb session: {e:?}");
                        }
                    });
                }
                Err(e) => {
                    eprint!("failed to spawn tokio runtime: {e}");
                }
            })
            .join();
    }
}

fn spawn_keepalive(
    service: InterceptedService<Channel, SessionInterceptor>,
) -> (CancellationToken, JoinHandle<()>) {
    let cancel = CancellationToken::new();
    let svc = service.clone();
    let handle = tokio::spawn({
        let cancel = cancel.clone();
        async move {
            let mut cli = ImmuServiceClient::new(svc);
            let mut tick = tokio::time::interval(Duration::from_secs(30));
            loop {
                tokio::select! {
                    _ = tick.tick() => { let _ = cli.keep_alive(()).await; }
                    _ = cancel.cancelled() => break,
                }
            }
        }
    });
    (cancel, handle)
}
