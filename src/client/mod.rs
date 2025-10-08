use anyhow::anyhow;
use tonic::metadata::{Ascii, MetadataValue};
use tonic::service::Interceptor;
use tonic::{
    service::interceptor::InterceptedService,
    transport::{Channel, Uri},
};

use crate::client::conv::to_struct;
use crate::error::Error;
use crate::model::{
    DeleteCollectionRequest, DocumentAtRevision, GetCollectionsRequest,
    GetCollectionsResponse, InsertDocumentsResponse, SearchDocumentsRequest,
};
use crate::schema::{DatabaseListRequestV2, DatabaseListResponseV2};

use super::Result;
use super::protocol::model;
use super::protocol::model::authorization_service_client::AuthorizationServiceClient;
use super::protocol::model::document_service_client::DocumentServiceClient;
use super::protocol::schema;
use super::protocol::schema::immu_service_client::ImmuServiceClient;

mod conv;

#[derive(Clone)]
pub struct SessionInterceptor {
    _server_uuid: String,
    session_id: MetadataValue<Ascii>,
}

impl SessionInterceptor {
    pub fn new(session_id: String, server_uuid: String) -> Self {
        let session_id_value = MetadataValue::try_from(session_id)
            .expect("Session ID must be valid ASCII");
        Self {
            session_id: session_id_value,
            _server_uuid: server_uuid,
        }
    }
}

impl Interceptor for SessionInterceptor {
    fn call(
        &mut self,
        mut req: tonic::Request<()>,
    ) -> tonic::Result<tonic::Request<()>> {
        req.metadata_mut()
            .insert("sessionid", self.session_id.clone());
        Ok(req)
    }
}

pub struct ImmuDB {
    service: InterceptedService<Channel, SessionInterceptor>,
}

impl ImmuDB {
    pub async fn new(
        url: Uri,
        username: &str,
        password: &str,
        database: &str,
    ) -> anyhow::Result<Self> {
        let channel = tonic::transport::Channel::builder(url).connect().await?;
        let schema::OpenSessionResponse {
            session_id,
            server_uuid,
        } = ImmuServiceClient::new(channel.clone())
            .open_session(schema::OpenSessionRequest {
                username: username.as_bytes().to_vec(),
                password: password.as_bytes().to_vec(),
                database_name: database.into(),
            })
            .await?
            .into_inner();
        let interceptor = SessionInterceptor::new(session_id, server_uuid);
        Ok(Self {
            service: InterceptedService::new(channel, interceptor),
        })
    }
    pub fn doc(
        &self,
    ) -> DocumentServiceClient<InterceptedService<Channel, SessionInterceptor>>
    {
        DocumentServiceClient::new(self.service.clone())
    }
    pub fn auth(
        &self,
    ) -> AuthorizationServiceClient<
        InterceptedService<Channel, SessionInterceptor>,
    > {
        AuthorizationServiceClient::new(self.service.clone())
    }
    pub fn main(
        &self,
    ) -> ImmuServiceClient<InterceptedService<Channel, SessionInterceptor>>
    {
        ImmuServiceClient::new(self.service.clone())
    }
}

impl Drop for ImmuDB {
    fn drop(&mut self) {
        let mut client = self.main();
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

#[async_trait::async_trait]
impl super::interface::Interface for ImmuDB {
    async fn list_databases(&self) -> Result<Vec<schema::DatabaseInfo>> {
        let DatabaseListResponseV2 { databases } = self
            .main()
            .database_list_v2(DatabaseListRequestV2 {})
            .await?
            .into_inner();
        Ok(databases)
    }
    async fn list_collections(&self) -> Result<Vec<model::Collection>> {
        let GetCollectionsResponse { collections } = self
            .doc()
            .get_collections(GetCollectionsRequest {})
            .await?
            .into_inner();
        Ok(collections)
    }
    async fn create_collection(
        &self,
        param: super::builder::CreateCollection,
    ) -> Result<()> {
        let mut fields: Vec<model::Field> = Vec::new();
        let mut indexes: Vec<model::Index> = Vec::new();

        for custom_field in param.fields.into_iter() {
            let parts = conv::ProtobufFieldParts::try_from(custom_field)?;
            fields.push(parts.proto_field);
            if let Some(index) = parts.proto_index {
                indexes.push(index);
            }
        }

        let req = model::CreateCollectionRequest {
            name: param.name,
            document_id_field_name: param.document_id_field_name,
            fields,
            indexes,
        };

        self.doc().create_collection(req).await?;
        Ok(())
    }
    async fn delete_collection(&self, name: &str) -> Result<()> {
        self.doc()
            .delete_collection(DeleteCollectionRequest { name: name.into() })
            .await?;
        Ok(())
    }
    async fn insert_documents(
        &self,
        collection: &str,
        docs: Vec<serde_json::Value>,
    ) -> Result<InsertDocumentsResponse> {
        let data = docs
            .into_iter()
            .map(|doc| {
                if let serde_json::Value::Object(map) = doc {
                    Ok(to_struct(map))
                } else {
                    Err(anyhow::anyhow!(
                        "root of document must be a JSON object"
                    ))
                }
            })
            .collect::<std::result::Result<Vec<_>, _>>();

        let documents = data.map_err(Error::Unexpected)?;

        let result = self
            .doc()
            .insert_documents(model::InsertDocumentsRequest {
                collection_name: collection.into(),
                documents,
            })
            .await?
            .into_inner();

        Ok(result)
    }
    async fn search_document(
        &self,
        param: crate::builder::SearchDocuments,
    ) -> Result<Vec<DocumentAtRevision>> {
        let query = conv::json_to_immudb_query(param.query)?;
        let model::SearchDocumentsResponse { revisions, .. } = self
            .doc()
            .search_documents(SearchDocumentsRequest {
                search_id: param.search_id,
                query: Some(query),
                page: param.page,
                page_size: param.page_size,
                keep_open: param.keep_open,
            })
            .await?
            .into_inner();
        Ok(revisions)
    }
}
