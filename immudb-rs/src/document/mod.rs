use crate::ImmuDB;
use crate::error::Error;
use crate::interceptor::SessionInterceptor;
use crate::model::document_service_client::DocumentServiceClient;
use crate::model::{
    DeleteCollectionRequest, DocumentAtRevision, GetCollectionsRequest,
    GetCollectionsResponse, InsertDocumentsResponse, SearchDocumentsRequest,
};

use super::Result;
use super::protocol::model;

pub mod builder;
mod conv;

pub struct DocClient {
    inner: DocumentServiceClient<
        tonic::service::interceptor::InterceptedService<
            tonic::transport::Channel,
            SessionInterceptor,
        >,
    >,
}

impl DocClient {
    pub(crate) fn new(db: &ImmuDB) -> Self {
        Self {
            inner: db.raw_doc(),
        }
    }

    pub async fn list_collections(&mut self) -> Result<Vec<model::Collection>> {
        let GetCollectionsResponse { collections } = self
            .inner
            .get_collections(GetCollectionsRequest {})
            .await?
            .into_inner();
        Ok(collections)
    }

    pub async fn create_collection(
        &mut self,
        param: builder::CreateCollection,
    ) -> Result<()> {
        let mut fields: Vec<model::Field> = Vec::new();
        let mut indexes: Vec<model::Index> = Vec::new();

        for custom_field in param.fields.into_iter() {
            let parts = conv::ProtobufFieldParts::from(custom_field);
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

        self.inner.create_collection(req).await?;
        Ok(())
    }

    pub async fn delete_collection(&mut self, name: &str) -> Result<()> {
        self.inner
            .delete_collection(DeleteCollectionRequest { name: name.into() })
            .await?;
        Ok(())
    }

    pub async fn insert_documents(
        &mut self,
        collection: &str,
        docs: Vec<serde_json::Value>,
    ) -> Result<InsertDocumentsResponse> {
        let data = docs
            .into_iter()
            .map(|doc| {
                if let serde_json::Value::Object(map) = doc {
                    Ok(conv::to_struct(map))
                } else {
                    Err("root of document must be a JSON object".to_string())
                }
            })
            .collect::<std::result::Result<Vec<_>, _>>();

        let documents = data.map_err(Error::Unexpected)?;

        let result = self
            .inner
            .insert_documents(model::InsertDocumentsRequest {
                collection_name: collection.into(),
                documents,
            })
            .await?
            .into_inner();

        Ok(result)
    }

    pub async fn search_document(
        &mut self,
        param: builder::SearchDocuments,
    ) -> Result<Vec<DocumentAtRevision>> {
        let query = conv::json_to_immudb_query(param.query)?;
        let model::SearchDocumentsResponse { revisions, .. } = self
            .inner
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
