use crate::builder::{CreateCollection, SearchDocuments};
use crate::model::{Collection, DocumentAtRevision, InsertDocumentsResponse};
use crate::schema::DatabaseInfo;
use crate::Result;

#[async_trait::async_trait]
pub trait Interface {
    async fn list_databases(&self) -> Result<Vec<DatabaseInfo>>;
    async fn list_collections(&self) -> Result<Vec<Collection>>;
    async fn create_collection(&self, param: CreateCollection) -> Result<()>;
    async fn delete_collection(&self, name: &str) -> Result<()>;
    async fn insert_documents(
        &self,
        collection: &str,
        docs: Vec<serde_json::Value>,
    ) -> Result<InsertDocumentsResponse>;
    async fn search_document(
        &self,
        param: SearchDocuments,
    ) -> Result<Vec<DocumentAtRevision>>;
}
