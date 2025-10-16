use crate::document::DocClient;

use super::Result;

// ─────────────────────────── Create Collection ──────────────────────────── //

#[derive(bon::Builder)]
#[builder(start_fn = name)]
#[builder(finish_fn(vis = "", name = build_internal))]
pub struct CreateCollection {
    #[builder(start_fn, into)]
    pub(crate) name: String,
    #[builder(field)]
    pub(crate) fields: Vec<Field>,
    /// Primary key for collection
    #[builder(into, default = "")]
    pub(crate) document_id_field_name: String,
}

#[derive(Default)]
pub enum FieldType {
    #[default]
    String,
    Boolean,
    Integer,
    Double,
    Uuid,
}

#[derive(Default, bon::Builder)]
#[builder(start_fn = name)]
pub struct Field {
    #[builder(start_fn, into)]
    pub(crate) name: String,
    pub(crate) field_type: FieldType,
    #[builder(default = false)]
    pub(crate) unique: bool,
    #[builder(default = false)]
    pub(crate) indexed: bool,
}

// Now define custom `arg/args` methods on the builder itself.
impl<S: create_collection_builder::State> CreateCollectionBuilder<S> {
    pub fn field(mut self, arg: Field) -> Self {
        // We have access to `self.args` private 🔒 field on `CommandBuilder`!
        self.fields.push(arg);
        self
    }
}

impl<S> CreateCollectionBuilder<S>
where
    S: create_collection_builder::IsComplete,
{
    pub async fn create(self, doc: &mut DocClient) -> Result<()> {
        let collection = self.build_internal();
        doc.create_collection(collection).await
    }
}

// ──────────────────────────── Search Documents ──────────────────────────── //

#[derive(bon::Builder)]
#[builder(start_fn = query)]
#[builder(finish_fn(vis = "", name = build_internal))]
pub struct SearchDocuments {
    #[builder(start_fn)]
    pub(crate) query: serde_json::Value,
    #[builder(into, default = "")]
    pub(crate) search_id: String,
    #[builder(default = 50)]
    pub(crate) page_size: u32,
    #[builder(default = 1)]
    pub(crate) page: u32,
    /// Это поле нужно, чтобы явно указать Immudb сохранить состояние поиска на сервере.
    /// Если не параметризовать, вы блокируете функционал continuous search/cursor.
    #[builder(default = false)]
    pub(crate) keep_open: bool,
}

impl<S> SearchDocumentsBuilder<S>
where
    S: search_documents_builder::IsComplete,
{
    pub async fn execute(
        self,
        doc: &mut DocClient,
    ) -> Result<Vec<crate::model::DocumentAtRevision>> {
        let mut param = self.build_internal();

        if !param.search_id.is_empty() {
            param.keep_open = true;
        }

        doc.search_document(param).await
    }
}
