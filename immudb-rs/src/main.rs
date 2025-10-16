use immudb_rs::document::builder::{
    CreateCollection, Field, FieldType, SearchDocuments,
};
use immudb_rs::{ImmuDB, Result};
use serde_json::json;

#[tokio::main]
async fn main() -> Result<()> {
    let client = ImmuDB::builder()
        .username("immudb")
        .password("immudb")
        .database("defaultdb")
        .connect("http://localhost:3322")
        .await?;

    let mut doc = client.doc();

    let collection_name = "UserDocumentsBuilder";

    // NB: Pub method
    let _ = doc.delete_collection(collection_name).await;

    // 1.2. Создание коллекции (новый Builder Interface)
    CreateCollection::name(collection_name)
        .document_id_field_name("my_id")
        .field(
            Field::name("group_id")
                .field_type(FieldType::String)
                .indexed(true)
                .build(),
        )
        .field(Field::name("value").field_type(FieldType::String).build())
        .field(
            Field::name("is_active")
                .field_type(FieldType::Boolean)
                .indexed(true)
                .build(),
        )
        .create(&mut doc)
        .await?;

    doc.insert_documents(
        collection_name,
        vec![json!({
            "group_id": "mpc_group_a",
            "value": "Zm9vYmFyCg==",
            "is_active": true
        })],
    )
    .await?;

    let search_json = json!({
        "collection_name": collection_name,
        "limit": 50,
        "order_by": [
            {"field": "group_id", "desc": true}
        ],
        "where": {
            "AND": [
                {"field": "group_id", "op": "EQ", "value": "mpc_group_a"},
                {"field": "is_active", "op": "EQ", "value": true}
            ]
        }
    });

    let docs = SearchDocuments::query(search_json)
        .page(1)
        .page_size(10)
        .execute(&mut doc)
        .await?;

    println!("{docs:#?}");

    Ok(())
}
