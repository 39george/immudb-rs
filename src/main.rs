use immudb_rs::builder::{CreateCollection, Field, FieldType, SearchDocuments};
use immudb_rs::ImmuDB;
use immudb_rs::Interface;
use serde_json::json;

#[tokio::main]
pub async fn main() -> anyhow::Result<()> {
    // 1. Инициализация клиента
    let client = ImmuDB::new(
        "http://localhost:3322".parse().unwrap(),
        "immudb",
        "immudb",
        "defaultdb",
    )
    .await?;

    let collection_name = "UserDocumentsBuilder";

    println!("--- 1. Очистка и создание коллекции через Builder ---");

    // 1.1. Очистка (для идемпотентности)
    let _ = client.delete_collection(collection_name).await;
    println!("Старая коллекция удалена (если существовала).");

    // 1.2. Создание коллекции (Новый Builder Interface)
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
        .create(&client)
        .await?;

    println!(
        "Коллекция '{}' успешно создана через Builder.",
        collection_name
    );

    println!("\n--- 2. Вставка документа ---");
    client
        .insert_documents(
            collection_name,
            vec![json!({
                "group_id": "mpc_group_a",
                "value": "Zm9vYmFyCg==",
                "is_active": true
            })],
        )
        .await
        .unwrap();

    println!("Документ (my_id: key_001) успешно вставлен.");

    println!("\n--- 3. Поиск документа через Search Builder ---");
    // 3. Поиск документа (Новый SearchDocuments Builder Interface)
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

    let docs = SearchDocuments::query(search_json) // Начало с обязательного Query
        .page(1)
        .page_size(10)
        // .search_id("cursor_id") // Если задать, keep_open будет true автоматически!
        .execute(&client) // Вызываем execute через Interface
        .await?;

    println!("Результаты поиска (найдено {} документов):", docs.len());
    println!("{docs:?}");

    Ok(())
}
