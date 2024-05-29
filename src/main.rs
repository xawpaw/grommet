// Copyright Mad Max, 2024.

use reqwest::Client;
use serde::{Deserialize, Serialize};
use std::error::Error;
use tokio::task;

#[derive(Serialize)]
struct SnowflakeQueryRequest<'a> {
    sql_text: &'a str,
}

#[derive(Deserialize)]
struct SnowflakeQueryResponse {
    data: QueryData,
}

#[derive(Deserialize)]
struct QueryData {
    rowset: Vec<Vec<String>>,
    total: usize,
}

async fn execute_snowflake_query(
    client: &Client,
    url: &str,
    query: &str,
    token: &str,
) -> Result<SnowflakeQueryResponse, Box<dyn Error>> {
    let request = SnowflakeQueryRequest { sql_text: query };
    let response = client
        .post(url)
        .header("Authorization", format!("Bearer {}", token))
        .json(&request)
        .send()
        .await?
        .json::<SnowflakeQueryResponse>()
        .await?;
    Ok(response)
}

async fn execute_multiple_queries() -> Result<(), Box<dyn Error>> {
    let client = Client::new();
    let url = "https://<account>.snowflakecomputing.com/api/v2/statements";
    let token = "<your_oauth_token>";

    let queries = vec![
        "SELECT CURRENT_TIMESTAMP;",
        "SELECT COUNT(*) FROM my_table;",
        "SELECT * FROM my_table LIMIT 10;",
    ];

    let mut tasks = vec![];

    for query in queries {
        let client = client.clone();
        let url = url.to_string();
        let token = token.to_string();

        tasks.push(task::spawn(async move {
            match execute_snowflake_query(&client, &url, query, &token).await {
                Ok(response) => {
                    println!("Query: {}", query);
                    for row in response.data.rowset {
                        println!("{:?}", row);
                    }
                    println!("Total rows: {}", response.data.total);
                }
                Err(err) => {
                    eprintln!("Error executing query '{}': {}", query, err);
                }
            }
        }));
    }

    for task in tasks {
        task.await??;
    }

    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    execute_multiple_queries().await
}
