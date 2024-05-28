// Copyright Mad Max, 2024.

use reqwest::Client;
use serde::{Deserialize, Serialize};
use std::error::Error;

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

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let client = Client::new();
    let url = "https://<account>.snowflakecomputing.com/api/v2/statements";
    let query = "SELECT CURRENT_TIMESTAMP;";
    let token = "<your_oauth_token>";

    match execute_snowflake_query(&client, url, query, token).await {
        Ok(response) => {
            for row in response.data.rowset {
                println!("{:?}", row);
            }
            println!("Total rows: {}", response.data.total);
        }
        Err(err) => {
            eprintln!("Error executing query: {}", err);
        }
    }

    Ok(())
}
