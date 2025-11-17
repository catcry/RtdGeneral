use anyhow::Result;
use chrono::NaiveDateTime;
use clickhouse::{Client, Row};
use rayon::prelude::*;
use std::fs;
use tokio::fs::read_to_string;
use serde::{Deserialize, Serialize};



#[derive(Row, Debug, Serialize,Deserialize)]
struct ProcessedData {
    file_name: String,
    msisdn: String,
    timestamp: u32,
    cat_list: Vec<String>,
    p_dis_list: Vec<String>,
    trigger_list: Vec<String>,
}

const FOLDER_PATH: &str = "/root/cc_ch";

#[tokio::main]
async fn main() -> Result<()> {
    // Collect all files
    let all_files: Vec<_> = fs::read_dir(FOLDER_PATH)?
        .filter_map(|entry| entry.ok())
        .map(|entry| entry.path())
        .filter(|path| path.is_file())
        .collect();

    println!("Reading files...");

    // 2. Read files asynchronously
    let mut tasks = Vec::new();
    for file_path in &all_files {
        let path_clone = file_path.clone();
        let file_name = path_clone
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap()
        .to_string();
        tasks.push(tokio::spawn(async move {
            let data = read_to_string(&path_clone).await?;
            Ok::<_, anyhow::Error>((file_name, data))
        }));
    }

    let results: Vec<(String, String)> = futures::future::try_join_all(tasks)
        .await?
        .into_iter()
        .collect::<Result<Vec<_>, _>>()?;
    println!("Done reading. Parsing in parallel...");

    // Process files in parallel
    let processed_data: Vec<ProcessedData> = results
    .par_iter()
    .flat_map(|(file_name, data)| {
        main_process_aggregated(data, file_name)
            
    })
    .collect();

    println!("Inserting {} records into ClickHouse...", processed_data.len());

    // ClickHouse client
    let client = Client::default()
        .with_url("http://192.168.5.197:8123")
        .with_user("default")
        .with_password("123123")
        .with_database("rtd");

    // Batch insert
    let batch_size = 10_000;
    for chunk in processed_data.chunks(batch_size) {
        let mut insert = client.insert("hamid_processed_data")?;
        for row in chunk {
            insert.write(row).await?;
        }
        insert.end().await?;
    }
 

    println!("All done!");
    Ok(())
}

// ================= Parsing Functions =================

// get_field returns a slice to avoid allocations
fn get_field<'a>(field_name: &str, data: &'a str) -> Option<&'a str> {
    let idx = data.find(field_name)?;
    let end = data[idx..].find('\n').map(|i| i + idx)?;
    Some(data[idx + field_name.len()..end].trim())
}

// Parse block lines starting with "F " and return keys
fn get_block_keys<'a>(data: &'a str) -> Vec<&'a str> {
    let mut keys = Vec::new();
    for line in data.lines() {
        if let Some(rest) = line.strip_prefix("F ") {
            if let Some(space_idx) = rest.find(' ') {
                keys.push(&rest[..space_idx]);
            }
        }
    }
    keys
}
// Extract all Logic block IDs (B logic ... .) from an ACTION_MONITOR section
fn get_action_monitor_logic_ids(data: &str) -> Vec<String> {
    let mut ids = Vec::new();
    let mut search_pos = 0;

    while let Some(block_start_rel) = data[search_pos..].find("B LOGIC") {
        let block_start = search_pos + block_start_rel;

        // Find the end of this logic block (ends with a single '.')
        let block_end = data[block_start..]
            .find(".\n")
            .map(|i| block_start + i + 1)
            .unwrap_or(data.len());

        let block_data = &data[block_start..block_end];

        // Find and capture the "F ID ..." line
        for line in block_data.lines() {
            if let Some(rest) = line.strip_prefix("F ID ") {
                ids.push(rest.trim().to_string());
            }
        }

        search_pos = block_end;
        if search_pos >= data.len() {
            break;
        }
    }

    ids
}

fn main_process_aggregated(data: &str, file_name: &str) -> Vec<ProcessedData> {
    let mut result = Vec::new();
    let record_tag = "RECORD";
    let mut start = match data.find(record_tag) {
        Some(i) => i,
        None => return result,
    };

    loop {
        let next = data[start + 1..].find(record_tag).map(|i| i + start + 1);
        let record_data = match next {
            Some(end) => &data[start..end],
            None => &data[start..],
        };

        if let Some(msisdn) = get_field("callingNumber", record_data) {
            if let Some(timestamp_str) = get_field("eventStartTimestamp", record_data) {
                if let Ok(timestamp) =
                    NaiveDateTime::parse_from_str(timestamp_str, "%Y%m%d%H%M%S")
                {
                    // single scan to collect block indices
                    let mut cat_list = Vec::new();
                    let mut p_dis_list = Vec::new();
                    let mut trigger_list = Vec::new();

                    let mut idx = 0;
                    while let Some(block_start) = record_data[idx..].find("B ") {
                        let block_start = block_start + idx;
                        let block_end = record_data[block_start..]
                            .find('\n')
                            .map(|i| i + block_start)
                            .unwrap_or(record_data.len());

                        let block_line = &record_data[block_start..block_end];

                        if block_line.contains("__CATEGORIZATION__") {
                            if let Some(f_idx) = record_data[block_start..].find("FAILED_LOGICS") {
                                let f_start = block_start + f_idx;
                                let f_end = record_data[f_start..]
                                    .find('.')
                                    .map(|i| i + f_start)
                                    .unwrap_or(record_data.len());
                                cat_list.extend(
                                    get_block_keys(&record_data[f_start..f_end])
                                        .into_iter()
                                        .map(|s| s.to_string()),
                                );
                            }
                        } else if block_line.contains("__RULE_ENGINE__") {
                            if let Some(f_idx) = record_data[block_start..].find("TRIGGERED_ACTIONS") {
                                let f_start = block_start + f_idx;
                                let f_end = record_data[f_start..]
                                    .find('.')
                                    .map(|i| i + f_start)
                                    .unwrap_or(record_data.len());
                                p_dis_list.extend(
                                    get_block_keys(&record_data[f_start..f_end])
                                        .into_iter()
                                        .map(|s| s.to_string()),
                                );
                            }
                        } else if block_line.contains("__ACTION_MONITOR__") {
                            // ACTION_MONITOR extends to end of record, so take everything after this point
                            let action_block = &record_data[block_start..];

                            // Extract all logic IDs from B logic blocks
                            let logic_ids = get_action_monitor_logic_ids(action_block);
                            trigger_list.extend(logic_ids);
                        }

                        idx = block_end;
                        if idx >= record_data.len() {
                            break;
                        }
                    }

                    result.push(ProcessedData {
                        file_name: file_name.to_string(),
                        msisdn: msisdn.to_string(),
                        timestamp:timestamp.and_utc().timestamp() as u32,
                        cat_list,
                        p_dis_list,
                        trigger_list,
                    });
                }
            }
        }

        if let Some(end) = next {
            start = end;
        } else {
            break;
        }
    }

    result
}
