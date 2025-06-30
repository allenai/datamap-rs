use anyhow::{anyhow, Error, Result};
use arrow::array::{Array, StringArray};
use arrow::record_batch::RecordBatch;
use dashmap::DashMap;
use mj_io::{build_pbar, expand_dirs, get_output_filename, read_pathbuf_to_mem, write_mem_to_pathbuf};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use rayon::prelude::*;
use serde_json::{Map, Value};
use std::fs::File;
use std::io::BufRead;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;

pub fn merge_parquet_jsonl(
    parquet_dir: &PathBuf,
    jsonl_dir: &PathBuf,
    output_dir: &PathBuf,
    id_field: &str,
    blob_id_field: Option<&str>,
) -> Result<(), Error> {
    let start_time = Instant::now();
    
    // Step 1: Find all parquet files in directory
    println!("Scanning parquet directory for .parquet files...");
    let all_files = expand_dirs(vec![parquet_dir.clone()], None)?;
    let parquet_files: Vec<PathBuf> = all_files
        .into_iter()
        .filter(|path| {
            path.extension()
                .and_then(|ext| ext.to_str())
                .map(|ext| ext == "parquet")
                .unwrap_or(false)
        })
        .collect();
    
    if parquet_files.is_empty() {
        return Err(anyhow!("No .parquet files found in directory: {}", parquet_dir.display()));
    }
    
    println!("Found {} parquet files", parquet_files.len());
    
    // Step 2: Build lookup table from parquet files
    println!("Building lookup table from parquet files...");
    let lookup_table = build_parquet_lookup(&parquet_files, id_field, blob_id_field)?;
    println!("Built lookup table with {} entries", lookup_table.len());
    
    // Step 3: Process JSONL files and merge
    println!("Processing JSONL files...");
    let all_files = expand_dirs(vec![jsonl_dir.clone()], None)?;
    let jsonl_files: Vec<PathBuf> = all_files
        .into_iter()
        .filter(|path| {
            path.extension()
                .and_then(|ext| ext.to_str())
                .map(|ext| ext.ends_with("zst") || ext.ends_with("zstd"))
                .unwrap_or(false)
        })
        .collect();
    let pbar = build_pbar(jsonl_files.len(), "Files");
    let processed_count = AtomicUsize::new(0);
    let merged_count = AtomicUsize::new(0);
    
    jsonl_files.par_iter().for_each(|jsonl_file| {
        match process_single_jsonl_file(
            jsonl_file,
            jsonl_dir,
            output_dir,
            &lookup_table,
            id_field,
            blob_id_field,
            &processed_count,
            &merged_count,
        ) {
            Ok(_) => {},
            Err(e) => eprintln!("Error processing {}: {}", jsonl_file.display(), e),
        }
        pbar.inc(1);
    });
    
    let final_processed = processed_count.load(Ordering::SeqCst);
    let final_merged = merged_count.load(Ordering::SeqCst);
    
    println!(
        "Completed in {:.2}s. Processed {} documents, merged {} documents ({:.1}%)",
        start_time.elapsed().as_secs_f64(),
        final_processed,
        final_merged,
        (final_merged as f64 / final_processed as f64) * 100.0
    );
    
    Ok(())
}

fn build_parquet_lookup(
    parquet_files: &[PathBuf],
    id_field: &str,
    blob_id_field: Option<&str>,
) -> Result<DashMap<String, Map<String, Value>>, Error> {
    let lookup_table: DashMap<String, Map<String, Value>> = DashMap::new();
    let pbar = build_pbar(parquet_files.len(), "Parquet files");
    
    parquet_files.par_iter().for_each(|parquet_file| {
        match process_single_parquet_file(parquet_file, id_field, blob_id_field, &lookup_table) {
            Ok(_) => {},
            Err(e) => eprintln!("Error processing parquet {}: {}", parquet_file.display(), e),
        }
        pbar.inc(1);
    });
    
    Ok(lookup_table)
}

fn process_single_parquet_file(
    parquet_file: &PathBuf,
    id_field: &str,
    blob_id_field: Option<&str>,
    lookup_table: &DashMap<String, Map<String, Value>>,
) -> Result<(), Error> {
    let file = File::open(parquet_file)?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
    let reader = builder.build()?;
    
    for batch_result in reader {
        let batch = batch_result?;
        process_record_batch(&batch, id_field, blob_id_field, lookup_table)?;
    }
    
    Ok(())
}

fn process_record_batch(
    batch: &RecordBatch,
    id_field: &str,
    blob_id_field: Option<&str>,
    lookup_table: &DashMap<String, Map<String, Value>>,
) -> Result<(), Error> {
    let schema = batch.schema();
    let num_rows = batch.num_rows();
    
    // Find the ID column
    let id_column_index = schema
        .fields()
        .iter()
        .position(|field| field.name() == id_field)
        .ok_or_else(|| anyhow!("ID field '{}' not found in parquet schema", id_field))?;
    
    // Find the blob_id column if specified
    let blob_id_column_index = if let Some(blob_field) = blob_id_field {
        schema
            .fields()
            .iter()
            .position(|field| field.name() == blob_field)
    } else {
        None
    };
    
    for row_idx in 0..num_rows {
        let mut record = Map::new();
        
        // Extract all fields from this row
        for (col_idx, field) in schema.fields().iter().enumerate() {
            let column = batch.column(col_idx);
            let field_name = field.name();
            
            if let Some(value) = extract_value_from_array(column.as_ref(), row_idx)? {
                record.insert(field_name.clone(), value);
            }
        }
        
        // Get the primary ID for lookup
        let id_array = batch.column(id_column_index);
        if let Some(id_value) = extract_string_from_array(id_array.as_ref(), row_idx)? {
            lookup_table.insert(id_value.clone(), record.clone());
            
            // Also insert by blob_id if different from id
            if let Some(blob_col_idx) = blob_id_column_index {
                let blob_id_array = batch.column(blob_col_idx);
                if let Some(blob_id_value) = extract_string_from_array(blob_id_array.as_ref(), row_idx)? {
                    if blob_id_value != id_value {
                        lookup_table.insert(blob_id_value, record);
                    }
                }
            }
        }
    }
    
    Ok(())
}

fn extract_value_from_array(array: &dyn Array, index: usize) -> Result<Option<Value>, Error> {
    if array.is_null(index) {
        return Ok(None);
    }
    
    use arrow::datatypes::DataType;
    match array.data_type() {
        DataType::Utf8 => {
            let string_array = array.as_any().downcast_ref::<StringArray>()
                .ok_or_else(|| anyhow!("Failed to downcast to StringArray"))?;
            Ok(Some(Value::String(string_array.value(index).to_string())))
        },
        DataType::Int64 => {
            let int_array = array.as_any().downcast_ref::<arrow::array::Int64Array>()
                .ok_or_else(|| anyhow!("Failed to downcast to Int64Array"))?;
            Ok(Some(Value::Number(serde_json::Number::from(int_array.value(index)))))
        },
        DataType::Int32 => {
            let int_array = array.as_any().downcast_ref::<arrow::array::Int32Array>()
                .ok_or_else(|| anyhow!("Failed to downcast to Int32Array"))?;
            Ok(Some(Value::Number(serde_json::Number::from(int_array.value(index)))))
        },
        DataType::Float64 => {
            let float_array = array.as_any().downcast_ref::<arrow::array::Float64Array>()
                .ok_or_else(|| anyhow!("Failed to downcast to Float64Array"))?;
            let float_val = float_array.value(index);
            if let Some(num) = serde_json::Number::from_f64(float_val) {
                Ok(Some(Value::Number(num)))
            } else {
                Ok(Some(Value::Null))
            }
        },
        DataType::Float32 => {
            let float_array = array.as_any().downcast_ref::<arrow::array::Float32Array>()
                .ok_or_else(|| anyhow!("Failed to downcast to Float32Array"))?;
            let float_val = float_array.value(index) as f64;
            if let Some(num) = serde_json::Number::from_f64(float_val) {
                Ok(Some(Value::Number(num)))
            } else {
                Ok(Some(Value::Null))
            }
        },
        DataType::Boolean => {
            let bool_array = array.as_any().downcast_ref::<arrow::array::BooleanArray>()
                .ok_or_else(|| anyhow!("Failed to downcast to BooleanArray"))?;
            Ok(Some(Value::Bool(bool_array.value(index))))
        },
        _ => {
            // For unsupported types, convert to string representation
            Ok(Some(Value::String(format!("{:?}", array))))
        }
    }
}

fn extract_string_from_array(array: &dyn Array, index: usize) -> Result<Option<String>, Error> {
    if array.is_null(index) {
        return Ok(None);
    }
    
    match array.data_type() {
        arrow::datatypes::DataType::Utf8 => {
            let string_array = array.as_any().downcast_ref::<StringArray>()
                .ok_or_else(|| anyhow!("Failed to downcast to StringArray"))?;
            Ok(Some(string_array.value(index).to_string()))
        },
        _ => {
            // Try to convert other types to string
            if let Some(value) = extract_value_from_array(array, index)? {
                match value {
                    Value::String(s) => Ok(Some(s)),
                    Value::Number(n) => Ok(Some(n.to_string())),
                    Value::Bool(b) => Ok(Some(b.to_string())),
                    _ => Ok(Some(value.to_string())),
                }
            } else {
                Ok(None)
            }
        }
    }
}

fn process_single_jsonl_file(
    jsonl_file: &PathBuf,
    input_dir: &PathBuf,
    output_dir: &PathBuf,
    lookup_table: &DashMap<String, Map<String, Value>>,
    id_field: &str,
    blob_id_field: Option<&str>,
    processed_count: &AtomicUsize,
    merged_count: &AtomicUsize,
) -> Result<(), Error> {
    let data = read_pathbuf_to_mem(jsonl_file)?;
    let lines: Vec<_> = data.lines().map(|line| line.unwrap()).collect();
    
    let mut output_records = Vec::new();
    
    for line in lines {
        processed_count.fetch_add(1, Ordering::SeqCst);
        
        let mut json_doc: Value = serde_json::from_str(&line)?;
        let mut was_merged = false;
        
        // Try to find matching record by id field
        if let Some(id_value) = json_doc.get(id_field).and_then(|v| v.as_str()) {
            if let Some(parquet_record) = lookup_table.get(id_value) {
                merge_records(&mut json_doc, &parquet_record)?;
                was_merged = true;
            }
        }
        
        // If not found and blob_id_field is specified, try blob_id
        if !was_merged {
            if let Some(blob_field) = blob_id_field {
                if let Some(blob_id_value) = json_doc.get(blob_field).and_then(|v| v.as_str()) {
                    if let Some(parquet_record) = lookup_table.get(blob_id_value) {
                        merge_records(&mut json_doc, &parquet_record)?;
                        was_merged = true;
                    }
                }
            }
        }
        
        if was_merged {
            merged_count.fetch_add(1, Ordering::SeqCst);
        }
        
        output_records.push(json_doc);
    }
    
    // Write output
    if !output_records.is_empty() {
        let output_file = get_output_filename(jsonl_file, input_dir, output_dir)?;
        write_output_jsonl(&output_records, &output_file)?;
    }
    
    Ok(())
}

fn merge_records(json_doc: &mut Value, parquet_record: &Map<String, Value>) -> Result<(), Error> {
    if let Some(json_obj) = json_doc.as_object_mut() {
        for (key, value) in parquet_record {
            // Only add fields that don't already exist in the JSON document
            // This preserves existing JSON fields and only adds new ones from parquet
            json_obj.entry(key.clone()).or_insert(value.clone());
        }
    }
    Ok(())
}

fn write_output_jsonl(records: &[Value], output_file: &PathBuf) -> Result<(), Error> {
    let mut output_bytes: Vec<u8> = Vec::new();
    
    for record in records {
        output_bytes.extend(serde_json::to_vec(record)?);
        output_bytes.push(b'\n');
    }
    
    write_mem_to_pathbuf(&output_bytes, output_file)
}