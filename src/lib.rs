// koala-diff/src/lib.rs
// The Rust core for fast data diffing

use polars::prelude::*;
use pyo3::prelude::*;
use pyo3::types::PyDict;
use pyo3::wrap_pyfunction;
// No longer needed

/// Compares two CSV or Parquet files and returns a difference summary
///
/// Args:
///     file_a (str): Path to first file
///     file_b (str): Path to second file
///     key_cols (list[str]): Columns to join on
///
/// Returns:
///     dict: {
///         "total_rows_a": int,
///         "total_rows_b": int,
///         "matched": int,
///         "added": int,
///         "removed": int,
///         "modified_cols": list[str],
///         "schema_diff": list[dict],  // New!
///         "null_counts": dict,        // New! { "col_name": [nulls_in_a, nulls_in_b] }
///     }
#[pyfunction]
fn diff_files<'py>(
    py: Python<'py>,
    file_a: String,
    file_b: String,
    _key_cols: Vec<String>,
) -> PyResult<Bound<'py, PyDict>> {
    // 1. Read files lazily using Polars
    let scan_df = |path: &str| -> PyResult<LazyFrame> {
        if path.ends_with(".parquet") || path.ends_with(".pq") {
            LazyFrame::scan_parquet(path.into(), Default::default())
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(e.to_string()))
        } else if path.ends_with(".jsonl") || path.ends_with(".ndjson") {
            LazyJsonLineReader::new(path.into())
                .finish()
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(e.to_string()))
        } else if path.ends_with(".json") {
            // Standard JSON doesn't have a native lazy scanner in Polars
            let df = JsonReader::new(
                std::fs::File::open(path)
                    .map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(e.to_string()))?,
            )
            .finish()
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(e.to_string()))?;
            Ok(df.lazy())
        } else {
            LazyCsvReader::new(path.into())
                .finish()
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(e.to_string()))
        }
    };

    let mut lf_a = scan_df(&file_a)?;
    let mut lf_b = scan_df(&file_b)?;

    // Get schemas for analysis
    let schema_a = lf_a
        .collect_schema()
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;
    let schema_b = lf_b
        .collect_schema()
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;

    // 2. Core Diffing Logic using Joins
    let keys: Vec<Expr> = _key_cols.iter().map(|s| col(s.as_str())).collect();
    let keys_strs: Vec<&str> = _key_cols.iter().map(|s| s.as_str()).collect();

    // 2.1 Matches and Modifications
    // Join A and B to find common rows and then compare columns
    let joined_lf = lf_a.clone().join(
        lf_b.clone(),
        keys.clone(),
        keys.clone(),
        JoinArgs::new(JoinType::Inner).with_suffix(Some("_right".into())),
    );

    // Height of A and B
    let height_a = lf_a
        .clone()
        .select([len().alias("len")])
        .with_new_streaming(true)
        .collect()
        .unwrap()
        .column("len")
        .unwrap()
        .get(0)
        .unwrap()
        .try_extract::<u32>()
        .unwrap_or(0) as usize;
    let height_b = lf_b
        .clone()
        .select([len().alias("len")])
        .with_new_streaming(true)
        .collect()
        .unwrap()
        .column("len")
        .unwrap()
        .get(0)
        .unwrap()
        .try_extract::<u32>()
        .unwrap_or(0) as usize;

    // 2.3 Build Large Aggregation for Single Pass
    let mut aggs = Vec::new();
    aggs.push(len().alias("_total_matched"));

    let mut total_modified_mask: Option<Expr> = None;

    for (col_name, dtype_a) in schema_a.iter() {
        let name_str = col_name.as_str();
        if keys_strs.contains(&name_str) {
            continue;
        }

        if schema_b.contains(name_str) {
            let right_name = format!("{}_right", name_str);
            let dtype_b = schema_b.get(name_str).unwrap();

            // Diff count
            let is_diff_expr = col(name_str).eq_missing(col(&right_name)).not();
            aggs.push(
                is_diff_expr
                    .clone()
                    .cast(DataType::Float64)
                    .sum()
                    .alias(&format!("{}_diff_count", name_str)),
            );

            // Track total modified rows
            total_modified_mask = match total_modified_mask {
                Some(m) => Some(m.or(is_diff_expr)),
                None => Some(is_diff_expr),
            };

            // Null counts
            aggs.push(
                col(name_str)
                    .is_null()
                    .cast(DataType::Int32)
                    .sum()
                    .alias(&format!("{}_null_a", name_str)),
            );
            aggs.push(
                col(&right_name)
                    .is_null()
                    .cast(DataType::Int32)
                    .sum()
                    .alias(&format!("{}_null_b", name_str)),
            );

            // Max Diff (numeric)
            if dtype_a.is_numeric() && dtype_b.is_numeric() {
                let diff_expr = col(name_str).cast(DataType::Float64)
                    - col(&right_name).cast(DataType::Float64);
                let abs_diff = when(diff_expr.clone().gt(0.0))
                    .then(diff_expr.clone())
                    .otherwise(diff_expr * lit(-1.0));
                aggs.push(abs_diff.max().alias(&format!("{}_max_diff", name_str)));
            }
        }
    }

    if let Some(mask) = &total_modified_mask {
        aggs.push(
            mask.clone()
                .cast(DataType::Float64)
                .sum()
                .alias("_total_modified"),
        );
    }

    // Run the massive aggregation pass
    let stats_res = joined_lf
        .clone()
        .select(aggs)
        .with_new_streaming(true)
        .collect()
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;

    let matched = stats_res
        .column("_total_matched")
        .unwrap()
        .get(0)
        .unwrap()
        .try_extract::<f64>()
        .unwrap_or(0.0) as usize;
    let modified_rows_count = if total_modified_mask.is_some() {
        stats_res
            .column("_total_modified")
            .unwrap()
            .get(0)
            .unwrap()
            .try_extract::<f64>()
            .unwrap_or(0.0) as usize
    } else {
        0
    };

    let removed = height_a.saturating_sub(matched);
    let added = height_b.saturating_sub(matched);
    let identical_rows_count = matched.saturating_sub(modified_rows_count);

    // 2.4 Assemble Stats Dictionary
    let column_stats = PyDict::new(py);
    for (col_name, dtype_a) in schema_a.iter() {
        let name_str = col_name.as_str();
        let is_key = keys_strs.contains(&name_str);

        let stats = PyDict::new(py);
        stats.set_item("column_name", name_str)?;
        stats.set_item("is_key", is_key)?;
        stats.set_item("source_dtype", format!("{:?}", dtype_a))?;

        if let Some(dtype_b) = schema_b.get(name_str) {
            stats.set_item("target_dtype", format!("{:?}", dtype_b))?;
            stats.set_item("total_count", matched)?;

            if is_key {
                stats.set_item("match_count", matched)?;
                stats.set_item("non_match_count", 0)?;
                stats.set_item("match_rate", 100.0)?;
                stats.set_item("all_match", true)?;
            } else {
                let diff_count = stats_res
                    .column(&format!("{}_diff_count", name_str))
                    .unwrap()
                    .get(0)
                    .unwrap()
                    .try_extract::<f64>()
                    .unwrap_or(0.0) as usize;
                let match_count = matched.saturating_sub(diff_count);
                let match_rate = if matched > 0 {
                    (match_count as f64 / matched as f64) * 100.0
                } else {
                    100.0
                };

                stats.set_item("match_count", match_count)?;
                stats.set_item("non_match_count", diff_count)?;
                stats.set_item("match_rate", match_rate)?;
                stats.set_item("all_match", diff_count == 0)?;

                if dtype_a.is_numeric() && dtype_b.is_numeric() {
                    if let Ok(col) = stats_res.column(&format!("{}_max_diff", name_str)) {
                        let max_v = col.get(0).unwrap().try_extract::<f64>().unwrap_or(0.0);
                        stats.set_item("max_value_diff", max_v)?;
                    }
                }

                let n_a = stats_res
                    .column(&format!("{}_null_a", name_str))
                    .unwrap()
                    .get(0)
                    .unwrap()
                    .try_extract::<i32>()
                    .unwrap_or(0);
                let n_b = stats_res
                    .column(&format!("{}_null_b", name_str))
                    .unwrap()
                    .get(0)
                    .unwrap()
                    .try_extract::<i32>()
                    .unwrap_or(0);
                stats.set_item("null_count_diff", (n_b - n_a) as i64)?;

                // Samples (separate small pass)
                if diff_count > 0 {
                    let right_name = format!("{}_right", name_str);
                    let is_diff_expr = col(name_str).eq_missing(col(&right_name)).not();
                    let sample_head = joined_lf
                        .clone()
                        .filter(is_diff_expr)
                        .limit(5)
                        .collect()
                        .map_err(|e| {
                            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string())
                        })?;

                    let sample_keys = pyo3::types::PyList::empty(py);
                    let sample_values = pyo3::types::PyList::empty(py);
                    for i in 0..sample_head.height() {
                        let mut key_map = String::new();
                        for k in &keys_strs {
                            let val = sample_head.column(k).unwrap().get(i).unwrap();
                            key_map.push_str(&format!("{}: {} ", k, val));
                        }
                        sample_keys.append(key_map.trim())?;
                        let val_a = sample_head.column(name_str).unwrap().get(i).unwrap();
                        let val_b = sample_head.column(&right_name).unwrap().get(i).unwrap();
                        sample_values.append(format!("{} -> {}", val_a, val_b))?;
                    }
                    stats.set_item("mismatched_sample_keys", sample_keys)?;
                    stats.set_item("mismatched_value_samples", sample_values)?;
                }
            }
        } else {
            stats.set_item("target_dtype", "MISSING")?;
            stats.set_item("all_match", false)?;
        }
        column_stats.set_item(name_str, stats)?;
    }

    // --- Final Assembly ---
    let dict = pyo3::types::PyDict::new(py);
    dict.set_item("total_rows_a", height_a)?;
    dict.set_item("total_rows_b", height_b)?;
    dict.set_item("joined_count", matched)?;
    dict.set_item("identical_rows_count", identical_rows_count)?;
    dict.set_item("modified_rows_count", modified_rows_count)?;
    dict.set_item("added", added)?;
    dict.set_item("removed", removed)?;
    dict.set_item("column_stats", column_stats)?;

    Ok(dict)
}

/// A Python module implemented in Rust.
#[pymodule]
fn _internal(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(diff_files, m)?)?;
    Ok(())
}
