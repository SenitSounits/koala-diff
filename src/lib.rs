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

    // 2.2 Perform the Join (Lazy)
    let joined_lf = lf_a.clone().join(
        lf_b.clone(),
        keys.clone(),
        keys.clone(),
        JoinArgs::new(JoinType::Inner).with_suffix(Some("_right".into())),
    );

    // 2.2 Pre-Calculation: Height and Uniqueness (Small passes)
    // We don't use streaming here because these are lightweight and streaming adds overhead for small files
    let get_meta = |lf: LazyFrame, name: &str, key: &str| -> PyResult<(usize, usize)> {
        let res = lf
            .select([len().alias("total"), col(key).n_unique().alias("unique")])
            .collect()
            .map_err(|e| {
                PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                    "Error reading {}: {}",
                    name, e
                ))
            })?;

        let total = res
            .column("total")
            .unwrap()
            .get(0)
            .unwrap()
            .try_extract::<u32>()
            .unwrap_or(0) as usize;
        let unique = res
            .column("unique")
            .unwrap()
            .get(0)
            .unwrap()
            .try_extract::<u32>()
            .unwrap_or(0) as usize;

        if unique < total && total > 0 {
            println!(
                "⚠️ WARNING: Join keys are not unique in {} ({} unique / {} total).",
                name, unique, total
            );
        }
        Ok((total, unique))
    };

    let (height_a, unique_a) = get_meta(lf_a.clone(), "File A", keys_strs[0])?;
    let (height_b, unique_b) = get_meta(lf_b.clone(), "File B", keys_strs[0])?;

    // 2.2.1 Join Safety Guard (Cartesian Product Estimation)
    // If keys are not unique, the worst case join size is (non-unique_a * non-unique_b)
    // We'll use a conservative heuristic: if either has duplicates, we check the ratio.
    if unique_a < height_a || unique_b < height_b {
        let dups_a = height_a - unique_a;
        let dups_b = height_b - unique_b;

        // Worst case: all duplicates match the same key
        // This is a simplified check to prevent the 10TB explosion.
        if dups_a > 1000 && dups_b > 1000 {
            let msg = format!(
                "❌ ABORTED: Potential Cartesian Product Explosion detected!\n\
                File A: {} duplicates, File B: {} duplicates.\n\
                This could result in a multi-terabyte memory allocation.\n\
                Please refine your 'key_columns' to be more unique.",
                dups_a, dups_b
            );
            return Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(msg));
        }
    }

    // 2.3 Core Statistics Calculation

    // 2.3.1 Build Statistics Query
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
            let is_diff_expr = col(name_str).eq_missing(col(&right_name)).not();
            aggs.push(
                is_diff_expr
                    .clone()
                    .cast(DataType::Float64)
                    .sum()
                    .alias(&format!("{}_diff_count", name_str)),
            );
            total_modified_mask = match total_modified_mask {
                Some(m) => Some(m.or(is_diff_expr)),
                None => Some(is_diff_expr),
            };
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

    // Run the main statistics pass (Streaming is only forced here for big data)
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

    // 2.4 Global Sample Pass (Fetch samples for ALL columns in one pass)
    let global_samples = if let Some(mask) = total_modified_mask {
        joined_lf
            .clone()
            .filter(mask)
            .limit(100) // Fetch up to 100 modified rows once
            .collect()
            .ok()
    } else {
        None
    };

    // 2.5 Assemble Stats Dictionary
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

                // Extract samples from biological sample buffer in memory
                if diff_count > 0 {
                    if let Some(samples) = &global_samples {
                        let right_name = format!("{}_right", name_str);
                        let sample_keys = pyo3::types::PyList::empty(py);
                        let sample_values = pyo3::types::PyList::empty(py);

                        let mut found_samples = 0;
                        for i in 0..samples.height() {
                            let val_a = samples.column(name_str).unwrap().get(i).unwrap();
                            let val_b = samples.column(&right_name).unwrap().get(i).unwrap();

                            // Only include if THIS specific column differs in this row
                            if val_a != val_b {
                                let mut key_map = String::new();
                                for k in &keys_strs {
                                    let val = samples.column(k).unwrap().get(i).unwrap();
                                    key_map.push_str(&format!("{}: {} ", k, val));
                                }
                                sample_keys.append(key_map.trim())?;
                                sample_values.append(format!("{} -> {}", val_a, val_b))?;
                                found_samples += 1;
                                if found_samples >= 5 {
                                    break;
                                }
                            }
                        }
                        stats.set_item("mismatched_sample_keys", sample_keys)?;
                        stats.set_item("mismatched_value_samples", sample_values)?;
                    }
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
