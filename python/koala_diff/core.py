# koala_diff/core.py
#
# The Python wrapper that exposes the Rust binary.

import polars as pl
from typing import List, Dict, Any, Optional
from pathlib import Path

# This import assumes the package was built and installed
try:
    from ._internal import diff_files as _rust_diff_files
except ImportError:
    # Fallback for development/IDE linting without binary
    def _rust_diff_files(a, b, k):
        return {"error": "Rust extension not compiled"}

class DataDiff:
    """
    Main entry point for comparing datasets.
    """
    def __init__(self, key_columns: List[str]):
        self.key_columns = key_columns
        self.last_result = None
        self.file_a = None
        self.file_b = None

    def compare(self, file_a: str, file_b: str) -> Dict[str, Any]:
        """
        Compares two files and returns a dictionary of results.
        """
        self.file_a = str(file_a)
        self.file_b = str(file_b)
        
        # Validate files exist
        if not Path(self.file_a).exists():
            raise FileNotFoundError(f"File not found: {self.file_a}")
        if not Path(self.file_b).exists():
            raise FileNotFoundError(f"File not found: {self.file_b}")

        # Call Rust!
        print(f"🐨 Comparing {self.file_a} vs {self.file_b} using Rust engine...")
        result = _rust_diff_files(self.file_a, self.file_b, self.key_columns)
        self.last_result = result
        
        return result

    def get_mismatch_df(self) -> pl.DataFrame:
        """
        Returns a Polars DataFrame containing rows that exist in both files
        but have differing values in at least one column.
        """
        if not self.file_a or not self.file_b:
            raise ValueError("No comparison has been run yet.")
            
        # Scan files lazily
        def scan_df(path):
            if path.endswith(".parquet") or path.endswith(".pq"):
                return pl.scan_parquet(path)
            if path.endswith(".json"):
                # Standard JSON doesn't support lazy scanning in Polars yet
                return pl.read_json(path).lazy()
            if path.endswith(".jsonl") or path.endswith(".ndjson"):
                return pl.scan_ndjson(path)
            return pl.scan_csv(path)

        lf_a = scan_df(self.file_a)
        lf_b = scan_df(self.file_b)

        # Join on keys
        inner = lf_a.join(lf_b, on=self.key_columns, suffix="_right")
        
        # Build a filter mask for any column difference
        mask = None
        for col_name in lf_a.collect_schema().names():
            if col_name in self.key_columns:
                continue
            
            # Compare values and handle nulls
            # We use neq_missing equivalent: (a != b) | (a.is_null() != b.is_null())
            # In modern Polars, we can also use pl.col(col_name).eq_missing(pl.col(f"{col_name}_right")).not_()
            col_diff = pl.col(col_name).eq_missing(pl.col(f"{col_name}_right")).not_()
            
            if mask is None:
                mask = col_diff
            else:
                mask = mask | col_diff
        
        if mask is not None:
            # We collect using streaming to keep memory usage low
            return inner.filter(mask).collect(streaming=True)
        return pl.DataFrame(schema=inner.collect_schema())
