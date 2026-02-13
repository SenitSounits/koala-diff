# koala_diff/core.py
#
# The Python wrapper that exposes the Rust binary.

import json
from typing import List, Dict, Any, Optional
from pathlib import Path

# This import assumes the package was built and installed
# When developing locally, you might need to build first (maturin develop)
try:
    from ._internal import diff_files as _rust_diff_files
except ImportError:
    # Fallback for development/IDE linting without binary
    def _rust_diff_files(a, b, k):
        return {"error": "Rust extension not compiled"}

class DataDiff:
    """
    Main class for comparing datasets.
    """
    def __init__(self, key_columns: List[str]):
        self.key_columns = key_columns

    def compare(self, file_a: str, file_b: str) -> Dict[str, Any]:
        """
        Runs the Rust engine to compare two files.
        Args:
            file_a: Path to first file (CSV/Parquet).
            file_b: Path to second file.
        Returns:
            Dict containing diff statistics.
        """
        # Validate files exist
        if not Path(file_a).exists():
            raise FileNotFoundError(f"File not found: {file_a}")
        if not Path(file_b).exists():
            raise FileNotFoundError(f"File not found: {file_b}")

        # Call Rust!
        # This will be blazing fast compared to loading Python objects.
        print(f"🐨 Comparing {file_a} vs {file_b} using Rust engine...")
        result = _rust_diff_files(str(file_a), str(file_b), self.key_columns)
        
        return result

    def generate_html_report(self, diff_result: Dict[str, Any], output_path: str = "diff_report.html"):
        """
        Generates a beautiful HTML report from the diff result.
        """
        from jinja2 import Template
        
        # Simple template for MVP
        template_str = """
        <!DOCTYPE html>
        <html>
        <head>
            <title>Koala Diff Report</title>
            <style>
                body { font-family: sans-serif; padding: 20px; background: #f4f4f9; }
                .card { background: white; padding: 20px; border-radius: 8px; box-shadow: 0 2px 5px rgba(0,0,0,0.1); margin-bottom: 20px; }
                h1 { color: #2c3e50; }
                .stat { font-size: 24px; font-weight: bold; color: #3498db; }
                .label { font-size: 14px; color: #7f8c8d; }
            </style>
        </head>
        <body>
            <div class="card">
                <h1>🐨 Koala Diff Report</h1>
                <p>Comparison Result</p>
            </div>
            
            <div class="card">
                <h3>Summary</h3>
                <div><span class="stat">{{ matched }}</span> <span class="label">Rows Matched</span></div>
                <div><span class="stat">{{ added }}</span> <span class="label">Rows Added (in New)</span></div>
                <div><span class="stat">{{ removed }}</span> <span class="label">Rows Removed (from Old)</span></div>
            </div>

            <div class="card">
                <h3>Modified Columns</h3>
                <ul>
                {% for col in modified_cols %}
                    <li>{{ col }}</li>
                {% endfor %}
                </ul>
            </div>
        </body>
        </html>
        """
        
        template = Template(template_str)
        html_content = template.render(**diff_result)
        
        with open(output_path, "w") as f:
            f.write(html_content)
            
        print(f"✅ Report generated: {output_path}")
