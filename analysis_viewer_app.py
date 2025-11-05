#!/usr/bin/env python3
"""
Desktop viewer for the Access analysis results.

The app lets you browse table summaries, inspect preview rows, and draw quick
visuals for categorical and numeric columns captured in analyze_access_db.py
outputs. It also detects simple lookup tables (ID + label) so foreign-key
columns render with human-readable names.
"""

from __future__ import annotations

import argparse
import json
import numbers
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Tuple

try:
    import pandas as pd
except ModuleNotFoundError:
    print(
        "pandas is required. Install it with 'pip install pandas'.",
        file=sys.stderr,
    )
    raise

try:
    import pyodbc
except ModuleNotFoundError:
    print(
        "pyodbc is required to connect to Access databases. "
        "Install it with 'pip install pyodbc'.",
        file=sys.stderr,
    )
    raise

try:
    import matplotlib
    matplotlib.use("TkAgg")
    from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
    from matplotlib.figure import Figure
except ModuleNotFoundError:
    print(
        "matplotlib is required for rendering charts. "
        "Install it with 'pip install matplotlib'.",
        file=sys.stderr,
    )
    raise

import tkinter as tk
from tkinter import ttk, messagebox, filedialog


@dataclass
class TableData:
    table_name: str
    sanitized_name: str
    summary_path: Optional[Path]
    preview_path: Optional[Path]
    summary: Dict[str, object]
    preview: Optional[pd.DataFrame]
    connection: Optional[pyodbc.Connection] = None


@dataclass
class LookupInfo:
    table: TableData
    mapping: Dict[str, str]


CAT_SORT_DISPLAY = [
    ("Count \u2193", "count_desc"),
    ("Count \u2191", "count_asc"),
    ("Label A\u2192Z", "label_asc"),
    ("Label Z\u2192A", "label_desc"),
    ("Original order", "original"),
]

CAT_ORIENTATION_OPTIONS = ["Horizontal", "Vertical"]

NUMERIC_STAT_DISPLAY = [
    ("min", "Minimum"),
    ("median", "Median"),
    ("mean", "Mean"),
    ("max", "Maximum"),
    ("count", "Count"),
    ("std", "Std Dev"),
]

DEFAULT_NUMERIC_STATS = {"min", "median", "mean", "max"}
NUMERIC_CHART_TYPES = ["Bar", "Line"]

def sanitize_identifier(name: str, fallback: Optional[str] = None) -> str:
    safe_chars: List[str] = []
    for char in str(name):
        if char.isalnum() or char in ("-", "_"):
            safe_chars.append(char)
        elif char.isspace():
            safe_chars.append("_")
        else:
            safe_chars.append("_")
    sanitized = "".join(safe_chars).strip("_")
    if sanitized:
        return sanitized
    if fallback is not None:
        return sanitize_identifier(fallback)
    return ""


def build_connection(db_path: Path) -> pyodbc.Connection:
    """Build a connection to the Access database."""
    # Convert to absolute path and ensure proper formatting
    abs_path = db_path.resolve()
    conn_str = (
        r"DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};"
        f"DBQ={abs_path};"
    )
    try:
        return pyodbc.connect(conn_str, timeout=30)
    except pyodbc.InterfaceError as exc:
        print(
            "Unable to connect to the Access database. "
            "Ensure the 'Microsoft Access Database Engine' is installed.",
            file=sys.stderr,
        )
        raise
    except pyodbc.Error:
        raise


def get_table_names(connection: pyodbc.Connection) -> List[str]:
    """Get all table names from the database."""
    cursor = connection.cursor()
    tables = []
    for row in cursor.tables(tableType="TABLE"):
        tables.append(row.table_name)
    cursor.close()
    return tables


def summarize_numeric_columns(df: pd.DataFrame) -> Dict[str, Dict[str, float]]:
    """Summarize numeric columns in a DataFrame."""
    summary: Dict[str, Dict[str, float]] = {}
    numeric_df = df.select_dtypes(include="number")
    for column in numeric_df.columns:
        series = numeric_df[column].dropna()
        if series.empty:
            continue
        summary[column] = {
            "count": int(series.count()),
            "mean": float(series.mean()),
            "min": float(series.min()),
            "max": float(series.max()),
            "std": float(series.std(ddof=0)),
            "median": float(series.median()),
        }
    return summary


def summarize_categorical_columns(
    df: pd.DataFrame, top_n: int = 5
) -> Dict[str, List[Dict[str, object]]]:
    """Summarize categorical columns in a DataFrame."""
    summary: Dict[str, List[Dict[str, object]]] = {}
    categorical_df = df.select_dtypes(exclude="number")
    for column in categorical_df.columns:
        value_counts = categorical_df[column].fillna("<<NULL>>").value_counts()
        top = value_counts.head(top_n)
        summary[column] = [
            {"value": str(idx), "count": int(count)} for idx, count in top.items()
        ]
    return summary


def analyze_table_live(
    connection: pyodbc.Connection,
    table_name: str,
    max_rows: int = 1000,
) -> Tuple[Dict[str, object], pd.DataFrame]:
    """Analyze a table directly from the database connection."""
    cursor = connection.cursor()
    count_query = f"SELECT COUNT(*) AS total_rows FROM [{table_name}]"
    total_rows = cursor.execute(count_query).fetchone()[0]
    cursor.close()

    if max_rows and max_rows > 0:
        select_query = f"SELECT TOP {max_rows} * FROM [{table_name}]"
    else:
        select_query = f"SELECT * FROM [{table_name}]"

    df = pd.read_sql(select_query, connection)

    summary: Dict[str, object] = {
        "table_name": table_name,
        "total_rows": int(total_rows),
        "sampled_rows": int(len(df)),
        "columns": {
            column: str(dtype) for column, dtype in df.dtypes.items()
        },
        "numeric_columns": summarize_numeric_columns(df),
        "categorical_columns": summarize_categorical_columns(df),
    }

    return summary, df


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Launch a desktop app for exploring Access database tables directly "
            "or from analysis_results generated by analyze_access_db.py."
        )
    )
    parser.add_argument(
        "--results-dir",
        type=Path,
        help="Directory containing *_summary.json and *_preview.csv files.",
    )
    parser.add_argument(
        "--database",
        type=Path,
        help="Path to Access database file (.accdb or .mdb) to connect directly.",
    )
    parser.add_argument(
        "--max-rows",
        type=int,
        default=1000,
        help="Maximum number of rows to load from each table when connecting directly to database.",
    )
    return parser.parse_args()


def load_summary_file(path: Path) -> Dict[str, object]:
    with path.open("r", encoding="utf-8") as fh:
        return json.load(fh)


def load_preview_file(path: Path) -> pd.DataFrame:
    return pd.read_csv(path)


def load_analysis_results(results_dir: Path) -> Tuple[List[TableData], Optional[Dict[str, object]]]:
    tables: List[TableData] = []
    overall_summary: Optional[Dict[str, object]] = None

    if not results_dir.exists():
        raise FileNotFoundError(
            f"The results directory does not exist: {results_dir}"
        )

    for summary_file in sorted(results_dir.glob("*_summary.json")):
        if summary_file.name == "database_summary.json":
            overall_summary = load_summary_file(summary_file)
            continue

        table_summary = load_summary_file(summary_file)
        preview_file = summary_file.with_name(
            summary_file.name.replace("_summary.json", "_preview.csv")
        )
        preview_df: Optional[pd.DataFrame] = None
        if preview_file.exists():
            try:
                preview_df = load_preview_file(preview_file)
            except Exception as exc:
                print(f"Failed to load preview for {summary_file}: {exc}", file=sys.stderr)

        table_name = str(table_summary.get("table_name", summary_file.stem))
        sanitized_name = sanitize_identifier(table_name, fallback=summary_file.stem)
        preview_path = preview_file if preview_file.exists() else None

        tables.append(
            TableData(
                table_name=table_name,
                sanitized_name=sanitized_name,
                summary_path=summary_file,
                preview_path=preview_path,
                summary=table_summary,
                preview=preview_df,
                connection=None,
            )
        )

    return tables, overall_summary


def load_database_direct(db_path: Path, max_rows: int = 1000) -> Tuple[List[TableData], Optional[Dict[str, object]]]:
    """Load data directly from Access database."""
    tables: List[TableData] = []
    
    if not db_path.exists():
        raise FileNotFoundError(f"Database file not found: {db_path}")
    
    try:
        connection = build_connection(db_path)
    except pyodbc.Error as exc:
        raise ConnectionError(f"Failed to connect to database: {exc}")
    
    table_names = get_table_names(connection)
    if not table_names:
        connection.close()
        raise ValueError("No tables found in the Access database.")
    
    overall_summary = {
        "database": str(db_path),
        "tables": [],
    }
    
    for table_name in table_names:
        try:
            table_summary, preview_df = analyze_table_live(connection, table_name, max_rows)
        except Exception as exc:
            print(f"Failed to analyze table '{table_name}': {exc}", file=sys.stderr)
            continue
        
        sanitized_name = sanitize_identifier(table_name, fallback=table_name)
        
        tables.append(
            TableData(
                table_name=table_name,
                sanitized_name=sanitized_name,
                summary_path=None,
                preview_path=None,
                summary=table_summary,
                preview=preview_df,
                connection=connection,
            )
        )
        
        overall_summary["tables"].append(table_summary)
    
    return tables, overall_summary


class AnalysisApp(tk.Tk):
    def __init__(
        self,
        tables: List[TableData],
        overall_summary: Optional[Dict[str, object]],
        results_dir: Optional[Path] = None,
        database_path: Optional[Path] = None,
        max_rows: int = 1000,
    ) -> None:
        super().__init__()
        self.tables = tables
        self.overall_summary = overall_summary
        self.results_dir = results_dir
        self.database_path = database_path
        self.max_rows = max_rows
        self.current_table: Optional[TableData] = None
        self.lookup_cache: Dict[str, LookupInfo] = self._build_lookup_cache()
        self.cat_topn_var = tk.StringVar(value="10")
        self.cat_sort_var = tk.StringVar(value=CAT_SORT_DISPLAY[0][0])
        self.cat_orientation_var = tk.StringVar(value=CAT_ORIENTATION_OPTIONS[0])
        self.cat_title_var = tk.StringVar(value="")
        self.cat_xlabel_var = tk.StringVar(value="")
        self.cat_ylabel_var = tk.StringVar(value="")

        self.numeric_chart_type_var = tk.StringVar(value=NUMERIC_CHART_TYPES[0])
        self.numeric_title_var = tk.StringVar(value="")
        self.numeric_xlabel_var = tk.StringVar(value="")
        self.numeric_ylabel_var = tk.StringVar(value="")
        self.numeric_ylim_min_var = tk.StringVar(value="")
        self.numeric_ylim_max_var = tk.StringVar(value="")
        self.numeric_show_grid_var = tk.BooleanVar(value=True)
        self.numeric_stat_order = [key for key, _ in NUMERIC_STAT_DISPLAY]
        self.numeric_stat_vars = {
            stat: tk.BooleanVar(value=stat in DEFAULT_NUMERIC_STATS)
            for stat in self.numeric_stat_order
        }

        # Set window title based on mode
        if self.database_path:
            self.title(f"Access Analysis Viewer - Live Database: {self.database_path.name}")
        elif self.results_dir:
            self.title(f"Access Analysis Viewer - Results: {self.results_dir.name}")
        else:
            self.title("Access Analysis Viewer")
            
        self.geometry("1200x800")

        self._build_widgets()
        self._register_variable_traces()
        self._populate_table_list()

    def _build_widgets(self) -> None:
        self.columnconfigure(1, weight=1)
        self.rowconfigure(0, weight=1)

        sidebar = ttk.Frame(self, padding=(10, 10))
        sidebar.grid(row=0, column=0, sticky="nsew")
        sidebar.rowconfigure(1, weight=1)

        ttk.Label(sidebar, text="Tables", font=("Segoe UI", 12, "bold")).grid(
            row=0, column=0, sticky="w"
        )

        self.table_list = tk.Listbox(sidebar, exportselection=False)
        self.table_list.grid(row=1, column=0, sticky="nsew", pady=(6, 0))
        scrollbar = ttk.Scrollbar(sidebar, orient="vertical", command=self.table_list.yview)
        scrollbar.grid(row=1, column=1, sticky="ns")
        self.table_list.configure(yscrollcommand=scrollbar.set)
        self.table_list.bind("<<ListboxSelect>>", self._on_table_select)

        ttk.Button(
            sidebar,
            text="Reload Results",
            command=self._reload_results,
        ).grid(row=2, column=0, sticky="ew", pady=(10, 0))

        content = ttk.Notebook(self)
        content.grid(row=0, column=1, sticky="nsew")

        # Overview tab
        self.overview_tab = ttk.Frame(content, padding=10)
        content.add(self.overview_tab, text="Overview")
        self.overview_tab.columnconfigure(0, weight=1)
        self.overview_tab.rowconfigure(1, weight=1)

        self.overview_info = tk.Text(
            self.overview_tab,
            state="disabled",
            height=6,
            wrap="word",
            bg=self.cget("bg"),
            relief="flat",
        )
        self.overview_info.grid(row=0, column=0, sticky="nsew")

        self.columns_tree = ttk.Treeview(
            self.overview_tab,
            columns=("column", "dtype", "lookup"),
            show="headings",
            height=12,
        )
        self.columns_tree.heading("column", text="Column")
        self.columns_tree.heading("dtype", text="Detected Type")
        self.columns_tree.heading("lookup", text="Lookup Table")
        self.columns_tree.column("column", width=220)
        self.columns_tree.column("dtype", width=140)
        self.columns_tree.column("lookup", width=220)
        self.columns_tree.grid(row=1, column=0, sticky="nsew", pady=(10, 0))

        columns_scroll = ttk.Scrollbar(
            self.overview_tab, orient="vertical", command=self.columns_tree.yview
        )
        columns_scroll.grid(row=1, column=1, sticky="ns")
        self.columns_tree.configure(yscrollcommand=columns_scroll.set)

        # Categorical tab
        self.categorical_tab = ttk.Frame(content, padding=10)
        content.add(self.categorical_tab, text="Categorical")
        self.categorical_tab.columnconfigure(0, weight=1)
        self.categorical_tab.rowconfigure(2, weight=1)

        cat_control = ttk.Frame(self.categorical_tab)
        cat_control.grid(row=0, column=0, sticky="ew")
        ttk.Label(cat_control, text="Column:").grid(row=0, column=0, padx=(0, 6))
        self.categorical_combo = ttk.Combobox(cat_control, state="readonly")
        self.categorical_combo.grid(row=0, column=1, sticky="ew")
        cat_control.columnconfigure(1, weight=1)
        self.categorical_combo.bind("<<ComboboxSelected>>", self._render_categorical_chart)
        self.cat_lookup_label = ttk.Label(cat_control, text="", foreground="#5a5a5a")
        self.cat_lookup_label.grid(row=1, column=0, columnspan=2, sticky="w", pady=(4, 0))

        cat_settings = ttk.Labelframe(self.categorical_tab, text="Chart Settings")
        cat_settings.grid(row=1, column=0, sticky="ew", pady=(8, 0))
        for col in range(6):
            cat_settings.columnconfigure(col, weight=1 if col in (1, 3, 4, 5) else 0)

        ttk.Label(cat_settings, text="Top N:").grid(row=0, column=0, padx=(0, 4), pady=4, sticky="w")
        self.cat_topn_spin = ttk.Spinbox(
            cat_settings,
            from_=1,
            to=100,
            width=6,
            textvariable=self.cat_topn_var,
        )
        self.cat_topn_spin.grid(row=0, column=1, padx=(0, 12), pady=4, sticky="w")

        ttk.Label(cat_settings, text="Sort by:").grid(row=0, column=2, padx=(0, 4), pady=4, sticky="w")
        sort_labels = [label for label, _ in CAT_SORT_DISPLAY]
        self.cat_sort_combo = ttk.Combobox(
            cat_settings,
            state="readonly",
            values=sort_labels,
            textvariable=self.cat_sort_var,
        )
        self.cat_sort_combo.grid(row=0, column=3, padx=(0, 12), pady=4, sticky="w")
        self.cat_sort_combo.bind("<<ComboboxSelected>>", self._render_categorical_chart)

        ttk.Label(cat_settings, text="Orientation:").grid(row=0, column=4, padx=(0, 4), pady=4, sticky="w")
        self.cat_orientation_combo = ttk.Combobox(
            cat_settings,
            state="readonly",
            values=CAT_ORIENTATION_OPTIONS,
            textvariable=self.cat_orientation_var,
        )
        self.cat_orientation_combo.grid(row=0, column=5, padx=(0, 12), pady=4, sticky="w")
        self.cat_orientation_combo.bind("<<ComboboxSelected>>", self._render_categorical_chart)

        ttk.Label(cat_settings, text="Title:").grid(row=1, column=0, padx=(0, 4), pady=4, sticky="w")
        self.cat_title_entry = ttk.Entry(cat_settings, textvariable=self.cat_title_var)
        self.cat_title_entry.grid(row=1, column=1, columnspan=2, padx=(0, 12), pady=4, sticky="ew")

        ttk.Label(cat_settings, text="X label:").grid(row=1, column=3, padx=(0, 4), pady=4, sticky="w")
        self.cat_xlabel_entry = ttk.Entry(cat_settings, textvariable=self.cat_xlabel_var)
        self.cat_xlabel_entry.grid(row=1, column=4, columnspan=2, padx=(0, 12), pady=4, sticky="ew")

        ttk.Label(cat_settings, text="Y label:").grid(row=2, column=0, padx=(0, 4), pady=4, sticky="w")
        self.cat_ylabel_entry = ttk.Entry(cat_settings, textvariable=self.cat_ylabel_var)
        self.cat_ylabel_entry.grid(row=2, column=1, columnspan=2, padx=(0, 12), pady=4, sticky="ew")

        ttk.Button(
            cat_settings,
            text="Reset chart settings",
            command=self._reset_categorical_settings,
        ).grid(row=2, column=3, columnspan=3, padx=(0, 12), pady=4, sticky="e")

        self.cat_fig = Figure(figsize=(6, 4), dpi=100)
        self.cat_ax = self.cat_fig.add_subplot(111)
        self.cat_canvas = FigureCanvasTkAgg(self.cat_fig, master=self.categorical_tab)
        self.cat_canvas.get_tk_widget().grid(row=2, column=0, sticky="nsew", pady=(10, 0))

        # Numeric tab
        self.numeric_tab = ttk.Frame(content, padding=10)
        content.add(self.numeric_tab, text="Numeric")
        self.numeric_tab.columnconfigure(0, weight=1)
        self.numeric_tab.rowconfigure(2, weight=1)

        num_control = ttk.Frame(self.numeric_tab)
        num_control.grid(row=0, column=0, sticky="ew")
        ttk.Label(num_control, text="Column:").grid(row=0, column=0, padx=(0, 6))
        self.numeric_combo = ttk.Combobox(num_control, state="readonly")
        self.numeric_combo.grid(row=0, column=1, sticky="ew")
        num_control.columnconfigure(1, weight=1)
        self.numeric_combo.bind("<<ComboboxSelected>>", self._render_numeric_chart)

        num_settings = ttk.Labelframe(self.numeric_tab, text="Chart Settings")
        num_settings.grid(row=1, column=0, sticky="ew", pady=(8, 0))
        for col in range(6):
            num_settings.columnconfigure(col, weight=1 if col in (1, 3, 4, 5) else 0)

        ttk.Label(num_settings, text="Chart type:").grid(row=0, column=0, padx=(0, 4), pady=4, sticky="w")
        self.numeric_chart_type_combo = ttk.Combobox(
            num_settings,
            state="readonly",
            values=NUMERIC_CHART_TYPES,
            textvariable=self.numeric_chart_type_var,
        )
        self.numeric_chart_type_combo.grid(row=0, column=1, padx=(0, 12), pady=4, sticky="w")
        self.numeric_chart_type_combo.bind("<<ComboboxSelected>>", self._render_numeric_chart)

        ttk.Label(num_settings, text="Stats:").grid(row=0, column=2, padx=(0, 4), pady=4, sticky="w")
        stats_frame = ttk.Frame(num_settings)
        stats_frame.grid(row=0, column=3, padx=(0, 12), pady=4, sticky="w")
        for idx, (stat, label) in enumerate(NUMERIC_STAT_DISPLAY):
            chk = ttk.Checkbutton(
                stats_frame,
                text=label,
                variable=self.numeric_stat_vars[stat],
                command=self._render_numeric_chart,
            )
            chk.grid(row=0, column=idx, padx=(0, 6), sticky="w")

        self.numeric_show_grid_check = ttk.Checkbutton(
            num_settings,
            text="Show grid",
            variable=self.numeric_show_grid_var,
            command=self._render_numeric_chart,
        )
        self.numeric_show_grid_check.grid(row=0, column=4, padx=(0, 12), pady=4, sticky="w")

        ttk.Label(num_settings, text="Title:").grid(row=1, column=0, padx=(0, 4), pady=4, sticky="w")
        self.numeric_title_entry = ttk.Entry(num_settings, textvariable=self.numeric_title_var)
        self.numeric_title_entry.grid(row=1, column=1, columnspan=2, padx=(0, 12), pady=4, sticky="ew")

        ttk.Label(num_settings, text="X label:").grid(row=1, column=3, padx=(0, 4), pady=4, sticky="w")
        self.numeric_xlabel_entry = ttk.Entry(num_settings, textvariable=self.numeric_xlabel_var)
        self.numeric_xlabel_entry.grid(row=1, column=4, columnspan=2, padx=(0, 12), pady=4, sticky="ew")

        ttk.Label(num_settings, text="Y min:").grid(row=2, column=0, padx=(0, 4), pady=4, sticky="w")
        self.numeric_ymin_entry = ttk.Entry(num_settings, width=10, textvariable=self.numeric_ylim_min_var)
        self.numeric_ymin_entry.grid(row=2, column=1, padx=(0, 12), pady=4, sticky="w")

        ttk.Label(num_settings, text="Y max:").grid(row=2, column=2, padx=(0, 4), pady=4, sticky="w")
        self.numeric_ymax_entry = ttk.Entry(num_settings, width=10, textvariable=self.numeric_ylim_max_var)
        self.numeric_ymax_entry.grid(row=2, column=3, padx=(0, 12), pady=4, sticky="w")

        ttk.Label(num_settings, text="Y label:").grid(row=2, column=4, padx=(0, 4), pady=4, sticky="w")
        self.numeric_ylabel_entry = ttk.Entry(num_settings, textvariable=self.numeric_ylabel_var)
        self.numeric_ylabel_entry.grid(row=2, column=5, padx=(0, 12), pady=4, sticky="ew")

        ttk.Button(
            num_settings,
            text="Reset chart settings",
            command=self._reset_numeric_settings,
        ).grid(row=3, column=0, columnspan=6, padx=(0, 12), pady=4, sticky="e")

        self.num_fig = Figure(figsize=(6, 4), dpi=100)
        self.num_ax = self.num_fig.add_subplot(111)
        self.num_canvas = FigureCanvasTkAgg(self.num_fig, master=self.numeric_tab)
        self.num_canvas.get_tk_widget().grid(row=2, column=0, sticky="nsew", pady=(10, 0))

        # Preview tab
        self.preview_tab = ttk.Frame(content, padding=10)
        content.add(self.preview_tab, text="Preview")
        self.preview_tab.columnconfigure(0, weight=1)
        self.preview_tab.rowconfigure(0, weight=1)

        self.preview_tree = ttk.Treeview(self.preview_tab, show="headings")
        self.preview_tree.grid(row=0, column=0, sticky="nsew")
        preview_scroll_y = ttk.Scrollbar(
            self.preview_tab, orient="vertical", command=self.preview_tree.yview
        )
        preview_scroll_y.grid(row=0, column=1, sticky="ns")
        preview_scroll_x = ttk.Scrollbar(
            self.preview_tab, orient="horizontal", command=self.preview_tree.xview
        )
        preview_scroll_x.grid(row=1, column=0, sticky="ew")
        self.preview_tree.configure(
            yscrollcommand=preview_scroll_y.set,
            xscrollcommand=preview_scroll_x.set,
        )

    def _register_variable_traces(self) -> None:
        cat_vars = (
            self.cat_topn_var,
            self.cat_sort_var,
            self.cat_orientation_var,
            self.cat_title_var,
            self.cat_xlabel_var,
            self.cat_ylabel_var,
        )
        for var in cat_vars:
            var.trace_add("write", lambda *_: self._on_cat_params_change())

        numeric_vars = (
            self.numeric_chart_type_var,
            self.numeric_title_var,
            self.numeric_xlabel_var,
            self.numeric_ylabel_var,
            self.numeric_ylim_min_var,
            self.numeric_ylim_max_var,
        )
        for var in numeric_vars:
            var.trace_add("write", lambda *_: self._on_numeric_params_change())

    def _on_cat_params_change(self) -> None:
        if self.current_table is None:
            return
        if not self.categorical_combo.get():
            return
        self._render_categorical_chart()

    def _on_numeric_params_change(self) -> None:
        if self.current_table is None:
            return
        if not self.numeric_combo.get():
            return
        self._render_numeric_chart()

    def _reset_categorical_settings(self) -> None:
        self.cat_topn_var.set("10")
        self.cat_sort_var.set(CAT_SORT_DISPLAY[0][0])
        self.cat_orientation_var.set(CAT_ORIENTATION_OPTIONS[0])
        self.cat_title_var.set("")
        self.cat_xlabel_var.set("")
        self.cat_ylabel_var.set("")
        if self.current_table:
            self._render_categorical_chart()

    def _reset_numeric_settings(self) -> None:
        self.numeric_chart_type_var.set(NUMERIC_CHART_TYPES[0])
        for stat, var in self.numeric_stat_vars.items():
            var.set(stat in DEFAULT_NUMERIC_STATS)
        self.numeric_show_grid_var.set(True)
        self.numeric_title_var.set("")
        self.numeric_xlabel_var.set("")
        self.numeric_ylabel_var.set("")
        self.numeric_ylim_min_var.set("")
        self.numeric_ylim_max_var.set("")
        if self.current_table:
            self._render_numeric_chart()

    def _build_lookup_cache(self) -> Dict[str, LookupInfo]:
        cache: Dict[str, LookupInfo] = {}
        for table in self.tables:
            mapping = self._extract_lookup_map(table)
            if mapping:
                cache[table.sanitized_name] = LookupInfo(table=table, mapping=mapping)
        return cache

    def _extract_lookup_map(self, table: TableData) -> Optional[Dict[str, str]]:
        if table.preview is None or table.preview.empty:
            return None
        if "ID" not in table.preview.columns:
            return None

        label_columns = [col for col in table.preview.columns if col != "ID"]
        if not label_columns:
            return None

        label_col = label_columns[0]
        mapping: Dict[str, str] = {}
        for _, row in table.preview.iterrows():
            key = row.get("ID")
            if pd.isna(key):
                continue
            if isinstance(key, numbers.Real) and float(key).is_integer():
                key_str = str(int(float(key)))
            else:
                key_str = str(key)
            key_str = key_str.strip()
            if not key_str:
                continue
            label_value = row.get(label_col)
            if pd.isna(label_value):
                continue
            mapping[key_str] = str(label_value).strip()

        return mapping or None

    def _lookup_for_column(
        self, column_name: str, current_table: Optional[TableData] = None
    ) -> Optional[LookupInfo]:
        sanitized = sanitize_identifier(column_name)
        lookup = self.lookup_cache.get(sanitized)
        if not lookup:
            return None
        if current_table is not None and lookup.table is current_table:
            return None
        return lookup

    def _lookup_label_for_value(
        self, mapping: Dict[str, str], raw_value: str
    ) -> Optional[str]:
        candidates = [raw_value, raw_value.strip()]
        try:
            numeric_value = float(raw_value)
            if numeric_value.is_integer():
                candidates.append(str(int(numeric_value)))
        except ValueError:
            pass
        for candidate in candidates:
            if candidate in mapping:
                return mapping[candidate]
            if candidate.rstrip("0").rstrip(".") in mapping:
                trimmed = candidate.rstrip("0").rstrip(".")
                if trimmed:
                    return mapping[trimmed]
        return None

    def _populate_table_list(self) -> None:
        self.table_list.delete(0, tk.END)
        for table in self.tables:
            self.table_list.insert(tk.END, table.table_name)
        if self.tables:
            self.table_list.selection_set(0)
            self._show_table(self.tables[0])

    def _get_fresh_numeric_stats(self, table: TableData, column: str) -> Optional[Dict[str, float]]:
        """Get fresh numeric statistics from database or cached data."""
        if table.connection and self.database_path:
            # Calculate fresh statistics directly from the database
            try:
                query = f"""
                SELECT 
                    COUNT([{column}]) as count,
                    MIN([{column}]) as min_val,
                    MAX([{column}]) as max_val,
                    AVG([{column}]) as mean_val,
                    STDEV([{column}]) as std_val
                FROM [{table.table_name}] 
                WHERE [{column}] IS NOT NULL
                """
                df = pd.read_sql(query, table.connection)
                
                if df.empty:
                    return None
                
                row = df.iloc[0]
                
                # For median, we need a separate query since Access doesn't have a MEDIAN function
                median_query = f"SELECT [{column}] FROM [{table.table_name}] WHERE [{column}] IS NOT NULL ORDER BY [{column}]"
                median_df = pd.read_sql(median_query, table.connection)
                median_val = median_df[column].median() if not median_df.empty else 0
                
                return {
                    "count": float(row['count']) if not pd.isna(row['count']) else 0,
                    "min": float(row['min_val']) if not pd.isna(row['min_val']) else 0,
                    "max": float(row['max_val']) if not pd.isna(row['max_val']) else 0,
                    "mean": float(row['mean_val']) if not pd.isna(row['mean_val']) else 0,
                    "std": float(row['std_val']) if not pd.isna(row['std_val']) else 0,
                    "median": float(median_val) if not pd.isna(median_val) else 0,
                }
                
            except Exception as exc:
                print(f"Failed to get fresh numeric stats for {column}: {exc}", file=sys.stderr)
                # Fall back to cached summary data
                num_summary = table.summary.get("numeric_columns", {})
                return num_summary.get(column) if hasattr(num_summary, 'get') else None
        else:
            # Use cached summary data
            num_summary = table.summary.get("numeric_columns", {})
            return num_summary.get(column) if hasattr(num_summary, 'get') else None

    def _get_fresh_categorical_data(self, table: TableData, column: str, top_n: int = 50) -> List[Dict[str, object]]:
        """Get fresh categorical data from database or cached data."""
        if table.connection and self.database_path:
            # Load fresh data from database for this specific column
            try:
                # Get fresh value counts directly from the database
                query = f"SELECT [{column}], COUNT(*) as cnt FROM [{table.table_name}] GROUP BY [{column}] ORDER BY COUNT(*) DESC"
                df = pd.read_sql(query, table.connection)
                
                result = []
                for _, row in df.iterrows():
                    value = row.iloc[0]  # First column (the grouped column)
                    count = row.iloc[1]  # Second column (the count)
                    
                    if pd.isna(value):
                        value = "<<NULL>>"
                    else:
                        value = str(value)
                    
                    result.append({"value": value, "count": int(count)})
                
                return result[:top_n]
                
            except Exception as exc:
                print(f"Failed to get fresh categorical data for {column}: {exc}", file=sys.stderr)
                # Fall back to cached summary data
                cat_summary = table.summary.get("categorical_columns", {})
                return cat_summary.get(column, [])
        else:
            # Use cached summary data
            cat_summary = table.summary.get("categorical_columns", {})
            return cat_summary.get(column, [])

    def _get_fresh_table_data(self, table: TableData) -> pd.DataFrame:
        """Get fresh data from database or return cached preview data."""
        if table.connection and self.database_path:
            # Load fresh data from database
            try:
                if self.max_rows and self.max_rows > 0:
                    select_query = f"SELECT TOP {self.max_rows} * FROM [{table.table_name}]"
                else:
                    select_query = f"SELECT * FROM [{table.table_name}]"
                return pd.read_sql(select_query, table.connection)
            except Exception as exc:
                print(f"Failed to get fresh data for {table.table_name}: {exc}", file=sys.stderr)
                # Fall back to cached preview if available
                return table.preview if table.preview is not None else pd.DataFrame()
        else:
            # Use cached preview data
            return table.preview if table.preview is not None else pd.DataFrame()

    def _reload_results(self) -> None:
        if self.database_path:
            # Database mode - refresh from database
            try:
                tables, overall = load_database_direct(self.database_path, self.max_rows)
            except Exception as exc:
                messagebox.showerror("Database reload failed", str(exc))
                return
        else:
            # Results directory mode
            if self.results_dir is None:
                self.results_dir = Path("analysis_results")
            selected_dir = filedialog.askdirectory(
                title="Select analysis_results folder",
                initialdir=self.results_dir,
            )
            if selected_dir:
                self.results_dir = Path(selected_dir).expanduser().resolve()
            try:
                tables, overall = load_analysis_results(self.results_dir)
            except Exception as exc:
                messagebox.showerror("Reload failed", str(exc))
                return

        self.tables = tables
        self.overall_summary = overall
        # Rebuild lookup cache with fresh data
        self.lookup_cache = self._build_lookup_cache()
        if not self.tables:
            messagebox.showwarning(
                "No results found",
                f"No tables found in {'database' if self.database_path else 'results directory'}",
            )
        self._populate_table_list()
        
        # If we have a current table selected, refresh its display with new data
        if self.current_table:
            # Find the corresponding table in the new data
            for table in self.tables:
                if table.table_name == self.current_table.table_name:
                    self._show_table(table)
                    break

    def _on_table_select(self, event: tk.Event) -> None:
        selection = event.widget.curselection()
        if not selection:
            return
        index = selection[0]
        if 0 <= index < len(self.tables):
            self._show_table(self.tables[index])

    def _show_table(self, table: TableData) -> None:
        self.current_table = table
        self._update_overview(table)
        self._prepare_categorical_options(table)
        self._prepare_numeric_options(table)
        self._update_preview(table)

    def _update_overview(self, table: TableData) -> None:
        summary = table.summary
        source_info = "Live database connection" if table.summary_path is None else f"Summary file: {table.summary_path.name}"
        info_lines = [
            f"Table: {table.table_name}",
            source_info,
            f"Total rows: {summary.get('total_rows', 'N/A')}",
            f"Sampled rows: {summary.get('sampled_rows', 'N/A')}",
        ]

        for item in self.columns_tree.get_children():
            self.columns_tree.delete(item)
        lookup_lines: List[str] = []
        columns_info = summary.get("columns", {})
        for column, dtype in columns_info.items():
            lookup = self._lookup_for_column(column, table)
            lookup_text = lookup.table.table_name if lookup else ""
            self.columns_tree.insert(
                "", tk.END, values=(column, dtype, lookup_text)
            )
            if lookup:
                lookup_lines.append(
                    f"{column} \u2192 {lookup.table.table_name}"
                )

        if lookup_lines:
            info_lines.append("")
            info_lines.append("Recognized lookups:")
            info_lines.extend(f"  {line}" for line in lookup_lines)

        self.overview_info.configure(state="normal")
        self.overview_info.delete("1.0", tk.END)
        self.overview_info.insert(tk.END, "\n".join(info_lines))
        self.overview_info.configure(state="disabled")

    def _prepare_categorical_options(self, table: TableData) -> None:
        cat_summary = table.summary.get("categorical_columns", {})
        columns = list(cat_summary.keys())
        self.categorical_combo["values"] = columns
        self.cat_lookup_label.configure(text="")

        if columns:
            self.categorical_combo.configure(state="readonly")
            self.categorical_combo.set(columns[0])
            self._render_categorical_chart()
        else:
            self.categorical_combo.set("")
            self.categorical_combo.configure(state="disabled")
            self.cat_ax.clear()
            self.cat_ax.text(
                0.5,
                0.5,
                "No categorical summaries available.",
                ha="center",
                va="center",
            )
            self.cat_canvas.draw_idle()
            self.cat_lookup_label.configure(text="")

    def _prepare_numeric_options(self, table: TableData) -> None:
        num_summary = table.summary.get("numeric_columns", {})
        columns = list(num_summary.keys())
        self.numeric_combo["values"] = columns

        if columns:
            self.numeric_combo.configure(state="readonly")
            self.numeric_combo.set(columns[0])
            self._render_numeric_chart()
        else:
            self.numeric_combo.set("")
            self.numeric_combo.configure(state="disabled")
            self.num_ax.clear()
            self.num_ax.text(
                0.5,
                0.5,
                "No numeric summaries available.",
                ha="center",
                va="center",
            )
            self.num_canvas.draw_idle()

    def _render_categorical_chart(self, event: Optional[tk.Event] = None) -> None:
        if not self.current_table:
            return
        column = self.categorical_combo.get()
        if not column:
            return
        
        # Get fresh categorical data (from database if available, otherwise from summary)
        try:
            top_n_setting = int(self.cat_topn_var.get())
        except (ValueError, tk.TclError):
            top_n_setting = 10
        if top_n_setting <= 0:
            top_n_setting = 10
            
        # Get much more data than we'll display to ensure we have complete data for sorting/filtering
        values = self._get_fresh_categorical_data(self.current_table, column, top_n=200)
        
        sanitized_column = sanitize_identifier(column)
        lookup_info = self._lookup_for_column(column, self.current_table)
        if lookup_info:
            self.cat_lookup_label.configure(
                text=f"Lookup: {column} \u2192 {lookup_info.table.table_name}"
            )
        else:
            self.cat_lookup_label.configure(text="")

        label_index: Dict[str, int] = {}
        aggregated_labels: List[str] = []
        aggregated_counts: List[int] = []
        for entry in values:
            raw_value_obj = entry.get("value")
            count_value = entry.get("count", 0)
            try:
                count = int(count_value)
            except (TypeError, ValueError):
                count = 0
            raw_value = "" if raw_value_obj is None else str(raw_value_obj)
            trimmed_value = raw_value.strip()
            if not trimmed_value or trimmed_value == "<<NULL>>":
                continue
            if sanitized_column == sanitize_identifier("From Location"):
                if trimmed_value in {"Assembly Number", "From Location"}:
                    continue

            display_label = trimmed_value
            if lookup_info:
                mapped = self._lookup_label_for_value(lookup_info.mapping, trimmed_value)
                if mapped:
                    display_label = f"{mapped} (ID {trimmed_value})"

            if display_label not in label_index:
                label_index[display_label] = len(aggregated_labels)
                aggregated_labels.append(display_label)
                aggregated_counts.append(count)
            else:
                idx = label_index[display_label]
                aggregated_counts[idx] += count

        data = list(zip(aggregated_labels, aggregated_counts))
        sort_selection = self.cat_sort_var.get()
        sort_code = next(
            (code for label, code in CAT_SORT_DISPLAY if label == sort_selection),
            "count_desc",
        )
        if sort_code == "count_desc":
            data.sort(key=lambda item: item[1], reverse=True)
        elif sort_code == "count_asc":
            data.sort(key=lambda item: item[1])
        elif sort_code == "label_asc":
            data.sort(key=lambda item: item[0].lower())
        elif sort_code == "label_desc":
            data.sort(key=lambda item: item[0].lower(), reverse=True)
        # "original" preserves insertion order

        # Apply the top_n limit after sorting
        data = data[:top_n_setting] if data else []

        labels = [label for label, _ in data]
        counts = [count for _, count in data]

        self.cat_ax.clear()
        if not labels:
            self.cat_ax.text(
                0.5,
                0.5,
                "No data for selected column.",
                ha="center",
                va="center",
            )
        else:
            orientation = self.cat_orientation_var.get().strip().lower()
            if orientation not in {"horizontal", "vertical"}:
                orientation = "horizontal"

            title = self.cat_title_var.get().strip() or f"Top values for {column}"
            if orientation == "horizontal":
                x_label = self.cat_xlabel_var.get().strip() or "Count"
                y_label = self.cat_ylabel_var.get().strip() or column
                positions = range(len(labels))
                self.cat_ax.barh(positions, counts, color="#4C72B0")
                self.cat_ax.set_yticks(list(positions))
                self.cat_ax.set_yticklabels(labels)
                self.cat_ax.invert_yaxis()
                self.cat_ax.set_xlabel(x_label)
                self.cat_ax.set_ylabel(y_label)
                self.cat_ax.margins(x=0.1, y=0.05)
                grid_axis = "x"
            else:
                x_label = self.cat_xlabel_var.get().strip() or column
                y_label = self.cat_ylabel_var.get().strip() or "Count"
                positions = range(len(labels))
                self.cat_ax.bar(positions, counts, color="#4C72B0")
                self.cat_ax.set_xticks(list(positions))
                self.cat_ax.set_xticklabels(labels, rotation=45, ha="right")
                self.cat_ax.set_xlabel(x_label)
                self.cat_ax.set_ylabel(y_label)
                self.cat_ax.margins(x=0.1, y=0.1)
                grid_axis = "y"

            self.cat_ax.set_title(title)
            self.cat_ax.grid(axis=grid_axis, linestyle="--", alpha=0.3)
            self.cat_fig.tight_layout()
        self.cat_canvas.draw_idle()

    def _render_numeric_chart(self, event: Optional[tk.Event] = None) -> None:
        if not self.current_table:
            return
        column = self.numeric_combo.get()
        if not column:
            return
        
        # Get fresh numeric statistics (from database if available, otherwise from summary)
        stats = self._get_fresh_numeric_stats(self.current_table, column)

        self.num_ax.clear()
        if not stats:
            self.num_ax.text(
                0.5,
                0.5,
                "No statistics for selected column.",
                ha="center",
                va="center",
            )
        else:
            selected_stats = [
                stat for stat in self.numeric_stat_order if self.numeric_stat_vars[stat].get()
            ]
            stat_labels = dict(NUMERIC_STAT_DISPLAY)
            data: List[Tuple[str, float]] = []
            for stat in selected_stats:
                if stat not in stats:
                    continue
                try:
                    value = float(stats[stat])
                except (TypeError, ValueError):
                    continue
                data.append((stat, value))

            if not data:
                self.num_ax.text(
                    0.5,
                    0.5,
                    "No statistics selected.",
                    ha="center",
                    va="center",
                )
                self.num_canvas.draw_idle()
                return

            stat_keys = [stat for stat, _ in data]
            values = [value for _, value in data]
            display_labels = [stat_labels.get(stat, stat.title()) for stat in stat_keys]

            chart_type = self.numeric_chart_type_var.get().strip().lower()
            if chart_type not in {"bar", "line"}:
                chart_type = "bar"

            if chart_type == "line":
                self.num_ax.plot(display_labels, values, marker="o", color="#55A868")
            else:
                self.num_ax.bar(display_labels, values, color="#55A868")

            xlabel = self.numeric_xlabel_var.get().strip() or "Statistic"
            ylabel = self.numeric_ylabel_var.get().strip() or column
            title = self.numeric_title_var.get().strip() or f"Distribution summary for {column}"

            self.num_ax.set_xlabel(xlabel)
            self.num_ax.set_ylabel(ylabel)
            self.num_ax.set_title(title)
            self.num_ax.margins(x=0.15, y=0.1)

            grid_enabled = bool(self.numeric_show_grid_var.get())
            self.num_ax.grid(grid_enabled, axis="y", linestyle="--", alpha=0.4)

            ymin_value: Optional[float] = None
            ymax_value: Optional[float] = None
            ymin_text = self.numeric_ylim_min_var.get().strip()
            ymax_text = self.numeric_ylim_max_var.get().strip()
            if ymin_text:
                try:
                    ymin_value = float(ymin_text)
                except ValueError:
                    ymin_value = None
            if ymax_text:
                try:
                    ymax_value = float(ymax_text)
                except ValueError:
                    ymax_value = None

            if ymin_value is not None or ymax_value is not None:
                self.num_ax.set_ylim(
                    bottom=ymin_value if ymin_value is not None else None,
                    top=ymax_value if ymax_value is not None else None,
                )
            self.num_fig.tight_layout()
        self.num_canvas.draw_idle()

    def _update_preview(self, table: TableData) -> None:
        for column in self.preview_tree["columns"]:
            self.preview_tree.heading(column, text="")
        self.preview_tree.delete(*self.preview_tree.get_children())

        df = self._get_fresh_table_data(table)
        if df is None or df.empty:
            self.preview_tree["columns"] = ("info",)
            self.preview_tree.heading("info", text="No preview data available.")
            return

        columns = list(df.columns)
        self.preview_tree["columns"] = columns
        for column in columns:
            self.preview_tree.heading(column, text=column)
            self.preview_tree.column(column, width=max(100, len(column) * 12))

        for _, row in df.iterrows():
            self.preview_tree.insert("", tk.END, values=[row.get(col, "") for col in columns])


def main() -> int:
    args = parse_args()
    
    # Determine if we should use database mode or results directory mode
    if args.database:
        # Direct database connection mode
        db_path = args.database.expanduser().resolve()
        try:
            tables, overall_summary = load_database_direct(db_path, args.max_rows)
        except Exception as exc:
            print(f"Failed to connect to database: {exc}", file=sys.stderr)
            return 1
        
        if not tables:
            print("No tables found in the Access database.", file=sys.stderr)
            return 1
        
        app = AnalysisApp(
            tables, 
            overall_summary, 
            results_dir=None, 
            database_path=db_path, 
            max_rows=args.max_rows
        )
    
    elif args.results_dir:
        # Results directory mode
        results_dir = args.results_dir.expanduser().resolve()
        try:
            tables, overall_summary = load_analysis_results(results_dir)
        except Exception as exc:
            print(f"Failed to load analysis results: {exc}", file=sys.stderr)
            return 1

        if not tables:
            print(
                f"No *_summary.json files found in {results_dir}. "
                "Run analyze_access_db.py first.",
                file=sys.stderr,
            )
            return 1

        app = AnalysisApp(
            tables, 
            overall_summary, 
            results_dir=results_dir,
            database_path=None,
            max_rows=args.max_rows
        )
    
    else:
        # Try to find database in current directory
        db_files = list(Path(".").glob("*.accdb")) + list(Path(".").glob("*.mdb"))
        if db_files:
            db_path = db_files[0]
            print(f"Found database file: {db_path}")
            try:
                tables, overall_summary = load_database_direct(db_path, args.max_rows)
            except Exception as exc:
                print(f"Failed to connect to database: {exc}", file=sys.stderr)
                return 1
            
            if not tables:
                print("No tables found in the Access database.", file=sys.stderr)
                return 1
            
            app = AnalysisApp(
                tables, 
                overall_summary, 
                results_dir=None, 
                database_path=db_path, 
                max_rows=args.max_rows
            )
        else:
            # Fall back to default results directory
            results_dir = Path("analysis_results")
            try:
                tables, overall_summary = load_analysis_results(results_dir)
            except Exception as exc:
                print(f"Failed to load analysis results: {exc}", file=sys.stderr)
                print("Try specifying --database or --results-dir", file=sys.stderr)
                return 1

            if not tables:
                print(
                    f"No *_summary.json files found in {results_dir}. "
                    "Run analyze_access_db.py first or specify --database.",
                    file=sys.stderr,
                )
                return 1

            app = AnalysisApp(
                tables, 
                overall_summary, 
                results_dir=results_dir,
                database_path=None,
                max_rows=args.max_rows
            )

    app.mainloop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
