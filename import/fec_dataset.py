# fec_dataset.py
import csv
from dataclasses import dataclass
from typing import ClassVar
from pathlib import Path
import io
import zipfile
import requests
from textwrap import dedent
from dotenv import load_dotenv; load_dotenv()

# Snowflake connector
import snowflake.connector


@dataclass
class FecDataset:
    """
    Represents a single FEC bulk dataset (header CSV + data ZIP).
    Creates a Snowflake table from the header and loads the data file.
    """
    name: str       # table name to create in Snowflake
    code: str       # short code used by FEC for header & zip naming
    data_code: str  # base name of the .txt inside the zip (may differ from code)
    fec_name: str   # Section on FEC website

    path: ClassVar[Path] = None
    conn: ClassVar[snowflake.connector.SnowflakeConnection] = None
    stage_name: ClassVar[str] = "FEC_STAGE"
    file_format_name: ClassVar[str] = "FEC_PIPE_FMT"

    @property
    def header_url(self) -> str:
        return f"https://www.fec.gov/files/bulk-downloads/data_dictionaries/{self.code}_header_file.csv"

    @property
    def header_file(self) -> Path:
        return self.path / f"{self.code}_header_file.csv"

    @property
    def zip_url(self) -> str:
        # adjust year as needed
        return f"https://www.fec.gov/files/bulk-downloads/2026/{self.code}26.zip"

    @property
    def data_file(self) -> Path:
        # extracted from the ZIP (pipe-delimited .txt)
        return self.path / f"{self.data_code}.txt"

    @classmethod
    def setup(cls, path: Path, conn: snowflake.connector.SnowflakeConnection):
        """
        Must be called before using any FecDataset instances.
        Creates a shared stage and a shared file format (pipe-delimited, no header).
        """
        cls.path = path
        cls.conn = conn

        cur = conn.cursor()
        try:
            # Stage to hold local uploads via PUT
            cur.execute(f"CREATE STAGE IF NOT EXISTS {cls.stage_name}")

            # File format for FEC txt files (pipe-delimited, no header)
            cur.execute(
                dedent(f"""
                CREATE FILE FORMAT IF NOT EXISTS {cls.file_format_name}
                TYPE = CSV
                FIELD_DELIMITER = '|'
                SKIP_HEADER = 0
                FIELD_OPTIONALLY_ENCLOSED_BY = '\"'
                NULL_IF = ('', 'NULL')
                EMPTY_FIELD_AS_NULL = TRUE
                TRIM_SPACE = TRUE
                """)
            )
        finally:
            cur.close()

    
    def _download_to_path(self, url: str, out_path: Path):
        resp = requests.get(url, timeout=120)
        resp.raise_for_status()
        out_path.write_bytes(resp.content)
        print(f"Downloaded: {url} → {out_path}")

    def _download_and_extract_zip(self, url: str, out_dir: Path):
        resp = requests.get(url, timeout=300)
        resp.raise_for_status()
        with zipfile.ZipFile(io.BytesIO(resp.content)) as zf:
            zf.extractall(out_dir)
        print(f"Downloaded and extracted: {url} → {out_dir}")

    def download(self):
        self._download_to_path(self.header_url, self.header_file)
        self._download_and_extract_zip(self.zip_url, self.path)
        if not self.data_file.exists():
            raise FileNotFoundError(f"Expected data file not found: {self.data_file}")

    
    def make_table(self):
        """
        1) Read the header CSV to get column names
        2) CREATE OR REPLACE TABLE (VARCHAR columns)
        3) PUT local .txt file to stage (auto_compress)
        4) COPY INTO table using the shared file format
        """
        # 1) columns from header file (first row)
        with self.header_file.open(newline='', encoding='utf-8') as f:
            reader = csv.reader(f)
            column_names = next(reader)
        if not column_names:
            raise ValueError(f"No header columns found in {self.header_file}")

        # 2) create table
        # Note: quote identifiers to preserve source header names
        col_defs = ', '.join(f'"{c}" VARCHAR' for c in column_names)
        create_sql = f'CREATE OR REPLACE TABLE "{self.name}" ({col_defs})'
        cur = self.conn.cursor()
        try:
            cur.execute(create_sql)

            # 3) PUT local file to stage (auto-compress → .gz)
            # Use forward slashes for file URI; Snowflake connector handles upload
            local_uri = f"file://{self.data_file.as_posix()}"
            cur.execute(
                f"PUT '{local_uri}' @{self.stage_name} AUTO_COMPRESS=TRUE OVERWRITE=TRUE"
            )

            # 4) COPY INTO table. Use pattern to match the just-uploaded file (.gz name Snowflake adds)
            copy_sql = f"""
                COPY INTO "{self.name}"
                FROM @{self.stage_name} 
                FILE_FORMAT = (FORMAT_NAME = {self.file_format_name})
                PATTERN = '.*{self.data_file.name}\\.gz$'
                ON_ERROR = 'CONTINUE';
            """
            cur.execute(copy_sql)

            print(f'Loaded data into "{self.name}"')
        finally:
            cur.close()