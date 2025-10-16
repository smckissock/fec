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

import snowflake.connector


@dataclass
class FecDataset:
    """
    Represents a single FEC bulk dataset (header CSV + data ZIP).
    Loads data into an existing Snowflake table.
    """
    name: str       # table name in Snowflake (must already exist)
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

    def zip_url(self, year: str) -> str:
        """Generate ZIP URL for a specific year (e.g., '00', '02', '04'....'26')"""
        return f"https://www.fec.gov/files/bulk-downloads/20{year}/{self.code}{year}.zip"

    def data_file(self, year: str) -> Path:
        """Path to extracted data file for a specific year"""
        # Rename the file to include the year to avoid overwriting
        return self.path / f"{self.data_code}{year}.txt"

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

    def _download_year(self, year: str):
        """Download data files for a specific year and rename to include year"""
        self._download_and_extract_zip(self.zip_url(year), self.path)
        
        # The extracted file has the base name (e.g., cn.txt)
        extracted_file = self.path / f"{self.data_code}.txt"
        if not extracted_file.exists():
            raise FileNotFoundError(f"Expected extracted file not found: {extracted_file}")
        
        # Rename to include the year (e.g., cn00.txt)
        target_file = self.data_file(year)
        extracted_file.rename(target_file)

    def _truncate_table(self):
        """Truncate the table before loading new data"""
        cur = self.conn.cursor()
        try:
            cur.execute(f'TRUNCATE TABLE IF EXISTS "{self.name}"')
            print(f'Truncated table "{self.name}"')
        finally:
            cur.close()

    def _get_column_count(self) -> int:
        """Get the number of columns from the header file"""
        with self.header_file.open(newline='', encoding='utf-8') as f:
            reader = csv.reader(f)
            column_names = next(reader)
        return len(column_names)
    
    def _load_data_year(self, year: str):
        """
        Loads data into an existing Snowflake table for a specific year.
        Assumes the table already exists with the correct schema.
        
        1) PUT local .txt file to stage (auto_compress)
        2) COPY INTO table using the shared file format with metadata columns
        """
        cur = self.conn.cursor()
        try:
            col_count = self._get_column_count()
            
            # 1) PUT local file to stage (auto-compress → .gz)
            # Use forward slashes for file URI; Snowflake connector handles upload
            data_file_path = self.data_file(year)
            local_uri = f"file://{data_file_path.as_posix()}"
            cur.execute(
                f"PUT '{local_uri}' @{self.stage_name} AUTO_COMPRESS=TRUE OVERWRITE=TRUE"
            )

            # 2) COPY INTO table with audit columns populated
            # Build column list: $1, $2, ..., $N for data columns
            data_cols = ', '.join(f'${i+1}' for i in range(col_count))
            
            copy_sql = f"""
                COPY INTO "{self.name}"
                FROM (
                    SELECT 
                        {data_cols},
                        CURRENT_TIMESTAMP() AS INGEST_TIMESTAMP,
                        METADATA$FILENAME AS SOURCE_FILE_NAME,
                        METADATA$FILE_ROW_NUMBER AS SOURCE_FILE_ROW_NUMBER
                    FROM @{self.stage_name}
                )
                FILE_FORMAT = (FORMAT_NAME = {self.file_format_name})
                PATTERN = '.*{data_file_path.name}\\.gz$'
                ON_ERROR = 'CONTINUE';
            """
            cur.execute(copy_sql)

            print(f'Loaded data from year 20{year} into "{self.name}"')
        finally:
            cur.close()

    def process(self, start_year: int = 2000, end_year: int = 2026):
        """
        Download and load all data from start_year to end_year (even years only).
        
        Args:
            start_year: Starting year (e.g., 2000)
            end_year: Ending year (e.g., 2026)
        """
        self._truncate_table()
        
        # Download header file once (same for all years)
        self._download_to_path(self.header_url, self.header_file)
        
        # Generate year strings (00, 02, 04, ..., 26)
        years = [f"{y % 100:02d}" for y in range(start_year, end_year + 1, 2)]
        
        for year in years:
            print(f"\nProcessing {self.name} for year 20{year}...")
            try:
                self._download_year(year)
                self._load_data_year(year)
            except Exception as e:
                print(f"Error processing {self.name} for year 20{year}: {e}")
                continue
        
        print(f"\nCompleted processing {self.name}")