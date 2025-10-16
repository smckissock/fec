# main.py
import os
import shutil
from pathlib import Path

import snowflake.connector
from fec_dataset import FecDataset


def connect_snowflake():
    """
    Connection params come from environment variables to keep secrets out of code.
    Required:
      - SNOWFLAKE_USER
      - SNOWFLAKE_PASSWORD   (or use key pair / SSO if preferred)
      - SNOWFLAKE_ACCOUNT    (e.g., xy12345.us-east-1)
      - SNOWFLAKE_WAREHOUSE
      - SNOWFLAKE_DATABASE
      - SNOWFLAKE_RAW_SCHEMA
      - SNOWFLAKE_ROLE       (optional but recommended)
    """
    params = dict(
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
        database=os.environ["SNOWFLAKE_DATABASE"],
        schema=os.environ["SNOWFLAKE_RAW_SCHEMA"],
    )
    role = os.environ.get("SNOWFLAKE_ROLE")
    if role:
        params["role"] = role
    return snowflake.connector.connect(**params)


def main():
    workdir = Path("d:/fec")
    if workdir.exists():
        shutil.rmtree(workdir)
    workdir.mkdir(parents=True, exist_ok=True)

    conn = connect_snowflake()

    # Initialize shared stage + file format
    FecDataset.setup(workdir, conn)

    # Note: Tables must already exist in Snowflake with the correct schema
    file_types = [
        # FecDataset("CANDIDATE",               "cn",        "cn",     "Candidate master"),
        # FecDataset("CANDIDATE_COMMITTEE",     "ccl",       "ccl",    "Candidate-committee linkages"),
        # FecDataset("COMMITTEE",               "cm",        "cm",     "Committee master"),
        # FecDataset("COMMITTEE_CONTRIBUTION",  "pas2",      "itpas2", "Contributions from committees to candidates and independent expenditures"),
        FecDataset("INDIVIDUAL_CONTRIBUTION", "indiv",     "itcont", "Contributions by individuals"),
    ]

    # Process each dataset (downloads and loads all years 2000-2026)
    for ft in file_types:
        ft.process()

    conn.close()


if __name__ == "__main__":
    main()