import os
import logging
from urllib.parse import urljoin

import click
import pandas as pd
from tqdm.auto import tqdm
from sqlalchemy import create_engine

# Column dtypes and dates preserved from original
dtype = {
    "VendorID": "Int64",
    "passenger_count": "Int64",
    "trip_distance": "float64",
    "RatecodeID": "Int64",
    "store_and_fwd_flag": "string",
    "PULocationID": "Int64",
    "DOLocationID": "Int64",
    "payment_type": "Int64",
    "fare_amount": "float64",
    "extra": "float64",
    "mta_tax": "float64",
    "tip_amount": "float64",
    "tolls_amount": "float64",
    "improvement_surcharge": "float64",
    "total_amount": "float64",
    "congestion_surcharge": "float64"
}

parse_dates = [
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime"
]


def ingest(source_url_or_path: str,
           db_url: str,
           table_name: str = "yellow_taxi_data",
           chunksize: int = 100000,
           create_table: bool = True):
    engine = create_engine(db_url)
    logging.info("Using DB: %s", db_url)

    df_iter = pd.read_csv(
        source_url_or_path,
        dtype=dtype,
        parse_dates=parse_dates,
        iterator=True,
        chunksize=chunksize
    )

    first = True
    total_inserted = 0

    for df_chunk in tqdm(df_iter, desc="ingesting"):
        if first and create_table:
            df_chunk.head(0).to_sql(
                name=table_name,
                con=engine,
                if_exists="replace",
                index=False
            )
            first = False
            logging.info("Created table schema: %s", table_name)
        elif first:
            first = False

        df_chunk.to_sql(
            name=table_name,
            con=engine,
            if_exists="append",
            index=False
        )
        inserted = len(df_chunk)
        total_inserted += inserted
        logging.info("Inserted %d rows (total %d)", inserted, total_inserted)

    logging.info("Finished ingestion. Total rows inserted: %d", total_inserted)
    return total_inserted


@click.command()
@click.option(
    "--db-url",
    envvar="NYC_TAXI_DB",
    default="postgresql://root:root@localhost:5432/ny_taxi",
    help="SQLAlchemy DB URL (ENV NYC_TAXI_DB)"
)
@click.option(
    "--prefix",
    envvar="NYC_TAXI_PREFIX",
    default="https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/",
    help="URL prefix or directory containing the file (ENV NYC_TAXI_PREFIX)"
)
@click.option(
    "--year",
    type=int,
    envvar="NYC_TAXI_YEAR",
    default=2021,
    help="Year for the tripdata file (e.g. 2021)"
)
@click.option(
    "--month",
    type=click.IntRange(1, 12),
    envvar="NYC_TAXI_MONTH",
    default=1,
    help="Month for the tripdata file (1-12)"
)
@click.option(
    "--file-name",
    default=None,
    help="Optional override filename (if provided, year/month are ignored)"
)
@click.option(
    "--table",
    default="yellow_taxi_data",
    help="Destination table name"
)
@click.option(
    "--chunksize",
    type=int,
    default=100000,
    help="Rows per chunk"
)
@click.option(
    "--no-create-table",
    is_flag=True,
    default=False,
    help="Don't replace/create table schema; only append"
)
@click.option(
    "--log-level",
    default="INFO",
    help="Logging level"
)
def main(db_url, prefix, year, month, file_name, table, chunksize, no_create_table, log_level):
    logging.basicConfig(level=getattr(logging, log_level.upper(), logging.INFO),
                        format="%(asctime)s %(levelname)s %(message)s")

    if file_name:
        filename = file_name
    else:
        filename = f"yellow_tripdata_{year}-{month:02d}.csv.gz"

    # Build source path/url
    if prefix.startswith("http://") or prefix.startswith("https://"):
        source = urljoin(prefix, filename)
    else:
        source = os.path.join(prefix, filename)

    logging.info("Source: %s", source)
    logging.info("Table: %s", table)
    logging.info("Chunksize: %d", chunksize)

    try:
        ingest(
            source_url_or_path=source,
            db_url=db_url,
            table_name=table,
            chunksize=chunksize,
            create_table=not no_create_table
        )
    except Exception:
        logging.exception("Ingestion failed")
        raise


if __name__ == "__main__":
    main()