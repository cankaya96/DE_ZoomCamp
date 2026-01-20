#!/usr/bin/env python
# coding: utf-8

import duckdb
import click

def ingest_data(year, month, pg_conn, parquet_url, zones_csv_url):

    trips_table = f"green_tripdata_{year:04d}_{month:02d}"
    zones_table = "taxi_zone_lookup"

    con = duckdb.connect()

    con.execute("INSTALL postgres;")
    con.execute("LOAD postgres;")

    con.execute(f"ATTACH '{pg_conn}' AS pg (TYPE POSTGRES);")

    con.execute(f"DROP TABLE IF EXISTS pg.public.{trips_table};")
    con.execute(f"""
      CREATE TABLE pg.public.{trips_table} AS
      SELECT * FROM read_parquet('{parquet_url}');
    """)


    con.execute(f"DROP TABLE IF EXISTS pg.public.{zones_table};")
    con.execute(f"""
      CREATE TABLE pg.public.{zones_table} AS
      SELECT * FROM read_csv_auto('{zones_csv_url}', header=True);
    """)

    print("Done ✅ : ")
    print("-", trips_table)
    print("-", zones_table)

@click.command()
@click.option('--pg-user', default='root', help='PostgreSQL username')
@click.option('--pg-pass', default='root', help='PostgreSQL password')
@click.option('--pg-host', default='localhost', help='PostgreSQL host')
@click.option('--pg-port', default='5432', help='PostgreSQL port')
@click.option('--pg-db', default='ny_taxi', help='PostgreSQL database name')
@click.option('--year', default=2025, type=int, help='Year of the data')
@click.option('--month', default=11, type=int, help='Month of the data')

def main(pg_user, pg_pass, pg_host, pg_port, pg_db, year, month):

    parquet_url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_{year:04d}-{month:02d}.parquet"
    zones_csv_url = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/misc/taxi_zone_lookup.csv"

    pg_conn = f'postgresql://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}'

    ingest_data(year, month, pg_conn, parquet_url, zones_csv_url)



if __name__ == '__main__':
    main()


