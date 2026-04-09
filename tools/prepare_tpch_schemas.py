#!/usr/bin/env python3
"""Prepare TPC-H schemas on PostgreSQL, CedarDB, and StarRocks for cross-engine validation."""
from __future__ import annotations
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent


def setup_postgresql():
    import psycopg
    ddl = (ROOT / "TPC-H" / "postgresql" / "ddl.sql").read_text()
    conn = psycopg.connect("host=127.0.0.1 port=5432 dbname=mydb user=myuser password=mypassword")
    conn.autocommit = True
    cur = conn.cursor()
    # Check if already exists
    cur.execute("SELECT count(*) FROM information_schema.tables WHERE table_schema='public' AND table_name='lineitem'")
    if cur.fetchone()[0] > 0:
        print("PostgreSQL TPC-H: tables already exist")
        conn.close()
        return
    for stmt in ddl.split(";"):
        stmt = stmt.strip()
        if stmt:
            try:
                cur.execute(stmt)
            except Exception as e:
                print(f"  PG DDL warning: {e}")
    print("PostgreSQL TPC-H: schema created")
    conn.close()


def setup_cedardb():
    import psycopg
    ddl = (ROOT / "TPC-H" / "cedardb" / "ddl.sql").read_text()
    conn = psycopg.connect("host=localhost port=5433 dbname=db user=admin password=admin")
    conn.autocommit = True
    cur = conn.cursor()
    # Check if already exists
    try:
        cur.execute("SELECT count(*) FROM lineitem LIMIT 0")
        print("CedarDB TPC-H: tables already exist")
        conn.close()
        return
    except Exception:
        conn.rollback()
    for stmt in ddl.split(";"):
        stmt = stmt.strip()
        if stmt:
            try:
                cur.execute(stmt)
            except Exception as e:
                print(f"  Cedar DDL warning: {e}")
    print("CedarDB TPC-H: schema created")
    conn.close()


def setup_starrocks():
    import pymysql
    ddl = (ROOT / "TPC-H" / "starrocks" / "ddl.sql").read_text()
    conn = pymysql.connect(host="127.0.0.1", port=9030, user="root", password="")
    cur = conn.cursor()
    # Create database if needed
    cur.execute("CREATE DATABASE IF NOT EXISTS tpch")
    cur.execute("USE tpch")
    # Check if already exists
    cur.execute("SHOW TABLES LIKE 'lineitem'")
    if cur.fetchone():
        print("StarRocks TPC-H: tables already exist")
        conn.close()
        return
    for stmt in ddl.split(";"):
        stmt = stmt.strip()
        if stmt:
            try:
                cur.execute(stmt)
            except Exception as e:
                print(f"  SR DDL warning: {e}")
    print("StarRocks TPC-H: schema created")
    conn.close()


def setup_cedardb_tpcds():
    """Also verify CedarDB TPC-DS tables exist."""
    import psycopg
    conn = psycopg.connect("host=localhost port=5433 dbname=db user=admin password=admin")
    conn.autocommit = True
    cur = conn.cursor()
    try:
        cur.execute("SELECT count(*) FROM store_sales LIMIT 0")
        print("CedarDB TPC-DS: tables already exist")
    except Exception:
        print("CedarDB TPC-DS: tables NOT found - need to run prepare")
    conn.close()


def setup_starrocks_tpcds():
    """Also verify StarRocks TPC-DS tables exist."""
    import pymysql
    conn = pymysql.connect(host="127.0.0.1", port=9030, user="root", password="")
    cur = conn.cursor()
    cur.execute("CREATE DATABASE IF NOT EXISTS tpcds")
    cur.execute("USE tpcds")
    cur.execute("SHOW TABLES LIKE 'store_sales'")
    if cur.fetchone():
        print("StarRocks TPC-DS: tables already exist")
    else:
        print("StarRocks TPC-DS: tables NOT found - need to run prepare")
    conn.close()


if __name__ == "__main__":
    print("Setting up TPC-H schemas...")
    setup_postgresql()
    setup_cedardb()
    setup_starrocks()
    print("\nVerifying TPC-DS schemas...")
    setup_cedardb_tpcds()
    setup_starrocks_tpcds()
    print("\nDone.")
