#!/usr/bin/env python3
"""Check exclusive body-reader files and execution-time filesystem policy.

Run with a supported native DuckDB runtime and an extension binary path.
The POSIX fixture owns its planted file through exclusive creation.
"""

import argparse
import os
from pathlib import Path

import _duckdb
import duckdb


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("extension", type=Path)
    args = parser.parse_args()
    if os.name != "posix":
        parser.error("this fixture requires the POSIX /tmp reader path")

    extension = str(args.extension.resolve()).replace("'", "''")
    con = duckdb.connect(config={"allow_unsigned_extensions": "true"})
    planted = Path(f"/tmp/ducknng_body_{os.getpid()}_0000000000000000.tmp")
    pattern = f"ducknng_body_{os.getpid()}_*.tmp"
    owned = False
    query = """
        SELECT * FROM ducknng_parse_csv(
            encode('a,b' || chr(10) || '1,x'),
            schema := NULL::STRUCT(a BIGINT, b VARCHAR)
        )
    """
    try:
        version = con.execute("SELECT version()").fetchone()[0]
        print(f"duckdb {version} from {_duckdb.__file__}", flush=True)
        con.execute(f"LOAD '{extension}'")
        with planted.open("xb") as fixture:
            owned = True
            fixture.write(b"planted ownership fixture")

        try:
            con.execute(query).fetchall()
        except duckdb.Error as error:
            assert "exists" in str(error).lower(), str(error)
        else:
            raise AssertionError("reader accepted a pre-existing temporary file")
        assert planted.read_bytes() == b"planted ownership fixture"
        assert set(Path("/tmp").glob(pattern)) == {planted}
        planted.unlink()
        owned = False

        assert con.execute(query).fetchall() == [(1, "x")]
        assert not list(Path("/tmp").glob(pattern)), "reader left an owned file"

        con.execute("SET enable_external_access = false")
        try:
            con.execute(query).fetchall()
        except duckdb.Error as error:
            assert "disabled" in str(error).lower(), str(error)
        else:
            raise AssertionError("reader ignored disabled external access")
        assert not list(Path("/tmp").glob(pattern)), "denied reader created a file"
        print("body tempfile ownership: ok", flush=True)
    finally:
        con.close()
        if owned:
            planted.unlink(missing_ok=True)


if __name__ == "__main__":
    main()
