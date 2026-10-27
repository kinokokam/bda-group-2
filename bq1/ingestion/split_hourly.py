"""Split the raw transactions CSV into sorted hourly files for the replay Lambda.

One-off job. The raw file is not in time order, so the Lambda cannot stream it
directly; this rewrites it as one sorted file per hour plus a manifest.
"""

import csv
import sys
from collections import defaultdict
from pathlib import Path

# Fixed-width "YYYY/MM/DD HH:MM" means the hour key is a slice and lexical
# order within an hour is chronological order.
HOUR_KEY_LENGTH = len("2022/09/01 00")


def split_into_hours(source_path: Path, output_directory: Path) -> Path:
    """Write one sorted CSV per hour of the source file, plus a manifest.

    Parameters
    ----------
    source_path : Path
        Raw transactions CSV, header included, in any row order.
    output_directory : Path
        Destination for the hourly files and `manifest.csv`.

    Returns
    -------
    Path
        Path of the written manifest.
    """
    output_directory.mkdir(parents=True, exist_ok=True)
    handles: dict[str, object] = {}

    with source_path.open("r", newline="", encoding="utf-8") as source:
        header = source.readline()
        for line in source:
            hour_key = line[:HOUR_KEY_LENGTH]
            handle = handles.get(hour_key)
            if handle is None:
                name = hour_key.replace("/", "-").replace(" ", "-") + ".csv"
                handle = (output_directory / name).open("w", newline="", encoding="utf-8")
                handles[hour_key] = handle
            handle.write(line)

    for handle in handles.values():
        handle.close()

    return _sort_and_describe(header, output_directory)


def _sort_and_describe(header: str, output_directory: Path) -> Path:
    """Sort each hourly file in place, prepend the header, and write the manifest.

    Parameters
    ----------
    header : str
        Header line of the source file, copied into every hourly file.
    output_directory : Path
        Directory holding the unsorted, headerless hourly files.

    Returns
    -------
    Path
        Path of the written manifest.
    """
    laundering_column = header.rstrip("\r\n").split(",").index("Is Laundering")
    manifest_path = output_directory / "manifest.csv"
    hourly_paths = sorted(p for p in output_directory.glob("*.csv") if p != manifest_path)

    with manifest_path.open("w", newline="", encoding="utf-8") as manifest_file:
        manifest = csv.writer(manifest_file)
        manifest.writerow(["hour", "rows", "laundering"])
        for hourly_path in hourly_paths:
            rows = hourly_path.read_text(encoding="utf-8").splitlines(keepends=True)
            rows.sort()
            laundering_count = sum(
                1 for row in rows if row.rstrip("\r\n").split(",")[laundering_column] == "1"
            )
            with hourly_path.open("w", newline="", encoding="utf-8") as sorted_file:
                sorted_file.write(header)
                sorted_file.writelines(rows)
            manifest.writerow([hourly_path.stem, len(rows), laundering_count])

    return manifest_path


if __name__ == "__main__":
    if len(sys.argv) != 3:
        raise SystemExit("usage: split_hourly.py <source csv> <output dir>")
    split_into_hours(Path(sys.argv[1]), Path(sys.argv[2]))
