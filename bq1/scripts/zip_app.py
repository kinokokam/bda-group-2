"""Package a Managed Flink Python application zip.

Produces an outer zip holding the main script, the shaded connector JAR under
`lib/`, and `deps.zip`. Managed Flink rejects a `pyFiles` value naming a
directory, and compiled extensions cannot be imported from inside an archive,
so Python dependencies ship as a nested zip declared through `pyArchives` and
extracted by Flink before the job starts.
"""

import argparse
import zipfile
from pathlib import Path

NESTED_NAME = "deps.zip"
LIB_DIRECTORY = "lib"


def build(
    output_path: Path,
    main_script: Path,
    deps_directory: Path,
    jar_path: Path | None,
    data_files: list[Path],
    py_files: list[Path] | None = None,
) -> int:
    """Write the application zip and return its size in bytes.

    Parameters
    ----------
    output_path : Path
        Outer zip to create, overwritten if present.
    main_script : Path
        Entry-point script, stored at the outer zip root.
    deps_directory : Path
        Directory whose contents form the root of the nested `deps.zip`.
    jar_path : Path | None
        Shaded connector JAR, stored under `lib/`. Omitted when None.
    data_files : list[Path]
        Extra files stored at the root of the nested zip.
    py_files : list[Path] | None
        Pure-Python modules stored at the outer zip root, for `pyFiles`. The
        worker imports these by name, which a nested archive cannot satisfy.

    Returns
    -------
    int
        Size of the written outer zip in bytes.
    """
    nested_path = output_path.with_name(NESTED_NAME)
    with zipfile.ZipFile(nested_path, "w", zipfile.ZIP_DEFLATED, compresslevel=6) as nested:
        for source in sorted(deps_directory.rglob("*")):
            if source.is_file():
                nested.write(source, source.relative_to(deps_directory).as_posix())
        for data_file in data_files:
            nested.write(data_file, data_file.name)

    with zipfile.ZipFile(output_path, "w", zipfile.ZIP_STORED) as archive:
        archive.write(main_script, main_script.name)
        archive.write(nested_path, NESTED_NAME)
        if jar_path is not None:
            archive.write(jar_path, f"{LIB_DIRECTORY}/{jar_path.name}")
        for py_file in py_files or []:
            archive.write(py_file, py_file.name)
    return output_path.stat().st_size


def main() -> None:
    """Parse arguments and write the application zip.

    Returns
    -------
    None
    """
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", type=Path, required=True, help="output zip path")
    parser.add_argument("--main", type=Path, required=True, help="entry-point script")
    parser.add_argument("--deps", type=Path, required=True, help="dependency directory")
    parser.add_argument("--jar", type=Path, help="shaded connector JAR for lib/")
    parser.add_argument("--data", type=Path, nargs="*", default=[], help="model artifacts")
    parser.add_argument(
        "--py-files", type=Path, nargs="*", default=[], help="modules for pyFiles"
    )
    arguments = parser.parse_args()

    size = build(
        arguments.out,
        arguments.main,
        arguments.deps,
        arguments.jar,
        arguments.data,
        arguments.py_files,
    )
    print(f"{arguments.out} {size / 1e6:.1f} MB")


if __name__ == "__main__":
    main()
