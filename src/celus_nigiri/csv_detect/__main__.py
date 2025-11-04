import itertools
import pathlib
import sys

from . import csv_line_length_histogram, detect_csv_dialect, detect_file_encoding


def main():
    root_path = pathlib.Path(sys.argv[1])

    paths = []
    if root_path.is_dir():
        paths = list(
            itertools.chain(
                root_path.glob("**/*.csv"),
                root_path.glob("**/*.tsv"),
            )
        )
    elif root_path.is_file():
        if root_path.suffix.lower() in [".tsv", ".csv"]:
            paths = [root_path]

    for path in paths:
        with path.open("rb") as f:
            encoding = detect_file_encoding(f)

        try:
            with path.open("r", encoding=encoding) as f:
                dialect = detect_csv_dialect(f)
                histogram = csv_line_length_histogram(f, dialect)
        except UnicodeDecodeError as e:
            print(f"{path} {e}")
        else:
            print(f"{path}: {dialect}|{encoding} - {histogram}")


if __name__ == "__main__":
    main()
