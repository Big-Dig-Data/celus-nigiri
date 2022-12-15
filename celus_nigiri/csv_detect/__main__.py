import itertools
import pathlib
import sys

from . import csv_line_length_histogram, detect_csv_dialect, detect_file_encoding


def main():
    for path in itertools.chain(
        pathlib.Path(sys.argv[1]).glob("**/*.csv"), pathlib.Path(sys.argv[1]).glob("**/*.tsv")
    ):
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
