from pathlib import Path

import pytest

from celus_nigiri.csv_detect import detect_csv_dialect, detect_file_encoding


@pytest.mark.parametrize(
    "path,encoding,dialect",
    (
        ("counter4/BR1-semicolons.csv", "ascii", "excel-semicolon"),
        ("counter5/IR_M1-commas.csv", "utf-8-sig", "excel"),
        ("counter5/TR-tabs.tsv", "ascii", "excel-tab"),
    ),
)
def test_dialect(path, encoding, dialect):
    path = Path(__file__).parent / 'data' / path
    with path.open("rb") as f:
        detected_encoding = detect_file_encoding(f)
        assert detected_encoding == encoding
    with path.open("r") as f:
        assert detect_csv_dialect(f, detected_encoding) == dialect
