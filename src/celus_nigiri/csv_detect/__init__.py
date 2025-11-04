import csv
import itertools
import logging
import typing
from collections import Counter

from chardet.universaldetector import UniversalDetector

DEFAULT_ENCODING = "utf-8-sig"

logger = logging.getLogger(__name__)

# Register new dialects
csv.register_dialect("excel-semicolon", csv.get_dialect("excel"), delimiter=";")
csv.register_dialect("excel-pipe", csv.get_dialect("excel"), delimiter="|")
csv.register_dialect("unix-tab", csv.get_dialect("unix"), delimiter="\t")
csv.register_dialect("unix-semicolon", csv.get_dialect("unix"), delimiter=";")
csv.register_dialect("unix-pipe", csv.get_dialect("unix"), delimiter="|")


def detect_file_encoding(
    file: typing.Union[typing.IO[bytes], typing.IO[str]], min_confidence: float = 0.8
) -> str:
    orig_file_pos = file.tell()
    file.seek(0)

    encoding = DEFAULT_ENCODING
    detector = UniversalDetector()
    try:
        for line in file.readlines():
            detector.feed(line)
            if detector.done:
                break
        detector.close()
        if detector.result["confidence"] >= min_confidence:
            encoding = detector.result["encoding"]
    except TypeError:
        logger.warn("File provided for encoding detection is not opened in binary mode!")
    finally:
        file.seek(orig_file_pos)

    return encoding


def detect_csv_dialect(file: typing.IO[str], max_lines: typing.Optional[int] = 200) -> str:
    """Detect which csv dialect is most appropriate for counter-like data"""
    histograms = []

    for dialect in csv.list_dialects():
        histogram = csv_line_length_histogram(file, dialect, max_lines)
        histograms.append((histogram, dialect))

    scores = sorted(histograms, key=lambda x: histogram_score(x[0]), reverse=True)
    winner = scores[0][1]

    if name := getattr(file, "name", None):
        logger.debug("The best dialect for the file '%s' is '%s'", name, winner)
    else:
        logger.debug("The best dialect is '%s'", winner)

    return winner


def histogram_score(histogram: typing.Counter[int]) -> int:
    """Calculate the score of the histogram for comprisons"""
    # Basically the more cells the better score
    return sum(k * v for k, v in histogram.items())


def csv_line_length_histogram(
    file: typing.IO[str], dialect: csv.Dialect, max_lines: typing.Optional[int] = 200
) -> typing.Counter[int]:
    orig_file_pos = file.tell()
    file.seek(0)

    try:
        reader = csv.reader(file, dialect=dialect)
        if max_lines:
            reader = itertools.islice(reader, max_lines)
        counter: Counter[int] = Counter()
        for line in reader:
            counter[len(line)] += 1
    finally:
        file.seek(orig_file_pos)

    return counter
