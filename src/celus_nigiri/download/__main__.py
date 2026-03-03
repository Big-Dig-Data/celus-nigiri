import argparse
import json
import logging
import sys
import typing
from copy import deepcopy
from datetime import date, datetime

from celus_nigiri.client import CounterVersion, Sushi5Client


def parse_date(date_str) -> date:
    return datetime.strptime(date_str, "%Y-%m")


def available_report_types() -> typing.List[str]:
    res = []
    for master_rt in Sushi5Client.report_types:
        res.append(master_rt)
        res.extend(f"{master_rt}_{st}" for st in Sushi5Client.report_types[master_rt]["subreports"])
    return res


def add_connection_args(parser: argparse.ArgumentParser):
    """Add args shared by all subcommands."""
    parser.add_argument("--debug", "-d", action="store_true", default=False)
    parser.add_argument("--requestor-id", "-R", metavar="ID", default=None)
    parser.add_argument("--customer-id", "-C", metavar="ID", required=True)
    parser.add_argument("--api-key", "-A", metavar="KEY", default=None)
    parser.add_argument(
        "--counter-version",
        "-V",
        metavar="COUNTER",
        type=CounterVersion,
        required=True,
        default=CounterVersion.C5,
    )
    parser.add_argument("url", metavar="URL", nargs=1, help="Base URL")


def make_client(args, extra_params=None):
    return args.counter_version.sushi_client_class(
        url=args.url[0],
        requestor_id=args.requestor_id,
        customer_id=args.customer_id,
        extra_params=extra_params or {},
    )


def cmd_download(args):
    extra_params = deepcopy(args.counter_version.get_report_class(args.report_type).extra_params)

    if args.api_key:
        extra_params["api_key"] = args.api_key

    client = make_client(args, extra_params)
    response = client.fetch_report_data(
        args.report_type,
        args.begin_date,
        args.end_date,
        long_date_format=args.long_date_format,
    )

    if 200 <= response.status_code < 300 or 400 <= response.status_code < 600:
        for data in response.iter_content(1024 * 1024):
            sys.stdout.buffer.write(data)
    else:
        raise RuntimeError(f"Wrong response status code - {response.status_code}")


def cmd_reports(args):
    extra_params = {}
    if args.api_key:
        extra_params["api_key"] = args.api_key

    client = make_client(args)
    status_code, data = client.get_reports(extra_params=extra_params or None)

    if data is None:
        raise RuntimeError(f"Could not parse response from /reports endpoint (HTTP {status_code})")

    print(json.dumps(data, ensure_ascii=False, indent=2))


def main():
    parser = argparse.ArgumentParser(description="Nigiri downloader")
    subparsers = parser.add_subparsers(dest="command", required=True)

    # --- download subcommand ---
    download_parser = subparsers.add_parser("download", help="Download a SUSHI report")
    add_connection_args(download_parser)
    download_parser.add_argument(
        "--report-type", "-T", metavar="RT", required=True, choices=available_report_types()
    )
    download_parser.add_argument(
        "--begin-date", "-B", metavar="YYYY-MM", type=parse_date, required=True
    )
    download_parser.add_argument(
        "--end-date", "-E", metavar="YYYY-MM", type=parse_date, required=True
    )
    download_parser.add_argument("--long-date-format", "-l", action="store_true", default=False)
    download_parser.set_defaults(func=cmd_download)

    # --- reports subcommand ---
    reports_parser = subparsers.add_parser(
        "reports", help="List available reports from the /reports endpoint"
    )
    add_connection_args(reports_parser)
    reports_parser.set_defaults(func=cmd_reports)

    args = parser.parse_args()

    if args.debug:
        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig()

    args.func(args)


if __name__ == "__main__":
    main()
