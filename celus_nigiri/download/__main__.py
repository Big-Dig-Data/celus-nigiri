import argparse
import sys
import typing
from datetime import date, datetime

from celus_nigiri.client import Sushi5Client


def parse_date(date_str) -> date:
    return datetime.strptime(date_str, "%Y-%m")


def available_report_types() -> typing.List[str]:
    res = []
    for master_rt in Sushi5Client.report_types:
        res.append(master_rt)
        res.extend(f"{master_rt}_{st}" for st in Sushi5Client.report_types[master_rt]["subreports"])
    return res


def main():
    parser = argparse.ArgumentParser(description="Nigiri downloader")
    parser.add_argument(
        "--report-type", "-T", metavar="RT", required=True, choices=available_report_types()
    )
    parser.add_argument("--requestor-id", "-R", metavar="ID", required=True)
    parser.add_argument("--customer-id", "-C", metavar="ID", default=None)
    parser.add_argument("--api-key", "-A", metavar="KEY", default=None)
    parser.add_argument("--begin-date", "-B", metavar="YYYY-MM", type=parse_date, required=True)
    parser.add_argument("--end-date", "-E", metavar="YYYY-MM", type=parse_date, required=True)
    parser.add_argument("url", metavar="URL", nargs=1, help="Base URL")
    args = parser.parse_args()

    client = Sushi5Client(
        url=args.url[0],
        requestor_id=args.requestor_id,
        customer_id=args.customer_id,
        extra_params={"api_key": args.api_key} if args.api_key else {},
    )

    response = client.fetch_report_data(args.report_type, args.begin_date, args.end_date)

    print(f"HTTP {response.status_code}", file=sys.stderr)
    if 200 <= response.status_code < 300 or 400 <= response.status_code < 600:
        for data in response.iter_content(1024 * 1024):
            sys.stdout.buffer.write(data)
    else:
        raise RuntimeError(f"Wrong response status code - {response.status_code}")


if __name__ == "__main__":
    main()
