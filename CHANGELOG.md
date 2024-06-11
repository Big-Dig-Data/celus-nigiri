
# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).


## [3.0.0] - 2024-06-11

### Added
- CounterRecord.item_authors and CounterRecord.item_publication_date

### Changed
- using dataclasses for CounterRecord.title_ids and CounterRecord.item_ids


## [2.2.1] - 2024-05-10

### Fixed
- header extraction of stringified input


## [2.2.0] - 2024-05-10

### Added
- autofill Counter5ReportBase.header in Counter5ReportBase.fd_to_dicts function


## [2.1.0] - 2024-04-16

### Added
- Parent_Data_Type dimension for IR reports
- make_download_url and make_download_params functions for Sushi5Client


## [2.0.0] - 2024-03-18

### Added
- cli application to download reports
- get_months function for C5 json, C5 tabular and celus formats
- support to process and download IR report

### Changed
- structure of CounterRecord (added item and item_ids)


## [1.3.3] - 2023-11-13

### Fixed
- fix crash when some item in C5 report does not have Performance key

### Changed
- use celus-pycounter instead of pycounter

## [1.3.1] - 2023-06-05

### Changed
- pycounter was bumped to newer version in order to process time when the harvest was run more properly

### Fixed
- formatting of an error message was fixed


## [1.3.0] - 2023-02-16

### Fixed
- nigiri-csv binary used older API

### Added
- handle sushi errors inside responses whit 500 status code


## [1.2.1] - 2022-11-08

### Fixed
- use csv detection while parsing Counter5TabularReport


## [1.2.0] - 2022-11-04

### Fixed
- CounterRecord.start and CounterRecord.end are converted to datetime.date during the parsing

### Added
- added functions for detecting csv encoding and dialect


## [1.1.1] - 2022-07-28

### Fixed
- creation of CounterRecord during C4 parsing (test added)


## [1.1.0] - 2022-07-22

### Added
- code which is used to parse celus format from CSV file


## [1.0.0] - 2022-07-22

### Changed
- CounterRecord can be imported from package root
- CounterRecord is a dataclass now
- added organization field to CounterRecord

### Added
- added as_csv function to serialize CounterRecord


## [0.1.0] - 2022-07-20

### Added
- code moved from celus project
