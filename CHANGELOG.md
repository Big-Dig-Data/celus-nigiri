
# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).



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
