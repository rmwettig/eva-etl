# Changelog

## [1.2.0] - 2018-09-27
### Added
  * Pid hash calculation based on existing files
  * FixDateTransformer to fill 'behandl_beginn' and 'behandl_ende' if missing

### Changed
  * Reports work now with new AU structure
  * Reports work now with new Morbi structure

## [1.1.0] - 2018-05-16
### Added
  * Year slices are compressed with gzip

## [1.0.0] - 2018-01-05
### Added
  * Columns can be excluded by using the "excludeColumns" field in a view definition
  * Load where conditions from file
  * Defined sources are saved into subfolder given the dataset name
  * Rows can be excluded by regular expression defined in the filters field
  * Rows can be modified by defining rules in the transformer section
  * Option for creating data statistics
  * Option for merging yearwise data slices
  * Option for calculating Charlson scores
  * Wido DDD transformer
  * Pseudo-IK for FDB transformer

### Removed
  * External FastExport tool 