# Flight ETL Testing Strategy

## Objective
Verify that raw flight records are processed into curated and quarantine datasets without record loss, unexpected duplication, or incorrect metric calculations.

## Test coverage
- Schema conversion and type-error detection
- Required-field and business-rule validation
- Deterministic `flight_id` deduplication
- Operational metric calculations
- Preservation of original rejected values
- Partitioned Parquet persistence
- Repeat-run overwrite behavior
- Record-count reconciliation

## Reconciliation rules

1. `raw_count = curated_count + quarantine_count`
2. `quarantine_count = quality_rejected_count + duplicate_count`
3. Curated `flight_id` values must be unique.
4. Every quarantined record must contain at least one error reason.
5. Repeated successful processing of the same input must not accumulate records.

## Execution strategy

PySpark integration tests will run in a Linux-based GCP environment.

A small deterministic dataset will be used for the initial integration test. The full synthetic dataset will be processed after the integration test passes.

## Known limitations

- Full-dataset overwrite is not atomic.
- Curated and quarantine datasets are not published transactionally.
- Successful rerun tests do not prove recovery from interrupted writes.
- Cloud integration tests are required before AIR-21 can be marked complete.