Feature: meta summary

  Scenario: meta reports the row count and writer
    Given the cities fixture
    When I run: meta cities.parquet
    Then the exit code is 0
    And stdout contains "rows:"
    And stdout contains "parquetry"
