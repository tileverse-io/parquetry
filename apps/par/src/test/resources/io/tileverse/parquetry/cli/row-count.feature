Feature: row-count reports the number of rows

  Scenario: row-count returns the total row count
    Given the cities fixture
    When I run: row-count cities.parquet
    Then the exit code is 0
    And stdout line 1 contains "4"

  Scenario: row-count counts only the rows matching a filter
    Given the cities fixture
    When I run: row-count cities.parquet --filter "pop > 1000000"
    Then the exit code is 0
    And stdout line 1 contains "3"
