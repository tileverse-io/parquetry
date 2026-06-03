Feature: stats prints per-column statistics

  Scenario: stats reports per-column min and max as text
    Given the cities fixture
    When I run: stats cities.parquet
    Then the exit code is 0
    And stdout contains "min"
    And stdout contains "pop"
    And stdout contains "3100000"

  Scenario: stats as json
    Given the cities fixture
    When I run: stats cities.parquet -o json
    Then the exit code is 0
    And stdout contains "\"column\""
