Feature: explain prints the filter/scan plan

  Scenario: explain shows the row-group filter outcome
    Given the cities fixture
    When I run: explain cities.parquet --filter "pop > 1000000"
    Then the exit code is 0
    And stdout contains "RG"
    And stdout contains "Outcome"

  Scenario: explain as json
    Given the cities fixture
    When I run: explain cities.parquet --filter "pop > 1000000" -o json
    Then the exit code is 0
    And stdout contains "originalPredicate"
