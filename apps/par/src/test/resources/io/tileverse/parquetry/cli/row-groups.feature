Feature: row-groups lists the file's row groups

  Scenario: row-groups lists each group as text
    Given the cities fixture
    When I run: row-groups cities.parquet
    Then the exit code is 0
    And stdout contains "index"
    And stdout contains "rows"
    And stdout contains "4"

  Scenario: row-groups as json
    Given the cities fixture
    When I run: row-groups cities.parquet -o json
    Then the exit code is 0
    And stdout contains "\"rows\""
