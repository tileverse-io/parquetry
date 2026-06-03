Feature: head decodes the first rows

  Scenario: head prints rows as jsonl
    Given the cities fixture
    When I run: head cities.parquet
    Then the exit code is 0
    And stdout has 4 lines
    And stdout line 1 contains "Rosario"

  Scenario: head honors an explicit row cap
    Given the cities fixture
    When I run: head cities.parquet --limit 2
    Then the exit code is 0
    And stdout has 2 lines

  Scenario: head rejects the json format like cat
    Given the cities fixture
    When I run: head cities.parquet -o json
    Then the exit code is 2
