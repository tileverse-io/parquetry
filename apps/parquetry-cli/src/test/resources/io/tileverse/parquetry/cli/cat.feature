Feature: cat decodes rows

  Scenario: project and filter to JSONL
    Given the cities fixture
    When I run: cat cities.parquet --columns name,pop --filter "pop > 1000000"
    Then the exit code is 0
    And stdout has 3 lines
    And stdout line 1 contains "Rosario"
