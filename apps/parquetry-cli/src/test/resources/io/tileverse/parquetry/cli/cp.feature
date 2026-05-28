Feature: cp transforms one Parquet file into another

  Scenario: round-trip a projected, filtered copy
    Given the cities fixture
    When I run: cp cities.parquet big.parquet --columns name,pop --filter "pop > 1000000"
    Then the exit code is 0
    When I run: cat big.parquet
    Then the exit code is 0
    And stdout has 3 lines
    And stdout line 1 contains "Rosario"

  Scenario: cp refuses to clobber the source
    Given the cities fixture
    When I run: cp cities.parquet cities.parquet
    Then the exit code is 1
    And stderr contains "refuses"
