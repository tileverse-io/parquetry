Feature: error reporting and exit codes

  Scenario: a bad filter expression exits 5
    Given the cities fixture
    When I run: cat cities.parquet --filter "pop >"
    Then the exit code is 5

  Scenario: an unknown column in projection exits 1
    Given the cities fixture
    When I run: cat cities.parquet --columns nope
    Then the exit code is 1
