Feature: schema printing

  Scenario: schema tree lists every column
    Given the cities fixture
    When I run: schema cities.parquet
    Then the exit code is 0
    And stdout contains "id"
    And stdout contains "INT32"
    And stdout contains "name"

  Scenario: schema as json
    Given the cities fixture
    When I run: schema cities.parquet -o json
    Then the exit code is 0
    And stdout contains "\"kind\":\"INT32\""
