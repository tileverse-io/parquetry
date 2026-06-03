Feature: cat decodes rows

  Scenario: project and filter to JSONL
    Given the cities fixture
    When I run: cat cities.parquet --columns name,pop --filter "pop > 1000000"
    Then the exit code is 0
    And stdout has 3 lines
    And stdout line 1 contains "Rosario"

  Scenario: cat as CSV writes a header then one line per row
    Given the cities fixture
    When I run: cat cities.parquet --columns name,pop -o csv
    Then the exit code is 0
    And stdout has 5 lines
    And stdout line 1 contains "name,pop"
    And stdout line 2 contains "Rosario,1300000"

  Scenario: cat as TSV separates the columns with tabs
    Given the cities fixture
    When I run: cat cities.parquet --columns name,pop -o tsv
    Then the exit code is 0
    And stdout has 5 lines
    And stdout line 1 contains "pop"
    And stdout does not contain "name,pop"

  Scenario: cat as text writes name=value pairs per row
    Given the cities fixture
    When I run: cat cities.parquet --columns name,pop -o text
    Then the exit code is 0
    And stdout has 4 lines
    And stdout line 1 contains "name=Rosario"
    And stdout line 1 contains "pop=1300000"
