Feature: cp preserves GeoParquet metadata

  Scenario: meta surfaces the geo metadata a GeoParquet file carries
    Given the geoparquet fixture
    When I run: meta geo.parquet
    Then the exit code is 0
    And stdout contains "geo"

  Scenario: cp keeps geo metadata when the geometry column survives
    Given the geoparquet fixture
    When I run: cp geo.parquet out.parquet --columns id,geometry
    Then the exit code is 0
    When I run: meta out.parquet
    Then the exit code is 0
    And stdout contains "geo"

  Scenario: cp preserves the geometry CRS
    Given the geoparquet fixture
    When I run: cp geo.parquet out.parquet --columns id,geometry
    Then the exit code is 0
    When I run: meta out.parquet -o json
    Then the exit code is 0
    And stdout contains "EPSG"
    And stdout contains "4326"

  Scenario: cp recomputes the geometry bbox from the rows actually written
    Given the geoparquet fixture
    When I run: cp geo.parquet out.parquet --columns id,geometry --filter "id = 1"
    Then the exit code is 0
    When I run: meta out.parquet -o json
    Then the exit code is 0
    And stdout contains "[-60.65,-32.94,-60.65,-32.94]"
    And stdout does not contain "-64.18"

  Scenario: cp emits no geo metadata when the geometry column is dropped
    Given the geoparquet fixture
    When I run: cp geo.parquet out.parquet --columns id
    Then the exit code is 0
    When I run: meta out.parquet
    Then the exit code is 0
    And stdout does not contain "geo"
