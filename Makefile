.DEFAULT_GOAL := help
.PHONY: help dev-setup format lint test verify clean compile package install build-benchmarks run-benchmarks run-benchmarks-smoke benchmarks benchmarks-smoke geoserver-plugin geoserver-demo geoserver-demo-down geoserver-dist

GEOSERVER_DIR := integrations/parquetry-geoserver

help: ## Show this help
	@awk 'BEGIN {FS = ":.*## "; printf "Targets:\n"} /^[a-zA-Z_-]+:.*## / {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)

dev-setup: ## Install parent POMs into the local repo (mvnw -N install)
	./mvnw -N install

format: ## Apply code formatting (mvnw validate)
	./mvnw validate

lint: ## Run static analysis (mvnw -Pqa validate)
	./mvnw -Pqa validate

test: ## Run unit tests
	./mvnw test

verify: ## Full build: unit + integration tests
	./mvnw verify

clean: ## Remove build output (mvnw clean)
	./mvnw clean

compile: ## Compile all modules
	./mvnw compile

package: ## Package all modules (skip tests)
	./mvnw package -DskipTests

install: ## Build and install all modules to the local repo
	./mvnw install

# build-benchmarks, run-benchmarks, and run-benchmarks-smoke are plumbing for CI and
# the composite targets below; the user-facing entry points are `benchmarks` and
# `benchmarks-smoke`.
build-benchmarks:
	./mvnw -Pbenchmarks -pl :parquetry-benchmarks -am package -DskipTests -ntp

run-benchmarks:
	java --enable-preview --enable-native-access=ALL-UNNAMED --sun-misc-unsafe-memory-access=allow \
	  -jar internal/parquetry-benchmarks/target/benchmarks.jar

run-benchmarks-smoke:
	java --enable-preview --enable-native-access=ALL-UNNAMED --sun-misc-unsafe-memory-access=allow \
	  -jar internal/parquetry-benchmarks/target/benchmarks.jar \
	  -p smoke=true -f 0 -wi 1 -i 1 -r 1 -foe true

benchmarks: build-benchmarks run-benchmarks ## Build then run the full benchmark suite (real measurement, slow)

benchmarks-smoke: build-benchmarks run-benchmarks-smoke ## Build then smoke-run the benchmarks (fast sanity check, no measurement)

# GeoServer + GeoParquet plugin and demo. These delegate to integrations/parquetry-geoserver/Makefile,
# which owns them (run `make help` there for the full target list).
geoserver-plugin: ## Build the GeoServer plugin zip (drop into any GeoServer install)
	$(MAKE) -C $(GEOSERVER_DIR) plugin

geoserver-demo: ## Build and start the GeoServer + GeoParquet demo
	$(MAKE) -C $(GEOSERVER_DIR) demo

geoserver-demo-down: ## Stop and remove the GeoServer demo container
	$(MAKE) -C $(GEOSERVER_DIR) down

geoserver-dist: ## Build the plugin and the self-contained customer demo zip
	$(MAKE) -C $(GEOSERVER_DIR) dist
