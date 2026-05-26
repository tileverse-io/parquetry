.PHONY: help dev-setup format lint test verify clean compile package install build-benchmarks run-benchmarks run-benchmarks-smoke benchmarks benchmarks-smoke

help:
	@echo "Targets: dev-setup format lint test verify clean compile package install"
	@echo "Benchmarks: benchmarks benchmarks-smoke"

dev-setup:
	./mvnw -N install

format:
	./mvnw validate

lint:
	./mvnw -Pqa validate

test:
	./mvnw test

verify:
	./mvnw verify

clean:
	./mvnw clean

compile:
	./mvnw compile

package:
	./mvnw package -DskipTests

install:
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
