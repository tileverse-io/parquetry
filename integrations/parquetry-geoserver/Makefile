# GeoParquet-in-GeoServer demo + plugin packaging. Run `make help` for the target list.
#
# The plugin zip is built on the host (Java 25 + ./mvnw); the image only unzips it into
# GeoServer's WEB-INF/lib. `make dist` bundles the build context so a customer needs only Docker.

REPO_ROOT := ../..
DEMO := demo
COMPOSE := docker compose -f $(DEMO)/docker-compose.yml
DIST := target/parquetry-geoserver-demo
URL := http://localhost:8080/geoserver
# Extra flags forwarded to the plugin Maven build, e.g. MVN_FLAGS=-Drevision=1.0.0 for a release
# (the POM uses CI-friendly ${revision} versioning; the release tag drives the version).
MVN_FLAGS ?=

.DEFAULT_GOAL := help
.PHONY: help demo build plugin image up down logs dist clean

help: ## Show this help
	@awk 'BEGIN {FS = ":.*## "; printf "Targets:\n"} /^[a-zA-Z_-]+:.*## / {printf "  \033[36m%-8s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)

demo: build up ## Build everything, start GeoServer, print the URL
	@echo ""
	@echo "GeoServer is starting at $(URL)  (admin / geoserver)"
	@echo "Layer preview: $(URL)/web  ->  Layer Preview  ->  parquetry:world"
	@echo "Follow startup with: make logs"

build: image ## Build the plugin zip and the demo image

plugin: ## Build only the GeoServer plugin zip
	cd $(REPO_ROOT) && ./mvnw -ntp $(MVN_FLAGS) -pl :parquetry-geoserver -am -Passembly package -Dmaven.test.skip=true
	mkdir -p $(DEMO)/build
	cp target/parquetry-geoserver-*-plugin.zip $(DEMO)/build/plugin.zip

image: plugin ## Build the demo Docker image
	$(COMPOSE) build

up: ## Start the container
	$(COMPOSE) up -d

down: ## Stop and remove the container
	$(COMPOSE) down

logs: ## Follow the container logs
	$(COMPOSE) logs -f

dist: plugin ## Build a self-contained customer zip (unzip -> `docker compose up`)
	rm -rf $(DIST) $(DIST).zip
	mkdir -p $(DIST)/build $(DIST)/data
	cp $(DEMO)/Dockerfile $(DEMO)/.dockerignore $(DEMO)/docker-compose.yml $(DIST)/
	cp $(DEMO)/dist-README.md $(DIST)/README.md
	cp $(DEMO)/build/plugin.zip $(DIST)/build/plugin.zip
	cp -r $(DEMO)/geoserver-data $(DIST)/geoserver-data
	cp -r $(DEMO)/data/ne $(DIST)/data/ne
	cp -r $(DEMO)/data/iceberg-warehouse $(DIST)/data/iceberg-warehouse
	cp -r $(DEMO)/data/stac $(DIST)/data/stac
	cp -r $(DEMO)/secrets $(DIST)/secrets
	cd target && zip -rq parquetry-geoserver-demo.zip parquetry-geoserver-demo
	@echo "distributable: $(CURDIR)/$(DIST).zip"

clean: down ## Remove build artifacts and the demo image
	rm -rf $(DEMO)/build target/parquetry-geoserver-demo target/parquetry-geoserver-demo.zip
	docker image rm parquetry-geoserver:demo 2>/dev/null || true
