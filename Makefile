.PHONY: help dev-setup format lint test verify clean compile package install

help:
	@echo "Targets: dev-setup format lint test verify clean compile package install"

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
