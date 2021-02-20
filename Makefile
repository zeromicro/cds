# run from repository root

# Example:
#   make up -- start whole staff
#   make down -- stop and clean whole staff


PROJECT="CDS"
include Makefile.common

# src =$(wildcard galaxy/*.go)  $(wildcard dm/*.go)  $(wildcard rtu/*.go)  $(wildcard tools/*.go)  $(wildcard tube/*.go)

.PHONY : logo
logo:
	@cat sit/logo

make_build.info: ${GO_FILES}
	@echo "=================docker build ======================"
	docker build -t cds .
	@$(call write_build_info)

.PHONY : docker_build
docker_build: make_build.info

.PHONY : docker_run
docker_run:
	@echo "================ docker run ========================"
	@docker-compose -f dockercompose/app.yml  up -d

.PHONY : docker_build_run
docker_build_run: docker_build docker_run

.PHONY : docker_infrastructrue_up
docker_infrastructrue_up:
	@echo "============ infrastructure ========================="
	@docker-compose -f dockercompose/infrastructure.yml  up -d

.PHONY : docker_infrastructrue_down
docker_infrastructrue_down:
	docker-compose -f dockercompose/infrastructure.yml  down

.PHONY : sit
sit: docker_build_run docker_infrastructrue_up

.PHONY : sit_down
sit_down: docker_infrastructrue_down
	docker-compose -f dockercompose/app.yml  down

.PHONY : app_down
app_down:
	docker-compose -f dockercompose/app.yml  down

.PHONY : up
up:  logo docker_build
	@echo "==================== launch infrastructure========================="
	@docker-compose -f sit/dockercompose/infrastructure.yml  up -d
	@echo "==================== launch app ===================================="
	@docker-compose -f sit/dockercompose/app.yml up -d
	cd sit/dockercompose/init && sh ./init.sh
	@cat sit/info

.PHONY : down
down:
	@docker-compose -f sit/dockercompose/app.yml   -f sit/dockercompose/infrastructure.yml down



