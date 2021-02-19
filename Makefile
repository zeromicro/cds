# run from repository root

# Example:
#   make up -- start whole staff
#   make down -- stop and clean whole staff
#   make docker-clean
#   make docker-start
#   make docker-kill
#   make docker-remove

PROJECT="CDS"
include Makefile.common

src =$(wildcard galaxy/*.go)  $(wildcard dm/*.go)  $(wildcard rtu/*.go)  $(wildcard tools/*.go)  $(wildcard tube/*.go)

.PHONY : logo
logo:
	@cat sit/logo
	echo $(src)
.PHONY :
docker_build: $(src)
	@echo "================= build ======================"
	docker build -t cds .
	echo $(DATE) > docker_build
.PHONY : docker_run
docker_run:
	@echo "================= run ==============================="
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
	@echo "==================== launch ========================="
	@docker-compose -f sit/dockercompose/infrastructure.yml  up -d
	@docker-compose -f sit/dockercompose/app.yml up -d
	cd sit/dockercompose/init && sh ./init.sh
	@cat sit/info

.PHONY : down
down:
	@docker-compose -f sit/dockercompose/app.yml   -f sit/dockercompose/infrastructure.yml down



