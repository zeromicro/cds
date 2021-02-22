# run from repository root

# Example:
#   make up -- start whole staff
#   make down -- stop and clean whole staff
include common.mk

PROJECT="CDS"

.PHONY : logo
logo:
	@cat VERSION
	@cat sit/logo

.PHONY : build
build:
	$(GO_BUILD)  -ldflags  "$(LD_FLAGS)" -o docker/build/rtu      	rtu/cmd/sync/rtu.go
	$(GO_BUILD)  -ldflags  "$(LD_FLAGS)" -o docker/build/dm        	dm/cmd/sync/dm.go
	$(GO_BUILD)  -ldflags  "$(LD_FLAGS)" -o docker/build/galaxy    	galaxy/galaxy.go

make_build.info:
	@echo "=================docker build ======================"
	docker build --target builder -t my/cds_builder:latest .
#	docker build --target builder -t my/cds_builder:latest .  > cds_builder.log || cat cds_builder.log #for docker cache,in order not be docker rmi
	docker build  --target cds  -t cds .
#	docker build --target cds -t cds . > cds.log || cat cds.log #for image
#	# cat cds_builder.log > make_build.info && cat cds.log >>make_build.info
#	 rm cds_builder.log cds.log

	@if [[ -n "$$(docker images -f "dangling=true" -q)" ]]; then \
	docker rmi $$(docker images -f "dangling=true" -q) ; \
	fi

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


