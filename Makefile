# run from repository root

# Example:
#   make up -- start whole staff
#   make down -- stop and clean whole staff
#	make docker_infrastructrue_up  -- start infrastructrue like etcd kafka clickhouse etc..
include common.mk

PROJECT="CDS"

.DEFAULT: usg
.PHONY : usg
usg:
	@echo =======================================================================================================================================================
	@echo ==== usage ====
	@echo		"make up : start whole staff"
	@echo		"make down : stop and clean whole staff"
	@echo		"make docker_infrastructrue_up :  start infrastructrue like etcd kafka clickhouse etc.."

.PHONY : logo
logo:
	@cat VERSION
	@cat sit/logo

.DELETE_ON_ERROR: build/build.log
build/build.log: $(GO_FILES)
	$(GO_BUILD)  -ldflags  "$(LD_FLAGS)" -o build/rtu      	rtu/cmd/sync/rtu.go
	$(GO_BUILD)  -ldflags  "$(LD_FLAGS)" -o build/dm        	dm/cmd/sync/dm.go
	$(GO_BUILD)  -ldflags  "$(LD_FLAGS)" -o build/galaxy    	galaxy/galaxy.go
	echo done >build/build.log

.PHONY : build
build : build/build.log

.DELETE_ON_ERROR: make_build.info
make_build.info:
	@echo "=================docker build ======================"
	docker build --target builder -t my/cds_builder:latest . 
	docker build  --target cds  -t cds . 
	@echo "================= docker clean ================================================="
	@if test -n "$$(docker images -f "dangling=true" -q)" ; then \
	docker rmi $$(docker images -f "dangling=true" -q) ; \
	fi

.PHONY : docker_build
docker_build: make_build.info

.PHONY : docker_app_run
docker_app_run:
	@echo "=================== launch docker app ===================================="
	@docker-compose -f sit/dockercompose/app.yml  up -d

.PHONY : docker_build_run
docker_build_run: docker_build docker_run

.PHONY : docker_infrastructrue_up
docker_infrastructrue_up:
	@echo "==================== launch docker infrastructure ========================="
	@docker-compose -f sit/dockercompose/infrastructure.yml  up -d

.PHONY : docker_infrastructrue_down
docker_infrastructrue_down:
	docker-compose -f sit/dockercompose/infrastructure.yml  down

.PHONY : up
up:  logo docker_build docker_infrastructrue_up docker_app_run
	cd sit/dockercompose/init && sh ./init.sh
	@cat sit/info

.PHONY : down
down:
	@docker-compose -f sit/dockercompose/app.yml   -f sit/dockercompose/infrastructure.yml down
