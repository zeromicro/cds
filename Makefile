# run from repository root
include common.mk

PROJECT="CDS"

WATCH_NPM_DICT=$(shell du -ah $$(ls ./web | grep -v 'node_modules' | awk '{printf "web/%s\n",$$1}') | awk '{print $$2}')

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
	@cat logo

.DELETE_ON_ERROR: build/build.log
build/build.log: $(GO_FILES)
	$(GO_BUILD)  -ldflags  "$(LD_FLAGS)" -o build/rtu		cmd/rtu/cmd/sync/rtu.go
	$(GO_BUILD)  -ldflags  "$(LD_FLAGS)" -o build/dm		cmd/dm/cmd/sync/dm.go
	$(GO_BUILD)  -ldflags  "$(LD_FLAGS)" -o build/galaxy	cmd/galaxy/galaxy.go
	echo done >build/build.log

.PHONY : build
build : build/build.log

.DELETE_ON_ERROR: make_build.info
make_build.info:
	@echo "=================docker build ======================"
	docker build --target builder -t my/cds_builder:latest . 
	docker build  --target cds  -t cds . 

.PHONY : docker_build
docker_build: make_build.info

.PHONY : docker_app_run
docker_app_run:
	@echo "=================== launch docker app ===================================="
	@docker-compose -f sit/docker/app.yml  up -d

.PHONY : docker_build_run
docker_build_run: docker_build docker_run

.PHONY : docker_infrastructrue_up
docker_infrastructrue_up:
	@echo "==================== launch docker deps ========================="
	@docker-compose -f sit/docker/deps.yml  up -d
	@if test -n "$$(docker volume ls -qf dangling=true)" ; then \
	docker volume rm $$(docker volume ls -qf dangling=true) ; \
	fi

.PHONY : docker_infrastructrue_down
docker_infrastructrue_down:
	docker-compose -f sit/docker/deps.yml  down

.PHONY : init 
init :  logo docker_build docker_infrastructrue_up docker_app_run
	cd  sit/docker && sh ./init.sh
	@cat sit/info

.PHONY : docker_clean
docker_clean :
	@echo "================= docker clean ================================================="
	@if test -n "$$(docker images -f "dangling=true" -q)" ; then \
	docker rmi $$(docker images -f "dangling=true" -q) ; \
	else echo no dangling image to clean ;\
	fi
	@if test -n "$$(docker volume ls -qf dangling=true)" ; then \
	docker volume rm $$(docker volume ls -qf dangling=true) ; \
	else echo no dangling volume  to clean; \
	fi


web/dist: $(WATCH_NPM_DICT)
	@echo "================= npm build ================================================="
	@cd web && npm install && npm run build:prod --report

.PHONY : up
up: logo web/dist docker_build docker_clean docker_infrastructrue_up docker_app_run init

.PHONY : end
end:
	@docker-compose -f sit/docker/app.yml   -f sit/docker/deps.yml down

.PHONY : down
down: end docker_clean

.PHONY :clean
clean:
	rm -rf build
