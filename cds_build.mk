include common.mk
PROJECT="CDS"

.PHONY : build 
build: print_all
	$(GO_BUILD)  -ldflags  "$(LD_FLAGS)" -o docker/build/rtu      	rtu/cmd/sync/rtu.go && \
	$(GO_BUILD)  -ldflags  "$(LD_FLAGS)" -o docker/build/dm        	dm/cmd/sync/dm.go && \
	$(GO_BUILD)  -ldflags  "$(LD_FLAGS)" -o docker/build/galaxy    	galaxy/galaxy.go
