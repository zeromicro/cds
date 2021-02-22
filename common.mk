# Example usage :
# Create the main Makefile in the root project directory.
# include Makefile.common
#=======================================================================================================================
# git info
GIT_BRANCH =$(shell git name-rev --name-only HEAD)
GIT_COMMIT =$(shell git rev-parse --short HEAD)
GIT_STATUS_HASH =$(shell git status -s -uno | shasum | awk '{ print $$1 }')
GIT_STATUS =$(shell git status -s -uno )
GIT_STATUS ?=no change
GIT_DIRTY =$(shell git describe --tags --dirty --always)
GIT_LAST_DATE=$(shell  echo $$(git log -1 --format=%cd))
#=======================================================================================================================
# go build info
GOPATH 		?= $(shell go env GOPATH)
# Ensure GOPATH is set before running build process.
ifeq "$(GOPATH)" ""
  $(error Please set the environment variable GOPATH before running `make`)
endif
$(info ----  Host machine GOPROXY now is :$(GOPROXY) ---)

GO_VERSION 				=$(shell go version | sed 's|go version ||')
GO_VERSION_NUMBER 		?= $(word 3, $(GO_VERSION))
GO   					:= GO111MODULE=on go
GO_BUILD    			:= $(GO) build $(BUILD_FLAG) -tags codes
GO_TEST					:= $(GO) test -p $(P)
ROOT_URL				:=$(shell head -n 1 go.mod | sed  "s|module ||")
PACKAGE_LIST  			:= go list ./...| grep -vE "cmd"
PACKAGE_URIS  			:= $$($(PACKAGE_LIST))
PACKAGE_RELATIVE_PATHS 	:= $(PACKAGE_LIST) | sed 's|$(ROOT_URL)/||'
GO_FILES     			:=$(shell echo $$(find $$($(PACKAGE_RELATIVE_PATHS)) -name "*.go"))
GO_BUILD_VERSION_PKG 	:= $(shell $(PACKAGE_LIST) | grep buildver)

VERSION =$(shell cat VERSION)
GO_BUILD_VERSION_PKG := $(shell $(PACKAGE_LIST) | grep buildver)
LD_FLAGS = -X '$(GO_BUILD_VERSION_PKG).Version=$(VERSION)'
LD_FLAGS += -X '$(GO_BUILD_VERSION_PKG).Branch=$(GIT_BRANCH)'
LD_FLAGS += -X '$(GO_BUILD_VERSION_PKG).Commit=$(GIT_COMMIT)'
LD_FLAGS += -X '$(GO_BUILD_VERSION_PKG).StatusHash=$(GIT_STATUS_HASH)'
LD_FLAGS += -X '$(GO_BUILD_VERSION_PKG).User=$(USER)'
LD_FLAGS += -X '$(GO_BUILD_VERSION_PKG).Time=$(DATE)'
LD_FLAGS += -X '$(GO_BUILD_VERSION_PKG).Status=$(GIT_STATUS)'
LD_FLAGS += -X '$(GO_BUILD_VERSION_PKG).GoVersion=$(GO_VERSION)'

#=======================================================================================================================
# env info
ARCH      := "`uname -s`"
DATE := $(shell date "+%Y-%m-%d %H:%M")
USER := $(shell id -u -n)

.PHONY : print_all
print_all:
	@echo PROJECT= $(PROJECT)
	@echo GIT_BRANCH= $(GIT_BRANCH)
	@echo GIT_COMMIT=$(GIT_COMMIT)
	@echo GIT_STATUS_HASH=$(GIT_STATUS_HASH)
	@echo GIT_STATUS=$(GIT_STATUS)
	@echo GIT_DIRTY=$(GIT_DIRTY)
	@echo GIT_LAST_DATE=$(GIT_LAST_DATE)
	@echo GOPATH=$(GOPATH)
	@echo GO_VERSION=$(GO_VERSION)
	@echo GO=$(GO)
	@echo GO_BUILD=$(GO_BUILD)
	@echo GO_TEST=$(GO_TEST)
	@echo ROOT_URL=$(ROOT_URL)
	@echo PACKAGE_LIST=$(PACKAGE_LIST)
	@echo PACKAGE_URIS=$(PACKAGE_URIS)
	@echo PACKAGE_RELATIVE_PATHS=$(PACKAGE_RELATIVE_PATHS)
	@echo GO_FILES=$(GO_FILES)
	@echo VERSION=$(VERSION)
	@echo GO_BUILD_VERSION_PKG=$(GO_BUILD_VERSION_PKG)
	@echo LD_FLAGS=$(LD_FLAGS)
	@echo ARCH=$(ARCH)
	@echo DATE=$(DATE)
	@echo USER=$(USER)

define write_build_info
	@echo PROJECT= $(PROJECT) > make_build.info
	@echo GIT_BRANCH= $(GIT_BRANCH) >> make_build.info
	@echo GIT_COMMIT=$(GIT_COMMIT) >> make_build.info
	@echo GIT_STATUS_HASH=$(GIT_STATUS_HASH) >> make_build.info
	@echo GIT_STATUS=$(GIT_STATUS) >> make_build.info
	@echo GIT_DIRTY=$(GIT_DIRTY) >> make_build.info
	@echo GIT_LAST_DATE=$(GIT_LAST_DATE) >> make_build.info
	@echo GOPATH=$(GOPATH) >> make_build.info
	@echo GO_VERSION=$(GO_VERSION) >> make_build.info
	@echo GO=$(GO) >> make_build.info
	@echo GO_BUILD=$(GO_BUILD) >> make_build.info
	@echo GO_TEST=$(GO_TEST) >> make_build.info
	@echo ROOT_URL=$(ROOT_URL) >> make_build.info
	@echo PACKAGE_LIST=$(PACKAGE_LIST) >> make_build.info
	@echo PACKAGE_URIS=$(PACKAGE_URIS) >> make_build.info
	@echo PACKAGE_RELATIVE_PATHS=$(PACKAGE_RELATIVE_PATHS) >> make_build.info
	@echo GO_FILES=$(GO_FILES) >> make_build.info
	@echo VERSION=$(VERSION) >> make_build.info
	@echo GO_BUILD_VERSION_PKG=$(GO_BUILD_VERSION_PKG) >> make_build.info
	@echo LD_FLAGS=$(LD_FLAGS) >> make_build.info
	@echo ARCH=$(ARCH) >> make_build.info
	@echo DATE=$(DATE) >> make_build.info
	@echo USER=$(USER) >> make_build.info
endef

.PHONY: common-style
common-style:
	@echo ">> checking code style"
	@fmtRes=$$(gofmt -d $$(find . -path ./vendor -prune -o -name '*.go' -print)); \
	if [ -n "$${fmtRes}" ]; then \
		echo "gofmt checking failed!"; echo "$${fmtRes}"; echo; \
		exit 1; \
	fi