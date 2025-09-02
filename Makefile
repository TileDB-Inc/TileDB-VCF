# This Makefile captures common developer build and test use cases.

MAKEFLAGS += --no-print-directory

# print help by default
help:

# install 
# -------------------------------------------------------------------

# set default variable values, if non-null
build ?= Release

.PHONY: install
install: clean
	mkdir -p libtiledbvcf/build && \
		cd libtiledbvcf/build && \
		cmake .. -DCMAKE_BUILD_TYPE=${build} && \
		make -j && make install-libtiledbvcf
	pip install -v apis/python[test,dev]

# incremental compile and update python install
# -------------------------------------------------------------------
.PHONY: update
update:
	cd libtiledbvcf/build && make -j && make -j install-libtiledbvcf
	pip install -v apis/python[test,dev]

# test
# -------------------------------------------------------------------
.PHONY: test
test:
	cd libtiledbvcf/build && make -j check
	cd libtiledbvcf/test && ./run-cli-tests.sh ../build/ ./inputs/
	cd apis/java && ./gradlew assemble test
	pytest apis/python
	python -c "import tiledbvcf; print(tiledbvcf.version)"

# docs
# -------------------------------------------------------------------
.PHONY: notebooks
notebooks:
	@for f in `find examples -name "*.ipynb"`; do \
		jupyter nbconvert --execute --to=notebook --inplace $$f; \
	done

.PHONY: docs
docs:
	@ln -sf apis/python/src/tiledbvcf
	quartodoc build
	@rm tiledbvcf
	quarto render --fail-if-warnings

# format
# -------------------------------------------------------------------
.PHONY: check-format
check-format:
	@./ci/run-clang-format.sh . clang-format 0 \
		`find libtiledbvcf/src -name "*.cc" -or -name "*.h"`
	@./ci/run-clang-format.sh . clang-format 0 \
		`find libtiledbvcf/test -name "*.cc" -or -name "*.h"`

.PHONY: format
format:
	 @./ci/run-clang-format.sh . clang-format 1 \
		`find libtiledbvcf/src -name "*.cc" -or -name "*.h"`
	 @./ci/run-clang-format.sh . clang-format 1 \
		`find libtiledbvcf/test -name "*.cc" -or -name "*.h"`

# venv
# -------------------------------------------------------------------
.PHONY: venv
venv:
	@if [ ! -d venv ]; then \
		python3.9 -m venv venv; \
		venv/bin/pip install --upgrade pip; \
	fi
	@printf "Run the following command to activate the venv:\nsource venv/bin/activate\n"

# docker
# -------------------------------------------------------------------
.PHONY: docker
docker:
	docker build -t tiledbvcf-cli:dev -f docker/Dockerfile-cli . && \
	docker run --rm -t tiledbvcf-cli:dev tiledbvcf version && \
	docker build -t tiledbvcf-py:dev -f docker/Dockerfile-py . && \
	docker run --rm -t tiledbvcf-py:dev -c "import tiledbvcf; print(tiledbvcf.version)"

# wheel
# -------------------------------------------------------------------
.PHONY: wheel
wheel: clean
	docker run --rm -v `pwd`:/io quay.io/pypa/manylinux_2_28_x86_64 /io/apis/python/build-wheels.sh
	sudo chown -R ${USER}:$(id -gn) .

# clean
# -------------------------------------------------------------------
.PHONY: clean
clean:
	@rm -rf libtiledbvcf/build dist apis/python/build

.PHONY: cleaner
cleaner:
	@printf "*** dry-run mode: remove -n to actually remove files\n"
	git clean -ffdx -e .vscode -e dev -e venv -n

# help
# -------------------------------------------------------------------
define HELP
Usage: make rule [options]

Rules:
  install [options]   Build C++ library and install python module
  update              Incrementally build C++ library and update python module
  test                Run tests
  notebooks           Execute notebooks and update cell outputs
  docs                Render the documentation
  docker              Build and test docker images
  check-format        Run C++ format check
  format              Run C++ format
  venv                Create a virtual environment
  wheel               Build python wheel
  clean               Remove build artifacts

Options:
  build=BUILD_TYPE    Cmake build type = Release|Debug|RelWithDebInfo|Coverage [Release]
  prefix=PREFIX       Install location [${PWD}/dist]
  tiledb=TILEDB_DIST  Absolute path to custom TileDB build 

Examples:
  Install Release build

    make install

  Install Debug build of libtiledbvcf and libtiledb

    make install build=Debug

  Install Release build with custom libtiledb

    make install tiledb=$$PWD/../TileDB/dist

  Incrementally build C++ changes and update the python module

    make update


endef 
export HELP

.PHONY: help
help:
	@printf "$${HELP}"

