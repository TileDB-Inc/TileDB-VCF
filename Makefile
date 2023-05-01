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
	cd apis/python && python setup.py install

# incremental compile and update python install
# -------------------------------------------------------------------
.PHONY: update
update:
	cd libtiledbvcf/build && make -j && make install-libtiledbvcf
	cd apis/python && python setup.py install

# test
# -------------------------------------------------------------------
.PHONY: test
test:
	cd libtiledbvcf/build && make check
	cd libtiledbvcf/test && ./run-cli-tests.sh ../build/ ./inputs/
	cd apis/java && ./gradlew assemble test
	cd apis/spark && ./gradlew assemble test
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
	quarto render --fail-if-warnings

# format
# -------------------------------------------------------------------
.PHONY: check-format
check-format:
	@./ci/run-clang-format.sh . clang-format 0 \
		`find libtiledbvcf/src -name "*.cc" -or -name "*.h"`

.PHONY: format
format:
	 @./ci/run-clang-format.sh . clang-format 1 \
		`find libtiledbvcf/src -name "*.cc" -or -name "*.h"`

# clean
# -------------------------------------------------------------------
.PHONY: clean
clean:
	@rm -rf libtiledbvcf/build dist

.PHONY: cleaner
cleaner:
	@printf "*** dry-run mode: remove -n to actually remove files\n"
	git clean -ffdx -e .vscode -n

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
  check-format        Run C++ format check
  format              Run C++ format
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

