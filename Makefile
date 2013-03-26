DIST ?=dist
EXEC ?=colog
INSTALL_OPTS ?=

default: $(DIST)/build/$(EXEC)/$(EXEC)

$(DIST)/setup-config: colog.cabal
	cabal configure

HSSRCS := $(shell find . -name '*.hs')

$(DIST)/build/$(EXEC)/$(EXEC): $(DIST)/setup-config $(HSSRCS)
	cabal build

.PHONY: run
run: $(DIST)/build/$(EXEC)/$(EXEC)
	time ./$(DIST)/build/$(EXEC)/$(EXEC)

colog:
	cabal install

depends: submodules
	( cd bzlib-conduit/ && cabal install $(INSTALL_OPTS) )
	cabal install --only-dependencies $(INSTALL_OPTS)

install: depends
	cabal install

.PHONY: submodules
submodules:
	git submodule update --init

.PHONY: clean
clean:
	rm -r ./$(DIST)
