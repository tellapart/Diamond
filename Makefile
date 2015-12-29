DESTDIR=/
PROJECT=diamond
VERSION :=$(shell bash version.sh )
FORMATTED_VERSION :=$(shell bash version.sh | sed 's/-/_/g' )
RELEASE :=$(shell ls -1 dist/*.noarch.rpm 2>/dev/null | wc -l )
HASH	:=$(shell git rev-parse HEAD )
DISTRO=precise
BUILDROOT=$(shell pwd)/dist-build
ITERATION=1

all:
	@echo "make run      - Run Diamond from this directory"
	@echo "make config   - Run a simple configuration CLI program"
	@echo "make watch    - Watch and continuously run tests"
	@echo "make test     - Run tests"
	@echo "make docs     - Build docs"
	@echo "make sdist    - Create source package"
	@echo "make bdist    - Create binary package"
	@echo "make pypi     - Update PyPI package"
	@echo "make install  - Install on local system"
	@echo "make develop  - Install on local system in development mode"
	@echo "make rpm      - Generate a rpm package"
	@echo "make pexrpm   - Generate a rpm package from PEX."
	@echo "make deb      - Generate a deb package"
	@echo "make sdeb     - Generate a deb source package"
	@echo "make ebuild   - Generate a ebuild package"
	@echo "make tar      - Generate a tar ball"
	@echo "make clean    - Get rid of scratch and byte files"
	@echo "make cleanws  - Strip trailing whitespaces from files"

run:
	./bin/diamond --configfile=conf/diamond.conf --foreground --log-stdout

config:
	./bin/diamond-setup --configfile=conf/diamond.conf

watch:
	watchr test.watchr

test:
	./test.py

docs: version
	./build_doc.py --configfile=conf/diamond.conf

sdist: version
	./setup.py sdist --prune

bdist: version
	./setup.py bdist --prune

install: version
	./setup.py install --root $(DESTDIR)

develop: version
	USE_SETUPTOOLS=1 ./setup.py develop

rpm: buildrpm

buildrpm: sdist
	./setup.py bdist_rpm \
		--release=`ls dist/*.noarch.rpm | wc -l`

pex: version
	./pants binary src/diamond

pexrpm: clean pex
	mkdir -vp $(BUILDROOT)/usr/bin
	cp dist/diamond.pex $(BUILDROOT)/usr/bin/diamond
	fpm -t rpm \
		-s dir \
		--version $(FORMATTED_VERSION) \
		--iteration $(ITERATION) \
		-n $(PROJECT) \
		-C $(BUILDROOT) \
		-p $(BUILDROOT) \
		--before-install rpm/preinstall \
		--force

deb: builddeb

sdeb: buildsourcedeb

builddeb: version
	dch --newversion $(VERSION) --distribution unstable --force-distribution -b "Last Commit: $(shell git log -1 --pretty=format:'(%ai) %H %cn <%ce>')"
	dch --release  "new upstream"
	./setup.py sdist --prune
	mkdir -p $(BUILDROOT)
	tar -C $(BUILDROOT) -zxf dist/$(PROJECT)-$(VERSION).tar.gz
	(cd $(BUILDROOT)/$(PROJECT)-$(VERSION) && debuild -us -uc -v$(VERSION))
	@echo "Package is at $(BUILDROOT)/$(PROJECT)_$(VERSION)_all.deb"

buildsourcedeb: version
	dch --newversion $(VERSION)~$(DISTRO) --distribution $(DISTRO) --force-distribution -b "Last Commit: $(shell git log -1 --pretty=format:'(%ai) %H %cn <%ce>')"
	dch --release  "new upstream"
	./setup.py sdist --prune
	mkdir -p $(BUILDROOT)
	tar -C $(BUILDROOT) -zxf dist/$(PROJECT)-$(VERSION).tar.gz
	(cd $(BUILDROOT)/$(PROJECT)-$(VERSION) && debuild -S -sa -v$(VERSION))
	@echo "Source package is at $(BUILDROOT)/$(PROJECT)_$(VERSION)~$(DISTRO)_source.changes"

ebuild: buildebuild

buildebuild: version
	cat gentoo/diamond.ebuild | sed "s/GIT_HASH/${HASH}/" >> gentoo/diamond-$(VERSION).ebuild
	@echo "ebuild is at gentoo/diamond-$(VERSION).ebuild"

tar: sdist

clean:
	./setup.py clean
	rm -rf dist $(BUILDROOT) MANIFEST .tox *.log
	find . -name '*.pyc' -delete

cleanws:
	find . -name '*.py' -exec sed -i'' -e 's/[ \t]*$$//' {} \;

version:
	./version.sh > version.txt

vertest: version
	echo "${VERSION}"

reltest:
	echo "$(RELEASE)"

distrotest:
	echo ${DISTRO}

pypi:
	python setup.py sdist upload

.PHONY: run watch config test docs sdist bdist install pexrpm rpm buildrpm deb sdeb builddeb buildsourcedeb ebuild buildebuild tar clean cleanws version reltest vertest distrotest pypi
