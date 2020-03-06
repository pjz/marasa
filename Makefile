
PROJ=marasa

.PHONY: default
default:
	@echo "wheel|$(PROJ) - build the $(PROJ) python package"
	@echo "venv - set up the venv for testing/use"
	@echo "pylint - run a linter on the codebase"
	@echo "mypy - run a typechecker on the codebase"
	@echo "release - tag a release and push it to github"
	@echo "clean - remove all build artifacts"

# specify the directory to make it in
VENV=.venv

PYTHON=python3.7
VBIN=$(VENV)/bin
PIP=$(VBIN)/pip

.PHONY: venv
venv $(VENV):
	virtualenv --python=$(PYTHON) $(VENV)
	# install wheels
	## vendored
	if [ -d vendor -a -n `ls vendor/*.whl` ]; then \
    	for f in vendor/*.whl ; do $(PIP) install $$f ; done \
	fi
	## this project
	$(PIP) install -e .

venv-dev $(VBIN)/pytest: $(VENV)
	$(PIP) install -e .[dev]

.PHONY: wheel $(PROJ)
wheel $(PROJ):
	$(PYTHON) setup.py bdist_wheel
	
.PHONY: pylint
pylint: $(VBIN)/pytest
	PYTHONWARNINGS="ignore,default:::$(PROJ)" \
	$(VBIN)/pytest $(PROJ) \
		--ignore=$(VENV)/lib \
		--pylint --pylint-rcfile=.pylintrc \
		--pylint-error-types=EF \
		-m pylint \
		--cache-clear \
		$(PYTEST_ARGS)
		--junit-xml=$@.xml

.PHONY: mypy
mypy: $(VBIN)/pytest
	PYTHONWARNINGS="ignore,default:::$(PROJ)" \
	$(VBIN)/pytest --mypy -m mypy $(PROJ) --junit-xml=$@.xml

.PHONY: test
test: $(VBIN)/pytest
	PYTHONWARNINGS="ignore,default:::$(PROJ)" \
	$(VBIN)/pytest tests $(PYTEST_ARGS)

.PHONY: ci
ci:: PYTEST_ARGS:=--junit-xml=$@.xml
ci:: pylint mypy test


.PHONY: release
release: venv
	@if git tag | grep -q `$(PYTHON) setup.py --version` ; then \
	        echo "Already released this version.";\
	        echo "Update the version number and try again.";\
	        exit 1;\
	fi
	@if [ `git status --short | wc -l` != 0 ]; then\
	        echo "Uncommited code. Aborting." ;\
	        exit 1;\
	fi
	VER=`$(PYTHON) setup.py --version` &&\
	$(PYTHON) setup.py bdist_wheel &&\
	git push &&\
	git tag --sign $$VER -m "Release v$$VER" &&\
	git push --tags &&\
	git checkout release &&\
	git merge $$VER &&\
	git push && git checkout master
	@echo "Released! Note you're now on the 'master' branch."


.PHONY: clean
clean:
	rm -rf $(VENV) build dist *.egg-info
	find . -name __pycache__ | xargs rm -rf
	find . -name \*.pyc | xargs rm -f

