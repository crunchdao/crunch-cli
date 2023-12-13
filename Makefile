PYTHON=python
PIP=$(PYTHON) -m pip

init:
	$(PIP) install -r requirements.txt

install: init
	$(PIP) install -e .

uninstall:
	$(PIP) uninstall crunch

test:
	$(PYTHON) -m pytest -v

build:
	rm -rf build *.egg-info dist
	python setup.py sdist bdist_wheel

.PHONY: init install uninstall test build
