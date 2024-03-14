PYTHON=python
PIP=$(PYTHON) -m pip

install:
	$(PIP) install -e .

install-runner:
	$(PIP) install -r requirements/runner.txt
	$(PIP) install --no-dependencies -e .

uninstall:
	$(PIP) uninstall crunch

test:
	$(PYTHON) -m pytest -v

build:
	rm -rf build *.egg-info dist
	python setup.py sdist bdist_wheel

.PHONY: install uninstall test build
