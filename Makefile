PYTHON=python
PIP=$(PYTHON) -m pip

init:
	$(PIP) install -r requirements.txt

install: init
	$(PIP) install -e .
uninstall: init
	$(PIP) uninstall crunch

.PHONY: init install
