PYTHON=python3
PIP=$(PYTHON) -m pip

install:
	$(PIP) install -e .[test]
	$(MAKE) install-pth

install-runner:
	$(PIP) install -r requirements/runner.txt
	$(PIP) install --no-dependencies -e .
	$(MAKE) install-pth

uninstall:
	$(MAKE) uninstall-pth
	$(PIP) uninstall crunch

test:
	$(PYTHON) -m pytest -v

build:
	rm -rf build *.egg-info dist
	python setup.py sdist bdist_wheel

# an editable install does not install the `.pth`, it must be next to the editable install's own `.pth` to run after it
install-pth:
	$(PYTHON) -c "import shutil, sysconfig; shutil.copy('crunch-monkeypatch.pth', sysconfig.get_paths()['purelib'])"

uninstall-pth:
	$(PYTHON) -c "import pathlib, sysconfig; pathlib.Path(sysconfig.get_paths()['purelib'], 'crunch-monkeypatch.pth').unlink(missing_ok=True)"

.PHONY: install install-runner install-pth uninstall uninstall-pth test build
