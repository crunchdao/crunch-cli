# CrunchDAO CLI

[![PyTest](https://github.com/crunchdao/crunch-cli/actions/workflows/pytest.yml/badge.svg)](https://github.com/crunchdao/crunch-cli/actions/workflows/pytest.yml)

This Python library is designed for the CrunchDAO Platform, offering convenient access to competition's data and enabling effortless submission. When utilized in the command-line interface (CLI), its goal is to deliver a user experience akin to GitHub, enabling you to seamlessly push the code from your local environment.

## Installation

Use [pip](https://pypi.org/project/crunch-cli/) to install the `crunch-cli`.

```bash
pip install crunch-cli --upgrade
```

## Usage

```python
import crunch
crunch = crunch.load_notebook()

# Getting the data
X_train, y_train, X_test = crunch.load_data()
```

`crunch.load_data()` accept arguments for `read_parquet`.

```python
crunch.load_data(
  engine="fastparquet"
)
```

## Submit with Crunch CLI

```bash
Usage: crunch push [OPTIONS]

  Send the new submission of your code.

Options:
  -m, --message TEXT      Specify the change of your code. (like a commit
                          message)

  -e, --main-file TEXT    Entrypoint of your code.  [default: main.py]
  --model-directory TEXT  Directory where your model is stored.  [default:
                          resources]

  --help                  Show this message and exit.
```

## Detecting the environment

Detecting whether you are running inside the runner or not, allows you to configure your program more precisely.

```python
import crunch

if crunch.is_inside_runner:
  print("running inside the runner")
else:
  print("running elsewhere")

  model.enable_debug()
  logger.set_level("TRACE")
```

## Competition Links

- [Competition Platform](https://www.crunchdao.com)
- [ADIA Lab Market Prediction Competition](https://www.crunchdao.com/live/adialab)
- [see more](https://hub.crunchdao.com/)

## Contributing

Pull requests are always welcome! If you find any issues or have suggestions for improvements, please feel free to submit a pull request or open an issue in the GitHub repository.

## License

[MIT](https://choosealicense.com/licenses/mit/)
