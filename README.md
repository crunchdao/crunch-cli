# CrunchDAO CLI

CrunchDAO CLI is a Python library designed for the ADIA Lab Market Prediction Competition, offering convenient access to competition's data and enabling effortless submission. When utilized in the command-line interface (CLI), its goal is to deliver a user experience akin to GitHub, enabling you to seamlessly push the code from your local environment.

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

## Competition Links

- [CrunchDAO Tournament](https://www.crunchdao.com)
- [ADIA Lab Market Prediction Competition](https://www.crunchdao.com/live/adialab)

## Contributing

Pull requests are always welcome! If you find any issues or have suggestions for improvements, please feel free to submit a pull request or open an issue in the GitHub repository.

## License

[MIT](https://choosealicense.com/licenses/mit/)
