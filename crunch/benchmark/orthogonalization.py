import logging

import pandas
import tqdm

from .. import orthogonalization, store


def benchmark(
    prediction: pandas.DataFrame,
):
    success, failure = 0, 0

    def print_status(print=print):
        print(f"success={success} failure={failure}")

    progress = tqdm.tqdm()

    try:
        while True:
            try:
                scores = orthogonalization.run(prediction, as_dataframe=False)

                progress.update()
                success += 1

                if store.debug:
                    print_status(progress.write)

                    for score in scores:
                        details = ", ".join((
                            f"{detail.key}:{detail.value}"
                            for detail in score.details
                        ))

                        progress.write(f"metric={score.metric.name} details=[{details}]")
            except Exception:
                logging.exception("orthogonalization failed")
                failure += 1
                print_status()

    except KeyboardInterrupt:
        progress.close()

        print()
        print_status()
