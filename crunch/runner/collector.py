import abc
import os
import shutil
import typing

import pandas

from .. import utils


class PredictionCollector(abc.ABC):

    @abc.abstractmethod
    def append(self, prediction: pandas.DataFrame) -> None:
        """
        Collect a new dataframe.
        """

    @abc.abstractmethod
    def persist(self, file_path: str) -> None:
        """
        Persist the entire dataframe to a file.

        This is a terminal operation, do not call `append' again afterwards.
        """

    @abc.abstractmethod
    def discard(self) -> None:
        """
        Discard the contents of the collector and close resources if necessary.

        This is a terminal operation, do not call `append' again afterwards.
        """


class MemoryPredictionCollector(PredictionCollector):

    def __init__(
        self,
        write_index=False,
    ):
        self.write_index = write_index

        self.dataframes: typing.List[pandas.DataFrame] = []

    def append(self, prediction):
        self.dataframes.append(prediction)

    def persist(self, file_path: str):
        dataframe = pandas.concat(self.dataframes)
        self._clear()

        utils.write(
            dataframe,
            file_path,
            kwargs={
                "index": self.write_index,
            }
        )

    def discard(self):
        self._clear()

    def _clear(self):
        self.dataframes.clear()


class FilePredictionCollector(PredictionCollector):

    def __init__(self):
        import tempfile

        import pyarrow
        import pyarrow.parquet

        self.temporary_directory = tempfile.TemporaryDirectory()
        self.temporary_file_path = os.path.join(self.temporary_directory.name, "file.parquet")

        self.schema: typing.Optional[pyarrow.Schema] = None
        self.writer: typing.Optional[pyarrow.parquet.ParquetWriter] = None

    def append(self, prediction):
        import pyarrow

        table = pyarrow.Table.from_pandas(prediction)

        if self.writer is None:
            import pyarrow.parquet

            self.schema = table.schema
            self.writer = pyarrow.parquet.ParquetWriter(self.temporary_file_path, self.schema)

        self.writer.write_table(table)

    def persist(self, file_path: str):
        self._reset()

        shutil.move(self.temporary_file_path, file_path)

        self._close()

    def discard(self):
        self._reset()
        self._close()

    def _reset(self):
        if self.writer is not None:
            self.writer.close()

        self.writer = None
        self.schema = None

    def _close(self):
        self.temporary_directory.cleanup()
