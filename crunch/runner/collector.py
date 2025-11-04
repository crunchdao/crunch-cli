import os
import shutil
from abc import ABC, abstractmethod
from typing import List, Optional

import pandas

from crunch.utils import write


class PredictionCollector(ABC):

    @abstractmethod
    def append(self, prediction: pandas.DataFrame) -> None:
        """
        Collect a new dataframe.
        """

    @abstractmethod
    def persist(self, file_path: str) -> None:
        """
        Persist the entire dataframe to a file.

        This is a terminal operation, do not call `append' again afterwards.
        """

    @abstractmethod
    def discard(self) -> None:
        """
        Discard the contents of the collector and close resources if necessary.

        This is a terminal operation, do not call `append' again afterwards.
        """

    @property
    @abstractmethod
    def is_write_index(self) -> bool:
        ...

    def __del__(self):
        self.discard()


class MemoryPredictionCollector(PredictionCollector):

    def __init__(
        self,
        write_index: bool = False,
    ):
        self.write_index = write_index

        self.dataframes: List[pandas.DataFrame] = []

    def append(self, prediction: pandas.DataFrame) -> None:
        self.dataframes.append(prediction)

    def persist(self, file_path: str):
        dataframe = pandas.concat(self.dataframes)
        self._clear()

        write(
            dataframe,
            file_path,
            kwargs={
                "index": self.write_index,
            },
        )

    def discard(self):
        self._clear()

    @property
    def is_write_index(self) -> bool:
        return self.write_index

    def _clear(self):
        self.dataframes.clear()


class FilePredictionCollector(PredictionCollector):

    def __init__(self):
        import tempfile

        import pyarrow
        import pyarrow.parquet

        self.temporary_directory = tempfile.TemporaryDirectory()
        self.temporary_file_path = os.path.join(self.temporary_directory.name, "file.parquet")

        self.schema: Optional[pyarrow.Schema] = None
        self.writer: Optional[pyarrow.parquet.ParquetWriter] = None

    def append(self, prediction: pandas.DataFrame) -> None:
        import pyarrow

        table = pyarrow.Table.from_pandas(prediction)

        if self.writer is None:
            import pyarrow.parquet

            self.schema = table.schema
            self.writer = pyarrow.parquet.ParquetWriter(self.temporary_file_path, self.schema)

        self.writer.write_table(table)

    def persist(self, file_path: str):
        self._reset()

        if self.writer is None:
            raise RuntimeError("no data collected")

        shutil.move(self.temporary_file_path, file_path)

        self._close()

    def discard(self):
        self._reset()
        self._close()

    @property
    def is_write_index(self) -> bool:
        return True

    def _reset(self):
        if self.writer is not None:
            self.writer.close()

        self.writer = None
        self.schema = None

    def _close(self):
        self.temporary_directory.cleanup()
