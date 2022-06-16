from io import TextIOWrapper
from os import getenv

CSV_SEPARATOR = getenv("CSV_SEPARATOR", ",")
NA = "NA"
EMPTY = ""

class Writer:

	_file: TextIOWrapper

	_cursor: int = 0

	def __init__(self, file_path: str, header: list[str]) -> None:
		self._file = open(file_path, mode="w", encoding="utf8")
		self.write_row(header)

	def write_row(self, data: list) -> None:
		self._file.write(f"{self.create_csv_row([self._cursor or EMPTY] + list(map(lambda i: i or NA, data)))}\n")
		self._cursor += 1

	def close(self) -> None:
		self._file.close()
	
	def create_csv_row(self, data: list) -> str:
		return CSV_SEPARATOR.join(list(map(lambda item: f"\"{item}\"", data)))
