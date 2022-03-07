import json
import base64
import os
from typing import Any, Union

class __info__:
	name = "DBJSON"
	version = ["0.1-alpha", 0.1]
	type = "dbjson"

class __default__:
	dbjson_data = {
		"info": {"type": "dbjson", "version": __info__.version},
		"tables": []
	}
	types = ["int", "float", "str", "bytes", "bool"]

class __func__:
	def encode_base64(data: Any) -> bytes:
		return base64.b64encode(json.dumps(data).encode())

	def decode_base64(data: bytes) -> Any:
		return json.loads(base64.b64decode(data).decode())

	def load_dbjson(path: str) -> dict:
		with open(path, "r") as file:
			return json.load(file)

	def upload_dbjson(path: str, data) -> None:
		with open(path, "w") as file:
			json.dump(data, file)
	
	def test_colons(colons: dict[str, tuple[str, bool]]) -> tuple[bool, Union[list, list[str]]]:
		# Создание переменых для проверки
		colons_items, errors, colons_names, type_errors, gc = list(colons.items()), [], [], 0, 0
		# Проверка
		for colon in colons_items:
			colons_names.append(colon[0])
			type_errors += 0 if (colon[1][0] in __default__.types) else 1
		for colon_name in colons_names:
			gc += colons_names.count(colon_name)
		# Обработка результатов
		if not(len(colons_names) == gc):
			errors.append("colons_names")
		if type_errors > 0:
			errors.append("types")
		# Создание и выдача результата
		return (len(errors) == 0), errors
	
	def test_primary_key(colons: list[tuple[str, bool]], db_data: list, add_data: list) -> bool:
		if len(db_data) != 0:
			for idx, is_primary in enumerate([i[1] for i in colons]):
				if is_primary:
					if [i[idx] for i in db_data].count(add_data[idx]) != 0:
						return False
		return True

	def type_handler(data: Union[int, float, str, bool, bytes], _type: str) -> Union[int, float, str, bool, bytes]:
		if _type == "bytes":
			return data if (type(data) == bytes) else __func__.encode_base64(data)
		else:
			return data if (type(data) == eval(_type)) else eval(f"{_type}(data)")

# Классы ошибок
class TableIndexError(Exception):
	def __init__(self, message: str="There is no such table in the DBJSON database") -> None:
		self.message = message
	
	def __str__(self) -> str:
		return self.message

class TableExistsError(Exception):
	def __init__(self, table_name: str, message: str="Table %s does not exist") -> None:
		self.message = message
		self.table_name = table_name
	
	def __str__(self) -> str:
		return self.message % self.table_name

class ColonParametersError(Exception):
	def __init__(self, errors: list, message: str="An error occurred with: ") -> None:
		self.message = message
		self.errors = errors
	
	def __str__(self) -> str:
		return self.message + ", ".join(self.errors)

class DataListLengthError(Exception):
	def __init__(self, message: str="Number of data in the list does not match the number of colons") -> None:
		self.message = message
	
	def __str__(self):
		return self.message

class DataPrimaryKeyError(Exception):
	def __init__(self, message: str="Unable to add the same data under primary key") -> None:
		self.message = message
	
	def __str__(self):
		return self.message

class TableIdOrNameNotFound(Exception):
	def __init__(self, message: str="Not specified table name or at least table ID") -> None:
		self.message = message
	
	def __str__(self):
		return self.message

# Рабочий класс
class DBJSON():
	def __init__(self, path: str) -> None:
		self.path = os.path.abspath(path)
		if os.path.exists(path):
			if not os.path.isfile(path):
				raise IsADirectoryError()
		else:
			__func__.upload_dbjson(path, __default__.dbjson_data)
	
	def tables_list(self, *, data=None) -> list[str]:
		return [i["name"] for i in (__func__.load_dbjson(self.path) if (data is None) else data)["tables"]]
	
	def exists_table(self, table_name: str, *, data=None) -> bool:
		return (table_name in self.tables_list(data=(__func__.load_dbjson(self.path) if (data is None) else data)))
	
	def get_table_index(self, table_name: str, *, data=None) -> int:
		index, c = None, 0
		for i in self.tables_list(data=(__func__.load_dbjson(self.path) if (data is None) else data)):
			if i == table_name:
				index = c
				break
			c += 1
		if index is None:
			raise TableExistsError(table_name)
		return index

	def colons_list(self, table_id_or_name: Union[int, str], *, data=None) -> dict[str, tuple[str, bool]]:
		data = __func__.load_dbjson(self.path) if (data is None) else data
		if table_id_or_name is str:
			if self.exists_table(table_id_or_name, data=data):
				return data["tables"][self.get_table_index(table_id_or_name, data=data)]["colons"]
			else:
				raise TableIndexError()
		elif table_id_or_name is int:
			try:
				return data["tables"][table_id_or_name]["colons"]
			except IndexError:
				raise TableIndexError()
	
	def get_colon_index(self, table_id_or_name: Union[int, str], colon_name: str, *, data=None) -> Union[int, None]:
		colons_list = list(self.colons_list(table_id_or_name, data=data).key())
		return colons_list.index(colon_name)

	def create_table(self, table_name: str, colons: dict[str, tuple[str, bool]]) -> None:
		data = __func__.load_dbjson(self.path)
		if not(table_name in self.tables_list(data=data)):
			successful, errors = __func__.test_colons(colons)
			if not successful:
				raise ColonParametersError(errors)
			data["tables"].append({"name": table_name, "colons": colons, "data": []})
			__func__.upload_dbjson(self.path, data)

	def add_data(self, table_name: str, data: list) -> None:
		db_data = __func__.load_dbjson(self.path)
		if self.exists_table(table_name, data=db_data):
			cl = list(self.colons_list(table_name).values())
			if len(cl) == len(data):
				for idx, d in enumerate(data):
					data[idx] = __func__.type_handler(d, cl[idx][0])
				table_index = self.get_table_index(table_name)
				if not __func__.test_primary_key(cl, db_data["tables"][table_index]["data"], data):
					raise DataPrimaryKeyError()
				db_data["tables"][table_index]["data"].append(data)
				__func__.upload_dbjson(self.path, db_data)
			else:
				raise DataListLengthError()
		else:
			raise TableExistsError(table_name)
	
	def find_data(self, table_name: str, colon_name: str, value: Any, *, max_count: int=-1, data=None) -> Union[list[tuple[int, list]], list]:
		db_data, finded_data = (__func__.load_dbjson(self.path) if (data is None) else data), []
		table_index = self.get_table_index(table_name, data=db_data)
		colons_index = self.get_colon_index(table_index, colon_name, data=db_data)
		for idx, d in enumerate(db_data["tables"][table_index]["data"]):
			if max_count == len(finded_data):
				break
			if d[colons_index] == value:
				finded_data.append((idx, d))
		return finded_data
	
	def delect_data(self, table_name: str, colon_name: str, value: Any, *, max_delect: int=-1, data=None) -> None:
		db_data = __func__.load_dbjson(self.path) if (data is None) else data
		datas, table_idx = self.find_data(table_name, colon_name, value, max_count=max_delect, data=db_data), self.get_table_index(table_name, data=db_data)
		for i in datas:
			db_data["tables"][table_idx].pop(i[0])
		__func__.upload_dbjson(self.path, db_data)