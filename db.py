from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Type
from dataclasses_json import dataclass_json
import db_api
import shelve
import os


DB_ROOT = Path('db_files')

@dataclass_json
@dataclass
class DBField(db_api.DBField):
    name: str
    type: Type


@dataclass_json
@dataclass
class SelectionCriteria(db_api.SelectionCriteria):
    field_name: str
    operator: str
    value: Any


@dataclass_json
@dataclass
class DBTable(db_api.DBTable):
    name: str
    fields: List[DBField]
    key_field_name: str

    def count(self) -> int:
        s = shelve.open(os.path.join('db_files', self.name + '.db'), writeback=True)
        try:
            count_rows = len(s[self.name].keys())
        finally:
            s.close()
        return count_rows

    def insert_record(self, values: Dict[str, Any]) -> None:
        if None == values.get(self.key_field_name): # there is no primary key
            raise ValueError
        s = shelve.open(os.path.join('db_files', self.name + '.db'), writeback=True)
        try:
            if s[self.name].get(values[self.key_field_name]): # record already exists
                s.close()
                raise ValueError

            s[self.name][values[self.key_field_name]] = {}

            for dbfield in self.fields:
                field = dbfield.name
                if field == self.key_field_name:
                    continue
                s[self.name][values[self.key_field_name]][field] = values[field] if values.get(field) else None
                values.pop(field)
            if 1 < len(values):
                self.delete_record(values[self.key_field_name])
                s.close()
                raise ValueError
        finally:
            s.close()

    def delete_record(self, key: Any) -> None:
        s = shelve.open(os.path.join('db_files', self.name + '.db'), writeback=True)
        try:
            if s[self.name].get(key):
                s[self.name].pop(key)
            else:
                s.close()
                raise ValueError
        finally:
            s.close()

    # def __is_condition_hold(self, key_field: Any, criterion: SelectionCriteria):
    def delete_records(self, criteria: List[SelectionCriteria]) -> None:
        s = shelve.open(os.path.join('db_files', self.name + '.db'), writeback=True)
        try:
            for row in s[self.name].values():
                for criterion in criteria:
                    #if the condition is on the key???

                    if self.__is_condition_hold(s, row, criterion):
                        break
                else:
                    self.delete_record(row)
        finally:
            s.close()

    def get_record(self, key: Any) -> Dict[str, Any]:
        s = shelve.open(os.path.join('db_files', self.name + '.db'), writeback=True)
        try:
            if None == s[self.name].get(key):
                s.close()
                raise ValueError
            else:
                row = s[self.name][key]
        finally:
            s.close()
        row[self.key_field_name] = key
        return row

    def update_record(self, key: Any, values: Dict[str, Any]) -> None:
        raise NotImplementedError

    def query_table(self, criteria: List[SelectionCriteria]) \
            -> List[Dict[str, Any]]:
        raise NotImplementedError

    def create_index(self, field_to_index: str) -> None:
        raise NotImplementedError


@dataclass_json
@dataclass
class DataBase(db_api.DataBase):
    db_tables = {}
    # Put here any instance information needed to support the API
    def create_table(self,
                     table_name: str,
                     fields: List[DBField],
                     key_field_name: str) -> DBTable:
        if self.db_tables.get(table_name): # if this table name already exist
            raise ValueError

        s = shelve.open(os.path.join('db_files', table_name + '.db'), writeback=True)
        try:
            s[table_name] = {}
        finally:
            s.close()
        new_table = DBTable(table_name, fields, key_field_name)
        self.db_tables[table_name] = new_table
        return new_table

    def num_tables(self) -> int:
        return len(self.db_tables)

    def get_table(self, table_name: str) -> DBTable:
        if self.db_tables.get(table_name):
            return self.db_tables[table_name]
        raise ValueError

    def delete_table(self, table_name: str) -> None:
        if None == self.db_tables.get(table_name):
            raise ValueError
        self.db_tables.pop(table_name)
        s = (os.path.join('db_files', table_name + ".db.bak"))
        os.remove(s)
        s = (os.path.join('db_files', table_name + ".db.dat"))
        os.remove(s)
        s = (os.path.join('db_files', table_name + ".db.dir"))
        os.remove(s)

    def get_tables_names(self) -> List[Any]:
        return [db_table for db_table in self.db_tables.keys()]

    def query_multiple_tables(
            self,
            tables: List[str],
            fields_and_values_list: List[List[SelectionCriteria]],
            fields_to_join_by: List[str]
    ) -> List[Dict[str, Any]]:
        raise NotImplementedError
