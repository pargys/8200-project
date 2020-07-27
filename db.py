from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Type
from dataclasses_json import dataclass_json
import db_api
import shelve
import os

flag = False


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

    def __is_condition_hold(self, s: Dict[Any, Any], criterion: SelectionCriteria):
        if None is s[criterion.field_name]:
            return False
        if criterion.operator == "=":
            return s[criterion.field_name] == criterion.value
        if criterion.operator == "!=":
            return s[criterion.field_name] != criterion.value
        if criterion.operator == "<":
            return s[criterion.field_name] < criterion.value
        if criterion.operator == ">":
            return s[criterion.field_name] > criterion.value
        if criterion.operator == "<=":
            return s[criterion.field_name] <= criterion.value
        if criterion.operator == ">=":
            return s[criterion.field_name] >= criterion.value
        return eval(f'{s[criterion.field_name]}{criterion.operator}{criterion.value}')


    def count(self) -> int:
        s = shelve.open(os.path.join('db_files', self.name + '.db'), writeback=True)
        try:
            count_rows = len(s[self.name].keys())
        finally:
            s.close()
        return count_rows

    def insert_record(self, values: Dict[str, Any]) -> None:
        if None is values.get(self.key_field_name): # there is no primary key
            raise ValueError
        s = shelve.open(os.path.join('db_files', self.name + '.db'), writeback=True)
        try:
            if s[self.name].get(values[self.key_field_name]): # record already exists
                raise ValueError

            s[self.name][values[self.key_field_name]] = {}

            for dbfield in self.fields:
                field = dbfield.name
                if field == self.key_field_name:
                    continue
                s[self.name][values[self.key_field_name]][field] = values[field] if values.get(field) else None
                values.pop(field)
            if 1 < len(values): # insert unnecessary field
                self.delete_record(values[self.key_field_name])
                raise ValueError
        finally:
            s.close()

    def delete_record(self, key: Any) -> None:
        s = shelve.open(os.path.join('db_files', self.name + '.db'), writeback=True)
        try:
            if s[self.name].get(key):
                s[self.name].pop(key)
            else:
                raise ValueError
        finally:
            s.close()

    def delete_records(self, criteria: List[SelectionCriteria]) -> None:
        list_to_delete = self.query_table(criteria)
        for row in list_to_delete:
            key = row[self.key_field_name]
            self.delete_record(key)

    def get_record(self, key: Any) -> Dict[str, Any]:
        s = shelve.open(os.path.join('db_files', self.name + '.db'), writeback=True)
        try:
            if None is s[self.name].get(key): # if this key isn't exist
                raise ValueError

            row = s[self.name][key]
        finally:
            s.close()
        row[self.key_field_name] = key
        return row

    def update_record(self, key: Any, values: Dict[str, Any]) -> None:
        s = shelve.open(os.path.join('db_files', self.name + '.db'), writeback=True)
        try:
            if None is s[self.name].get(key): # if this key isn't exist
                raise ValueError
            if values.get(self.key_field_name): # cannot update the primary key
                raise ValueError
            updated_row = {}
            for dbfield in self.fields:
                field = dbfield.name
                if field == self.key_field_name:
                    continue

                if values.get(field):
                    updated_row[field] = values[field]
                    values.pop(field)
                else:
                    updated_row[field] = s[self.name][key][field]

            if values: # insert unnecessary field
                raise ValueError
            s[self.name][key] = updated_row
        finally:
            s.close()

    def query_table(self, criteria: List[SelectionCriteria]) \
            -> List[Dict[str, Any]]:
        s = shelve.open(os.path.join('db_files', self.name + '.db'), writeback=True)
        try:
            desired_lines = []
            for row in s[self.name]:
                for criterion in criteria:
                    if criterion.field_name == self.key_field_name:  # condition on key
                        if self.__is_condition_hold({criterion.field_name: row}, criterion) is False:
                            break

                    elif s[self.name][row].get(criterion.field_name) is None:  # if this key isn't exist
                        raise ValueError

                    elif self.__is_condition_hold(s[self.name][row], criterion) is False:
                        break

                else:
                    result = s[self.name][row]
                    result[self.key_field_name] = row
                    desired_lines.append(result)
        finally:
            s.close()
        return desired_lines

    def create_index(self, field_to_index: str) -> None:
        raise NotImplementedError


@dataclass_json
@dataclass
class DataBase(db_api.DataBase):
    db_tables = {}

    def __init__(self):
        s = shelve.open(os.path.join('db_files', 'DataBase' + '.db'), writeback=True)
        for table_name in s:
            DataBase.db_tables[table_name] = DBTable(table_name, s[table_name]["fields"], s[table_name]["key_field_name"])

    def create_table(self,
                     table_name: str,
                     fields: List[DBField],
                     key_field_name: str) -> DBTable:
        if key_field_name not in [field.name for field in fields]:
            raise ValueError
        if DataBase.db_tables.get(table_name): # if this table name already exist
            raise ValueError
        s = shelve.open(os.path.join('db_files', 'DataBase' + '.db'), writeback=True)
        try:
            s[table_name] = {}
            s[table_name]["fields"] = fields
            s[table_name]["key_field_name"] = key_field_name
        finally:
            s.close()
        s = shelve.open(os.path.join('db_files', table_name + '.db'), writeback=True)
        try:
            s[table_name] = {}
        finally:
            s.close()
        new_table = DBTable(table_name, fields, key_field_name)
        DataBase.db_tables[table_name] = new_table
        return new_table

    def num_tables(self) -> int:
        return len(DataBase.db_tables)

    def get_table(self, table_name: str) -> DBTable:
        if DataBase.db_tables.get(table_name):
            return DataBase.db_tables[table_name]
        raise ValueError

    def delete_table(self, table_name: str) -> None:
        if None is DataBase.db_tables.get(table_name):
            raise ValueError
        s = shelve.open(os.path.join('db_files', 'DataBase' + '.db'), writeback=True)
        try:
            s.pop(table_name)
        finally:
            s.close()
        DataBase.db_tables.pop(table_name)
        s = (os.path.join('db_files', table_name + ".db.bak"))
        os.remove(s)
        s = (os.path.join('db_files', table_name + ".db.dat"))
        os.remove(s)
        s = (os.path.join('db_files', table_name + ".db.dir"))
        os.remove(s)

    def get_tables_names(self) -> List[Any]:
        return [db_table for db_table in DataBase.db_tables.keys()]

    def query_multiple_tables(
            self,
            tables: List[str],
            fields_and_values_list: List[List[SelectionCriteria]],
            fields_to_join_by: List[str]
    ) -> List[Dict[str, Any]]:
        raise NotImplementedError
