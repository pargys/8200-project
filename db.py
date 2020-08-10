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
    pass

@dataclass_json
@dataclass
class SelectionCriteria(db_api.SelectionCriteria):
    pass

@dataclass_json
@dataclass
class DBTable(db_api.DBTable):
    def __init__(self, name: str, fields: List[DBField], key_field_name:  str, hash_index=None):
        self.name = name
        self.fields = fields
        self.key_field_name = key_field_name
        self.hash_index = hash_index if hash_index else [False for i in range(len(fields))]

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

    def insert_into_hash_index(self, values):
        for i in range(len(self.hash_index)):  # update hash index
            if self.hash_index[i]:
                indexes_file = shelve.open(
                    os.path.join('db_files', self.name + '_' + self.fields[i].name + '_hash_index.db'), writeback=True)
                if values.get(self.fields[i].name):
                    indexes_file[values[self.fields[i].name]].append(values[self.key_field_name])
                indexes_file.close()

    def insert_record(self, values: Dict[str, Any]) -> None:
        has_primary_key = True if values.get(self.key_field_name) else False
        if not has_primary_key:
            raise ValueError
        s = shelve.open(os.path.join('db_files', self.name + '.db'), writeback=True)
        indexes_file = shelve.open(os.path.join('db_files', self.name + '_index.db'), writeback=True)

        try:
            is_record_exists = True if s[self.name].get(values[self.key_field_name]) else False
            if is_record_exists:
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

            self.insert_into_hash_index(values)
        finally:
            s.close()
            indexes_file.close()

    def delete_from_hash_index(self, s, key):
        for i in range(len(self.hash_index)):
            if self.hash_index[i]:
                if s[self.name][key][self.fields[i].name]:
                    indexes_file = shelve.open(
                        os.path.join('db_files', self.name + '_' + self.fields[i].name + '_hash_index.db'),
                        writeback=True)
                    indexes_file[s[self.name][key][self.fields[i].name]].remove(key)
                    indexes_file.close()

    def delete_record(self, key: Any) -> None:
        s = shelve.open(os.path.join('db_files', self.name + '.db'), writeback=True)
        try:
            is_key_exist = True if s[self.name].get(key) else False
            if not is_key_exist:
                raise ValueError

            self.delete_from_hash_index(s, key)
            s[self.name].pop(key)
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
            is_key_exist = True if s[self.name].get(key) else False
            if not is_key_exist:
                raise ValueError

            row = s[self.name][key]
        finally:
            s.close()
        row[self.key_field_name] = key
        return row

    def update_hash_index(self, s, values, key, field):
        indexes_file = shelve.open(os.path.join('db_files', self.name + '_' + field + '_hash_index.db'), writeback=True)
        indexes_file[s[self.name][key][field]].remove(key)
        if values[field]:
            if None is indexes_file.get(values[field]):
                indexes_file[values[field]] = list()
            indexes_file[values[field]].append(key)
        indexes_file.close()

    def update_record(self, key: Any, values: Dict[str, Any]) -> None:
        s = shelve.open(os.path.join('db_files', self.name + '.db'), writeback=True)
        try:
            is_key_exist = True if s[self.name].get(key) else False
            if not is_key_exist:
                raise ValueError
            if values.get(self.key_field_name): # cannot update the primary key
                raise ValueError
            updated_row = {}
            for dbfield in self.fields:
                field = dbfield.name
                if field == self.key_field_name:
                    continue

                if values.get(field):
                    index = self.fields.index(dbfield)
                    if self.hash_index[index]:
                        self.update_hash_index(s, values, key, field)
                    updated_row[field] = values[field]
                    values.pop(field)

                else:
                    updated_row[field] = s[self.name][key][field]

            if values: # insert unnecessary field
                raise ValueError
            s[self.name][key] = updated_row
        finally:
            s.close()

    def query_on_key(self, s, criterion):
        if s[self.name].get(criterion.value):
            result = s[self.name][criterion.value]
            result[self.key_field_name] = criterion.value
            return [result]
        return []

    def query_on_index(self, s, criteria):
        indexes = []
        for i in range(len(self.hash_index)):  # if the criterion is on a field that has an index
            if self.hash_index[i]:
                if indexes:
                    break
                for criterion in criteria:
                    if self.fields[i].name == criterion.field_name and criterion.operator == "=":
                        indexes_file = shelve.open(
                            os.path.join('db_files', self.name + '_' + criterion.field_name + '_hash_index.db'),
                            writeback=True)
                        indexes = indexes_file[criterion.value]
                        indexes_file.close()
                        if not indexes:
                            return [], True
                        break
        desired_lines = []
        if indexes:
            for index in indexes:
                for criterion in criteria:
                    if self.__is_condition_hold(s[self.name][index], criterion) is False:
                        break
                else:
                    result = s[self.name][index]
                    result[self.key_field_name] = index
                    desired_lines.append(result)
            return desired_lines, True

        return desired_lines, False

    def query_table(self, criteria: List[SelectionCriteria]) \
            -> List[Dict[str, Any]]:
        s = shelve.open(os.path.join('db_files', self.name + '.db'), writeback=True)
        try:
            for criterion in criteria: # if the criterion is on key
                if criterion.field_name == self.key_field_name and criterion.operator == '=':
                    return self.query_on_key(s, criterion)

            desired_lines, is_query_on_index = self.query_on_index(s, criteria)
            if is_query_on_index:
                return desired_lines

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

    def is_field_exist(self, s, field_to_index):
        for row in s[self.name]:  # if the field_to_index isn't exist(just 1 iteration)
            if None is s[self.name][row].get(field_to_index):
                return False
            break
        return True

    def create_index(self, field_to_index: str) -> None:
        if field_to_index == self.key_field_name: # no need to index the primary key
            return

        fields_names = [field.name for field in self.fields]
        index = fields_names.index(field_to_index)

        is_index_exist = True if self.hash_index[index] else False
        if is_index_exist:
            return

        s = shelve.open(os.path.join('db_files', self.name + '.db'), writeback=True)
        indexes_file = shelve.open(os.path.join('db_files', self.name + '_' + field_to_index + '_hash_index.db'), writeback=True)
        data_file = shelve.open(os.path.join('db_files', 'DataBase' + '.db'), writeback=True)
        try:
            if not self.is_field_exist(s, field_to_index):
                raise ValueError

            for row in s[self.name]:
                if None is s[self.name][row][field_to_index]:
                    continue
                if None is indexes_file.get(s[self.name][row][field_to_index]):
                    indexes_file[s[self.name][row][field_to_index]] = list()
                indexes_file[s[self.name][row][field_to_index]].append(row)

            data_file[self.name]["hash_index"][index] = True
            self.hash_index[index] = True
        finally:
            s.close()
            indexes_file.close()


@dataclass_json
@dataclass
class DataBase(db_api.DataBase):
    db_tables = {}

    def __init__(self):
        s = shelve.open(os.path.join('db_files', 'DataBase' + '.db'), writeback=True)
        for table_name in s:
            DataBase.db_tables[table_name] = DBTable(table_name, s[table_name]["fields"], s[table_name]["key_field_name"])

    def update_DataBase_file(self, table_name, fields, key_field_name):
        s = shelve.open(os.path.join('db_files', 'DataBase' + '.db'), writeback=True)
        try:
            s[table_name] = {}
            s[table_name]["fields"] = fields
            s[table_name]["key_field_name"] = key_field_name
            s[table_name]['hash_index'] = [False for i in range(len(fields))]
        finally:
            s.close()

    def create_table(self,
                     table_name: str,
                     fields: List[DBField],
                     key_field_name: str) -> DBTable:
        is_key_field_name_exist = True if key_field_name in [field.name for field in fields] else False
        if not is_key_field_name_exist:
            raise ValueError
        is_table_exist = True if DataBase.db_tables.get(table_name) else False
        if is_table_exist:
            raise ValueError

        self.update_DataBase_file(table_name, fields, key_field_name)
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
        s = shelve.open(os.path.join('db_files', 'DataBase.db'), writeback=True)
        try:
            for field in s[table_name]['fields']:
                if s[table_name]["hash_index"][s[table_name]["fields"].index(field)]:

                    a = (os.path.join('db_files', table_name + '_' + field.name + '_hash_index.db.bak'))
                    os.remove(a)
                    a = (os.path.join('db_files', table_name + '_' + field.name + '_hash_index.db.dat'))
                    os.remove(a)
                    a = (os.path.join('db_files', table_name + '_' + field.name + '_hash_index.db.dir'))
                    os.remove(a)
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


