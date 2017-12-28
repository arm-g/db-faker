"""Pg_database is responsible for postges implmentation of db_interface."""
from collections import OrderedDict

import psycopg2

from lib.db_interface import Db_handler
from utils.help import get_json_config, print_err, print_inf, print_warn


class Pg_handler(Db_handler):
    def __init__(self, state_column="is_faked"):
        """Initiating DB connection."""
        self.state_column = state_column
        self.conn = self.connect(get_json_config('connection.json'))
        self.cursor = self.conn.cursor()
        self.__data_inst, self.schemas_map, self.__schemas_config = {}, {}, {}

    def connect(self, db_params):
        """
        Establishe db connection according given parameters.

        Args:
            db_params (dict): Connection parameters
        Returns:
            cursor (object): Connection cursor
        """
        try:
            with psycopg2.connect(**db_params) as conn:
                return conn
        except Exception as e:
            print_err('Connection cannot be established by given parameters.')
            raise Exception(e.message)

    def __conf_schemas(self):
        """
        Get schemas list from config dict. flatten.

        Returns:
            schemas (list): Schema names
        """
        try:
            inheritants = self.schemas_config['inheritants']
            schemas_list = []
            for schm_def, schemas in inheritants.items():
                self.schemas_map.update({schema: schm_def for schema
                                         in schemas})
                schemas_list.extend(schemas)
            return schemas_list
        except Exception as e:
            print_err('Json structure definition is invalid.[1]')
            raise Exception(e.message)

    def __conf_schema_tables(self, schema):
        """
        Get tables list from config dict. flatten for given schema.

        Args:
            schema (str): Schema name
        Returns:
            tables (list): Schema names
        """
        schema = self.__get_inheritee(schema)
        try:
            return self.schemas_config['schemas'][schema]['tables'].keys()
        except Exception:
            raise Exception("""Schema "{}" wrong name. Take a look at the
                definition.""".format(schema))

    def __conf_table_columns(self, schema, table):
        """
        Get coulmns list from config dict. flatten for given table.

        Args:
            schema (str): Schema name
            table (str): Table name
        Returns:
            columns (list): Schema names
        """
        try:
            schema = self.__get_inheritee(schema)
            return self.schemas_config['schemas'][schema]['tables'][
                table]['columns'].keys()
        except Exception as e:
            print_err(e.message)
            raise Exception('Json structure definition is invalid.[2]')

    def __conf_table_keys(self, schema, table):
        """
        Get keys list from config dict. flatten for given table.

        !Note this is mandatory to have.

        Args:
            schema (str): Schema name
            table (str): Table name
        Returns:
            keys (list): Schema keys
        """
        try:
            schema = self.__get_inheritee(schema)
            return self.schemas_config['schemas'][schema]['tables'][
                table]['keys']
        except Exception as e:
            print_err(e.message)
            raise Exception('Json structure definition is invalid.[3]')

    def __schema_exists(self, schema):
        """
        Check if schema exists in information schema.

        Args:
            schema (str): Schema name
        Returns:
            Bool: True if exists False otherwise
        """
        select_schema_sql = """SELECT *
                                FROM information_schema.schemata
                                WHERE schema_name = '{}';""".format(schema)
        self.cursor.execute(select_schema_sql)
        return True if self.cursor.fetchone() else False

    def __table_exists(self, schema, table):
        """
        Check if table exists for a particular schema.

        Args:
            schema (str): Schema name
            table (str): Table name
        Returns:
            Bool: True if exists False otherwise
        """
        select_schema_sql = "SELECT to_regclass('{}.{}')".format(schema, table)
        self.cursor.execute(select_schema_sql)
        return True if self.cursor.fetchone()[0] else False

    def __column_exists(self, schema, table, column):
        """
        Check if column exists for a particular table.

        Args:
            schema (str): Schema name
            table (str): Table name
            column (str): Column name
        Returns:
            Bool: True if exists False otherwise
        """
        select_column_sql = """SELECT * FROM information_schema.columns
                                WHERE table_schema='{}' AND table_name='{}' AND
                                column_name='{}'""".format(schema, table,
                                                           column)
        self.cursor.execute(select_column_sql)
        return True if self.cursor.fetchone() else False

    def __conf_coulmn_rules(self, schema, table, column):
        """
        Get coulmns rules which will be used for data faking.

        Args:
            schema (str): Schema name
            table (str): Table name
        Returns:
            rules (list): User defined rulles
        """
        schema = self.__get_inheritee(schema)
        return self.schemas_config['schemas'][schema]['tables'][
            table]['columns'][column]

    def __get_inheritee(self, schema):
        """
        Get schema name which inherits its schema definition.

        Args:
            schema (str): Inheritant schema name
        Returns:
            schema (str): Inheritee schema name
        """
        try:
            return self.schemas_map[schema]
        except Exception:
            print_err("There isn't defined inheritee schema for {}".format(
                schema))

    def fake_table_data(self, schema, table, conf_table_keys):
        print_inf('Processing "{}.{}" table.'.format(schema, table), 1)
        counter, err_counter, row_exist = 0, 0, True
        while row_exist:
            failed_update = True
            while failed_update:
                update_query = self.__generate_update_query(schema, table,
                                                            conf_table_keys)
                try:
                    self.cursor.execute(update_query['sql'],
                                        update_query['values'])
                    row_exist = self.cursor.fetchone()
                    self.conn.commit()
                    if row_exist:
                        counter += 1
                    failed_update = False
                    break
                    # in case there is a unique constrain violation just pass
                except psycopg2.IntegrityError:
                    failed_update = True
                    err_counter += 1
                    self.conn.rollback()

            if counter and counter % 1000 == 0:
                print_inf('Updated {} rows in {}.{}.'.format(counter, schema,
                                                             table), 1)
                print_warn("Script passed by {} duplicate rows".format(
                    err_counter))
        if counter:
            print_inf('Updated {} rows in {}.{}.'.format(counter, schema,
                                                         table), 1)
        return True

    def fake_tables_data(self):
        schemas = self.__conf_schemas()
        length = len(schemas)
        current_pos = 0
        for schema in schemas:
            current_pos += 1
            print_warn("""Current position is {} from {}""".format(
                current_pos, length), 1)
            for table in self.__conf_schema_tables(schema):
                self.add_status_to_table(schema, table)
                conf_table_keys = self.__conf_table_keys(schema, table)
                self.fake_table_data(schema, table, conf_table_keys)
        return True

    def __generate_update_query(self, schema, table, conf_table_keys):
        """
        Generate update sql based on condition.

        Returns:
            {sql, values}(dict): Query, values.
        """
        keys = ", ".join(conf_table_keys)
        try:
            select_sql = """SELECT {} FROM {}.{} WHERE {} IS FALSE LIMIT 1
                FOR UPDATE""".format(keys, schema, table, self.state_column)
        except Exception as e:
            print_err(e.message)
            raise Exception('Select sql generating problem. {}'.format(
                select_sql))

        try:
            fake_data = OrderedDict()
            data_types = self.data_inst.get_types()
            for column in self.__conf_table_columns(schema, table):
                rule = self.__conf_coulmn_rules(schema, table, column)
                if rule['type'] in data_types:
                    fake_data[column] = data_types[rule['type']]
                else:
                    print_err("""Fake data type "{}" is not implemented
                                yet.""".format(rule['type']))
                    return False
            fake_data[self.state_column] = True
            set_sql = "=%s, ".join(fake_data.keys())
            set_sql += "=%s"
            update_sql = """UPDATE {}.{} SET {} WHERE {} IN ({}) RETURNING {}"""
            update_sql = update_sql.format(schema, table, set_sql, keys,
                                           select_sql, keys,)
            return {'sql': update_sql, 'values':
                    [val for _, val in fake_data.items()]}
        except Exception as e:
            print_err(e.message)
            raise Exception('Update query generating problem.')

    def conf_is_correct(self):
        """
        Check wheter defined config is correct.

        Means the defined schemas, tables, columns exist.

        Args:
            config (dict): Database tree started from schemas to its tcolumns
                            that need to be shuffled
        Returns:
            Bool: True if defined structure exist in connected DB
                    False otherwise
        """
        print_inf('Checking schemas correctness according to config.json')
        try:
            error = False
            for schema in self.__conf_schemas():
                if not self.__schema_exists(schema):
                    print_err("""Schema "{}" doesn't  exist""".format(schema))
                    error = True

                for table in self.__conf_schema_tables(schema):
                    if not self.__table_exists(schema, table):
                        print_err("""Table "{}" for schema "{}" doesn't
                                    exist""".format(table, schema))
                        error = True
                    for column in self.__conf_table_columns(schema, table):
                        if not self.__column_exists(schema, table, column):
                            print_err("""Column "{}"  in table "{}" for schema
                                    "{}" doesn't exist""".format(column, table,
                                                                 schema))
                            error = True
                print_inf('{} success'.format(schema))
            return False if error else True
        except Exception as e:
            print_err(e.message)

    def alter_column(self, schema, table, action='ADD'):
        """Add or remove state checking field."""
        alter_column_sql = """ALTER TABLE {}.{} {} COLUMN "{}" {};"""
        add_case = 'BOOLEAN DEFAULT FALSE'
        alter_column_sql = alter_column_sql.format(schema, table, action,
                                                   self.state_column,
                                                   add_case if action == 'ADD'
                                                   else '')
        try:
            self.cursor.execute(alter_column_sql)
            return self.conn.commit()
        except Exception:
            return self.conn.rollback()

    def add_status_to_table(self, schema, table):
        """Add state_column if it is missing in the table."""
        if not self.__column_exists(schema, table, self.state_column):
            print_inf("""New "{}" field is being added to {}.
                        Please wait....""".format(self.state_column, table))
            self.alter_column(schema, table)
        return True

    def clean_up(self):
        """Add state_column if it is missing in the table."""
        for schema in self.__conf_schemas():
            for table in self.__conf_schema_tables(schema):
                if self.__column_exists(schema, table, self.state_column):
                    print_inf("""The "{}" field is being removed. Please
                        wait...""".format(self.state_column))
                    self.alter_column(schema, table, 'DROP')
        return True

    @property
    def data_inst(self):
        """User defined data types getter."""
        return self.__data_inst

    @data_inst.setter
    def data_inst(self, inst):
        """User defined data types setter."""
        self.__data_inst = inst

    @property
    def schemas_config(self):
        """User defined configs setter."""
        return self.__schemas_config

    @schemas_config.setter
    def schemas_config(self, configs):
        """User defined configs setter."""
        self.__schemas_config = configs
