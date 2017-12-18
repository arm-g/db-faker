#!/usr/bin/env python
"""DB faker file is responsible for db data faking."""
import sys
import threading

from data_types import Data
from lib.pg_database import Pg_handler
from utils.help import get_json_config, print_err, print_warn


class Db_faker(threading.Thread):
    """Shuffles database data based on configs."""

    def __init__(self, threadID):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.schema_definition = 'schema_definition.json'
        self.db = Pg_handler()
        self.db.schemas_config = get_json_config(self.schema_definition)
        self.db.data_inst = Data()

    def run(self):
        """Runing the data faking process and dependant checkings."""
        print_warn('----- Data faking process has been started -----', 1)
        try:
            if self.db.conf_is_correct():
                self.db.fake_tables_data()
            print_warn('----- Data faking process finished -----', 1)
        except Exception as e:
            print_err(e.message)

    def clean_up(self):
        """Cleanup db changes after faking process."""
        print_warn('----- Data cleanup process has been started -----', 1)
        try:
            if self.db.conf_is_correct():
                self.db.clean_up()
            print_warn('----- Data cleanup process finished -----', 1)
        except Exception as e:
            print_err(e.message)


threads = {}

if len(sys.argv) > 1:
    threads_count = 1
    if sys.argv[2] and int(sys.argv[2]) > 1:
        threads_count = int(sys.argv[2])
    if sys.argv[1] == 'run':
        for i in range(threads_count):
            threads[i] = Db_faker('Thread_{}'.format(i))
            threads[i].start()
        print_err('----- {} Threads  are running -----'.format(
            len(threads.keys())))
    elif sys.argv[1] == 'cleanup':
        thread1 = Db_faker('Thread_1')
        thread1.clean_up()
    else:
        print_warn('usage: ./db_faker.py "run | cleanup" int(threads_count)')
else:
    print_warn('usage: ./db_faker.py "run | cleanup" int(threads_count)')
