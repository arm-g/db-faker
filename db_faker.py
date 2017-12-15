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
        """Cleanup db changes after faking process If schema definition is valid."""
        print_warn('----- Data cleanup process has been started -----', 1)
        try:
            if self.db.conf_is_correct():
                self.db.clean_up()
            print_warn('----- Data cleanup process finished -----', 1)
        except Exception as e:
            print_err(e.message)


threads = []
if len(sys.argv) > 1:
    if sys.argv[1] == 'run':
        thread1 = Db_faker('Thread_1')
        thread2 = Db_faker('Thread_2')
        thread3 = Db_faker('Thread_3')
        thread4 = Db_faker('Thread_4')

        thread1.start()
        thread2.start()
        thread3.start()
        thread4.start()

        threads.append(thread1)
        threads.append(thread2)
        threads.append(thread3)
        threads.append(thread4)
        for t in threads:
            t.join()
        print "Exiting Main Thread"
    elif sys.argv[1] == 'cleanup':
        thread1 = Db_faker('Thread_1')
        thread1.clean_up()
    else:
        print_warn('usage: ./db_faker.py "run | cleanup"')
else:
    print_warn('usage: ./db_faker.py "run | cleanup"')
