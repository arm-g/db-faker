"""Db interface is for future RDBMS faker implemetations. e.g. MySQL."""
import abc


class Db_handler(object):
    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def connect(self):
        """Db connection initialization."""
        pass

    @abc.abstractmethod
    def set_data_types(self):
        """Implement data types(defined by user in data_types.py) setter."""
        pass

    @abc.abstractmethod
    def conf_is_correct(self):
        """Check user's defined conf with db's structure."""
        pass

    @abc.abstractmethod
    def fake_tables_data(self):
        """Generate update sqls and run them using user defined data_types."""
        pass

    @abc.abstractmethod
    def clean_up(self):
        """Cleanup the DB structure changes which were done during faking."""
        pass
