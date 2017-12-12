"""help.py is for containing reusable functions."""
import json
from datetime import datetime


def print_err(txt, include_moment=True):
    """Red collor print. Include time optional."""
    print '\033[31m {} {} \n'.format(txt, str(datetime.now())
                                     if include_moment else '')


def print_inf(txt, include_moment=False):
    """Green collor print. Include time optional."""
    print '\033[32m {} {} \n'.format(txt, str(datetime.now())
                                     if include_moment else '')


def print_warn(txt, include_moment=False):
    """Yellow collor print. Include time optional."""
    print '\033[33m {} {} \n'.format(txt, str(datetime.now())
                                     if include_moment else '')


def get_json_config(file_name):
    """
    Get json file from given file.

    Prints info when something is wrong while reading "file_name" and
        translating to dictionarry.

    Returns:
        (dict)
    """
    try:

        with open(file_name, 'r') as myfile:
            conf_content = myfile.read()
    except IOError:
        print_err('Make sure that "{}" file exists.'.format(file_name))
    try:

        return json.loads(conf_content)
    except ValueError:
        print_err('Make sure "{}" is in json format.'.format(file_name))
