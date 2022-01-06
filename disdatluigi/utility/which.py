'''
Created on Sep 12, 2017

@author: twong
'''

import os


def which(cmd_name):
    '''Get the full path to an external command executable.

    :param cmd_name: The command name
    :return: The full path to the command executable, or `None` if the
    executable is not on the O/S path
    :rtype: str
    '''
    paths = os.environ['PATH'].split(os.pathsep)
    for p in paths:
        cmd_fq_name = os.path.join(p, cmd_name)
        if os.path.exists(cmd_fq_name) and os.access(cmd_fq_name, os.X_OK):
            return cmd_fq_name
    return None
