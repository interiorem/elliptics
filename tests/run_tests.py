#!/usr/bin/python
from __future__ import print_function

import sys
import subprocess
import time
import tarfile
import os
import sys
import shutil

def run_test(path, test):
    os.mkdir(path)
    p = subprocess.Popen([test, '--path', path], stdout=sys.stdout,
                         stderr=subprocess.STDOUT, cwd=path)
    p.communicate()

    return p.returncode


# Forced flushing is required to properly combine outputs
# from this script and test executable it launches into single stream.
def _print(s):
    print(s, file=sys.stdout)
    sys.stdout.flush()


def force_mkdir(path):
    if os.path.exists(path):
        _print('-- Removing path: {0}'.format(path))
        shutil.rmtree(path)

    os.mkdir(path)


def main():
    source_dir = sys.argv[1]
    binary_dir = sys.argv[2]

    tests = list()

    for i in xrange(3, len(sys.argv)):
        tests.append((binary_dir, sys.argv[i]))

    _print('\nRunning {0} tests'.format(len(tests)))

    tests_base_dir = binary_dir + '/result'
    force_mkdir(tests_base_dir)

    artifacts_dir = source_dir + '/artifacts'
    force_mkdir(artifacts_dir)

    all_ok = True
    for i in xrange(0, len(tests)):
        test = tests[i]
        _print('\n# Start {1} of {2}: {0}: '.format(test[1], i + 1, len(tests)))

        timer_begin = time.time()
        result = run_test(tests_base_dir + '/' + test[1],
                          test[0] + '/' + test[1])
        timer_end = time.time()

        if result == 0:
            str_result = 'Passed'
        else:
            str_result = 'Failed ({0})'.format(result)

        _print('# Result: {0}\t{1} sec'.format(str_result,
                                                timer_end - timer_begin))

        all_ok &= result == 0

        if result != 0:
            file_path = os.path.join(artifacts_dir, test[1] + '.tar.bz2')
            _print('Packing artifacts to {0}...'.format(file_path))
            file = tarfile.TarFile.open(file_path, 'w:bz2')
            file.add(tests_base_dir + '/' + test[1], test[1])
            file.close()
            _print('Done')

    _print('\nTests are finished\n')

    if all_ok:
        exit(0)
    else:
        exit(1)

if __name__ == "__main__":
    main()
