#!/usr/bin/python3

# Add parser that takes command-line arguments in the
# - input-format "<<{INPUT}" and in the
# - output-format: ">>{OUTPUT}"
# Usage: containerize gcc -Wall -c '<{foo.c}' -o '>{foo.o}'

# Write file to calls/xx/yy/xxyy... .txt with contents FILENAME MTIME HASH

# Only write outputs if newer or content different
# Detect undeclared files in output and temp directory

# TODO 1. Add wrapper for subprocess.Popen
# TODO 2. Add caching of stdout and stderr

# TODO Either allow `ExecFilePath` must be copied to box if relative or forbid
# it to be relative.

# TODO Should we allow `OutDirPath`?

# TODO Check before execution if outputs in working directory are writable

# TODO Should we allow `cache_dir` to be an instance of a specific `CacheDirPath('qac')`

import hashlib
import os
import os.path
import pathlib
import shutil
import stat
import subprocess
import tempfile
import fileinput
import logging
import unittest


_SUCCESS = 0                    # default success exit status
_FAILURE = 1                    # default failure exit status

_DEFAULT_HASH_NAME = 'sha256'   # either md5, sha1, sha256, sha512, etc

_HOME_DIR = os.path.expanduser('~')
_DEFAULT_CACHE_DIR = os.path.join(_HOME_DIR, '.cache', __name__)


# needed for cache pruning
def tree_files_sorted_by_recent_mtime(rootfolder, file_matcher=None):
    return sorted((os.path.join(dirname, filename)
                   for dirname, dirnames, filenames in os.walk(rootfolder)
                   for filename in filenames
                   if (not file_matcher or
                       file_matcher(filename))),
                  key=lambda fn: os.stat(fn).st_mtime,
                  reverse=True)
# print(tree_files_sorted_by_recent_mtime('.'))


# Input file (regular or directory) path.
class InFilePath(type(pathlib.Path())):
    def __init__(self, path, unboxed_abspath=None):
        assert not self.is_absolute(), "Input file path {} must not be absolute, make it relative and put the absolute source as keyword argument `unboxed_abspath`".format(self)
        self.unboxed_abspath = unboxed_abspath

    def as_unboxed(self):
        return self.unboxed_abspath or str(self)

    def as_boxed(self):
        return str(self)


# Output file (regular or directory) path.
class OutFilePath(type(pathlib.Path())):
    def __init__(self, path, unboxed_abspath=None):
        assert not self.is_absolute(), "Output file path {} must not be absolute, make it relative and put the absolute source as keyword argument `unboxed_abspath`".format(self)
        self.unboxed_abspath = unboxed_abspath

    def as_unboxed(self):
        return self.unboxed_abspath or str(self)

    def as_boxed(self):
        return str(self)


# Temporary file path (local to container).
class TempFilePath(type(pathlib.Path())):
    def __init__(self, path):
        assert not self.is_absolute(), "Temporary file path must '{}' be relative".format(self)


# Temporary directory path (local to container).
class TempDirPath(type(pathlib.Path())):
    def __init__(self, path):
        assert not self.is_absolute(), "Temporary directory path '{}' must be relative".format(self)


# Executable file path (absolute or relative).
class ExecFilePath(type(pathlib.Path())):
    def as_unboxed(self):
        return str(self)

    def as_boxed(self):
        return str(self)


# http://stackoverflow.com/questions/17412304/hashing-an-array-or-object-in-python-3
def _hash_update_data(hash_, data):
    if isinstance(data, str):
        ordered_data = data
    elif isinstance(data, set):
        ordered_data = sorted(data)
    elif isinstance(data, dict):
        ordered_data = sorted(data.items())
    elif isinstance(data, (list, tuple)):
        ordered_data = data
    else:
        ordered_data = data
    hash_.update(repr(ordered_data).encode('utf8'))


# http://stackoverflow.com/questions/43387738/robust-atomic-file-copying
def _atomic_copyfile(src, dst, overwrite, logger):
    try:
        with tempfile.NamedTemporaryFile(dir=os.path.dirname(dst),
                                         delete=False) as tmp_handle:
            # shutil.copy2(src=src, dst=tmp_handle.name)  # `copy2` doesn't change mtime
            with open(src, 'rb') as src_fd:
                shutil.copyfileobj(fsrc=src_fd,
                                   fdst=tmp_handle)
        if overwrite:
            # works both on Windows and Linux from Python 3.3+, os.rename raises an
            # exception on Windows if the file exists
            os.replace(src=tmp_handle.name,
                       dst=dst)
            return True
        else:
            if not os.path.exists(dst):
                os.rename(src=tmp_handle.name,
                          dst=dst)
                return True
    except:
        logger.warning("Failed to copy file {} to {}".format(src, dst))
        pass
    finally:
        try:
            os.remove(tmp_handle.name)
        except:
            pass
    return False


def _file_hexdigest(file_name,
                    hash_name):
    hash_state = hashlib.new(name=hash_name)
    with open(file_name, 'rb') as out_handle:
        hash_state.update(out_handle.read())
    return hash_state.hexdigest()


MANIFEST_FIELD_SEPARATOR = ' '


def _try_store_into_cache(out_files,
                          cache_manifest_file,
                          cache_artifacts_dir,
                          hash_name,
                          logger):
    try:
        with open(cache_manifest_file, 'w') as manifest_handle:
            for out_file in out_files:
                assert not out_file.is_absolute()
                out_file_name = str(out_file)  # just the name

                hexdig = _file_hexdigest(file_name=out_file_name,
                                         hash_name=hash_name)
                cache_artifact_file = os.path.join(cache_artifacts_dir, hexdig)

                # must not use link here
                if _atomic_copyfile(src=out_file_name,
                                    dst=cache_artifact_file,
                                    overwrite=False,  # keep existing if file was just written to
                                    logger=logger):
                    logger.info('Stored {} with contents {} into cache'.format(out_file_name, hexdig))
                else:
                    logger.info('Skipped storing {} with contents {} already in cache'.format(out_file_name, hexdig))

                # write entry in manifest file
                manifest_handle.write(hexdig + MANIFEST_FIELD_SEPARATOR +
                                      str(os.path.getmtime(out_file_name)) + MANIFEST_FIELD_SEPARATOR +
                                      out_file_name + '\n')

        return True
    except FileNotFoundError as exc:
        logger.warning('Could not store some of {} into cache, reason: {}'
                       .format(out_files, exc))
    return False


def _try_load_from_cache(cache_manifest_file,
                         out_files,
                         hash_name,
                         logger):
    try:
        # create hash-map
        manifest_map = {}
        with open(cache_manifest_file, 'r') as manifest_handle:
            for line in manifest_handle:
                entries = line.rstrip('\n').split(MANIFEST_FIELD_SEPARATOR)
                manifest_map[entries[2]] = tuple(entries[0:2])

        for out_file in out_files:
            assert isinstance(out_file, OutFilePath)
            assert not out_file.is_absolute()

            out_file_name = str(out_file)

            (manifest_hash, manifest_file_mtime) = manifest_map[out_file_name]
            if (  # TODO doesn't work until we can preserve mtime in _atomic_copyfile:
                  # manifest_file_mtime != os.path.getmtime(out_file_name) and  # if mtime and
                manifest_hash != _file_hexdigest(file_name=out_file_name,  # contents has changed
                                                 hash_name=hash_name)):
                print("Output file {} has changed".format(out_file_name))

                # must not use link here
                if _atomic_copyfile(src=manifest_hash,
                                    dst=out_file_name,
                                    overwrite=True,
                                    logger=logger):
                    logger.info('Loaded {} from cache'.format(out_file_name))

            manifest_map.pop(out_file_name, None)
        assert not manifest_map, "Output files {} didn't match contents of manifest file {}".format(out_files, cache_manifest_file)
        return True
    except FileNotFoundError:
        pass
    return False


def _atomic_link_or_copyfile(src, dst, logger):
    try:                        # first try
        os.link(src=src,        # hardlink
                dst=dst)
    except Exception:             # and if that fails
        _atomic_copyfile(src=src,  # do plain copy
                         dst=dst,
                         overwrite=True,
                         logger=logger)


def copy_input_to_box(work_dir, in_files,
                      in_dir_abspath,
                      logger):
    os.mkdir(in_dir_abspath)
    os.chdir(in_dir_abspath)
    for in_file in in_files:
        boxed_in_dir = os.path.dirname(in_file.as_boxed())
        if boxed_in_dir:    # only if in_file.as_boxed() lies in a subdir
            os.makedirs(boxed_in_dir, exist_ok=True)
        _atomic_link_or_copyfile(src=os.path.join(work_dir,
                                                  in_file.as_unboxed()),
                                 dst=in_file.as_boxed(),
                                 logger=logger)


def copy_output_from_box(out_files,
                         work_dir,
                         logger):
    for out_file in out_files:
        _atomic_link_or_copyfile(src=out_file.as_boxed(),
                                 dst=os.path.join(work_dir,
                                                  out_file.as_boxed()),
                                 logger=logger)


def create_out_dirs(out_files,
                    out_dir_abspath):
    os.mkdir(out_dir_abspath)
    for out_file in out_files:
        if not out_file.is_absolute():
            out_file = str(out_file)
            box_out_file = os.path.join(out_dir_abspath, out_file)
            os.makedirs(os.path.dirname(box_out_file), exist_ok=True)  # pre-create directory


def _strip_prefix(text, prefix):
    # TODO relax to paths not in the beginning
    if text.startswith(prefix):
        return text[len(prefix):]
    return text


def _strip_prefix_from_out_file_contents(out_files, prefix):
    """Remove sandbox absolute path PREFIX from the contents of OUT_FILES."""
    with fileinput.input(files=map(str, out_files),
                         inplace=True, backup='.bak') as f:
        for line in f:
            print(_strip_prefix(line, prefix), end='')


def assert_disjunct_file_sets(in_files,
                              out_files,
                              temp_dirs):
    '''Assert that in_files, out_files and temp_dirs have all disjunct names.'''

    # TODO can these conversions be optimized?
    in_file_names = set(map(str, in_files))
    out_file_names = set(map(str, out_files))
    temp_dir_names = set(map(str, temp_dirs))

    in_out_overlap_files = in_file_names & out_file_names
    if in_out_overlap_files:
        raise Exception("Inputs and outputs overlap for {}".format(in_out_overlap_files))

    in_temp_overlap_files = in_file_names & temp_dir_names
    if in_temp_overlap_files:
        raise Exception("Inputs and temp dirs overlap for {}".format(in_temp_overlap_files))

    out_temp_overlap_files = out_file_names & temp_dir_names
    if out_temp_overlap_files:
        raise Exception("Outputs and temp dirs overlap for {}".format(out_temp_overlap_files))


def isolated_call(typed_args,
                  typed_env=None,
                  extra_inputs=None,
                  extra_outputs=None,
                  cache_dir=_DEFAULT_CACHE_DIR,
                  call=subprocess.call,
                  hash_name=_DEFAULT_HASH_NAME,
                  shell=False,
                  timeout=None,
                  strip_box_in_dir_prefix=False):

    work_dir = os.getcwd()

    # cache directory
    use_caching = cache_dir is not None

    # log directory
    if use_caching:
        logger_dir = cache_dir
    else:
        logger_dir = os.path.join(_HOME_DIR, '.' + __name__)
    os.makedirs(logger_dir, exist_ok=True)

    # logging
    top_logger = logging.getLogger(__name__)
    top_logger.setLevel(logging.DEBUG)

    # log format
    formatter = logging.Formatter('%(asctime)s %(levelname)s: %(message)s')

    # file log
    log_file = logging.FileHandler(os.path.join(logger_dir, 'all.log'))
    log_file.setLevel(logging.DEBUG)
    log_file.setFormatter(formatter)
    top_logger.addHandler(log_file)

    load_from_cache = True  # for debugging purpose. TODO remove before deployment

    hash_state = hashlib.new(name=hash_name)

    in_files = set()
    out_files = set()
    temp_dirs = set()

    in_subdir_name = 'in'
    out_subdir_name = 'out'
    temp_subdir_name = 'temp'

    subdir_prefixes = {
        ExecFilePath: '',
        InFilePath: '',
        OutFilePath: os.path.join('..', out_subdir_name),
        TempFilePath: os.path.join('..', temp_subdir_name),
        TempDirPath: os.path.join('..', temp_subdir_name),
        str: '',
        bytes: '',
    }

    # process typed arguments
    args = []                # expand args
    for typed_arg in typed_args:

        arg_prefix = subdir_prefixes[type(typed_arg)]
        arg = str(typed_arg)

        if use_caching:
            hash_state.update(arg.encode('utf8'))  # file name

        if isinstance(typed_arg, InFilePath):
            in_files.add(typed_arg)
            if use_caching:
                hash_state.update(open(typed_arg.as_unboxed(), 'rb').read())  # file content
        elif isinstance(typed_arg, OutFilePath):
            out_files.add(typed_arg)
        elif isinstance(typed_arg, TempDirPath):
            temp_dirs.add(typed_arg)
        elif isinstance(typed_arg, ExecFilePath):
            if use_caching:
                hash_state.update(open(typed_arg.as_unboxed(), 'rb').read())  # file content
            # allow absolute file paths here for now
        else:
            assert isinstance(typed_arg, str)

        args.append(os.path.join(arg_prefix, arg))  # to string

    # hash extra input strings and paths
    if extra_inputs is not None:
        for extra_input in extra_inputs:
            if isinstance(extra_input, bytes):
                if use_caching:
                    hash_state.update(extra_input)
            elif isinstance(extra_input, InFilePath):
                in_files.add(extra_input)
                if use_caching:
                    hash_state.update(extra_input.as_boxed().encode('utf8'))  # file named
                    hash_state.update(open(extra_input.as_unboxed(), 'rb').read())  # file content
            else:
                raise Exception('Cannot handle extra_input {} of type {}'
                                .format(extra_input,
                                        type(extra_input)))

    assert_disjunct_file_sets(in_files=in_files,
                              out_files=out_files,
                              temp_dirs=temp_dirs)

    # expand environment
    env = {}
    if typed_env is not None:
        for name, typed_value in sorted(typed_env.items()):  # deterministic env
            if isinstance(typed_value, TempDirPath):
                temp_dirs.add(typed_value)
            else:
                assert isinstance(typed_value, str)

            value = str(typed_value)
            env[name] = value

            hash_state.update(name.encode('utf8'))
            hash_state.update(value.encode('utf8'))

    if use_caching:
        hexdig = hash_state.hexdigest()

        cache_prefix_dir = os.path.join(cache_dir,
                                        hexdig[0:2],
                                        hexdig[2:4])
        os.makedirs(cache_prefix_dir, exist_ok=True)

        cache_manifest_file = os.path.join(cache_prefix_dir,
                                           hexdig + '.manifest')

        cache_artifacts_dir = os.path.join(cache_dir, 'artifacts', hash_name)  # this could be made a common parameter
        os.makedirs(cache_artifacts_dir, exist_ok=True)

        if load_from_cache:
            if _try_load_from_cache(cache_manifest_file=cache_manifest_file,
                                    out_files=out_files,
                                    hash_name=hash_name,
                                    logger=top_logger):
                return _SUCCESS

    # within sandbox
    with tempfile.TemporaryDirectory() as box_dir:
        in_dir_abspath = os.path.join(box_dir, in_subdir_name)
        out_dir_abspath = os.path.join(box_dir, out_subdir_name)
        temp_dir_abspath = os.path.join(box_dir, temp_subdir_name)

        copy_input_to_box(work_dir=work_dir,
                          in_files=in_files,
                          in_dir_abspath=in_dir_abspath,
                          logger=top_logger)

        # create output directories
        create_out_dirs(out_files=out_files,
                        out_dir_abspath=out_dir_abspath)

        # create top directory for temporary box files
        os.makedirs(temp_dir_abspath)

        # NOTE keeping these because it's very useful when debugging file structure in container:
        # import print_fs
        # print_fs.print_tree(box_dir)

        # call in containerized read-only input directory
        os.chdir(in_dir_abspath)
        os.chmod(in_dir_abspath,
                 stat.S_IREAD | stat.S_IXUSR)  # read-only and executable
        exit_status = call(args=args,
                           env=env,
                           stderr=subprocess.STDOUT,
                           shell=shell,
                           timeout=timeout)
        os.chmod(in_dir_abspath,
                 stat.S_IREAD |
                 stat.S_IWRITE |  # make it writeable again so that it can be removed
                 stat.S_IXUSR)

        # handle result
        if exit_status == _SUCCESS:
            os.chdir(out_dir_abspath)  # enter sandbox output

            # TODO merge these three processings of out_files

            if strip_box_in_dir_prefix:
                _strip_prefix_from_out_file_contents(out_files=out_files,
                                                     prefix=in_dir_abspath + os.sep)

            if use_caching:
                _try_store_into_cache(out_files=out_files,
                                      cache_manifest_file=cache_manifest_file,
                                      cache_artifacts_dir=cache_artifacts_dir,
                                      hash_name=hash_name,
                                      logger=top_logger)

            copy_output_from_box(out_files=out_files,
                                 work_dir=work_dir,
                                 logger=top_logger)

        # restore working directory
        os.chdir(work_dir)

        return exit_status


class TestAll(unittest.TestCase):

    def test_gcc_compilation(self):

        with tempfile.TemporaryDirectory() as box_dir:
            os.chdir(box_dir)

            exec_file = ExecFilePath('/usr/bin/gcc')
            in_c_file = InFilePath('foo.c')
            out_o_file = OutFilePath('foo.o')
            out_gcda_file = OutFilePath('foo.gcda')
            out_su_file = OutFilePath('foo.su')

            with open(in_c_file.name, 'w') as f:
                f.write('''#include <stdio.h>

int f(int x) { return x*x; }

int main()
{
  printf("Hello world\\n");
  return 0;
}
''')
            assert in_c_file.exists()

            typed_args=[exec_file,
                        '-fprofile-generate',  # side-effect output `foo.gcda`
                        '-fstack-usage',       # side-effect output `foo.su`
                        '-c', in_c_file,
                        '-o', out_o_file]
            isolated_call(typed_args=typed_args,
                          extra_outputs=[out_gcda_file,
                                         out_su_file])

            assert out_o_file.exists()

            import print_fs
            print_fs.print_tree(box_dir)

        assert not os.path.exists(box_dir)


if __name__ == '__main__':
    unittest.main()
