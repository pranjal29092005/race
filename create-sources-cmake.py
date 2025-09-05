import os, sys
import glob, shutil
import subprocess

cmake_format = shutil.which('cmake-format')
clang_format = shutil.which('clang-format')

this_file = os.path.abspath(__file__)
race_cuda_dir = os.path.normpath(os.path.join(this_file, '../../RACE_CUDA'))


def write_cmake_lists_in_dir(cur_dir):
    out_file = os.path.join(cur_dir, 'CMakeLists.txt')
    sources = []
    for extn in ['cpp', 'cu', 'cuh', 'h']:
        sources.extend(glob.glob(f'{cur_dir}/*.{extn}'))

    if sources:
        sources.sort()
        sources = map(lambda f: os.path.basename(f), sources)
        sources_as_string = '\n'.join(sources)
        with open(out_file, 'w') as fout:
            fout.write(f'''add_sources_in_this_directory(RACE_SOURCES
{sources_as_string}
)''')
        subprocess.run([cmake_format, '-i', out_file])
        return os.path.basename(cur_dir)
    return None


def get_race_cuda_subdirs():
    return list(filter(lambda f: os.path.isdir(os.path.join(race_cuda_dir, f)), os.listdir(race_cuda_dir)))


def is_h_file(f):
    prefix, extn = os.path.splitext(f)
    return extn == '.h'


def cmake_format_file(f):
    if cmake_format is None:
        return
    print(f'Formatting {f} ..')
    subprocess.run([cmake_format, '-i', f])


def clang_format_file(f):
    if clang_format is None:
        return
    subprocess.run([clang_format, '-i', '--verbose', f])


def format_file(f):
    prefix, extn = os.path.splitext(f)
    if extn == '.cmake':
        cmake_format_file(f)
    elif extn in ['.h', '.cpp', '.cu']:
        clang_format_file(f)
    elif os.path.basename(f) == 'CMakeLists.txt':
        cmake_format_file(f)


def create_all_headers(sub_dir):
    out_file = os.path.join(sub_dir, 'All.h')
    try:
        os.remove(out_file)
    except FileNotFoundError:
        pass

    h_files = []
    for hf in glob.glob(f'{sub_dir}/*.h'):
        h_files.append(os.path.relpath(hf, start=race_cuda_dir))
    if not h_files:
        return

    h_files.sort()
    out = '\n'.join(map(lambda x: "#include " + f'<RACE_CUDA/{x}>', h_files))

    with open(out_file, 'w') as fout:
        fout.write(f'''#pragma once

{out}

''')
    format_file(out_file)


def write_cmake_lists_in_dir(cur_dir):
    out_file = os.path.join(cur_dir, 'CMakeLists.txt')
    try:
        os.remove(out_file)
    except FileNotFoundError:
        pass

    sources = []
    for extn in ['cpp', 'cu', 'cuh', 'h']:
        sources.extend(glob.glob(f'{cur_dir}/*.{extn}'))

    if sources:
        sources.sort()
        sources = map(lambda f: os.path.basename(f), sources)
        sources_as_string = '\n'.join(sources)
        with open(out_file, 'w') as fout:
            fout.write(f'''add_sources_in_this_directory(RACE_SOURCES
{sources_as_string}
)''')
        format_file(out_file)
        return True
    return False


def create_sources_cmake(sub_dirs):
    sub_dirs.sort()
    out_file = os.path.join(race_cuda_dir, 'sources.cmake')
    with open(out_file, 'w') as fout:
        fout.write('set(RACE_SOURCES)\n')
        fout.write('\n\n')
        for s in map(lambda f: os.path.relpath(f, race_cuda_dir), sub_dirs):
            fout.write(f'add_subdirectory({s})\n')
    format_file(out_file)


race_source_dirs = get_race_cuda_subdirs()
sub_dirs = []
for sub_dir in race_source_dirs:
    this_dir = os.path.join(race_cuda_dir, sub_dir)
    create_all_headers(this_dir)
    if (write_cmake_lists_in_dir(this_dir)):
        sub_dirs.append(this_dir)

create_sources_cmake(sub_dirs)
