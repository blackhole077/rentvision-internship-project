import logging
from os import makedirs, listdir, getcwd, rename
import os.path as path
from shutil import rmtree
from subprocess import run, TimeoutExpired, CalledProcessError
from re import compile, findall, match, search, sub
from time import gmtime, strftime

putils_logger = logging.getLogger(__name__)

def create_directories(directories=None, root_directory=None):
    """
        Create multiple directories at once.

        Create multiple directories, all using
        filepaths extending from the root_directory.
        If no root_directory is provided, the directory
        this script is located will be considered as such.
        If an entry is an absolute filepath, it won't use
        the root_directory.

        Parameters
        ----------
        directories : list(str)
            A list of directories, each entry being a filepath.
        root_directory : str
            The directory from which the other directories are
            to be created from. If none, argument defaults to
            the directory this script is located in.

        Returns
        -------
        None.

        Raises
        ------
        ValueError
            If the directory is not a list of strings, it will raise an error.
    """

    if directories is None:
        raise ValueError("Directories argument incorrect. Expecting list(str), got %s", str(type(directories)))
    if root_directory is None:
        root_directory = getcwd()

    for directory_path in directories:
        if not path.isdir(directory_path):
            pass
        if path.isabs(directory_path):
            putils_logger.info(f"\nFile path {directory_path} is absolute.")
            #Make all directories leading up to the end directory, if they do not already exist.
            makedirs(directory_path, mode=0o775, exist_ok=True)
        else:
            makedirs(path.join(root_directory, directory_path), mode=0o775, exist_ok=True)

def fetch_all_from_directory(directory_path=None, file_extension=None, file_name_only=False):
    """
        Grab all files that match some file extension.

        Check the directory given and return all, if any,
        files that match the extension provided.

        NOTE: This function is CASE INSENSITIVE. So it will match
        .MP4, .MOV the same as .mp4 and .mov
        
        Parameters
        ----------
        directory_path : str
            The path to the specified directory to fetch from
        file_extension : str
            The file extension that the files should match to be pulled.
        Returns
        -------
        file_list : list(str)
            A list of paths to the files that matched the file_extension
        Raises
        ------
        ValueError
            If either the directory_path or file_extension are not provided,
            it will raise a ValueError.
    """

    if directory_path is None or file_extension is None:
        raise ValueError("File Path or Extension not provided.")
    file_list = []
    try:
        putils_logger.debug("Directory Path in fetch all: {}".format(directory_path))
        for _file in listdir(directory_path):
            if _file.lower().endswith(file_extension.lower()):
                if file_name_only:
                    file_list.append(path.basename(_file))
                else:
                    file_list.append(_file)
        putils_logger.debug(f"\nContents to return {file_list}")
        return file_list
    except FileNotFoundError:
        return []

def rename_file(file_path=None, old_name=None, new_name=None, file_extension=None):
    """
        Rename a file.

        Renames a file and/or changes its extension.

        Parameters
        ----------
        file_path : str
            A string that points to where the file is kept
        old_name : str
            The name of the file that you wish to rename.
        new_name : str
            The name that you wish to change the file to.
        file_extension : str
            The file extension you want to change the file to.

        Returns
        -------
        new_name_path : str
            The file path (including the file name) of the new file.
    """

    if file_extension is None:
        file_extension = '.'+old_name.split('.')[-1] #Get the file extension from the first file.
    new_name = new_name+file_extension
    old_name_path = path.join(file_path, old_name)
    new_name_path = path.join(file_path, new_name)
    rename(old_name_path, new_name_path)
    return new_name_path

def run_multiprocess_command(command_args=None, timeout_duration=30, verbose=True):
    """
        Run a command in a multi-processing context.

        Attempts to run the command given, with a timeout_duration (default 30s).
        If the command cannot finish executing, then it will return a TimeoutError.

        Paramters
        ---------
        command_args : list(str)
            A list formatted command, identical to
            how it would be provided to subprocess.run.
        timeout_duration : int
            The amount of time, in seconds, the process is allowed before
            a TimeoutError is thrown.

        Returns
        -------
        None.

        Raises
        ------
        ValueError
            This error occurs if no command arguments is given.
        TimeoutError
            This error occurs if the process does not finish within the timeout duration.
        ChildProcessError
            This error occurs if the process encounters some error during its run (including non-zero exit codes)
    """

    if command_args is None:
        raise ValueError(msg="Command arguments was None or empty.")
    putils_logger.info("Running Command: {}\n".format(' '.join(command_args)))
    try:
        #Forces whatever called this to wait until this command finishes (to avoid any other issues.)
        run(command_args, check=True, timeout=timeout_duration)
    except TimeoutExpired:
        raise TimeoutError("Command {} has expired after {} seconds".format(command_args, timeout_duration))
    except CalledProcessError as _e:
        raise ChildProcessError(f"Command {' '.join(command_args)} has encountered an error {_e}")

def unzip_file(file_to_unzip=None, extraction_destinations=None, files_to_include=None, files_to_exclude=None, overwrite=True):
    """
        Unzip a ZIP file.

        A Python wrapper over Linux 'unzip' command.
        This function however, also attempts to programmatically
        build the unzip function to only extract certain files
        and actively exclude others, if specified by the user.
        Furthermore, it will attempt to extract to multiple
        directories, should more than one be provided.

        Parameters
        ----------
        file_to_unzip : str
            A file path to the ZIP file that is to be unzipped.
        extraction_destinations : list(str)
            A list of strings corresponding to file paths for where
            the contents retrieved should be placed. A single path
            should still be presented in list form (e.g., [../../test_directory/])
        files_to_include : list(str)
            A list of files or file endings to include for extraction.
            The predominant use for this is specifying extensions (e.g., '*.jpg', '*.mov')
        files_to_exclue : list(str)
            A list of files or file endings to exclude from extraction.
        
        Returns
        -------
        None.
    """
    
    if file_to_unzip is None:
        raise ValueError("No file was provided to unzip.")
    if overwrite:
        command_unzip = ['unzip', '-o', '-C', '-W', file_to_unzip]
    else:
        command_unzip = ['unzip', '-C', '-W', file_to_unzip]
    if files_to_include:
        if not isinstance(files_to_include, list):
            #If the thing is a string we don't want to cast to list because it'll break stuff.
            #Also int object not iterable will occur if trying to cast as list.
            files_to_include = [files_to_include]
            command_unzip += files_to_include
        else:
            for _file in files_to_include:
                command_unzip.append(_file)
    else:
        files_to_include = []
    if files_to_exclude:
        if type(files_to_exclude) is not list:
            #If the thing is a string we don't want to cast to list because it'll break stuff.
            #Also int object not iterable will occur if trying to cast as list.
            files_to_exclude = [files_to_exclude]
            command_unzip += files_to_exclude
        else:
            for _file in files_to_exclude:
                command_unzip.append(_file)

    #This section is handled differently since, as far as I know, unzip doesn't allow for multiple extraction points.
    if extraction_destinations:
        if type(extraction_destinations) is list:
            for destination in extraction_destinations:
                destination_component = ['-d', destination]
                list_command_unzip = command_unzip + destination_component
                run_multiprocess_command(list_command_unzip, 180)
            return
        else:
            extraction_destination_component = ['-d', extraction_destinations]
            command_unzip += extraction_destination_component
    #If there's no extraction destination, it should still run, but it will exctact to the directory the ZIP is in.
    run_multiprocess_command(command_unzip, 180)

def delete_tree(directory_path=None, ignore_read_only=True):
    """
        Delete a tree from the file system.

        A wrapper function for shutil.rmtree. For more information,
        consult the documentation of that function.

        Parameters
        ----------
        directory_path : str
            The path to the directory to be removed.
            This includes all subfolders of said directory.
        ignore_read_only : bool
            A flag to indicate if read-only files should be removed.

        Returns
        -------
        None.
    """

    if directory_path is None:
        raise ValueError("Must provide a file path for delete_tree.")
    if not path.isdir(directory_path):
        raise ValueError("Filepath is not a directory: {}".format(directory_path))
    rmtree(directory_path, ignore_errors=ignore_read_only)

def create_temp_file(temp_file_path=None, directory_path=None, list_to_write=None, verbose=False):
    if temp_file_path is None:
        raise ValueError("create_temp_file needs a temp_file_path.\
                          This is where the temporary file is stored. Keep it\
                          away from any watched directories.")
    if directory_path is None:
        raise ValueError("create_temp_file needs a directory path.\
                    This should also be the parent directory of any files you are\
                    writing to.")

    if list_to_write is None:
        raise ValueError("create_temp_file needs an iterable to write to temporary file.")
    temp_file_location = path_join_wrapper(temp_file_path, 'temp.txt')
    # Write the file paths of all video files into the temporary file
    with open(temp_file_location, 'w') as _file:
        for i in range(len(list_to_write)):
            _file.write('file \'')
            _file.write(str(path_join_wrapper(directory_path, list_to_write[i])))
            _file.write('\'')
            if i != len(list_to_write) - 1:
                _file.write('\n')
    if verbose:
        with open(temp_file_location, 'r') as _file:
            content = _file.readlines()
            putils_logger.debug("Content of temp file: {}\n".format(content))
    return temp_file_location

### OS PATH WRAPPER FUNCTIONS ###

def normalize_file_name_from_path(file_path=None):
    """A wrapper function for path.basename + path.normpath to avoid redundant imports."""

    return path.basename(path.normpath(file_path))

def path_join_wrapper(path_section_one=None, path_section_two=None):
    """A wrapper function for path.join to avoid redundant imports."""

    return path.join(path_section_one, path_section_two)

def verify_and_generate_directory_path(directory_path=None):
    """
        Function that verifies the uniqueness of a directory path.
        If a directory path provided is not unique, then it will
        attempt to create a unique path by appending some number
        to the end, similar to how certain file systems will handle
        copies.

        Parameters
        ----------
        directory_path : str
            The directory or file path that is being verified
        
        Returns
        -------
        directory_path : str
            A potentially modified version of the directory path.
    """

    if directory_path is None:
        raise ValueError("Expected directory_path. Got None.")
    offset = 0
    # Fetch the original file name (minus the extension if present)
    directory_head = path.dirname(directory_path)
    original_file_name, original_extension = path.splitext(path.basename(path.normpath(directory_path)))
    while path.isfile(directory_path):
        new_file_name = original_file_name+f'{offset:04d}'+original_extension
        directory_path = path.join(directory_head, new_file_name)
    return directory_path

def strip_extension_from_file_name(file_name=None, return_extension=None):
    """
        Strip all expected file extensions (e.g., ZIP, MOV, etc.) from a file_name.

        Using a regular expression, this method removes all instances of a file extension
        from a file_name. This is intended to catch duplicated extensions (e.g., .zip.zip)
        which will pass through regular methods.

        The flag to return extensions will only return the FIRST file extension that matches
        the regular expression provided. If you want different behavior you will need to modify
        the code.

        Parameters
        ----------
        file_name : str
            The file name with its extension(s).
        return_extension : bool
            A flag indicating if the file extension detected shoudl be returned.
        
        Returns
        -------
        file_name_no_ext : str
            The file name minus all instances of file extensions.
        file_extension : str
            The first file extension encountered by the regular expression.
    """

    pattern = r'\.[A-z0-9]{3,4}'
    regex = compile(pattern)
    file_extension_match = search(regex, file_name)
    if file_extension_match is None:
        putils_logger.info(f"Regex {pattern} does not find matches in {file_name}")
        return None, None
    else:
        file_extension = file_extension_match.group(0)
    file_name_no_ext = sub(regex, '', file_name)
    if return_extension:
        return file_name_no_ext, file_extension
    else:
        return file_name, None

def strip_bad_characters_from_file_name(file_name_no_ext=None):
    """
        Strip all unwanted characters (e.g., " ", ".", etc.) from a file_name without its extension.

        This method simply replaces all whitespace with an underscore and removes any other
        unwanted characters. This is done to try and prevent any hangups later on in the pipeline
        due to spaces being present in the name of the file.

        Parameters
        ----------
        file_name_no_ext : str
            The file name without its extension(s).
        
        Returns
        -------
        file_name_no_ext : str
            The file name minus all unwanted characters and its extension.
    """
    file_name_no_ext = file_name_no_ext.replace(".",'').replace(' ', '_')
    return file_name_no_ext

def is_only_one_true(iterable=None):
    """
        Check if there is only one True value within an iterable.

        This function returns True if only one element in the
        iterable is true, otherwise it returns False.

        NOTE: This works partially because of how Python handles
        iterators. The first any() call stops iterating once it
        finds a True value. However, the iterator is not reset.
        Therefore when the next any() call is made, it starts from
        after that first True value.

        Parameters
        ----------
        iterable : iterable
            Some iterable object in Python. Most common
            example of this is a list
    
        Returns
        -------
        result  : bool
            A boolean value that represents if only one True value is in the iterable.
    """

    if iterable:    
        i = iter(iterable)
        return any(i) and not any(i)
    else:
        return False
