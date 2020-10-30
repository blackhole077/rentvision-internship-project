from os import path
from os import listdir
import datetime as dt
from utils import pipeline_utils as _putils
from typing import Optional, TypeVar
VT = TypeVar('VT')

class Task():
    """
    General class structure to wrap data that needs to be processed.

    For the PipelineManager to work as intended, everything that goes in
    must adhere to some sort of standard. The Task class is meant specifically
    to ensure that this standard is adhered to. While it can be used as-is, it
    is intended to be used as a template by which new classes can be made to
    custom-fit the needs of the problem at hand.

    Parameters
    ----------
    directory_path: str
        The directory path that contains the file to be handled by the pipeline
    file_name: str
        The name of the file eto be handled by the pipeline
    file_name_no_ext: Optional[str]
        The file name minus any extensions
    info_dict: Optional[dict]
        A dictionary containing auxiliary information for the pipeline
    
    Returns
    -------
    task: Task.Task
        A Task object that codifies all information given for pipeline processing.
    """

    def __init__(self, directory_path: str, file_name: str, file_name_no_ext: Optional[str]=None, info_dict: Optional[dict]=None) -> None:
        self.directory_path = directory_path
        self.file_name = file_name
        if file_name_no_ext:
            self.file_name_no_ext = file_name_no_ext
        else:
            self.file_name_no_ext, _ = _putils.strip_extension_from_file_name(file_name=self.file_name, return_extension=None)
        self.job_id = self.generate_job_id()
        self.info_dict = dict()
        if info_dict:
            self.info_dict.update(info_dict)

    def set_directory_path(self, directory_path: str) -> None:
        self.directory_path = directory_path
    
    def set_file_name(self, file_name: str) -> None:
        self.file_name = file_name
        self.file_name_no_ext, _ = _putils.strip_extension_from_file_name(file_name=self.file_name, return_extension=None)

    def insert_info(self, key: Optional[str]=None, value:Optional[VT]=None) -> None:
        if key is None:
            return
        if not isinstance(key, str):
            key = str(key)
        self.info_dict[key] = value

    '''GETTER FUNCTIONS'''

    def get_directory_path(self) -> str:
        return self.directory_path

    def get_file_name(self) -> str:
        return self.file_name

    def get_info_dict(self) -> dict:
        return self.info_dict

    def get_job_id(self) -> str:
        return self.job_id
    
    def get_file_name_no_ext(self) -> str:
        return self.file_name_no_ext

    def fetch_info(self, key:Optional[str]=None) -> Optional[VT]:
        if key is None:
            return None
        if not isinstance(key, str):
            key = str(key)
        return self.info_dict.get(key)

    def _print(self) -> None:
        """
            Print the relevant information of a Task.

            Prints out the Job ID, its current directory path,
            the file name that's attached, and any other
            auxiliary pieces of information attached.

            Parameters
            ----------
            None.

            Returns
            -------
            None.
        """

        print("Job ID: {}".format(self.get_job_id()))
        print("Current Directory to Use: {}\n".format(self.get_directory_path()))
        print("File Name Attached: {}".format("None" if self.file_name is None else self.file_name))
        print("File Name No Extension: {}".format("None" if self.file_name_no_ext is None else self.file_name_no_ext))
        print("Information Dict:\n")
        for _key, _value in self.info_dict.items():
            print("Key: {:24}\tValue: {}\n".format(_key, _value))
    
    def _to_string(self) -> str:
        """
            Generate a string representation of the object.

            Generates a string that contains all the information
            about the object found inside of the __dict__ attribute.
            This includes variable names and values, and is predominately
            used for logging purposes.

            Parameters
            ----------
            None.

            Returns
            -------
            result : str
                A string that contains all information about an object found in the __dict__ attribute.
        """

        string_list = []
        for key, value in self.__dict__.items():
            if isinstance(value, dict):
                string_list.append(f"{key}\n")
                string_list.extend('\n'.join(["Key: {:24}\tValue: {}".format(_key, _value) for _key, _value in value.items()]))
            else:
                string_list.append("\nKey: {:24}\tValue: {}\n".format(key, value))
        return ''.join(string_list)

    def generate_job_id(self) -> str:
        """
            Generate a unique identifier for the Task.

            This function uses the time the function is called
            along with the directory path of the Task to create
            a unique indentifier.

            Parameters
            ----------
            None.

            Returns
            -------
            None.
        """

        time_str = str(dt.datetime.now().strftime("%m-%d-%H%M%S"))
        if self.get_directory_path():
            prefix = path.basename(path.normpath(self.get_directory_path()))
        else:
            prefix = "unknown_dir_path"
        return f"{prefix}_{self.file_name_no_ext}_{time_str}"