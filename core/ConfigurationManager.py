import json
from utils import pipeline_utils as _putils

class ConfigurationManager():
    def __init__(self, configuration_directory=None):
        if configuration_directory is None:
            print("Configuration Directory not provided.")
        self.configuration_directory = configuration_directory
        self.pipeline_command_line_arguments = dict()
        self.pipeline_configuration = list()
        self.slack_bot_configuration = dict()
        self.start_pipeline_configuration = self.load_json(None, "task_start_pipeline_tags.json")
    
    # GET FUNCTIONS WITH KEY   
    
    def fetch_from_pipeline_command_line_arguments(self, key=None):
        if not isinstance(key, str):
            key = str(key)
        return self.pipeline_command_line_arguments.get(key)
    
    def fetch_from_slack_bot_configuration(self, key=None):
        if not isinstance(key, str):
            key = str(key)
        return self.slack_bot_configuration.get(key)

    def fetch_from_start_pipeline_configuration(self, key=None):
        if not isinstance(key, str):
            key = str(key)
        return self.start_pipeline_configuration.get(key)


    # GET FUNCTIONS ATTRIBUTES

    def get_pipeline_command_line_arguments(self):
        return self.pipeline_command_line_arguments

    def get_pipeline_configuration(self):
        return self.pipeline_configuration

    def get_slack_bot_configuration(self):
        return self.slack_bot_configuration

    def get_start_pipeline_configuration(self):
        return self.start_pipeline_configuration

    # MISCELLANEOUS FUNCTIONS

    def load_json(self, json_directory=None, json_file_name=None):
        """
            Load a JSON configuration file.
            
            Given the name of the JSON file and a directory,
            this function will load the configuration file and
            return its parsing. If json_directory is not provided,
            then the configuration directory profided at construction
            is assumed to contain the JSON file.

            Parameters
            ----------
            json_directory : str
                The directory path that leads to where the
                JSON configuration file is located.
            json_file_name : str
                The file name of the JSON configuration file.
            
            Returns
            -------
            parsed_json_dict : dict
                A dictionary that is the Python equivalent of
                the JSON file.
        """

        if json_directory is None:
            json_directory = self.configuration_directory
        with open(_putils.path_join_wrapper(json_directory, json_file_name), "r") as _file:
            return json.load(_file)

    def determine_start_pipeline(self, file_name_no_ext=None):
        """
            Find the starting pipeline section that the task should use.
        """

        tags_present = [t in file_name_no_ext for t in self.start_pipeline_configuration.keys()]
        if _putils.is_only_one_true(tags_present):
            return self.fetch_from_start_pipeline_configuration(next((start_tag for start_tag in self.start_pipeline_configuration.keys() if start_tag in file_name_no_ext), None))
        # If no tags (or multiple tags) are detected, then use the default starting point.
        else:
            return self.fetch_from_start_pipeline_configuration('default')

    def _print(self):
        for key, value in self.__dict__.items():
            if isinstance(value, dict):
                print(key)
                for _key, _value in value.items():
                    print('Key: {:24}\tValue: {}\n'.format(_key, _value))
            else:
                print("{:24}\tValue: {}\n".format(key, value))

    def _to_string(self):
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