import asyncio
import os
import re
import time
from multiprocessing import JoinableQueue

from slack import RTMClient


class SlackQueue():
    """
        An auxiliary class designed to facilitate communication
        between the Slack Bot and the Deep Learning Pipeline.
    """

    def __init__(self):
        self._queue = JoinableQueue()
    
    async def aput(self, item):
        """
            An asyncio-friendly wrapper for the JoinableQueue put operation.

            Parameters
            ----------
            item : object
                The item to enqueue.
            
            Returns
            -------
            None.
        """

        self._queue.put_nowait(item)

    async def aget(self):
        """
            An asyncio-friendly wrapper for the JoinableQueue get operation.

            Parameters
            ----------
            None.
                        
            Returns
            -------
            None.
        """
        
        return await asyncio.get_event_loop().run_in_executor( None, self._queue.get )
    
    async def task_done(self):
        """
            An asyncio-friendly wrapper for the JoinableQueue put operation.

            Parameters
            ----------
            item : object
                The item to enqueue.
            
            Returns
            -------
            None.
        """

        return await asyncio.get_event_loop().run_in_executor(None, self._queue.task_done)
    
class SlackBot():
    # Create a constant that contains the default text for the message
    RTM_READ_DELAY = 1 # 1 second delay between reading from RTM
    COMMAND_LIST = ["config", "jobs", "running", "mock"]
    MENTION_REGEX = "^<@(|[WU].+?)>(.*)"
    def __init__(self, manager_dict = None, config_manager=None, slack_queue=None):
        self.manager_dict = manager_dict
        self.mock_data_struct = dict()
        self.slack_queue = slack_queue
        self.is_running = True
        self.config_manager = config_manager
        self.bot_id = self.config_manager.fetch_from_slack_bot_configuration('bot_id')
        self.rtm_client = None

    def get_is_running(self):
        return self.is_running
    
    def get_manager_dict(self):
        return self.manager_dict

    def get_config_manager(self):
        return self.config_manager
    
    def _to_string(self, _dict, string_args_dict=None):
        """
            Generate a string representation of the object.

            Generates a string that contains all the information
            about the object found inside of the __dict__ attribute.
            This includes variable names and values, and is predominately
            used for logging purposes.

            Parameters
            ----------
            string_args_dict : dict(str, str)
                A mapping that dictates the formatting
                of the resulting string.
            
            Returns
            -------
            result : str
                A string that contains all information about an object found in the __dict__ attribute.
        """

        string_list = []
        if string_args_dict is None:
            string_args_dict = {
                "key_string": "Key",
                "value_string": "Value",
                "default_key_string": "Key",
                "default_value_string": "Value"
            }
        for key, value in _dict.items():
            if value:
                if isinstance(value, dict):
                    string_list.append(f"{key}\n")
                    string_list.extend(self._to_string(value))
                else:
                    string_list.append("{}: {:24}\t{}: {}\n".format(string_args_dict.get('key_string'), key, string_args_dict.get('value_string'), value))
            else:
                string_list.append("{}: {:24}\t{}\n".format(string_args_dict.get('default_key_string'), key, string_args_dict.get('default_value_string')))
        return ''.join(string_list)

    def fetch_current_jobs(self):
        manager_dict_string_args = {
            "key_string": "Pipeline Name",
            "value_string": "Jobs in Pipeline",
            "default_key_string": "Pipeline Name",
            "default_value_string": "All Done and/or Waiting for Tasks"
        }
        return self._to_string(self.manager_dict, manager_dict_string_args)

    def fetch_current_jobs_by_user(self, user_initials=None):
        if user_initials is None:
            return "Seems you forgot to specify the user! Try using '@SlackBot jobs <your intials>'!"
        current_jobs_by_user = []
        print("Entering jobs by user")
        if not isinstance(user_initials, list):
            user_initials = [user_initials]
        for manager_name, jobs_in_manager in self.manager_dict.items():
            print(f"Jobs in manager: {jobs_in_manager}")
            if jobs_in_manager:
                # Add the current jobs after filtering (x[0] refers to the initials tied to a given job)
                current_jobs_by_user.extend(list(filter(lambda x: x[0] in user_initials)))
            else:
                current_jobs_by_user.append(("No Jobs", manager_name))
        current_jobs_by_user_payload = '\n'.join(map(str, current_jobs_by_user))
        print(f"Current Jobs Payload {current_jobs_by_user_payload}")
        return current_jobs_by_user_payload
    
    def fetch_pipeline_status(self):
        response_value = ""
        if self.get_is_running:
            response_value = "The pipeline is currently running!"
        else:
            response_value = "It's unclear how you got this message, but the pipeline is not running at this time."
        pipeline_status_payload = "{}".format(response_value)
        return pipeline_status_payload
        
    def fetch_pipeline_configuration(self):
        return self.get_config_manager()
    
    def parse_bot_commands(self, text_data=None):
        """
            Parses a list of events coming from the Slack RTM API to find bot commands.
            If a bot command is found, this function returns a tuple of command and channel.
            If its not found, then this function returns None, None.
        """

        user_id, message = self.parse_direct_mention(text_data)
        if user_id == self.bot_id:
            return message
        return None

    def parse_direct_mention(self, message_text):
        """
            Finds a direct mention (a mention that is at the beginning) in message text
            and returns the user ID which was mentioned. If there is no direct mention, returns None
        """

        matches = re.search(self.MENTION_REGEX, message_text)
        # the first group contains the username, the second group contains the remaining message
        return (matches.group(1), matches.group(2).strip()) if matches else (None, None)

    def handle_command(self, command_list=None):
        """
            Executes bot command if the command is known.

            If the bot is directly mentioned with a command
            word attached, this method will determine what
            the response will be. If the command is not known,
            a default response that indicates what commands
            are known will be returned instead.

            Parameters
            ----------
            command_list : list(str)
                A list of valid bot commands.
            
            Returns
            -------
            response : str
                The bot's response to a given command.
        """

        # Default response is help text for the user
        response = "Not sure what you mean. Try *{}*.".format(self.COMMAND_LIST)
        # Finds and executes the given command, filling in response
        command = command_list[0]            
        # This is where you start to implement more commands!
        if command in self.COMMAND_LIST:
            if command == "config":
                response = self.fetch_pipeline_configuration()._to_string()
            elif command == "running":
                response = self.fetch_pipeline_status()
            elif command == "jobs":
                if len(command_list) < 2:
                    response = self.fetch_current_jobs()
                else:
                    user_initials = command_list[1:]
                    response = self.fetch_current_jobs_by_user(user_initials)
            else:
                response = "That command hasn't been implemented yet."
            print(response)
        return response

    def setup_rtm_client(self, event_loop=None):
        slack_token = self.config_manager.fetch_from_slack_bot_configuration('bot_token')
        print("Connecting to RTMClient...")
        self.rtm_client = RTMClient(token=slack_token, run_async=True, loop=event_loop)
        print("Connected to RTMClient...")
        print("Starting Client")
        self.rtm_client.on(event='message', callback=[self.respond_to_user])

    def stop_bot(self):
        if self.rtm_client:
            self.rtm_client.stop()

    async def respond_to_user(self, **payload):
        print("Responding to user!")
        data = payload['data']
        command = self.parse_bot_commands(data['text'])
        print(f"Command: {command}")
        if command:
            command_list = command.split(" ")
            # task = asyncio.create_task(self.handle_command(command_list))
            response = self.handle_command(command_list)
            # response = self.handle_command(command_list)
            print(f"Response: {response}")
            web_client = payload['web_client']
            channel_id = data['channel']
            user = data['user']
            web_client.chat_postMessage(
                channel=channel_id,
                text=f"Hi <@{user}>!\n{response}",
            )
        await asyncio.sleep(1)
    
    async def say_hello(self, **payload):
        data = payload['data']
        web_client = payload['web_client']
        if 'Hello' in data['text']:
            channel_id = data['channel']
            user = data['user']
            web_client.chat_postMessage(
                channel=channel_id,
                text=f"Hi <@{user}>!",
            )
    async def print_payload(self, **payload):
        print(payload)
    

    async def update_information(self, q):
        while True:
            try:
                new_information = await q.aget()
                print(f"New Information {new_information}")
                if new_information is None:
                    break
                manager_names_value = new_information[0]
                submitted_file_name = new_information[2]
                task_statuses_value = new_information[3]
                task_updating_value = new_information[-1]
                info_to_handle = new_information[1:-1]
                # Adding new information
                manager = self.manager_dict.get(manager_names_value)
                if manager is None:
                    self.manager_dict[manager_names_value] = []
                    manager = self.manager_dict.get(manager_names_value)
                # Task updateing value handler
                if task_updating_value == -1:
                    manager.clear()
                elif task_updating_value == 1:
                    manager.append(info_to_handle)
                elif task_updating_value == 2:
                    update_index = next((i for i,item in enumerate(manager) if item[1] == submitted_file_name), None)
                    if update_index is None:
                        print(f"{new_information} could not update properly. Check this statement to get an understanding of why.")
                        print(f"{manager} This is the manager currently.")
                    else:
                        manager[update_index][2] = task_statuses_value
                else:
                    print(f"{info_to_handle} does not have proper formatting. Ignoring request.")
                print(self.manager_dict)
            finally:
                await q.task_done()
                await asyncio.sleep(1)
