import argparse
import asyncio
import logging
import logging.handlers
import os
import threading
from multiprocessing import JoinableQueue
from sys import exit, modules

import inotify_simple as sn

import job_functions as _jobs
import notify_user as _notify
from core import (ConfigurationManager, SlackBot, PipelineManager,
                  Task)


def generate_log_file_name():
    """
        Generate a file name for logging.
        If the file name does not exist, a
        new file is created.
    """

    file_name = "logs/rajagopal_pipeline_log_file.log"
    if not os.path.exists(file_name):
        with open(file_name, 'w') as _file:
            pass
    return file_name

def logging_queue_listener_configurer(log_level=logging.INFO):
    """
        Configure a logger to listen to handle queue-based messages.

        Given the desired level of logging the file logger should be set to,
        generate a rotating file structure to write messages to as a handler
        and add it to the logger.

        NOTE: If other handlers are desired, modify this function.

        Paramters
        ---------
        log_level : int
            An integer representation of what level of log messages
            are shown. The loging type must be equal or greater than
            this value to be handled.

        Returns
        -------
        None.
    """

    listener_logger = logging.getLogger()
    log_file_name = generate_log_file_name()
    # Create a timed file rotation that should cover 24 hours in 6 hour shifts. Oldest copies do get deleted so this shouldn't be an issue.
    file_handler = logging.handlers.TimedRotatingFileHandler(log_file_name, when='H', interval=6, backupCount=4)
    file_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(file_formatter)
    listener_logger.setLevel(log_level)
    listener_logger.addHandler(file_handler)

def logging_background_thread(logging_queue=None, log_level=None):
    """
        Initialize the logging thread to run in the background.

        Given a logging queue to process messages and the desired
        level of logging the file logger should be set to, generate
        a logger that exclusively reads from the logging_queue and
        handles results according to how it is configured.

        Paramters
        ---------
        logging_queue : JoinableQueue
            The queue structure meant to handle logging messages
        log_level : int
            An integer representation of what level of log messages
            are shown. The loging type must be equal or greater than
            this value to be handled.

        Returns
        -------
        None.
    """

    logging_queue_listener_configurer(log_level=log_level)
    while True:
        record = logging_queue.get()
        if record is None:
            break
        logger = logging.getLogger(record.name)
        logger.handle(record)
        logging_queue.task_done()

def slack_bot_background_thread(slack_bot, slack_loop, slack_queue):
    """
        Initialize the Slack bot thread to run in the background.

        Given a configured Slack bot, the asyncio event loop it
        is meant to run on and a specialized queue structure to
        handle information given to it by the pipeline, generate
        a thread that serves as a bridge between the user and the
        pipeline.

        Parameters
        ----------
        slack_bot : SlackBot
            A variant of the Slack RTM Client that is
            designed specifically for use in the pipeline.
        slack_loop : asyncio.event_loop
            An event loop designated for use by the Slack bot
        slack_queue : SlackBot.SlackQueue
            A special variant of the Queue structure meant for async/sync interaction.
            Used to communicate pipleine information to the Slack bot.

        Returns
        -------
        None.
    """

    asyncio.set_event_loop(slack_loop)
    slack_bot.setup_rtm_client(event_loop=slack_loop)
    slack_loop.run_until_complete(asyncio.gather(slack_bot.rtm_client.start(), slack_bot.update_information(slack_queue)))
    slack_loop.stop()
    slack_loop.close()

def setup_watchdog(directories=None,
                   watch_description_to_dir=None,
                   watch_flags=None):
    """
        Setup the watchdog on specific directories.

        Setup a inotify instance on given directories
        and return it if it successfully adds the
        directories to its watchlist.

        Parameters
        ----------
        directories : list(str)
            The list of directories to place a watcher on.
        watch_descripton_to_dir : dictionary(int, str)
            A dictionary mapping existing new/existing watch description
            to its corresponding directory path. If None, a new dictionary
            is created.
        watch_flags : Inotify EnumType
            A series of enumerated types corresponding
            to a hexadecimal value. Multiple flags end
            up getting bitwise OR'd together. If None,
            then default flags are assumed (MOVE_TO, CREATE)
        Returns
        -------
        watchdog : Inotify
            An Inotify class that handles the
            notification for when a directory
            changes.
        watch_descripton_to_dir : dictionary(int, str)
            An updated version of watch_description_to_dir.
        Raises
        ------
        ValueError
            If no directories are provided, it will raise this error.
        FileNotFoundError
            If a directories in the list does not exist, it will raise this error.
    """
    #If no watch flags were provided, use the default flags (file CREATED in directory, file MOVED TO directory).
    if watch_flags is None:
        watch_flags = sn.flags.MOVED_TO
    if watch_description_to_dir is None:
        watch_description_to_dir = dict()
    if directories is None:
        directories = [os.getcwd()]
    watchdog = sn.INotify()
    for directory in directories:
        #If the path is valid and the location itself is a directory, place a watcher on it.
        if os.path.exists(directory) and os.path.isdir(directory):
            watch_descriptor = watchdog.add_watch(directory, watch_flags)
            #Create a dictionary that maps watch descriptors (integers) to the directory its watching (str).
            watch_description_to_dir.setdefault(watch_descriptor,
                                                str(directory))
        else:
            pass
    print("Watchdog using flags: {}".format(sn.flags.from_mask(watch_flags)))
    return watchdog, watch_description_to_dir

def fetch_function_from_string(astr):
    """
        Fetch the function whose name matches the string given.

        Given a string that refers to some function name, this
        method will search through the currently available functions
        (which may be in different modules) and attempts to return
        the module and function. If the module said function is tied
        to is not already imported, it will attempt to import it.

        Parameters
        ----------
        astr : str
            The string that represents the function. It may be in
            the form modulename.functioname, where modulename refers
            to the name of the Python script (e.g., upload_media).

        Returns
        -------
        mod : Module
            The module that may be imported in order to access the
            function. It is not used aside from this purpose.
        function : Function
            The function whose name corresponds to astr.
        
        Raises
        ------
        ImportError : If the module needed cannot be imported,
        then this error is raised.
    """

    # Parse our the module and function name.
    module, _, function = astr.rpartition('.')
    # If there is a module component
    if module:
        # If the module is not already loaded in, import it.
        if modules.get(module) is None:
            try:
                __import__(module)
                mod = modules[module]
            except:
                raise ImportError(
                    "Cannot import the module {}, if importing from another file use full script name."
                    .format(module))
        else:
            mod = modules.get(module)
    else:
        mod = modules['__main__']  # or whatever's the "default module"
    # Attempt to return the function present in the module
    return getattr(mod, function)

# Taken from https://stackoverflow.com/questions/312443/how-do-you-split-a-list-into-evenly-sized-chunks
def generate_chunked_list(list_to_chunk=None, chunk_size=None):
    """
        Split a list into chunks that contain n-elements.

        Creates a Generator function that takes a list and
        breaks it down such that each chunk contains at most
        chunk_size elements. The final chunk will contain any
        remainder if modulo(list_to_chunk, chunk_size) != 0.

        Parameters
        ----------
        list_to_chunk : list
            A list that needs to be broken into chunks
        chunk_size : int
            An integer representing the maximum number
            of elements a resulting sublist can contain.

        Returns
        -------
        func : Generator function
            An iterable Generator function
    """
    for i in range(0, len(list_to_chunk), chunk_size):
        yield list_to_chunk[i:i + chunk_size]

def load_tasks_into_queues(task_list=None, manager_list=None, config_manager=None, logger=None):
    """
        Load a list of tasks into their respective starting queues.

        Given a list of tasks, the list of PipelineManagers,
        the ConfigurationManager and the logger (if present), consume
        the list of tasks and enqueue them to their appropriate
        locations. This function can also enforce batch tasks,
        should the flag be set in the ConfigurationManager.

        Parameters
        ----------
        task_list : list(Task)
            A list of Task objects to be handled
        manager_list : list(PipelineManager)
            A list of all PipelineManagers (and thus the pipeline sections)
        config_manager : ConfigurationManager
            The structure holding the current configuration of the pipeline
        logger : logging.Logger
            A file-logger meant to keep track of events occuring
        
        Returns
        -------
        None.
    """

    for task in task_list:
        manager_to_insert = next((manager for manager in manager_list if manager.name == task.start_pipeline), None)
        if manager_to_insert:
            logger.debug(f"Inserting {task.get_file_name()} into manager {manager_to_insert.name}")
            manager_to_insert.input_queue.put(task)
        else:
            logger.debug(f"Could not insert {task.get_file_name()} into manager {manager_to_insert.name}")
        # Notify the user that their Task has been taken on by the pipeline.
        if config_manager.fetch_from_pipeline_command_line_arguments('enable_notifications'):
            pass

def run_manager_loop(manager_list=None, logger=None, slack_bot_enabled=None):
    """
        Perform the control loop for all managers.

        Wrapper function to help readability of code.
        See PipelineManager for explanation of each
        function call.
        
        Parameters
        ----------
        manager_list : list(PipelineManager)
            A list of all PipelineManagers (and thus the pipeline sections)
        logger : logging.Logger
            A file-logger meant to keep track of events occuring
        slack_bot_enabled : bool
            A flag indicating if the SlackBot is active or not.
        
        Returns
        -------
        None.
    """

    for manager in manager_list:
        logger.info(f"Running manager {manager.name}")
        # Set up the manager by creating Process objects to manage
        manager.setup_manager()
        # Place the sentinel items to kill processes (Poison Pill Method)
        manager.end_manager()
        # Run the manager (this will only exit when all its processes are dead.)
        manager.run_manager()
        # Clean-up of resources (from my understanding of the code.)
        manager.cleanup_manager()
        # Tell the slack bot to clean out its dict structure.
        if slack_bot_enabled:
            manager.push_cleaning_signal_to_slack_bot()
    # Once all sections are finished processing, then perform cleanup
    logger.info(f"Cleaning up output queue of {manager_list[-1].name}")
    _jobs.media_cleanup_job(manager_list[-1].output_queue)
    logger.info(f"Cleaning up error queue of {manager_list[-1].name}")
    _jobs.media_cleanup_job(manager_list[-1].error_queue)

def generate_pipeline_system(config_manager=None, slack_queue=None, logging_queue=None, logger=None):
    """
        Generate the pipeline system for this run.

        Using the ConfigurationManager, along with a few extras
        create the pipeline system that is going to be used for
        this iteration, tailored to the user arguments provided.

        Parameters
        ----------
        config_manager : ConfigurationManager
            The structure holding the current configuration of the pipeline
        slack_queue : SlackBot.SlackQueue
            A special variant of the Queue structure meant for async/sync interaction.
            Used to communicate pipleine information to the Slack bot SlackBot.
        logging_queue : JoinableQueue
            A multiprocessing-friendly Queue that is used to handle logging messages
            from the various PipelineManagers.
        logger : logging.Logger
            A file-logger meant to keep track of events occuring

        Returns
        -------
        manager_list : list(PipelineManager)
            A list of all PipelineManagers (and thus the pipeline sections)          
    """

    manager_list = []
    queues = dict()
    for config in config_manager.get_pipeline_configuration():
        input_queue_name = config.get("input_queue")
        output_queue_name = config.get("output_queue")
        error_queue_name = config.get("error_queue")
        # If the queue hasn't been made already, make it and insert it into the placeholder dictionary
        if queues.get(input_queue_name) is None:
            input_queue = JoinableQueue()
            queues[input_queue_name] = input_queue
        else:
            input_queue = queues.get(input_queue_name)

        if queues.get(output_queue_name) is None:
            output_queue = JoinableQueue()
            queues[output_queue_name] = output_queue
        else:
            output_queue = queues.get(output_queue_name)

        if queues.get(error_queue_name) is None:
            error_queue = JoinableQueue()
            queues[error_queue_name] = error_queue
        else:
            error_queue = queues.get(error_queue_name)
        # Construct the PipelineManager instance using the queues fetched/created and append it to the list
        manager = PipelineManager.PipelineManager(
            name=config.get("name"),
            input_queue=input_queue,
            output_queue=output_queue,
            error_queue=error_queue,
            slack_queue=slack_queue,
            logging_queue=logging_queue,
            process_job=fetch_function_from_string(config.get("process_job")),
            num_processes=config.get("num_workers"),
            timeout_duration=config.get("timeout_duration"))
        # For readability purposes, assign names to the I/O Queues
        manager.input_queue_name = input_queue_name
        manager.output_queue_name = output_queue_name
        # Log the current configuration of the PipelineManager
        if logger and logger.getEffectiveLevel() == logging.DEBUG:
            logger.debug(f"\n{manager._to_string()}")
        else:
            logger.info(f"{manager.name} initialized.")
        manager_list.append(manager)
    return manager_list


async def _main(args):

    '''LOGGER CREATION SECTION'''
    logging_queue = JoinableQueue()
    # Fetch which level the logger is set to from the logging module directly
    log_level = logging._nameToLevel.get(args.get('log_level').upper())
    # Create and start the logging thread with the queue and logging level
    log_thread = threading.Thread(target=logging_background_thread, args=(logging_queue, log_level), daemon=True)
    log_thread.start()
    # Fetch/create a logger for this particular script ((__name__) will return manage_pipelines.py)
    logger = logging.getLogger(__name__)
    # TODO: Find all these statements and somehow get them to be equal to log_level (this one is easy but the others?)
    logger.setLevel(args.get('log_level').upper())
    '''CONFIGURATIONMANAGER CREATION SECTION'''
    # Initialize the ConfigurationManager
    config_manager = ConfigurationManager.ConfigurationManager("config/")
    # Update the dictionary with whatever variables were passed in via command-line arguments
    config_manager.pipeline_command_line_arguments.update(args)
    # Update the ConfigurationManager to include the information about how this pipeline is going to be built
    pipeline_config_name = "pipeline_config.json"
    config_manager.pipeline_configuration = config_manager.load_json(json_file_name=pipeline_config_name)

    # Determine if SlackBot is enabled or not.
    slack_bot_enabled = config_manager.fetch_from_pipeline_command_line_arguments('enable_slackbot')

    # Configure the root directory that contains all files the watchdog will be supervising
    # If the user provided an argument for this, then use that instead of the default
    if config_manager.fetch_from_pipeline_command_line_arguments('directory'):
        input_directory_path = config_manager.fetch_from_pipeline_command_line_arguments('directory')
    else:
        raise ValueError("Need to have a directory to watch!")

    logger.info(f"\n{config_manager._to_string()}")
    # If SlackBot was enabled then create the loop and SlackQueue for it
    if slack_bot_enabled:
        slack_loop = asyncio.new_event_loop()
        # Create the special async-sync friendly SlackQueue to deal with incoming information
        slack_queue = SlackBot.SlackQueue()
        # Load the Slack RTM secrets
        config_manager.slack_bot_configuration = config_manager.load_json(json_file_name='secrets/slack_bot_secrets.json')
    else:
        # Set it to None so that other stuff doesn't break
        slack_queue = None
    # Declare which flags are going to be looked for. See inotify_simple for the complete list
    flags = sn.flags.MOVED_TO | sn.flags.ISDIR
    watchdog, wd2dir = setup_watchdog([input_directory_path], watch_flags=flags)
    # Generate the pipeline structure and place it into manager_list
    manager_list = generate_pipeline_system(config_manager=config_manager,
                                            slack_queue=slack_queue,
                                            logging_queue=logging_queue,
                                            logger=logger)
    if slack_bot_enabled:
        # Initialize the data structure and start the SlackBot thread
        slack_manager_dict = dict.fromkeys([manager.name for manager in manager_list])
        slack_bot = SlackBot.SlackBot(config_manager=config_manager, manager_dict=slack_manager_dict, slack_queue=slack_queue)
        if logger and logger.getEffectiveLevel() == logging.DEBUG:
            logging.info(f"\n{slack_bot._to_string(slack_bot.__dict__)}")
        else:
            logger.info(f"Slack bot initialized.")
        t = threading.Thread(target=slack_bot_background_thread, args=(slack_bot, slack_loop, slack_queue), daemon=True)
        t.start()
    
    # If notifications are enabled, then inform people about the pipeline being active
    if config_manager.fetch_from_pipeline_command_line_arguments('enable_notifications') == 1:
        _notify.notify_channel_pipeline_status(pipeline_status="enabled")
    
    '''MAIN CONTROL LOOP'''
    while True:
        # Try to do this forever until a CTRL-C/SIGINT event occurs (or something else that kills this)
        try:
            # Restart/Setup all pipeline sections (Create multiprocessing.Process instances and append them to process list)
            for manager in manager_list:
                if manager.restart_required:
                    manager.setup_manager()
            tasks = []
            print("Polling {} now...\n".format(input_directory_path))
            #Set up the watchdog to wait 10 seconds after receiving the first file before doing anything.
            for event in watchdog.read(read_delay=10000):
                # sn.flags.from_mask should return a list of something that may contain the ISDIR flag. This is used to detect certain types of tasks.
                if sn.flags.ISDIR in sn.flags.from_mask(event.mask):
                    is_directory = True
                else:
                    is_directory = False
                # Coalese the event information into a dictionary
                event_information = {
                    "name": event.name,
                    "directory_path": wd2dir.get(event.wd),
                    "is_directory": is_directory
                }
                # Add all the tasks to the list of tasks.
                try:
                    # NOTE: Insert Task generation function here.
                    task_to_add = None
                except BaseException as _e:
                    logger.error(f"\nCould not generate task from event {event.name} due to error {_e}.")
                    print(f"Could not generate task from event {event.name} due to error {_e}.")
                    continue
                if task_to_add is None:
                    logger.error(f"\n{event.name} was unable to be converted into a Task and will be ignored.")
                    print(f"{event.name} was unable to be converted into a Task and will be ignored.")
                else:
                    tasks.append(task_to_add)
                    print(f"{event.name} was converted into a Task and appended.")
            batch_size = config_manager.fetch_from_pipeline_command_line_arguments('batch_size')
            # If no batch size was provided or it's too small, just do all tasks at once.
            if batch_size is None or batch_size < 1:
                batch_size = len(tasks)
            logger.debug(f"\nBatch Size for tasks is currently {batch_size}")
            # chunked_sumbission_list is a generator (but it is iterable)
            chunked_task_list = generate_chunked_list(tasks, batch_size)
            # task_chunk is a list so it can be treated like a regular list
            for task_chunk in chunked_task_list:
                # Note which task chunk is being processed (This may return object addresses)
                logger.debug(f"\nTask Chunk to process: {task_chunk}")
                # Load the tasks into their appropriate queue structures (this may enforce batch tasks)
                load_tasks_into_queues(task_chunk, manager_list, config_manager, logger)
                # Run the main control loop for each manager
                run_manager_loop(manager_list=manager_list, logger=logger, slack_bot_enabled=slack_bot_enabled)
        # Handle the Control-C event (manual interruption of the pipeline.)
        except KeyboardInterrupt:
            # Inform via console and log file
            print("Control-C/SIGINT detected. Beginning cleanup.")
            logger.info("Pipeline has been turned off via KeyboardInterrupt (CTRL-C/SIGINT)")
            # If notifications are enabled, inform the users via Slack of the pipeline status
            if config_manager.fetch_from_pipeline_command_line_arguments('enable_notifications'):
                _notify.notify_channel_pipeline_status(pipeline_status="disabled")

            for manager in manager_list:
                # Purge the input queue of unfinished tasks (placing them in error queue)
                manager.purge_manager()
                # Terminate and close any surviving processes
                manager.cleanup_manager()
                # Clean out the error queue.
            logger.info(f"Cleaning up error queue of {manager_list[-1].name}")
            _jobs.media_cleanup_job(manager_list[-1].error_queue)
            # End the logging thread
            if logging_queue:
                logger.info("Ending Log File due to SIGINT event.")
                logging_queue.put(None)
                log_thread.join()
            # If SlackBot was active, end its thread
            if slack_bot_enabled:
                for manager in manager_list:
                    manager.push_cleaning_signal_to_slack_bot()
                #Send poison pill to slack queue
                print("Killing Slack Queue")
                asyncio.run_coroutine_threadsafe(slack_queue.aput(None), slack_loop).result()
                #Stop the slack bot.
                print("Stopping Slack Bot")
                slack_bot.rtm_client.stop()
                print("Waiting for thread..")
                t.join()
            exit(0)

if __name__ == "__main__":
    PARSER = argparse.ArgumentParser(
        description="Activate the media pipeline.\n\
            Example of program: python manage_pipelines.py -d <Location of root task directory> -m <machine_selection>\n\
            Activation of the Rajagopal Pipeline System creates watche(s) on a given directory.\n")
    PARSER.add_argument('-d',
                        '--directory',
                        dest='directory',
                        default=None,
                        help='The parent directory of all user tasks.',
                        type=str)
    PARSER.add_argument(
        '-n',
        '--notifications',
        dest='enable_notifications',
        default=1,
        choices=[0, 1],
        help='Do you want notifications enabled?\n\
              By default (1), notifications sent via Slack are\
              enabled. This option is separate from the SlackBot\
              notification system, which is toggled via the --bot flag.',
        type=int)
    PARSER.add_argument(
        '-b',
        '--bot',
        dest='enable_slackbot',
        default=1,
        choices=[0, 1],
        help='Do you want to enable the SlackBot?\n\
              By default (1), SlackBot is enabled when the\
              pipeline is turned on. It is recommended to turn\
              this option off only when debugging. This option\
              does not control the Robot Overlord notifications,\
              which can be toggled via the --notifications flag.',
        type=int)
    PARSER.add_argument(
        '-l',
        '--loglevel',
        dest='log_level',
        default='info',
        choices=['notset', 'debug', 'info', 'warning', 'error', 'fatal', 'critical'],
        help='What level do you want the logging to be at?\nChoices made are\
              based on the logging library standard, so consult the documentation\
              if you need more information. By default, this is set to INFO.',
        type=str
    )

    PARSER.add_argument(
        '--batch_size',
        dest='batch_size',
        default=10,
        help='How many tasks should be processed at a time by the pipeline?\n\
              A positive non-zero value will indicate that tasks should be handled\
              in batches of size equal to the value provided. Otherwise the tasks are\
              processed all at once. The default value for this is 10.',
        type=int
    )
    #Parse the arguments into a dictionary (key, value) structure
    ARGUMENTS = vars(PARSER.parse_args())
    try:
        asyncio.run(_main(ARGUMENTS), debug=True)
    except KeyboardInterrupt:
        print("Control C Detected, shutting down.")
        exit(0)
