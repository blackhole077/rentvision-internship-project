import logging
import logging.handlers
import queue
from multiprocessing import JoinableQueue, Process, active_children
from time import sleep
# IMPORTS FOR TYPE HINTING
from typing import Callable, List, Optional, Type, Union

class PipelineManager:
    """
        Class encapsulating multiprocessing library in Python.

        This class is meant to encapsulate the multiprocessing
        functionality already present in Python and combine it
        with several other useful libraries, to serve as a way
        to have both convenience and functionality.

        Parameters
        ----------
        name : str
            The name of the PipelineManager. Meant for differentiation.
        input_queue : multiprocessing.JoinableQueue
            The JoinableQueue from which Processes will be pulled from.
            See the Multiprocessing documentation for more information.
        output_queue : multiprocessing.JoinableQueue
            The JoinableQueue which Processes will place finished items in.
            See the Multiprocessing documentation for more information.
        error_queue : multiprocessing.JoinableQueue
            The JoinableQueue which Processes will place bugged/unfinished items in.
            See the Multiprocessing documentation for more information.
        slack_queue : SlackBot.SlackQueue
            A wrapper class around multiprocessing.JoinableQueue that allows for
            asyncio-friendly operations.
        logging_queue : multiprocessing.JoinableQueue
            The JoinableQueue which is meant to handle logging messages.
            See the Multiprocessing documentation for more information.
        num_processes : int
            The number of Process objects that will be made by this PipelineManager.
        process_job : function
            The function that each Process will be given to work on.
            Make sure that this lines up with what the input/output Queues expect.
        timeout_duration : int
            The number of seconds which a Process will attempt to get an item from
            input_queue before throwing a TimeoutError.
    """

    def __init__(self,
                 input_queue: JoinableQueue,
                 output_queue: JoinableQueue,
                 error_queue: JoinableQueue,
                 slack_queue: 'SlackBot.SlackQueue',
                 logging_queue: JoinableQueue,
                 process_job: Callable[[Type['Task.Task']], Type['Task.Task']],
                 name: str ="PipelineManager",
                 num_processes: int = 1,
                 timeout_duration: int = 1) -> None:
        """
            Initialize a PipelineManager Object.

            Initializes an instance of PipelineManager with a name,
            given pair of input/output queues (important for chaining),
            the number of processes the PipelineManager will deal with,
            the actual target function used (see Process in multiprocessing),
            and how long a given Process should wait until returning a TimeoutError.

            NOTE: Requres multiprocessing package

            Parameters
            ----------
            name : str
                The name of the PipelineManager. Meant for differentiation.
            input_queue : multiprocessing.JoinableQueue
                The JoinableQueue from which Processes will be pulled from.
                See the Multiprocessing documentation for more information.
            output_queue : multiprocessing.JoinableQueue
                The JoinableQueue which Processes will place finished items in.
                See the Multiprocessing documentation for more information.
            error_queue : multiprocessing.JoinableQueue
                The JoinableQueue which Processes will place bugged/unfinished items in.
                See the Multiprocessing documentation for more information.
            slack_queue : SlackBot.SlackQueue
                A wrapper class around multiprocessing.JoinableQueue that allows for
                asyncio-friendly operations.
            logging_queue : multiprocessing.JoinableQueue
                The JoinableQueue which is meant to handle logging messages.
                See the Multiprocessing documentation for more information.
            num_processes : int
                The number of Process objects that will be made by this PipelineManager.
            process_job : function
                The function that each Process will be given to work on.
                Make sure that this lines up with what the input/output Queues expect.
            timeout_duration : int
                The number of seconds which a Process will attempt to get an item from
                input_queue before throwing a TimeoutError.
                
            Returns
            -------
            self : PipelineManager
                A PipelineManager object.

            Raises
            ------
            None.
        """

        self.name = name
        #An attempt to idiot-proof the PipelineManager by instantiating a JoinableQueue() if one didn't exist already.
        self.input_queue = input_queue if input_queue else JoinableQueue()
        self.output_queue = output_queue if output_queue else JoinableQueue()
        self.error_queue = error_queue if error_queue else JoinableQueue()
        self.slack_queue = slack_queue
        self.logging_queue = logging_queue
        self.num_processes = num_processes
        self.process_job = process_job
        self.timeout_duration = timeout_duration
        #A list of active processes comprised of Process objects
        self.process_list: List[Process] = []
        #An internal restart flag (used when all processes managed die)
        self.restart_required = False
        self.logger = logging.getLogger(self.name)
        self.logger.setLevel(logging.DEBUG)

    def setup_manager(self) -> None:
        """
            Create Processes to be run by the PipelineManager.

            This function will create a number of Process objects,
            but does not run them immediately. This function is meant
            to be used if data is not yet ready in the JoinableQueue.

            Parameters
            ----------
            None.

            Returns
            -------
            None.
        """

        #Clean out the process list.
        self.process_list.clear()
        for _ in range(self.num_processes):
            p = Process(target=self.multiprocessing_job,
                        args=(self.process_job,))
            self.process_list.append(p)
        self.restart_required = False

    def run_manager(self) -> None:
        """
            Begin running the PipelineManager.

            This function will start any Process objects
            inside of the PipelineManager. From there,
            it will poll every 1 seconds to check if any of
            the Process objects are still active.

            Parameters
            ----------
            None.

            Returns
            -------
            None.
        """
        
        for p in self.process_list:
            try:
                p.daemon = True
                p.start()
            except:
                self.process_list.remove(p)
                p = Process(target=self.multiprocessing_job, args=(self.process_job,))
                p.daemon = True
                self.process_list.append(p)
                p.start()
        #Every 1 seconds, check for active Processes.
        while True:
            sleep(1)
            running = any(p.is_alive() for p in self.process_list)
            if not running or not active_children:
                self.restart_required = True
                break
        self.logger.info(self.name + " has finished managing.")

    def end_manager(self) -> None:
        """
            Send poison pills to the processes under the manager.

            Sends None Tasks into the JoinableQueue, which
            will cause the Process that picks them
            up to terminate.

            Parameters
            ----------
            None.

            Returns
            -------
            None.
        """
        
        for _ in range(self.num_processes):
            self.input_queue.put(None)
    
    def kill_manager(self) -> None:
        """
            Kill all processes under the manager
            in the event of interruption.

            This function will terminate all processes
            and attempt to release any resources held
            in the event of interruption to avoid potential
            leakage and orphaned processes.

            Parameters
            ----------
            None.

            Returns
            -------
            None.
        """

        for p in self.process_list:
            p.terminate()
            # NOTE: Seems Python does not appreciate if close is called too quickly.
            sleep(0.5)
            # Release the resources held by the Proess (Python 3.7 and up)
            p.close()
    
    def purge_manager(self) -> None:
        """
            Purge the manager in the event of interruption.
            
            This function flushes the input queue and attempts
            to place any pending tasks into the error queue for
            further handling/disposal, flushing the queue afterwards.

            Parameters
            ----------
            None.

            Returns
            -------
            None.
        """

        self.logger.debug(f"Beginning purging of {self.name}")
        # Don't need to join an empty queue, likely will result in more issues if you do.
        if self.input_queue.empty():
            self.logger.debug(f"Input Queue of {self.name} is empty.")
        else:
            # Try to process all the remaining values put in (no need to change behavior around poison pills)
            while not self.input_queue.empty():
                try:
                    task = self.input_queue.get_nowait()
                    self.logger.debug(f"Receiving Task to purge: {task.get_job_id()}")
                    self.error_queue.put(task)
                    # NOTE: This sleep call may be unnecessary but I placed it here to err on the side of caution.
                    sleep(1)
                    self.input_queue.task_done()
                except:
                    break
            # NOTE: This loop is a bit hacky probably, but it does ensure that the correct number of task_done calls are made.
            # NOTE: This is meant to handle any "unfinished tasks" (meaning ones that haven't had their task_done calls).
            while not self.input_queue._unfinished_tasks._semlock._is_zero():
                self.input_queue.task_done()
        self.restart_required = True

    def cleanup_manager(self) -> None:
        """
            Clean up the manager by releasing the resources held by each process.

            Parameters
            ----------
            None.

            Returns
            -------
            None.
        """
        
        for p in self.process_list:
            if p.is_alive():
                p.terminate()
                sleep(1)
                p.close()

    def print_process_list(self) -> None:
        """
            Print the process list.

            A convenience function that
            prints the processes under
            this PipelineManager.

            Parameters
            ----------
            None.

            Returns
            -------
            None.
        """

        print(f"Process List: {self.process_list}")

    def print(self) -> None:
        """
            Print all relevant information on this PipelineManager.

            Prints out all arguments (except the JoinableQueue objects) that
            are associated with this PipelineManager. Meant for debugging
            purposes.

            Parameters
            ----------
            None.

            Returns
            -------
            None.
        """

        print("Name: {}".format(self.name))
        print("Input Queue: {}".format(self.input_queue))
        print("Output Queue: {}".format(self.output_queue))
        print("Restart Required: {}".format(str(self.restart_required)))
        print("Number of Processes: {}".format(str(self.num_processes)))
        print("Process Job: {}".format(self.process_job.__name__))
        print("Timeout Duration: {}".format(str(self.timeout_duration)))
        self.print_process_list()

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
                string_list.append(key)
                string_list.extend('\n'.join(["Key: {:24}\tValue: {}".format(_key, _value) for _key, _value in value.items()]))
            else:
                string_list.append("Key: {:24}\tValue: {}\n".format(key, value))
        return ''.join(string_list)  

    def push_information_to_slack_bot(self, q: 'SlackBot.SlackQueue', item: List[Optional[Union[str, int]]]) -> None:
        """
            Push some information into the Slack Bot.

            A wrapping function around the synchronous
            'put' function for the Slack Queue. It takes
            in some item and enqueues it for processing
            by the Slack Bot.

            NOTE: This function is only necessary when the
            Slack Bot is enabled.

            NOTE: The current implementation of the Slack
            Bot does assume that item is a list. However,
            this function does not necessarily abide that
            assumption.

            Parameters
            ----------
            q : SlackQueue
                The queue that serves as the bridge between
                the PipleineManager(s) and the Slack Bot.
            item : Object
                The item to be enqueued for processing by
                the Slack Bot.
        """

        q._queue.put(item)
    
    def push_cleaning_signal_to_slack_bot(self) -> None:
        """
            Push the signal to flush the contents in the Slack Bot.
            
            This function is used to remove information about Submissions
            within this manager from the Slack Bot. As such, this function
            should only be called when cleaning up or exiting the program.

            NOTE: This function is only necessary when the Slack Bot is active.

            Parameters
            ----------
            None.
            
            Returns
            -------
            None.
        """

        item = [self.name, None, None, "Purge", -1]
        if self.slack_queue:
            self.push_information_to_slack_bot(self.slack_queue, item)
        else:
            self.logger.debug(f"No Slack Queue detected in {self.name}")

    def multiprocessing_job(self, job_function: Callable[['Task.Task'], 'Task.Task']=None) -> None:
        """
            Build a multiprocessing-friendly job.

            Wraps a function in JoinableQueue-specific
            functionality, including the error-handling
            and moving of finished tasks to the next section.
            Ideally, this should allow for DRY code and more
            flexibility in what functions are passed in.

            Parameters
            ----------
            self : PipelineManager
                The PipelineManager instance that this will be tied to.
            job_function : function
                A function that takes a Submission or similar parameter(s)
                as its parameters. It should return the same thing to ensure
                everything works as intended.

            Returns
            -------
            None.

            Raises
            ------
            TimeoutError : If nothing could be fetched from the InputQueue within
                the timeout_duration, then this is raised
            queue.Empty : If the input_queue is empty, this is raised.
        """

        if job_function is None:
            raise ValueError("Job Function object not provided.")
        while True:
            try:
                task = self.input_queue.get(timeout=1)
                if task:
                    '''
                        Based on how this should work, job_function must take
                        in a Submission object as its sole parameter, and
                        return a Submission object. To avoid major overhauls,
                        it's best to keep the '*_job' functions mostly the same,
                        but changing the function parametes and what it returns.
                    '''
                    if self.logger.getEffectiveLevel() == logging.DEBUG:
                        self.logger.debug(f"\nTask Received: {task._to_string()}\n")
                    else:
                        self.logger.info(f"\nTask Received: {task.get_job_id()} containing file {task.get_file_name()}\n")
                    try:
                        # Push the name of the manager and its current jobs.
                        if self.slack_queue:
                            slack_information = [self.name, task.fetch_info('user_initials'), task.get_file_name(), "Pending", 1]
                            self.push_information_to_slack_bot(self.slack_queue, slack_information)
                        # Perform the job function on the task
                        task = job_function(task)
                        # Upon success, push information to the Slack Bot (if enabled)
                        if self.slack_queue:
                            slack_information = [self.name, task.fetch_info('user_initials'), task.get_file_name(), "Complete", 2]
                            self.push_information_to_slack_bot(self.slack_queue, slack_information)
                        # Log the task completion
                        self.logger.info(f"\nTask Completed: {task.get_job_id()} containing file {task.get_file_name()}\n")
                        # Place the task in the output queue
                        self.output_queue.put(task)
                        # Mark the task as finished via task_done
                        self.input_queue.task_done()
                        # Take a short breather and let the CPU focus on other stuff.
                        sleep(1)
                    except KeyboardInterrupt as k:
                        if self.logger.getEffectiveLevel() == logging.DEBUG:
                            self.logger.exception(msg=f"Task {task.job_id} encountered an exception in job {job_function.__name__} due to SIGINT event.\n")
                        else:
                            self.logger.error(msg=f"\nTask {task.job_id} encountered an exception in job {job_function.__name__}\nError Message: {k}\n")
                    # If any errors that can be caught occur
                    except BaseException as e:
                        # Log the event with varying degrees of information
                        if self.logger.getEffectiveLevel() == logging.DEBUG:
                            self.logger.exception(msg=f"Task {task.job_id} encountered an exception in job {job_function.__name__}\nTask Information:\n{task._to_string()}")
                        else:
                            self.logger.error(msg=f"\nTask {task.job_id} encountered an exception in job {job_function.__name__}\nError Message: {e}\n")
                        # Inform the submitter via Slack Notification (not Slack Bot)
                        # If the Slack Bot is enabled pass the error information to it
                        if self.slack_queue:
                            slack_information = [self.name, task.fetch_info('user_initials'), task.get_file_name(), "Error", 2]
                            self.push_information_to_slack_bot(self.slack_queue, slack_information)
                        # Place the task in the error queue
                        self.error_queue.put(task)
                        # Mark the task as finished via task_done
                        self.input_queue.task_done()
                else:
                    #Media was None signal (this kills the Process)
                    self.input_queue.task_done()
                    break
            except ValueError:
                break
            # If the worker does not receive anything it will throw this exception, which will kill the process.
            except queue.Empty:
                self.logger.debug(f"\nA process has enountered the Queue.Empty exception indicating there are no more things to process.")
                break
