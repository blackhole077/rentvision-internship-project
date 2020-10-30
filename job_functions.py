import logging
from queue import Empty
from time import sleep
from utils import pipeline_utils as _putils

job_logger = logging.getLogger(__name__)

def job_one(task):
    print("This is Job One")
    sleep(1)
    return task

def job_two(task):
    print("This is Job two")
    sleep(2)
    return task

def job_three(task):
    print("This is Job three")
    sleep(5)
    return task

def job_four(task):
    print("This is Job four")
    sleep(10)
    return task

def job_final(task):
    print("This is the final job")
    sleep(1)
    return task

### JOB FUNCTIONS THAT DON'T USE PIPELINEMANAGER ###
def cleanup_job(finished_tasks_queue):
    """
        Perform the cleanup process on finished tasks.

        This function serves as the final stop for any Task
        that has gone through the pipleine, regardless of whether
        uploading is in effect or not, or its success state. The
        function will remove intermediate files generated during
        the Task's stay in the pipeline to reclaim as much
        hard disk space as possible.

        NOTE: If 'resumable tasks' are slated for design, this
        function must change accordingly to prevent deletion of resumable
        Tasks.

        NOTE: Though the variables are named under the assumption of
        failed tasks, tasks that executed successfully are also handled
        by this function.

        Parameters
        ----------
        finished_tasks_queue : JoinableQueue
            A JoinableQueue that houses all Tasks that were
            submitted to the pipeline. It handles both successfully
            executed Tasks and failed Tasks.

        Returns
        -------
        None.
    """

    finished_tasks_queue.put(None)
    while True:
        try:
            failed_task = finished_tasks_queue.get(timeout=5)
            if failed_task:
                # NOTE: This is where any 'cleanup' of resources, files made, etc. would occur.
                failed_task._print()
            else:
                break
        except TimeoutError:
            break
        except Empty:
            break
        finally:
            finished_tasks_queue.task_done()
