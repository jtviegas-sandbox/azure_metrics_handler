import abc
import enum
import logging
import os
import time
import random

from azmeha.metrics import DagMetrics, DagMetric, DagStatus

LOGGING_MSG_FORMAT = "%(asctime)s [%(levelname)8s] %(message)s (%(filename)s:%(lineno)s)"
LOGGING_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"
CONNECTION_STRING = os.getenv("CNX_STR")

logging.basicConfig(
        level="INFO",
        format=LOGGING_MSG_FORMAT,
        datefmt=LOGGING_DATE_FORMAT,
    )
logger = logging.getLogger(__name__)


class ProcessorException(Exception):
    pass


class ProcessorOrder(enum.Enum):
    START = 1
    MIDDLE = 2
    END = 3


class Processor(abc.ABC):

    def __str__(self):
        return f"{self.__runid}|{self.__dagid}|{self.__taskid}|{self.__order.name}|"

    def __init__(self, runid: str, dagid: str, taskid: str, order: ProcessorOrder = ProcessorOrder.MIDDLE):
        self.__runid = runid
        self.__dagid = dagid
        self.__taskid = taskid
        self.__order = order

    @abc.abstractmethod
    def init(self):
        pass

    @abc.abstractmethod
    def process(self):
        pass

    @abc.abstractmethod
    def leave(self):
        pass

    @property
    def runid(self) -> str:
        return self.__runid

    @property
    def dagid(self) -> str:
        return self.__dagid

    @property
    def taskid(self) -> str:
        return self.__taskid

    @property
    def order(self) -> ProcessorOrder:
        return self.__order


def run_processor(instance: Processor):
    logger.info(f"[run_processor|in] ({instance})")
    metrics = DagMetrics(CONNECTION_STRING)

    start_time = time.perf_counter()
    status = DagStatus.ACTIVE
    try:

        # processor start
        metrics.push(metric=DagMetric.DAG_OUTCOME, value=status.value,
                     tags={"runid": instance.runid, "dagid": instance.dagid, "taskid": instance.taskid})

        instance.init()
        time.sleep(random.randint(1, 6))
        # stage end
        # stage start
        instance.process()
        time.sleep(random.randint(1, 6))
        # stage end
        # stage start
        instance.leave()
        time.sleep(random.randint(1, 6))
        # stage end
        if instance.order == ProcessorOrder.END:
            status = DagStatus.FINISHED
        logger.info(f"[run_processor|out] => {instance}")
    except Exception as ex:
        logger.error(f"[run_processor] dag task failed: {instance}", exc_info=ex)
        status = DagStatus.FAILED
    finally:
        time_taken = int((time.perf_counter() - start_time))
        logger.info(f"[run_processor] dag task took: {time_taken} seconds")
        metrics.push(metric=DagMetric.DAG_DURATION, value=time_taken,
                     tags={"runid": instance.runid, "dagid": instance.dagid, "taskid": instance.taskid})
        metrics.push(metric=DagMetric.DAG_OUTCOME, value=status.value,
                     tags={"runid": instance.runid, "dagid": instance.dagid, "taskid": instance.taskid})

