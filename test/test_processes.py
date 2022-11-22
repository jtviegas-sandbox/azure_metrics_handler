import random
import time

from azmeha.impls import ProcessOne, ProcessTwo, ProcessThree
from azmeha.processor import ProcessorOrder, run_processor


def test_dag_stages():

    for i in range(100):

        runid = str(int(time.time()))

        stage = ProcessOne(runid=runid, dagid="thedag", taskid=f"ProcessOne_{i}", order=ProcessorOrder.START)
        run_processor(stage)
        stage = ProcessTwo(runid=runid, dagid="thedag", taskid=f"ProcessTwo_{i}")
        run_processor(stage)
        stage = ProcessThree(runid=runid, dagid="thedag", taskid=f"ProcessThree_{i}", order=ProcessorOrder.END)
        run_processor(stage)
        time.sleep(random.randint(1, 3))

    assert True

