import random

from azmeha.processor import Processor, ProcessorException

PROBABILITY = 0.1


class ProcessOne(Processor):

    def init(self):
        print("[ProcessOne] init")

    def process(self):
        if random.random() < PROBABILITY :
            raise ProcessorException("oops")
        else:
            print("[ProcessOne] process")

    def leave(self):
        print("[ProcessOne] leave")


class ProcessTwo(Processor):

    def init(self):
        if random.random() < PROBABILITY :
            raise ProcessorException("oops")
        else:
            print("[ProcessTwo] init")

    def process(self):
        print("[ProcessTwo] process")

    def leave(self):
        print("[ProcessTwo] leave")


class ProcessThree(Processor):

    def init(self):
        print("[ProcessThree] init")

    def process(self):
        print("[ProcessThree] process")

    def leave(self):
        if random.random() < PROBABILITY :
            raise ProcessorException("oops")
        else:
            print("[ProcessThree] leave")