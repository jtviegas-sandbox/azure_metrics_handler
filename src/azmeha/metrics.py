import collections
import enum
import logging
from typing import Any, Union, List, Dict

from opencensus.ext.azure import metrics_exporter
from opencensus.stats import aggregation as aggregation_module
from opencensus.stats import measure as measure_module
from opencensus.stats import stats as stats_module
from opencensus.stats import view as view_module
from opencensus.stats.measure import MeasureInt, MeasureFloat
from opencensus.tags import tag_key as tag_key
from opencensus.tags import tag_map as tag_map
from opencensus.tags import tag_value as tag_value

from azmeha.singleton import SingletonMeta


class DagStatus(enum.Enum):
    ACTIVE = 1
    FINISHED = 2
    FAILED = 3


class DagMetric(enum.Enum):
    DAG_DURATION = 1
    DAG_OUTCOME = 2


DagMetricSpec = collections.namedtuple("Metric", ["name", "metric_description", "view_description", "unit",
                                                  "type", "aggregation", "tags"])

logger = logging.getLogger(__name__)


class MetricsException(Exception):
    pass


class DagMetrics(metaclass=SingletonMeta):
    __specs = {
        DagMetric.DAG_DURATION: DagMetricSpec(DagMetric.DAG_DURATION.name, "dag elements and instances processing times",
                                          "sum of dag elements and instances processing times", "minutes",
                                          int, aggregation_module.LastValueAggregation(), ["runid", "dagid", "taskid"]),
        DagMetric.DAG_OUTCOME: DagMetricSpec(DagMetric.DAG_OUTCOME.name, "current status of dag and/or task",
                                            "last known status of task and/or dag", "status",
                                            int, aggregation_module.LastValueAggregation(),
                                            ["runid", "dagid", "taskid"])
    }

    def __init__(self, client_key: str):
        self.__view_manager = stats_module.stats.view_manager
        exporter = metrics_exporter.new_metrics_exporter(enable_standard_metrics=False, connection_string=client_key)
        self.__view_manager.register_exporter(exporter)
        self.__measurement_map = stats_module.stats.stats_recorder.new_measurement_map()
        self.__measures = {}

    def __get_measure(self, metric: DagMetric, value: Union[float, int], tags: List[str]):

        logger.info(f"[__get_measure] getting measure for metric :({metric.name})")
        if metric not in DagMetrics.__specs.keys():
            raise MetricsException(f"unknown metric: {metric.name}")
        metric_spec = DagMetrics.__specs[metric]

        # tags should be a subset of the tags in the metric spec
        if not set(tags).issubset(set(metric_spec.tags)):
            raise MetricsException(f"unknown tags provided: {tags}")

        # check type
        if not isinstance(value, metric_spec.type):
            raise MetricsException(f"value type provided is not {metric_spec.type}")

        if metric not in list(self.__measures.keys()):
            measure = self.__create_measure(metric, value, tags)
        else:
            measure = self.__measures[metric]

        return measure

    def __create_measure(self, metric: DagMetric, value: Union[float, int], tags: List[str]):

        metric_spec = DagMetrics.__specs[metric]

        logger.info(f"[__create_measure] creating measure for metric :({metric.name})")
        if metric_spec.type == int:
            measure = measure_module.MeasureInt(metric_spec.name, metric_spec.metric_description, metric_spec.unit)
        elif metric_spec.type == float:
            measure = measure_module.MeasureFloat(metric_spec.name, metric_spec.metric_description,
                                                  metric_spec.unit)
        else:
            raise MetricsException("metric type must be either int or float")

        view = view_module.View(name=metric_spec.name, description=metric_spec.view_description, columns=tags,
                                measure=measure, aggregation=metric_spec.aggregation)
        self.__view_manager.register_view(view)
        self.__measures[metric] = measure

        return measure

    def push(self, metric: DagMetric, value: Union[float, int], tags: Dict[str, Any]):

        measure = self.__get_measure(metric, value, list(tags.keys()))
        if isinstance(measure, MeasureInt):
            self.__measurement_map.measure_int_put(measure, value)
        elif isinstance(measure, MeasureFloat):
            self.__measurement_map.measure_float_put(measure, value)
        else:
            raise MetricsException("measure should be int or float type")

        tagmap = tag_map.TagMap()
        for k, v in tags.items():
            tagmap.insert(tag_key.TagKey(k), tag_value.TagValue(v))

        self.__measurement_map.record(tagmap)
