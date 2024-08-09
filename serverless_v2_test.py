import logging
import time
import functools
import concurrent.futures

from typing import Callable

from longevity_test import LongevityTest
from sdcm.utils.common import FileFollowerThread
from sdcm.cluster import BaseNode, MAX_TIME_WAIT_FOR_NEW_NODE_UP

# Create a thread per node to monitor disk usage
# When soft limit is reached, trigger possible cluster scale out
# When hard limit is reach notify tester


class DiskUsageMonitor(FileFollowerThread):
    def __init__(self, node: BaseNode, limits: dict[str, Callable[[], None]], log):
        super().__init__()
        self.node = node
        self.limits = limits
        self.size = self.get_current_disk_usage()
        self.log = log

    def get_current_disk_usage(self):
        output = self.node.remoter.run('df --output=used /var/lib/scylla').stdout
        # output is of the form:
        # Used
        #   YY (number of 1KiB blocks)
        return int(output.split()[1])*1024

    def run(self):
        while not self.stopped():
            self.size = self.get_current_disk_usage()
            if self.size > self.limits["soft"][0]:
                self.limits["soft"][1]()

            self.log.info(f"Node's {self.node.name} disk utilization: {self.size}")
            time.sleep(5)


class Serverless_v2(LongevityTest):
    def __init__(self, *args, **kwargs):
        super().__init__(*args)

        self.monitors_queue = []
        self.scale_cluster_out = False
        self.keep_running_forever = True

    def _init_logging(self):
        super()._init_logging()
        self.log.setLevel(logging.DEBUG)

    def test_cluster_scale_out(self):
        # A baseline set to N% of the total disk size. A fixed value in bytes.
        baseline = int(self.params.get('space_node_threshold'))
        self.soft_limit = int(baseline * 1.05)
        # hard_limit = int(baseline * 1.10)

        self.log.info("Starting tests")
        stress_queue = []
        keyspace_num = self.params.get('keyspace_num')
        stress_cmd = self.params.get('stress_cmd')
        self.assemble_and_run_all_stress_cmd(stress_queue, stress_cmd, keyspace_num)

        self.log.info("Starting disk monitors")
        for node in self.db_cluster.nodes:
            monitor = DiskUsageMonitor(node, {"soft": (self.soft_limit, self.soft_limit_reached)}, self.log)
            monitor.start()
            self.monitors_queue.append(monitor)

        futures = []
        for stress in stress_queue:
            futures.extend(stress.results_futures)

        while True:
            if self.scale_cluster_out:
                self.add_node()
                self.scale_cluster_out = False

            self.log.info("Checking stress thread")
            _, not_completed = concurrent.futures.wait(futures, 5, concurrent.futures.ALL_COMPLETED)
            if len(not_completed) == 0 and not self.keep_running_forever:
                break

        self.log.info("Stopping monitors")
        for monitor in self.monitors_queue:
            monitor.stop()

        for stress in stress_queue:
            self.verify_stress_thread(cs_thread_pool=stress)

    def soft_limit_reached(self):
        self.log.info("Soft limit reached on one of the nodes. Checking the average disk utilization...")
        avg_usage = functools.reduce(lambda total, monitor: total + monitor.size, self.monitors_queue, 0)/len(self.monitors_queue)

        self.log.info(f"Average disk utilization: {avg_usage}")
        if avg_usage > self.soft_limit:
            self.scale_cluster_out = True

    def add_node(self):
        self.log.info("Scaling out a cluster...")
        new_nodes = self.db_cluster.add_nodes(count=1, enable_auto_bootstrap=True)

        monitor = DiskUsageMonitor(new_nodes[0], {"soft": (self.soft_limit, self.soft_limit_reached)}, self.log)
        monitor.start()
        self.monitors_queue.append(monitor)

        self.monitors.reconfigure_scylla_monitoring()
        self.db_cluster.wait_for_init(node_list=new_nodes, timeout=MAX_TIME_WAIT_FOR_NEW_NODE_UP, check_node_health=False)
        self.db_cluster.wait_for_nodes_up_and_normal(nodes=new_nodes)

        self.log.info("Scaling out finished")
