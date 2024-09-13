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

# Map from AWS instance into its total disk storage in GB
aws_instance_storage = {
    "i4i.large": 468,
    "i4i.xlarge": 937,
    "i4i.2xlarge": 1875,
    "i4i.4xlarge": 3750,
    "i4i.8xlarge": 7500,
    "i4i.12xlarge": 11250,
    "i4i.16xlarge": 15000,
    "i4i.24xlarge": 22500,
    "i4i.32xlarge": 30000,
}

GB2B = 1024**3


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
        self.interrupt_prepare_cluster = False

    def _init_logging(self):
        super()._init_logging()
        self.log.setLevel(logging.DEBUG)

    def baseline_limit_reached(self):
        self.interrupt_prepare_cluster = True

    def prepare_cluster(self, baseline: int):
        self.log.info("Start preparing cluster")

        stress_queue = []
        keyspace_num = self.params.get("keyspace_num")
        prepare_write_cmd = self.params.get("prepare_write_cmd")
        self.assemble_and_run_all_stress_cmd(stress_queue, prepare_write_cmd, keyspace_num)

        self.log.info("Starting disk monitors")
        for node in self.db_cluster.nodes:
            monitor = DiskUsageMonitor(node, {"soft": (baseline, self.baseline_limit_reached)}, self.log)
            monitor.start()
            self.monitors_queue.append(monitor)

        futures = []
        for stress in stress_queue:
            futures.extend(stress.results_futures)

        while True:
            _, not_completed = concurrent.futures.wait(futures, 5, concurrent.futures.ALL_COMPLETED)
            if len(not_completed) == 0:
                self.log.info("All prepare write threads completed")
                break

            if self.interrupt_prepare_cluster:
                self.log.info("Reached baseline storage utilization")
                break

        self.log.info("Killing loaders' threads")
        # Kill the stress threads otherwise they will keep producing data
        # until duration limit is reached or a specific amount of data has
        # been produced.
        for thread in stress_queue:
            thread.kill()

        self.log.info("Stopping monitors")
        for monitor in self.monitors_queue:
            monitor.stop()
        self.monitors_queue.clear()

        self.log.info("Finished preparing cluster")

    def wait_for_steady_state_cluster(self):
        # Let it age until we get to the steady-state. The tablets get split and migrated
        # when necessary. How we can detect the steady-state?
        self.log.info("Let the cluster age so we get to the state state")
        time.sleep(60*60)

    def stress_cluster(self):
        self.log.info("Start stress cluster")

        stress_queue = []
        keyspace_num = self.params.get("keyspace_num")
        stress_cmd = self.params.get("stress_cmd")
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

        for stress in stress_queue:
            self.verify_stress_thread(cs_thread_pool=stress)

        self.log.info("Stopping monitors")
        for monitor in self.monitors_queue:
            monitor.stop()

        self.log.info("Finished stress cluster")

    def test_cluster_scale_out(self):
        # A baseline set to 85% of the total disk size. A fixed value in bytes.
        instance = self.params.get("instance_type_db")
        baseline = int(aws_instance_storage[instance]*GB2B*0.85)

        self.params.update({"space_node_threshold": baseline})
        self.soft_limit = int(aws_instance_storage[instance]*GB2B*0.9)

        self.log.info("Starting tests")

        # Populate the cluster to get the initial storage utilization
        self.prepare_cluster(baseline)

        # Let it age until we get to the steady-state. The tablets get split
        # and migrated when necessary.
        self.wait_for_steady_state_cluster()

        # Start writing data at 10% of cluster size per day
        self.stress_cluster()

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
