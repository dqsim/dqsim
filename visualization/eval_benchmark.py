import json
from dataclasses import dataclass
from pathlib import Path
import re
from typing import Callable

import numpy as np
from matplotlib import pyplot as plt


@dataclass
class SimulationBench:
    name: str
    algorithm: str
    execution_time: float
    scheduling_time: float
    query_name: str
    handcrafted: bool
    caching_name: str
    cluster_name: str
    n_nodes: int
    homogeneous: bool
    n_partitions: int

    def is_shared_nothing(self) -> bool:
        return (
            (not self.handcrafted)
            and (self.n_nodes == self.n_partitions)
            and self.caching_name == "All"
            and self.homogeneous
        )

def extract_timestamp(path: Path) -> str:
    """
    Extracts the ISO-8601 timestamp ending with 'Z' from a filename.
    Does not assume anything about the filename prefix or fractional-second length.
    """
    pattern = re.compile(
        r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?Z"
    )
    match = pattern.search(path.name)
    if not match:
        raise ValueError(f"No timestamp found in filename: {path.name}")
    return match.group(0)

def get_data(folder: str = ""):
    result = []
    for file in Path(f"benchmark_results/{folder}").iterdir():
        if file.is_file() and file.suffix == ".json":
            with file.open("r") as fd:
                current = json.load(fd)
                current["file_creation_time"] = extract_timestamp(file)
                result.append(current)
    return sorted(result, key=lambda x: x["assignmentAlgorithm"])


def get_cluster_size(cluster_name: str) -> int:
    if cluster_name == "handcrafted":
        return -1
    assert cluster_name[-1].isdigit()
    i_last_digit = len(cluster_name) - 1
    while cluster_name[i_last_digit].isdigit():
        i_last_digit -= 1
    assert cluster_name[i_last_digit] == "x"
    i_last_digit += 1
    return int(cluster_name[i_last_digit:])


def get_cluster_homogeneous(cluster_name: str) -> bool:
    if cluster_name == "handcrafted":
        return True
    assert cluster_name[-1].isdigit()
    assert cluster_name[-1].isdigit()
    i_last_digit = len(cluster_name) - 1
    while cluster_name[i_last_digit].isdigit():
        i_last_digit -= 1
    assert cluster_name[i_last_digit] == "x"
    node_name = cluster_name[:i_last_digit]
    return "-" in node_name


def parse_benchmarks(bench: dict) -> list[SimulationBench]:
    result = []
    for n, t, s in zip(bench["benchmarkNames"], bench["executionTimes"], bench["schedulingTimes"]):
        handcrafted = n.startswith("Handcrafted_")
        caching_name = "handcrafted"
        cluster_name = "handcrafted"
        query_name = n.split("_")[-1]
        n_nodes = -1
        homogeneous = True
        n_partitions = -1
        if not handcrafted:
            cl, ca, p, q = n.split("_")
            cluster_name = cl
            caching_name = ca
            n_nodes = get_cluster_size(cl)
            homogeneous = get_cluster_homogeneous(cl)
            n_partitions = int(p)
        result.append(
            SimulationBench(
                n,
                bench["assignmentAlgorithm"],
                t,
                s,
                query_name,
                handcrafted,
                caching_name,
                cluster_name,
                n_nodes,
                homogeneous,
                n_partitions,
            )
        )
    return result


def plot_n_partitions_with_property(
    names: list[str],
    data: list[list[SimulationBench]],
    property: Callable[[SimulationBench], bool],
    title: str,
    ax,
):
    markers = ["o", "v", "^", "<", ">", "s", "x", "P", "*"]
    partition_counts = sorted(set(d.n_partitions for d in data[0]))[1:]  # ignore handcrafted with -1 partitions
    for i, (name, d) in enumerate(zip(names, data)):
        y = []
        for n in partition_counts:
            current_execution_time = geo_mean([b.execution_time for b in d if (b.n_partitions == n) and (property(b))])
            y.append(current_execution_time)
        ax.plot(partition_counts, y, label=name, marker=markers[i % len(markers)], alpha=0.5)
    ax.legend()
    ax.set_xticks(partition_counts)
    ax.set_xlabel("nPartitions")
    ax.set_title(title)


def plot_bar_with_property(
    names: list[str],
    data: list[list[SimulationBench]],
    property: Callable[[SimulationBench], bool],
    title: str,
    ax,
):
    y = []
    for d in data:
        y.append(geo_mean([b.execution_time for b in d if property(b)]))
    ax.bar(names, y)
    ax.set_title(title)


def de_duplicate_names(names: list[str]) -> list[str]:
    result = []
    for name in names:
        while name in result:
            name = name + "I"
        result.append(name)
    return result


def categorize(names: list[str]) -> dict[str, list | np.ndarray]:
    handcrafted = np.zeros(len(names), np.bool)
    cluster = []
    cache_fill = []
    n_partitions = []
    query = []
    for i, n in enumerate(names):
        if n.startswith("Handcrafted_"):
            handcrafted[i] = True
            cluster.append("handcrafted")
            cache_fill.append("handcrafted")
            n_partitions.append("handcrafted")
            query.append(n)
        else:
            cl, ca, p, q = n.split("_")
            cluster.append(cl)
            cache_fill.append(ca)
            n_partitions.append(int(p))
            query.append(q)
    return {
        "handcrafted": handcrafted,
        "cluster": cluster,
        "cache": cache_fill,
        "n_partitions": n_partitions,
        "query": query,
    }


def get_execution_times(execution_times: list, info: dict, key: str, value):
    return [t for i, t in enumerate(execution_times) if info[key][i] == value]


def get_categories(infos: list[dict], key: str) -> list:
    return sorted([c for c in set(infos[0][key]) if c != "handcrafted"])


def get_execution_times_by_category(infos: list[dict], benchs: list[dict], key: str, value) -> list[list[float]]:
    return [get_execution_times(b["executionTimes"], infos[i], key, value) for i, b in enumerate(benchs)]


def plot_category(category: str, infos, names, benchs):
    for ch in get_categories(infos, category):
        x = names
        times = get_execution_times_by_category(infos, benchs, category, ch)
        y = [np.average(t) for t in times]
        plt.figure()
        plt.title(f"{category}: {ch}")
        plt.ylabel("Average Execution Time")
        plt.bar(x, y)


def geo_mean(data) -> float:
    return np.exp(np.log(np.array(data)).mean())


def get_geometric_means(data: list[list[SimulationBench]]):
    result = []
    for benchs in data:
        execution_times = np.array([b.execution_time for b in benchs])
        geometric_mean = geo_mean(execution_times)
        result.append(geometric_mean)
    return result


def get_algorithm(data: list[list[SimulationBench]], name: str) -> list[SimulationBench]:
    res = [d for d in data if d[0].algorithm == name]
    assert len(res) == 1
    return res[0]


def find_improved_shared_nothing_problems(data: list[list[SimulationBench]]):
    old = get_algorithm(data, "Greedy1")
    new = get_algorithm(data, "PolarisNoUp+B")
    # slowdown = [(n, o) for n, o in zip(new, old) if n.is_shared_nothing() and n.n_partitions <= 4 and n.execution_time > o.execution_time]
    slowdown = [
        (n, o)
        for n, o in zip(new, old)
        if n.n_partitions <= 4 and n.execution_time > o.execution_time and n.homogeneous
    ]
    # slowdown = [(a.execution_time / n.execution_time) for a, n in zip(arr, greedy1) if
    #             a.is_shared_nothing() and a.execution_time > n.execution_time]
    slowdown.sort(key=lambda x: x[1].execution_time / x[0].execution_time)
    print(slowdown[0])


def find_bad_component_schedulings(data: list[list[SimulationBench]]):
    gre = get_algorithm(data, "Greedy4")
    comp = get_algorithm(data, "Component")
    slowdown = [
        (a, b)
        for a, b in zip(gre, comp)
        if a.caching_name == "None" and a.n_partitions <= 16 and a.execution_time < b.execution_time
    ]
    slowdown.sort(key=lambda x: x[0].execution_time / x[1].execution_time)
    print(slowdown[0])


def find_good_component_schedulings(data: list[list[SimulationBench]]):
    netheft = get_algorithm(data, "NetHEFT")
    comp = get_algorithm(data, "Component")
    slowdown = [(a, b) for a, b in zip(netheft, comp) if a.execution_time > b.execution_time]
    slowdown.sort(
        key=lambda x: min(
            x[0].execution_time / x[1].execution_time,
            x[1].execution_time / x[0].execution_time,
        )
    )
    print(slowdown[0])


def analyze_individual_runtimes(bench: list[SimulationBench]):
    times = [b.execution_time for b in bench]

    long_ones = []
    for i, t in enumerate(times):
        if t > 1000:
            long_ones.append(bench[i])
    print(long_ones)

    fig, axs = plt.subplots(2, 1, figsize=(10, 8), gridspec_kw={"height_ratios": [3, 1]})

    # Histogram
    axs[0].hist(times, bins=20, color="skyblue", edgecolor="black")
    axs[0].set_title("Histogram of Execution Times")
    axs[0].set_xlabel("Execution Time")
    axs[0].set_ylabel("Frequency")

    # Boxplot
    axs[1].boxplot(times, vert=False)
    axs[1].set_title("Boxplot of Execution Times")
    axs[1].set_xlabel("Execution Time")

    plt.tight_layout()
    plt.show()


def main():
    benchs = get_data()
    names = [b["assignmentAlgorithm"] for b in benchs]
    data: list[list[SimulationBench]] = [parse_benchmarks(b) for b in benchs]
    geometric_means = get_geometric_means(data)
    # analyze_individual_runtimes(data[0])
    find_good_component_schedulings(data)
    # exit()
    find_improved_shared_nothing_problems(data)
    # plot_n_partitions_with_property(names, data, lambda b: True, "Execution Time over partitions with all caches")
    # plt.show()
    # plot_n_partitions_with_property(names, data, lambda b: b.caching_name == "All",
    #                                 "Execution Time over partitions with full caches")
    # plt.show()


if __name__ == "__main__":
    main()
