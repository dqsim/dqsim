import json
import os
import random
import shutil
import subprocess
import tempfile
import zipfile
from pathlib import Path

from matplotlib import pyplot as plt

from visualization.eval_benchmark import SimulationBench, geo_mean, get_data, parse_benchmarks
from visualization.infra import (
    configure_cmake,
    extract_zip,
    get_benchmark_runner_executable,
    get_figure_format,
    get_figure_path,
    set_figure_format,
    set_figure_path,
    set_use_latex,
    setup_matplotlib_latex_font,
)


def load_all_problem_files():
    # loads the problems from the provided zip instead of creating them anew
    extract_zip("problems.zip", "problems", "problems")


def run_benchmark(definition_file: Path):
    # compile runner
    runner_executable = Path("build/ds_bench_s")
    assert runner_executable.exists(), f"Executable {runner_executable} does not exist"

    # Open file as json and check whether name in json is identical to file name
    with open(definition_file, "r") as f:
        data = json.load(f)
        assert data["name"] == definition_file.stem, (
            f"Name in json ({data['name']}) does not match file name ({definition_file.stem})"
        )
    # Call executable
    print(f"{runner_executable} {definition_file}")
    subprocess.run([runner_executable, definition_file], cwd=os.getcwd(), stdout=subprocess.PIPE)


def get_benchmark_definition_path() -> Path:
    return Path("visualization/benchmarks/")


def run_all_benchmarks():
    for bench_name in get_benchmark_definition_path().iterdir():
        print(bench_name)
        run_benchmark(bench_name)


def get_benchmarks(benchmark_name: str) -> tuple[list[str], list[list[SimulationBench]]]:
    benchs = get_data(benchmark_name)
    names = [b["assignmentAlgorithm"] for b in benchs]
    data: list[list[SimulationBench]] = [parse_benchmarks(b) for b in benchs]
    # instead of crashing here, use always the newest ones
    if len(set(names)) != len(names):
        print(
            "Warning: Found multiple benchmarks with the same names. Did you benchmark repeatedly?\n"
            "Choosing the newest one.\n"
            "You can clear them in the folder benchmark_results."
        )
        found_names = set()
        names = []
        data = []
        name_times = {}
        for i, b in enumerate(benchs):
            name = b["assignmentAlgorithm"]
            current_time = b["file_creation_time"]
            if name not in found_names:
                names.append(name)
                name_times[name] = current_time
                data.append(parse_benchmarks(b))

            if current_time > name_times[name]:
                name_times[name] = current_time
                data[i] = parse_benchmarks(b)

    assert len(set(names)) == len(names), f"There are duplicate names in {benchmark_name}"
    for alg in data:
        assert len(alg) == len(data[0]), "Different algorithms have different benchmark counts"
    name_indices = {n: i for i, n in enumerate(names)}

    new_name_map = {
        "AlignedRR": "RR",
        "Rand_x1000": "Rand1k",
        "Greedy1": "GMN",
        "HEFT": "HEFT",
        "NetHEFT": "NetHEFT",
        "Component": "cCEFT",
        "BruteForce": "Brute\nForce",
        "StateSep": "Full",
        "StateSep+B": "Full+B",
        "StateSepNoUp": "NoUp",
        "StateSepNoUp+B": "NoUp+B",
        "StateSepP2P": "P2P",
        "StateSepP2P+B": "P2P+B",
    }

    for old_n in name_indices:
        if old_n not in new_name_map:
            print(f"Found algorithm {old_n} but did not find a new name for it")
            new_name_map[old_n] = old_n
    names = [new_n for old_n, new_n in new_name_map.items() if old_n in name_indices]
    data = [data[name_indices[n]] for n in new_name_map.keys() if n in name_indices]
    return names, data


def generic_bar_chart(name: str):
    names, data = get_benchmarks(name)

    fig, ax = plt.subplots(figsize=(6, 2.1))

    y = []
    for d in data:
        y.append(1 / geo_mean([b.execution_time for b in d]))
    ax.bar(names, y, edgecolor="black")
    ax.set_ylabel("Throughput\nin Queries/s\n(Geometric Mean)")
    Path(get_figure_path()).mkdir(parents=True, exist_ok=True)
    plt.savefig(f"{get_figure_path()}/{name}.{get_figure_format()}", bbox_inches="tight")


def log_bar_chart(name: str):
    names, data = get_benchmarks(name)

    fig, ax = plt.subplots(figsize=(6, 2.1))

    y = []
    for d in data:
        y.append(1 / geo_mean([b.execution_time for b in d]))
    ax.bar(names, y, edgecolor="black")

    # write little numbers on top of bars
    for i, val in enumerate(y):
        ax.annotate(
            f"{val:.1f}",
            xy=(i, val),
            xycoords="data",
            xytext=(0, 2),
            textcoords="offset points",
            ha="center",
            va="bottom",
            fontsize=8,
        )
    ax.set_ylabel("Throughput\nin Queries/s\n(Geometric Mean)")
    ax.set_yscale("log")
    Path(get_figure_path()).mkdir(parents=True, exist_ok=True)
    plt.savefig(f"{get_figure_path()}/{name}.{get_figure_format()}", bbox_inches="tight")


def merge_data(n_lists: list[list[str]], d_lists: list[list[list]]) -> tuple[list[str], list[list]]:
    merge_dict = {}
    for ns, ds in zip(n_lists, d_lists):
        for n, d in zip(ns, ds):
            if n not in merge_dict:
                merge_dict[n] = []
            merge_dict[n] += d
    new_names = list(x for x in merge_dict.keys())
    new_data = [merge_dict[n] for n in new_names]
    return new_names, new_data


def partition_scaling_chart():
    name = "scaling_partitions"
    n1, d1 = get_benchmarks(name)
    n2, d2 = get_benchmarks("scaling_partitions_high")
    n3, d3 = get_benchmarks("scaling_partitions_random")
    names, data = merge_data([n1, n2, n3], [d1, d2, d3])

    fig, ax = plt.subplots(figsize=(6, 4))
    markers = ["o", "s", "^", "D", "P", "X"]

    for n, d, m in zip(names, data, markers):
        partition_count_map = {}
        for b in d:
            if b.n_partitions not in partition_count_map:
                partition_count_map[b.n_partitions] = []
            partition_count_map[b.n_partitions].append(b.scheduling_time)
        x = sorted(list(partition_count_map.keys()))
        y = [geo_mean(partition_count_map[i]) * 1000 for i in x]
        ax.plot(x, y, label=f"{n}", marker=m)

    ax.set_xscale("log")
    ax.set_yscale("log")

    n_x_ticks = 15
    x_ticks = list(2**i for i in range(n_x_ticks))
    ax.set_xticks(x_ticks, [f"$2^{{{t}}}$" for t in range(n_x_ticks)])
    y_ticks = [-2, -1, 0, 1, 2]
    y_ticks = [10**i for i in y_ticks]
    ax.set_yticks(y_ticks, [f"${t}$ ms" for t in y_ticks])

    ax.set_ylim(None, 400)

    ax.legend(loc="lower right")
    ax.set_ylabel("Scheduling Time in ms\n(Geometric Mean)")
    ax.set_xlabel("Partition Count")

    Path(get_figure_path()).mkdir(parents=True, exist_ok=True)
    plt.savefig(f"{get_figure_path()}/{name}.{get_figure_format()}", bbox_inches="tight")


def node_scaling_chart():
    name = "scaling_nodes"
    n1, d1 = get_benchmarks(name)
    n2, d2 = get_benchmarks("scaling_nodes_random")
    names, data = merge_data([n1, n2], [d1, d2])
    fig, ax = plt.subplots(figsize=(6, 4))
    markers = ["o", "s", "^", "D", "P", "X"]

    for n, d, m in zip(names, data, markers):
        node_count_map = {}
        for b in d:
            if b.n_nodes not in node_count_map:
                node_count_map[b.n_nodes] = []
            node_count_map[b.n_nodes].append(b.scheduling_time)
        x = sorted(list(node_count_map.keys()))
        y = [geo_mean(node_count_map[i]) * 1000 for i in x]
        ax.plot(x, y, label=f"{n}", marker=m)

    ax.set_xscale("log")
    ax.set_yscale("log")

    n_x_ticks = 11
    x_ticks = list(2**i for i in range(1, n_x_ticks))
    ax.set_xticks(x_ticks, [f"${t}$" for t in x_ticks])
    y_ticks = [-1, 0, 1, 2]
    y_ticks = [10**i for i in y_ticks]
    ax.set_yticks(y_ticks, [f"${t}$ ms" for t in y_ticks])

    ax.set_ylim(0.1, 800)

    # ax.legend(loc="lower right")
    ax.legend()
    ax.set_ylabel("Scheduling Time in ms\n(Geometric Mean)")
    ax.set_xlabel("Node Count")

    Path(get_figure_path()).mkdir(parents=True, exist_ok=True)
    plt.savefig(f"{get_figure_path()}/{name}.{get_figure_format()}", bbox_inches="tight")


def full_enumeration_chart():
    name = "full_enumeration"
    names, data = get_benchmarks(name)
    fig, ax = plt.subplots(figsize=(6, 3))

    # Group by algorithm and query
    best_query_time = {}
    algorithm_query_time = {n: {} for n in names}

    for n, d in zip(names, data):
        for b in d:
            if b.query_name not in best_query_time or best_query_time[b.query_name] > b.execution_time:
                best_query_time[b.query_name] = b.execution_time
            algorithm_query_time[n][b.query_name] = b.execution_time

    query_names = [n for n in best_query_time]

    noise = [(random.random() - 0.5) * 0.5 for _ in query_names]
    for i_alg, alg in enumerate(names):
        y = [algorithm_query_time[alg][n] / best_query_time[n] for n in query_names]
        x = [i_alg + 1 + noise[i] for i in range(len(y))]
        ax.scatter(x, y, label=alg, alpha=0.5)

    ax.set_yscale("log")
    ax.set_ylabel("Slowdown Over Best")
    ax.set_xticks([i + 1 for i in range(len(names))], names)

    Path(get_figure_path()).mkdir(parents=True, exist_ok=True)
    plt.savefig(f"{get_figure_path()}/{name}.{get_figure_format()}", bbox_inches="tight")


def create_all_figures():
    load_all_problem_files()
    configure_cmake()
    get_benchmark_runner_executable()
    run_all_benchmarks()

    log_bar_chart("state_sep")
    generic_bar_chart("common")
    generic_bar_chart("scale_out")
    generic_bar_chart("scale_out_to_large")
    generic_bar_chart("heterogeneous")
    run_benchmark(Path(f"{get_benchmark_definition_path()}/growing_scale_out_to_large.json"))
    generic_bar_chart("growing_scale_out_to_large")
    partition_scaling_chart()
    node_scaling_chart()
    full_enumeration_chart()


def main():
    # create the problem files
    load_all_problem_files()

    configure_cmake()
    get_benchmark_runner_executable()
    # run_all_benchmarks()

    run_benchmark(Path(f"{get_benchmark_definition_path()}/state_sep.json"))
    log_bar_chart("state_sep")

    run_benchmark(Path(f"{get_benchmark_definition_path()}/common.json"))
    generic_bar_chart("common")

    run_benchmark(Path(f"{get_benchmark_definition_path()}/scale_out.json"))
    run_benchmark(Path(f"{get_benchmark_definition_path()}/scale_out_to_large.json"))
    generic_bar_chart("scale_out")
    generic_bar_chart("scale_out_to_large")

    run_benchmark(Path(f"{get_benchmark_definition_path()}/heterogeneous.json"))
    generic_bar_chart("heterogeneous")

    # run_benchmark(Path(f"{get_benchmark_definition_path()}/growing.json"))
    # run_benchmark(Path(f"{get_benchmark_definition_path()}/growing_scale_out.json"))
    run_benchmark(Path(f"{get_benchmark_definition_path()}/growing_scale_out_to_large.json"))
    # generic_bar_chart("growing")
    # generic_bar_chart("growing_scale_out")
    generic_bar_chart("growing_scale_out_to_large")

    run_benchmark(Path(f"{get_benchmark_definition_path()}/scaling_partitions.json"))
    run_benchmark(Path(f"{get_benchmark_definition_path()}/scaling_partitions_high.json"))
    run_benchmark(Path(f"{get_benchmark_definition_path()}/scaling_partitions_random.json"))
    partition_scaling_chart()

    run_benchmark(Path(f"{get_benchmark_definition_path()}/scaling_nodes.json"))
    run_benchmark(Path(f"{get_benchmark_definition_path()}/scaling_nodes_random.json"))
    node_scaling_chart()

    run_benchmark(Path(f"{get_benchmark_definition_path()}/full_enumeration.json"))
    full_enumeration_chart()


if __name__ == "__main__":
    main()
