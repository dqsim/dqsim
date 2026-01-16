import json
from pathlib import Path

import matplotlib.pyplot as plt
from visualization.eval_benchmark import geo_mean

from visualization.infra import (
    configure_cmake,
    extract_zip,
    get_figure_format,
    get_figure_path,
    get_simulator_executable,
)
from sparkbench.analysis.simulation_results import create_simulator_inputs, run_simulations


def run_sparkbench_simulation():
    # make simulator executable available
    configure_cmake()
    get_simulator_executable()
    # unpack sparkbench data
    extract_zip("sparkbench/output.zip", "output", "sparkbench/data/output")
    # build plans
    # write_plans(get_plans())
    create_simulator_inputs()
    # run simulator
    run_simulations()


def get_benchmark_config(name: str):
    cores, bandwidth = name.split("_")
    assert cores[:5] == "cores"
    n_cores = int(cores[5:])
    assert bandwidth[:2] == "bw"
    assert bandwidth[-4:] in ["mbit", "gbit"]
    bw = int(bandwidth[2:-4])
    if bandwidth[-4:] == "mbit":
        bw /= 1000
    return n_cores, bw


def get_spark_bench_results_path() -> Path:
    return Path("sparkbench/data/")


def create_per_query_prediction_figure():
    name = "spark_per_query_prediction"
    default_setting = "cores2_bw10gbit"
    execution_times = {}
    data_dir = get_spark_bench_results_path() / "output" / default_setting
    for f in data_dir.glob("*.json"):
        execution_times[f.stem] = json.load(open(f))[0]["totalTime"] / 1000

    simulation_file = get_spark_bench_results_path() / f"simulations/{default_setting}_results.json"
    simulation_times = json.load(open(simulation_file))

    x = sorted(list(simulation_times.keys()), key=lambda s: int(s[1:]))
    y1 = [execution_times[i] for i in x]
    y2 = [simulation_times[i] for i in x]

    fig, ax = plt.subplots(figsize=(6, 2.5))
    markers = ["o", "s", "^", "D", "P", "X"]
    names = ["Measured", "Simulated"]

    for y, m, n in zip([y1, y2], markers, names):
        ax.scatter(x, y, label=n, marker=m)

    ax.set_ylabel("Execution Time in s")
    ax.legend()
    Path(get_figure_path()).mkdir(parents=True, exist_ok=True)
    plt.savefig(f"{get_figure_path()}/{name}.{get_figure_format()}", bbox_inches="tight")


def create_network_scaling_figure():
    name = "spark_network_scaling"
    execution_times = {}
    simulation_times = {}
    for d in (get_spark_bench_results_path() / "output").iterdir():
        current_execution_times = {}
        if not d.is_dir:
            continue
        n_cores, bw = get_benchmark_config(d.stem)
        if n_cores != 2:
            continue
        for f in d.glob("*.json"):
            current_execution_times[f.stem] = json.load(open(f))[0]["totalTime"] / 1000
        assert bw not in execution_times
        execution_times[bw] = geo_mean(list(current_execution_times.values()))

        simulation_file = get_spark_bench_results_path() / f"simulations/{d.stem}_results.json"
        simulation_times[bw] = geo_mean(list(json.load(open(simulation_file)).values()))

    x = sorted(list(simulation_times.keys()))
    y1 = [execution_times[i] for i in x]
    y2 = [simulation_times[i] for i in x]

    fig, ax = plt.subplots(figsize=(6, 2.5))
    names = ["Measured", "Simulated"]

    for y, n in zip([y1, y2], names):
        ax.plot(x, y, label=n)

    # plt.title("geometric mean of all queries with different network speeds")
    ax.set_xscale("log")
    ax.set_yscale("log")
    ticks = [0.01, 0.1, 1, 10, 100]
    ax.set_xticks(ticks)
    ax.set_xticklabels([str(t) for t in ticks])

    ax.set_xlabel("Network Speed (Gbit/s)")
    ax.set_ylabel("Execution Time in s\n(Geometric Mean)")
    ax.legend()
    Path(get_figure_path()).mkdir(parents=True, exist_ok=True)
    plt.savefig(f"{get_figure_path()}/{name}.{get_figure_format()}", bbox_inches="tight")

def run_all_sparkbench():
    run_sparkbench_simulation()
    create_per_query_prediction_figure()
    create_network_scaling_figure()

def main():
    # create sparkbench data for figures
    run_sparkbench_simulation()

    # create figures
    create_per_query_prediction_figure()
    create_network_scaling_figure()


if __name__ == "__main__":
    main()
