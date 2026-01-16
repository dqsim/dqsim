import json
import subprocess
from collections import defaultdict
from pathlib import Path
from typing import Optional

import matplotlib.pyplot as plt

from sparkbench.analysis.create_plan import get_plan
from sparkbench.analysis.json_writing import get_problem_and_assignment, write_dataclass
from sparkbench.analysis.runtime_plot import geomean


def run_simulation(
    assignment_file: str,
    problem_file: str,
    result_name: Optional[str] = None,
    print_plan: bool = False,
) -> Optional[float]:
    simulator_executable = "build/ds_sim"

    commands = [simulator_executable, "-a", str(assignment_file), str(problem_file)]
    if result_name is not None:
        commands += ["-o", str(result_name)]
    if print_plan:
        commands += ["-p"]

    print(" ".join(commands))
    result = subprocess.run(
        commands,
        capture_output=True,
        text=True,
    )
    print(result.stdout)
    if result.stderr.strip() != "":
        print(result.stderr)
        return None
    return float(str(result.stdout).strip().split("\n")[-1])


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


def create_simulator_inputs():
    benchmark_dir = Path("sparkbench/data/output")
    out_dir = Path("sparkbench/data/written_plans")
    for d in benchmark_dir.iterdir():
        if not d.is_dir():
            continue

        n_cores, bw = get_benchmark_config(d.stem)

        current_out_dir = out_dir / d.stem
        current_out_dir.mkdir(parents=True, exist_ok=True)

        for f in d.glob("*.json"):
            plan = get_plan(f)
            if plan is None:
                continue
            problem, assignment = get_problem_and_assignment(plan, n_cores, bw)
            write_dataclass(problem, current_out_dir / Path(f"{f.stem}_problem.json"))
            write_dataclass(assignment, current_out_dir / Path(f"{f.stem}_assignment.json"))


def run_simulations():
    problems_dir = Path("sparkbench/data/written_plans")
    results_dir = Path("sparkbench/data/simulations")
    results_dir.mkdir(parents=True, exist_ok=True)

    for d in problems_dir.iterdir():
        if not d.is_dir():
            continue

        # n_cores, bw = get_benchmark_config(d.stem)
        queries = defaultdict(dict)
        current_results = {}

        for f in d.glob("*.json"):
            print(f)
            print(f.stem)
            print(f.stem.rsplit("_", 1))
            query, kind = f.stem.rsplit("_", 1)
            queries[query][kind] = f

        for name, files in queries.items():
            problem = files["problem"]
            assignment = files["assignment"]
            print(f"simulating {name}")
            time = run_simulation(assignment, problem)
            if time is not None:
                current_results[name] = float(time)

        json.dump(current_results, open(results_dir / Path(f"{d.stem}_results.json"), "w"))


def create_per_query_prediction_figure():
    default_setting = "cores2_bw10gbit"
    execution_times = {}
    for f in Path(f"sparkbench/data/output/{default_setting}").glob("*.json"):
        execution_times[f.stem] = json.load(open(f))[0]["totalTime"] / 1000

    simulation_file = Path(f"sparkbench/data/simulations/{default_setting}_results.json")
    simulation_times = json.load(open(simulation_file))

    x = sorted(list(simulation_times.keys()), key=lambda s: int(s[1:]))
    y1 = [execution_times[i] for i in x]
    y2 = [simulation_times[i] for i in x]
    plt.title(default_setting)
    plt.scatter(x, y1, label="Measured")
    plt.scatter(x, y2, label="Simulated")
    plt.legend()
    plt.savefig("per_query_predictions.pdf")
    plt.close()
    # plt.show()


def create_network_scaling_figure():
    execution_times = {}
    simulation_times = {}
    for d in Path("sparkbench/data/output/").iterdir():
        current_execution_times = {}
        if not d.is_dir:
            continue
        _, bw = get_benchmark_config(d.stem)
        for f in d.glob("*.json"):
            current_execution_times[f.stem] = json.load(open(f))[0]["totalTime"] / 1000
        execution_times[bw] = geomean(current_execution_times.values())

        simulation_file = Path(f"sparkbench/data/simulations/{d.stem}_results.json")
        simulation_times[bw] = geomean(json.load(open(simulation_file)).values())

    x = sorted(list(simulation_times.keys()))
    y1 = [execution_times[i] for i in x]
    y2 = [simulation_times[i] for i in x]
    plt.title("geometric mean of all queries with different network speeds")
    plt.plot(x, y1, label="Measured")
    plt.plot(x, y2, label="Simulated")
    plt.legend()
    plt.xscale("log")
    plt.yscale("log")
    plt.savefig("network_scaling.pdf")
    plt.close()



def debugging_code():
    setting = "cores2_bw1gbit"
    query = "q5"
    Path("sparkbench/data/recordings").mkdir(parents=True, exist_ok=True)
    trace_file = Path(f"sparkbench/data/output/{setting}/{query}.json")
    problem_dir = Path(f"sparkbench/data/written_plans/{setting}/")

    cores, bw = get_benchmark_config(setting)

    plan = get_plan(trace_file)
    assert plan is not None
    problem, assignment = get_problem_and_assignment(plan, cores, bw)
    write_dataclass(problem, problem_dir / Path(f"{query}_problem.json"))
    write_dataclass(assignment, problem_dir / Path(f"{query}_assignment.json"))

    recording_name = f"sparkbench/data/recordings/{setting}_{query}_recording.json"
    run_simulation(
        f"sparkbench/data/written_plans/{setting}/{query}_assignment.json",
        f"sparkbench/data/written_plans/{setting}/{query}_problem.json",
        recording_name,
        True,
    )


def main():
    create_simulator_inputs()
    run_simulations()
    create_per_query_prediction_figure()
    create_network_scaling_figure()


if __name__ == "__main__":
    main()
