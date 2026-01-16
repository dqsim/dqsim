import json
from pathlib import Path
from typing import Iterable

import matplotlib.pyplot as plt
import numpy as np


def geomean(data: Iterable) -> float:
    return np.exp(np.mean(np.log(np.array(list(data)))))


def get_runtimes() -> tuple[list[int], list[float], list[dict[str, float]]]:
    cores = []
    bandwidths = []
    runtimes = []
    for dir in Path("sparkbench/data/output").iterdir():
        if dir.is_dir():
            cores_desc, bw_desc = dir.name.split("_")
            assert cores_desc[:5] == "cores"
            cores.append(int(cores_desc[5:]))
            assert bw_desc[:2] == "bw"
            assert bw_desc[-4:] in ["mbit", "gbit"]
            bandwidths.append(float(bw_desc[2:-4]))
            if bw_desc[-4:] == "mbit":
                bandwidths[-1] /= 1000
            query_times = {}
            for query_file in sorted(dir.glob("*.json"), key=lambda p: p.name):
                query_name = query_file.stem
                with query_file.open() as f:
                    data = json.load(f)
                    times = [run["totalTime"] for run in data]
                    query_times[query_name] = np.median(times)
            runtimes.append(query_times)
    return cores, bandwidths, runtimes


def plot_network_figure():
    cores, bandwidths, runtimes = get_runtimes()
    plot_data = {}
    # only use runs with 2 cores
    for c, bw, runtime in zip(cores, bandwidths, runtimes):
        if c != 2:
            continue
        plot_data[bw] = geomean(runtime.values())
    x = sorted(plot_data)
    y = [plot_data[bw] for bw in x]
    plt.xscale("log")
    plt.plot(x, y)
    plt.show()


def main():
    plot_network_figure()


if __name__ == "__main__":
    main()
