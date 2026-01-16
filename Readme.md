# DQSim

This is an anonymous repository that contains the code for the paper "DQSim: Does the Task Assignment Slow Down Your Distributed Database System?".

## Dependencies

### Linux

Compilation dependencies:

```bash
sudo apt install build-essential cmake ninja-build g++-14 libboost-dev libboost-context-dev libglpk-dev
```

[uv](https://github.com/astral-sh/uv) for Python:

```
sudo apt install curl
curl -LsSf https://astral.sh/uv/install.sh | sh
```

### MacOS

```bash
brew install cmake ninja gcc@14 glpk uv
```

## Compile

To test whether compilation works, run the following from the root of this repository:

```bash
cmake -B build_test \
  -DCMAKE_CXX_COMPILER=g++-14 \
  -G Ninja \
  -DCMAKE_BUILD_TYPE=Debug

cmake --build build_test --parallel
```

## Reproduce Paper Figures

Go to the root of this repository:

```
cd dqsim
```

First setup the Python environment:

```bash
uv sync
```

Then run the automated script:
```
uv run python main.py
```

The figures will be placed in `dqsim/figure_output`.
