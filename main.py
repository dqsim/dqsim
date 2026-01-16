from visualization.figures import create_all_figures
from visualization.sparkbench_figures import run_all_sparkbench


def main():
    # build spark bench figures
    run_all_sparkbench()
    # build all other figures
    create_all_figures()

if __name__ == "__main__":
    main()
