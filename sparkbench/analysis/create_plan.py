import json
import statistics
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Optional

counter = 0


def get_id():
    global counter
    counter += 1
    return counter


@dataclass
class Pipeline:
    id: int
    parallelism: int
    dependencies: list[int]
    execution_time: float  # The time it would take to execute the whole pipeline on a single core in seconds
    acc_ids: list[int]
    is_result: bool
    task_assignment: dict[int, int]


def create_pipelines_plan(trace: dict) -> dict[int, Pipeline]:
    result_stage = None
    pipelines = {}

    stages = trace["stagePlan"]["stages"]

    existing_stages = {s["id"] for s in stages}
    # pre compute a mapping from rdds to stages
    rdd_to_stages = {}
    for stage in stages:
        for rdd in stage["rdds"]:
            if rdd not in rdd_to_stages:
                rdd_to_stages[rdd] = set()
            rdd_to_stages[rdd].add(stage["id"])

    # find dependencies by parents and shared rdds
    for stage in stages:
        id = stage["id"]

        if stage["isQueryResult"]:
            assert result_stage is None, "Multiple query results"
            result_stage = id

        current_dependencies = set(stage["parents"]).intersection(existing_stages)
        for rdd in stage["rdds"]:
            for other_stage in rdd_to_stages[rdd]:
                if other_stage < id:
                    current_dependencies.add(other_stage)

        task_assignment = {t["id"]: t["executor"] for t in stage["tasks"]}

        task_times = [t["duration"] for t in stage["tasks"]]
        execution_time = float(sum(task_times)) / 1000
        # execution_time = float(min(task_times)) * len(task_times) / 1000

        # if len(current_dependencies) == 0:
        #     execution_time = float(sum(task_times)) / 1000
        # else:
        #     execution_time = float(min(task_times)) * len(task_times) / 1000

        pipeline = Pipeline(
            id=id,
            parallelism=len(stage["tasks"]),
            dependencies=sorted(list(current_dependencies)),
            execution_time=execution_time,
            acc_ids=stage["accIds"],
            is_result=False,  # we will correct this in our connectedness check
            task_assignment=task_assignment,
        )
        pipelines[pipeline.id] = pipeline
    return pipelines


@dataclass
class PlanNode:
    id: int
    name: str
    parent_id: Optional[int]
    child_ids: list[int]
    acc_ids: list[int]
    json: dict


def parse_node(node: dict):
    acc_ids = [m["accId"] for m in node["metrics"]]
    return PlanNode(
        id=get_id(),
        name=node["nodeName"],
        parent_id=node.get("parentId", None),
        child_ids=[],
        acc_ids=acc_ids,
        json=node,
    )


def create_operator_plan(trace: dict) -> dict[int, PlanNode]:
    nodes = {}

    todo_list = [trace["sqlPlan"]]
    while len(todo_list) > 0:
        current_node = todo_list[0]
        todo_list = todo_list[1:]
        current_parsed_node = parse_node(current_node)
        nodes[current_parsed_node.id] = current_parsed_node
        todo_list += current_node["children"]
        for c in current_node["children"]:
            c["parentId"] = current_parsed_node.id
    # fill child nodes
    for n in nodes.values():
        if n.parent_id is not None:
            nodes[n.parent_id].child_ids.append(n.id)

    return nodes


def get_unique_acc_ids(pipeline_plan: dict[int, Pipeline]) -> tuple[dict[int, int], dict[int, set]]:
    # maps from acc id to pipeline id
    acc_id_to_pipeline = {}
    for i, p in pipeline_plan.items():
        for aid in p.acc_ids:
            if aid not in acc_id_to_pipeline:
                acc_id_to_pipeline[aid] = set()
            acc_id_to_pipeline[aid].add(i)
    unique_acc_ids = {a: next(iter(ps)) for a, ps in acc_id_to_pipeline.items() if len(ps) == 1}
    ambiguous_acc_ids = {a: ps for a, ps in acc_id_to_pipeline.items() if len(ps) > 1}
    return unique_acc_ids, ambiguous_acc_ids


def check_pipelines_connected(pipeline_plan: dict[int, Pipeline]):
    # this is now an inverse tree where a successor pipeline reads from a dependency
    successors = {}
    for p in pipeline_plan.values():
        for dep in p.dependencies:
            if dep in successors:
                print(
                    f"Wanted to add dependency {dep} to pipeline {p.id}, but {dep} is already succeeded by {successors[dep]}"
                )
            assert dep not in successors
            successors[dep] = p.id
    final_pipeline = None
    for p in pipeline_plan.values():
        id = p.id
        while id in successors:
            id = successors[id]
        if final_pipeline is None:
            final_pipeline = id
        if id != final_pipeline:
            return False
    assert final_pipeline is not None, "could not find the result pipeline"
    pipeline_plan[final_pipeline].is_result = True
    return True


def try_find_assigned_op_ancestor(operator_plan, op: PlanNode, op_pipelines) -> Optional[int]:
    # Returns the pipeline of the ancestor if one could be found
    allow_list = ["InputAdapter", "BroadcastHashJoin", "Project"]
    while op.id not in op_pipelines:
        if op.parent_id is None:
            print("Ended ancestor search with root")
            return None
        op = operator_plan[op.parent_id]
        if op.name not in allow_list:
            print(f"Ended ancestor search with {op.name}")
            return None
    return op_pipelines[op.id]


def analyze_broadcast_exchange(op: PlanNode, operator_plan, op_pipelines, pipeline_plan):
    # find pipeline of parent
    parent_pipeline = try_find_assigned_op_ancestor(operator_plan, op, op_pipelines)
    if parent_pipeline is None:
        print(f"failed to find pipeline of broadcast exchange parent: {operator_plan[op.parent_id].name}")
        return
    # find pipeline of child
    assert len(op.child_ids) == 1
    if op.child_ids[0] not in op_pipelines:
        print(f"failed to find pipeline of broadcast exchange child: {operator_plan[op.child_ids[0]].name}")
        return
    child_pipeline = op_pipelines[op.child_ids[0]]
    if parent_pipeline not in pipeline_plan[child_pipeline].dependencies:
        pipeline_plan[parent_pipeline].dependencies.append(child_pipeline)
    #     print("added a new dependency with broadcast exchange")
    # else:
    #     print("tried to add a dependency with broadcast exchange, but it was already there")


def merge_plans(
    pipeline_plan: dict[int, Pipeline], operator_plan: dict[int, PlanNode]
) -> tuple[dict[int, int], dict[int, set]]:
    unique_acc_ids_to_pipelines, ambiguous_acc_ids_to_pipelines = get_unique_acc_ids(pipeline_plan)

    # verify that each pipeline has a unique acc id
    found_pipelines = {p for p in unique_acc_ids_to_pipelines.values()}
    for p in pipeline_plan:
        assert p in found_pipelines, f"pipeline {p} has no unique id"

    # map each operator id to a pipeline id
    # some operators will not find matches
    # some pipelines will not have an op matched to them
    op_pipelines = {}
    for op in operator_plan.values():
        for acc_id in op.acc_ids:
            if acc_id in unique_acc_ids_to_pipelines:
                op_pipelines[op.id] = unique_acc_ids_to_pipelines[acc_id]
                break

    # map operators ambiguously to pipelines
    # find some operators for each pipeline, even though they might be part of other pipelines as well
    pipeline_ops = {p: set() for p in pipeline_plan.keys()}
    for op in operator_plan.values():
        for acc_id in op.acc_ids:
            if acc_id in unique_acc_ids_to_pipelines:
                pipeline_ops[unique_acc_ids_to_pipelines[acc_id]].add(op.id)
            if acc_id in ambiguous_acc_ids_to_pipelines:
                for p in ambiguous_acc_ids_to_pipelines[acc_id]:
                    pipeline_ops[p].add(op.id)

    # for all broadcast exchange / exchange ops: try to connect the involved pipelines
    for op in operator_plan.values():
        if op.name == "BroadcastExchange":
            analyze_broadcast_exchange(op, operator_plan, op_pipelines, pipeline_plan)
    return op_pipelines, pipeline_ops


def metrics_to_dict(metrics: list[dict]) -> dict[str, dict]:
    return {m["metricName"]: m for m in metrics}


@dataclass
class DU:
    id: int
    cardinality: int
    byte_size: float
    broadcast: bool
    single_node: bool
    is_base_relation: bool


@dataclass
class DUInfo:
    # maps from the id of a pipeline to the input DU id
    pipeline_inputs: dict[int, list[int]]
    # maps from the id of a pipeline to the output DU id
    pipeline_outputs: dict[int, int]


def find_closest_op_to_scan(op_mapping: dict[int, int], pipeline_id: int) -> Optional[int]:
    result = None
    for op, p in op_mapping.items():
        if p == pipeline_id:
            if result is None or op > result:
                result = op
    # assert result is not None, f"could not find a single operator for pipeline {pipeline_id}"
    return result


def du_from_exchange(op: PlanNode) -> DU:
    expected_ops = ["Exchange", "BroadcastExchange", "ReusedExchange"]
    assert op.name in expected_ops, f"expected [{', '.join(expected_ops)}] operator, got {op.name}"
    assert "metrics" in op.json, "exchange without metrics"
    metrics = metrics_to_dict(op.json["metrics"])

    cardinality = None
    cardinality_metric_names = ["shuffle records written", "number of output rows"]
    for n in cardinality_metric_names:
        if n in metrics:
            cardinality = metrics[n]["value"]
            break
    assert cardinality is not None, f"Could not find cardinality in {op.name}"

    byte_size = None
    byte_size_metric_names = ["shuffle bytes written", "data size"]
    for n in byte_size_metric_names:
        if n in metrics:
            byte_size = metrics[n]["value"] / 8
            break
    assert byte_size is not None, f"Could not find byte size in {op.name}"

    tuple_size = byte_size / cardinality

    return DU(get_id(), cardinality, tuple_size, False, False, False)


def find_pipeline_end(
    pipeline_ops: dict[int, set[int]], pipeline_id: int, operator_plan: dict[int, PlanNode]
) -> Optional[tuple[DU, PlanNode]]:
    result = None
    for op in pipeline_ops[pipeline_id]:
        if result is None or op < result:
            result = op
    assert result is not None, f"could not find a single operator for pipeline {pipeline_id}"
    op = operator_plan[result]
    while op.name.startswith("WholeStageCodegen") and op.parent_id is not None:
        result = op.parent_id
        op = operator_plan[result]
    if op.parent_id is None:
        # we found the query result
        return None
    exchange_names = ["Exchange", "BroadcastExchange"]
    if op.name in exchange_names:
        return du_from_exchange(op), op
    assert False, f"could not find pipeline end after {op.name}"


def get_dus(
    pipeline_plan: dict[int, Pipeline],
    operator_plan: dict[int, PlanNode],
    op_mapping: dict[int, int],
    pipeline_ops: dict[int, set[int]],
) -> tuple[dict[int, DU], DUInfo, list[tuple[int, int]], int]:
    du_info = DUInfo({}, {})
    dus: dict[int, DU] = {}
    shuffles: list[tuple[int, int]] = []

    # table scans (pipeline inputs)
    for p in pipeline_plan.values():
        op_id = find_closest_op_to_scan(op_mapping, p.id)
        if len(p.dependencies) > 0 and op_id is None:
            continue  # If there is no scan, we probably read from the dependent pipeline
        assert op_id is not None
        op = operator_plan[op_id]
        if len(p.dependencies) > 0 and not op.name.startswith("Scan parquet"):
            continue  # If there is no scan, we probably read from the dependent pipeline
        assert op.name.startswith("Scan parquet"), f"unexpected scan node {op.name}"
        metrics = metrics_to_dict(op.json["metrics"])
        assert "number of output rows" in metrics, f"could not find cardinality in {op.name}"
        cardinality: int = metrics["number of output rows"]["value"]
        is_broadcast = cardinality < 1000
        du_size: int = metrics["size of files read"]["value"]
        tuple_size = du_size / cardinality
        new_du = DU(
            get_id(),
            cardinality,
            tuple_size,
            is_broadcast,
            False,
            True,
        )
        dus[new_du.id] = new_du
        if p.id not in du_info.pipeline_inputs:
            du_info.pipeline_inputs[p.id] = []
        # the scan always has to be at index 0
        du_info.pipeline_inputs[p.id].insert(0, new_du.id)

    # pipeline outputs
    result_pipeline = None
    du_op_mapping = {}
    for p in pipeline_plan.values():
        end = find_pipeline_end(pipeline_ops, p.id, operator_plan)
        if end is None:
            assert result_pipeline is None
            result_pipeline = p.id
            continue
        new_du, op = end
        if p.parallelism == 1 and p.id in du_info.pipeline_inputs and dus[du_info.pipeline_inputs[p.id][0]].broadcast:
            new_du.single_node = True
        dus[new_du.id] = new_du
        du_info.pipeline_outputs[p.id] = new_du.id
        du_op_mapping[new_du.id] = op

    # create_pipeline_input_dus and shuffles
    for p in pipeline_plan.values():
        for d in p.dependencies:
            shuffle_in_du_id = du_info.pipeline_outputs[d]
            op = du_op_mapping[shuffle_in_du_id]
            shuffle_in_du = dus[shuffle_in_du_id]
            is_broadcast = op.name == "BroadcastExchange"

            if p.id not in du_info.pipeline_inputs:
                du_info.pipeline_inputs[p.id] = []

            output_du = DU(
                get_id(),
                shuffle_in_du.cardinality,
                shuffle_in_du.byte_size,
                is_broadcast,
                False,
                False,
            )
            du_info.pipeline_inputs[p.id].append(output_du.id)
            dus[output_du.id] = output_du
            shuffles.append((shuffle_in_du.id, output_du.id))

    # as a heuristic, set the result du to the same size as the input of the final pipeline
    assert result_pipeline is not None, "could not find the result pipeline"
    in_du = dus[du_info.pipeline_inputs[result_pipeline][0]]
    result_du = DU(
        get_id(),
        in_du.cardinality,
        in_du.byte_size,
        False,
        True,
        False,
    )
    dus[result_du.id] = result_du
    du_info.pipeline_outputs[result_pipeline] = result_du.id

    # result du
    return dus, du_info, shuffles, result_du.id


def remap_pipelines(pipeline_plan: dict[int, Pipeline], du_info: DUInfo) -> tuple[dict[int, Pipeline], DUInfo]:
    mapping: dict[int, int] = {}
    for p in pipeline_plan.values():
        mapping[p.id] = len(mapping)

    new_pipeline_plan = {mapping[k]: replace(v, id=mapping[k]) for k, v in pipeline_plan.items()}

    new_pipeline_inputs = {mapping[k]: v for k, v in du_info.pipeline_inputs.items()}
    new_pipeline_outputs = {mapping[k]: v for k, v in du_info.pipeline_outputs.items()}
    new_du_info = DUInfo(new_pipeline_inputs, new_pipeline_outputs)

    return new_pipeline_plan, new_du_info


def remap_dus(
    dus: dict[int, DU],
    du_info: DUInfo,
    shuffles: list[tuple[int, int]],
    result_du: int,
) -> tuple[dict[int, DU], DUInfo, list[tuple[int, int]]]:
    mapping: dict[int, int] = {result_du: 0}
    for du in dus.values():
        if du.id in mapping:
            assert du.id == result_du, "du id appears twice?"
            continue
        mapping[du.id] = len(mapping)

    new_dus = {mapping[k]: replace(v, id=mapping[k]) for k, v in dus.items()}
    new_shuffles = [(mapping[a], mapping[b]) for a, b in shuffles]

    new_pipeline_inputs = {k: [mapping[x] for x in v] for k, v in du_info.pipeline_inputs.items()}
    new_pipeline_outputs = {k: mapping[v] for k, v in du_info.pipeline_outputs.items()}
    new_du_info = DUInfo(new_pipeline_inputs, new_pipeline_outputs)

    return new_dus, new_du_info, new_shuffles


def get_plan(f: Path) -> Optional[tuple[dict[int, DU], DUInfo, list[tuple[int, int]], dict[int, Pipeline], str]]:
    print(f"\nlooking at {f.stem}:")
    with f.open() as fd:
        data = json.load(fd)
    pipeline_plan = create_pipelines_plan(data[0])
    operator_plan = create_operator_plan(data[0])

    op_mapping, pipeline_ops = merge_plans(pipeline_plan, operator_plan)

    if not check_pipelines_connected(pipeline_plan):
        print(f"{f.stem} is not connected!{'-'.join('' for _ in range(60))}")
        return None
    # print(f"{f.stem} is connected")
    dus, du_info, shuffles, result_du = get_dus(pipeline_plan, operator_plan, op_mapping, pipeline_ops)

    dus, du_info, shuffles = remap_dus(dus, du_info, shuffles, result_du)
    pipeline_plan, du_info = remap_pipelines(pipeline_plan, du_info)

    return dus, du_info, shuffles, pipeline_plan, f.stem


def get_plans() -> list[tuple[dict[int, DU], DUInfo, list[tuple[int, int]], dict[int, Pipeline], str]]:
    result = []
    for f in sorted([x for x in Path("sparkbench/data/output").glob("*.json")]):
        current = get_plan(f)
        if current is not None:
            result.append(current)
    return result


def main():
    get_plans()


if __name__ == "__main__":
    main()
