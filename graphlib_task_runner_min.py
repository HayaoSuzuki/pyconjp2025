"""
Minimal parallel task runner using graphlib (Python 3.13).

Idea: get_ready() -> submit all -> wait -> done() -> repeat.
"""

from dataclasses import dataclass, field
from graphlib import TopologicalSorter
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections.abc import Callable, Iterable
from typing import Optional

type Action = Callable[[], object]


@dataclass(frozen=True, slots=True)
class Task:
    name: str
    action: Action
    deps: set[str] = field(default_factory=set)


def run(tasks: Iterable[Task], *, max_workers: Optional[int] = None) -> dict[str, object]:
    by_name = {t.name: t for t in tasks}
    ts = TopologicalSorter({t.name: set(t.deps) for t in tasks})
    ts.prepare()

    ran = []
    done = []
    results: dict[str, object] = {}
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        while ts.is_active():
            ready = ts.get_ready()
            futs = {}
            for name in ready:
                print(f"run  {name}")
                ran.append(name)
                futs[pool.submit(by_name[name].action)] = name
            for fut in as_completed(futs):
                name = futs[fut]
                results[name] = fut.result()
                print(f"done {name}")
                done.append(name)
                ts.done(name)

    print(f"ran tasks: {ran}")
    print(f"done tasks: {done}")
    return results


if __name__ == "__main__":
    # Example DAG matching complex_dag_example.dot
    import time, random

    def sleep_task(label: str, lo=0.05, hi=0.2):
        def _run():
            time.sleep(random.uniform(lo, hi))
            return label
        return _run

    tasks = [
        # Roots
        Task("A", sleep_task("A")),
        Task("B", sleep_task("B")),
        Task("D", sleep_task("D")),

        # Layer 1
        Task("C", sleep_task("C"), deps={"B", "D"}),
        Task("E", sleep_task("E"), deps={"A", "B"}),
        Task("G", sleep_task("G"), deps={"D"}),

        # Layer 2
        Task("F", sleep_task("F"), deps={"A", "C", "G"}),
        Task("I", sleep_task("I"), deps={"B", "F"}),
        Task("M", sleep_task("M"), deps={"F"}),

        # Layer 3
        Task("J", sleep_task("J"), deps={"C", "M"}),
        Task("H", sleep_task("H"), deps={"E", "I"}),

        # Layer 4
        Task("K", sleep_task("K"), deps={"J"}),
        Task("L", sleep_task("L"), deps={"A", "C", "J"}),

        # Final
        Task("N", sleep_task("N"), deps={"D", "K"}),
    ]

    run(tasks, max_workers=8)
