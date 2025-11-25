import math
import random

from tqdm.auto import tqdm


def greedy(annos, timing):
    all_steps = set(timing.keys())
    ORDER = []
    REMAINING_STEPS = list(all_steps)
    survivors = {k: v for k, v in annos.items() if len(k) > 0}
    while len(REMAINING_STEPS) > 0:
        rule_counts = {}
        for k, v in survivors.items():
            for el in k:
                rule_counts[el] = rule_counts.get(el, 0) + v
        # Heuristic is # removed / timing
        bang_for_buck = {
            k: rule_counts.get(k, 0) / (timing[k] + 1e-6)
            for k in timing
            if k not in ORDER
        }
        best_rule = max(bang_for_buck.items(), key=lambda p: p[1])[0]
        ORDER.append(best_rule)
        REMAINING_STEPS = [_ for _ in REMAINING_STEPS if _ != best_rule]
        new_survivors = {k: v for k, v in survivors.items() if best_rule not in k}
        survivors = new_survivors
    return ORDER


def greedy_with_lookahead(annos, timing):
    all_steps = set(timing.keys())
    ORDER = []
    survivors = {k: v for k, v in annos.items() if len(k) > 0}

    while len(survivors) > 0 and len(ORDER) < len(all_steps):
        best_rule = None
        best_time_saved = -float("inf")

        for candidate in all_steps:
            if candidate in ORDER:
                continue

            # Calculate time saved by running this filter next
            docs_removed = sum(v for k, v in survivors.items() if candidate in k)
            total_docs = sum(survivors.values())

            # Time cost of running this filter on current documents
            time_cost = total_docs * timing[candidate]

            # Time saved on future filters (documents won't need to be processed)
            remaining_filters = [
                f for f in all_steps if f not in ORDER and f != candidate
            ]
            time_saved = sum(docs_removed * timing[f] for f in remaining_filters)

            net_benefit = time_saved - time_cost

            if net_benefit > best_time_saved:
                best_time_saved = net_benefit
                best_rule = candidate

        if best_rule is None:
            break

        ORDER.append(best_rule)
        survivors = {k: v for k, v in survivors.items() if best_rule not in k}

    # Add any remaining filters that don't filter anything
    ORDER.extend([f for f in all_steps if f not in ORDER])
    return ORDER


def simulated_annealing(
    annos, timing, initial_temp=100, cooling_rate=0.995, max_iterations=5000
):
    """Use simulated annealing to find good filter order"""
    # Start with greedy solution
    current_order = greedy_with_lookahead(annos, timing)
    current_time = calculate_total_time(current_order, annos, timing)

    best_order = current_order[:]
    best_time = current_time

    temp = initial_temp

    for iteration in range(max_iterations):
        # Generate neighbor by swapping two random positions
        new_order = current_order[:]
        i, j = random.sample(range(len(new_order)), 2)
        new_order[i], new_order[j] = new_order[j], new_order[i]

        new_time = calculate_total_time(new_order, annos, timing)

        # Accept if better, or with probability based on temperature
        delta = new_time - current_time
        if delta < 0 or random.random() < math.exp(-delta / temp):
            current_order = new_order
            current_time = new_time

            if current_time < best_time:
                best_order = current_order[:]
                best_time = current_time

        temp *= cooling_rate

    return best_order, best_time
