import itertools
from functools import lru_cache
from multiprocessing import Pool, cpu_count

from tqdm import tqdm


def dp_optimal_parallel(annos, timing, n_processes=None):
    """Find the truly optimal filter order using parallel DP"""
    if n_processes is None:
        n_processes = cpu_count()

    all_filters = tuple(sorted(timing.keys()))
    n = len(all_filters)

    print(f"Using {n_processes} processes for {n} filters (2^{n} = {2**n:,} states)")

    def count_survivors(used_filters_set):
        """Count documents that survive after applying filters in used_filters_set"""
        count = 0
        for triggered_filters, num_docs in annos.items():
            if not any(f in used_filters_set for f in triggered_filters):
                count += num_docs
        return count

    # Build DP table bottom-up, level by level
    print("\nBuilding DP table...")
    dp = {}

    # Process states grouped by number of filters used (Hamming weight)
    for num_used in tqdm(range(n + 1), desc="DP levels"):
        # Generate all states with exactly num_used filters
        states_at_level = []
        for i in range(2**n):
            if bin(i).count("1") == num_used:
                states_at_level.append(i)

        if num_used == n:
            # Base case: all filters used
            for state in states_at_level:
                dp[state] = 0
        else:
            # Parallel computation for this level
            with Pool(n_processes) as pool:
                results = pool.starmap(
                    compute_state_value,
                    [
                        (state, all_filters, n, annos, timing, dp)
                        for state in states_at_level
                    ],
                    chunksize=max(1, len(states_at_level) // (n_processes * 4)),
                )

            for state, value in zip(states_at_level, results):
                dp[state] = value

    optimal_time = dp[0]

    # Reconstruct the optimal order
    print("\nReconstructing optimal order...")
    order = []
    used_mask = 0

    for step in tqdm(range(n), desc="Reconstruction"):
        used_filters = {all_filters[i] for i in range(n) if used_mask & (1 << i)}
        survivors = count_survivors(used_filters)

        best_filter = None
        best_time = float("inf")

        for i in range(n):
            if used_mask & (1 << i):
                continue

            filter_id = all_filters[i]
            time = survivors * timing[filter_id] + dp[used_mask | (1 << i)]

            if time < best_time:
                best_time = time
                best_filter = filter_id
                best_idx = i

        order.append(best_filter)
        used_mask |= 1 << best_idx

    return order, optimal_time


def compute_state_value(state, all_filters, n, annos, timing, dp):
    """Compute the value for a single state (used by parallel workers)"""
    used_filters = {all_filters[i] for i in range(n) if state & (1 << i)}

    # Count surviving documents
    survivors = 0
    for triggered_filters, num_docs in annos.items():
        if not any(f in used_filters for f in triggered_filters):
            survivors += num_docs

    # Try each unused filter next
    best_time = float("inf")
    for i in range(n):
        if state & (1 << i):  # Already used
            continue

        filter_id = all_filters[i]
        next_state = state | (1 << i)
        time = survivors * timing[filter_id] + dp[next_state]
        best_time = min(best_time, time)

    return best_time


def compare_algorithms_with_progress(annos, timing, n_processes=None):
    """Compare greedy vs DP optimal with progress bars"""
    print("=" * 60)
    print("ALGORITHM COMPARISON")
    print("=" * 60)

    # Original greedy
    print("\n1. Running original greedy...")
    greedy_order = greedy(annos, timing)
    greedy_time = calculate_total_time(greedy_order, annos, timing)
    print(f"   Time: {greedy_time:.2f}")

    # DP optimal (parallel)
    print("\n2. Running parallel DP optimal...")
    dp_order, dp_time = dp_optimal_parallel(annos, timing, n_processes)
    print(f"   Time: {dp_time:.2f}")

    # Compare
    print("\n" + "=" * 60)
    print("RESULTS")
    print("=" * 60)
    print(f"Greedy time:  {greedy_time:.2f}")
    print(f"Optimal time: {dp_time:.2f}")

    if abs(greedy_time - dp_time) < 0.01:
        print("\n✓ Greedy found the optimal solution!")
    else:
        improvement = (greedy_time - dp_time) / greedy_time * 100
        print(f"\n✗ Greedy is suboptimal by {improvement:.2f}%")
        print(f"\nGreedy order:  {greedy_order}")
        print(f"Optimal order: {dp_order}")

    return dp_order, dp_time
