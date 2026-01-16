using RefinedSlippage, LinearAlgebra, DataFrames, DataFramesMeta, Random, HighFrequencyCovariance, StableRNGs, Statistics, Distributions

# =============================================================================
# Configuration
# =============================================================================
const NUM_EXECUTIONS = 20  # Number of executions to generate
const FILLS_PER_EXECUTION = 10  # Number of fills per execution
const TICKS_BETWEEN_EXECUTIONS = 500  # Time gap between execution windows

# =============================================================================
# Generate price data
# =============================================================================
dims = 8
# Need enough ticks: each execution needs TICKS_BETWEEN_EXECUTIONS unique times
# With synchronous data, ticks parameter = number of unique times
# We multiply by dims because generate_random_path with syncronous=true generates ticks/dims unique times
ticks = (NUM_EXECUTIONS + 2) * TICKS_BETWEEN_EXECUTIONS * dims  # Ensure enough for all executions
brownian_corr_matrix = Hermitian(0.5 .+ 0.5*I(dims))
twister = StableRNG(42)

ts, true_covar, true_micro_noise, true_update_rates = HighFrequencyCovariance.generate_random_path(
    dims, ticks; syncronous=true, brownian_corr_matrix=brownian_corr_matrix,
    vol_dist = Distributions.Uniform(0.0005, 0.001), micro_noise_dist = Distributions.Uniform(0, 0.0000000001),
)
assets = true_covar.labels

# Create bidask dataframe
bidask = copy(ts.df)
bidask[!, :Value] .= exp.(bidask.Value)
rename!(bidask, Dict(:Time => :time, :Name => :symbol, :Value => :bid_price))
bidask[:, :ask_price] = bidask[:, :bid_price] .* (1.0025 .+ (0.0025 .* rand(twister, size(bidask,1))))

# Create volume data for all assets
volume_times = unique(bidask.time[1:50:end])
n_intervals = length(volume_times) - 1
volume_dfs = DataFrame[]
for asset in assets
    push!(volume_dfs, DataFrame(
        time_from = volume_times[1:end-1],
        time_to = volume_times[2:end],
        symbol = fill(asset, n_intervals),
        volume = rand(twister, 50000:200000, n_intervals)
    ))
end
volume_df = reduce(vcat, volume_dfs)

# =============================================================================
# Generate executions - each uses a different time window (different realization)
# =============================================================================
function generate_executions(bidask, assets, num_executions, fills_per_exec, ticks_between, rng)
    allfills = DataFrame[]
    metadata = DataFrame[]

    unique_times = sort(unique(bidask.time))

    for exec_idx in 1:num_executions
        # Each execution trades a random asset (cycle through assets)
        asset = assets[mod1(exec_idx, length(assets))]

        # Get time window for this execution (different realization of price)
        start_tick = 1 + (exec_idx - 1) * ticks_between
        end_tick = min(start_tick + ticks_between - 1, length(unique_times))

        if end_tick <= start_tick + fills_per_exec
            @warn "Not enough ticks for execution $exec_idx, skipping"
            continue
        end

        exec_times = unique_times[start_tick:end_tick]

        # Filter bidask for this asset and time window
        subframe = bidask[(bidask.symbol .== asset) .& (bidask.time .>= exec_times[1]) .& (bidask.time .<= exec_times[end]), :]

        if nrow(subframe) < fills_per_exec
            @warn "Not enough data for execution $exec_idx, skipping"
            continue
        end

        # Generate fills at evenly spaced intervals
        fill_indices = round.(Int, range(1, nrow(subframe), length=fills_per_exec))
        fill_times = subframe.time[fill_indices]
        fill_prices = [subframe.bid_price[i] + rand(rng) * (subframe.ask_price[i] - subframe.bid_price[i])
                       for i in fill_indices]
        fill_quantities = rand(rng, 100:500, fills_per_exec)

        exec_name = "Execution_$(exec_idx)_$(asset)"

        subdf = DataFrame(
            time = fill_times,
            quantity = fill_quantities,
            price = fill_prices,
            execution_name = fill(exec_name, fills_per_exec),
            asset = fill(asset, fills_per_exec)
        )
        push!(allfills, subdf)

        # Metadata
        arrival_price = (subframe.bid_price[1] + subframe.ask_price[1]) / 2
        side = rand(rng, ["buy", "sell"])

        meta = DataFrame(
            execution_name = [exec_name],
            arrival_price = [arrival_price],
            side = [side],
            desired_quantity = [sum(fill_quantities)]
        )
        push!(metadata, meta)
    end

    fills = reduce(vcat, allfills)
    metadata_df = reduce(vcat, metadata)

    return fills, metadata_df
end

fills, metadata_df = generate_executions(bidask, assets, NUM_EXECUTIONS, FILLS_PER_EXECUTION, TICKS_BETWEEN_EXECUTIONS, twister)









println("Generated $(length(unique(fills.execution_name))) executions with $(nrow(fills)) total fills")

# =============================================================================
# Test 1: ExecutionData WITH peers (refined slippage)
# =============================================================================
println("\n" * "=" ^ 80)
println("TEST 1: ExecutionData with covariance matrix (refined slippage)")
println("=" ^ 80)

exec_data_with_peers = ExecutionData(fills, metadata_df, bidask, true_covar; volume=volume_df)
calculate_slippage!(exec_data_with_peers)

println("\nSlippage Summary in BPS (first 10 rows):")
println(first(get_slippage!(exec_data_with_peers, :bps), 10))

println("\nSlippage Summary in PCT (first 5 rows):")
println(first(get_slippage!(exec_data_with_peers, :pct), 5))

println("\nSlippage Summary in USD (first 5 rows):")
println(first(get_slippage!(exec_data_with_peers, :usd), 5))

println("\nFill Returns (first 5 rows):")
println(first(exec_data_with_peers.fill_returns, 5))

bps_summary = get_slippage!(exec_data_with_peers, :bps)
println("\nSummary Statistics (with peers) - in BPS:")
println("Classical Slippage - Mean: $(round(mean(bps_summary.classical_slippage), digits=2)) bps, Std: $(round(std(bps_summary.classical_slippage), digits=2)) bps")
println("Refined Slippage   - Mean: $(round(mean(bps_summary.refined_slippage), digits=2)) bps, Std: $(round(std(bps_summary.refined_slippage), digits=2)) bps")
println("Spread Crossing    - Mean: $(round(mean(bps_summary.spread_cross_pct) * 100, digits=1))%")

# =============================================================================
# Test 2: ExecutionData WITHOUT peers (classical slippage only)
# =============================================================================
println("\n" * "=" ^ 80)
println("TEST 2: ExecutionData without peers (classical slippage only)")
println("=" ^ 80)

exec_data_no_peers = ExecutionData(fills, metadata_df, bidask; volume=volume_df)
calculate_slippage!(exec_data_no_peers)

println("\nSlippage Summary in BPS (first 10 rows):")
println(first(get_slippage!(exec_data_no_peers, :bps), 10))

println("\nFill Returns (first 5 rows):")
println(first(exec_data_no_peers.fill_returns, 5))

bps_summary_no_peers = get_slippage!(exec_data_no_peers, :bps)
println("\nSummary Statistics (no peers) - in BPS:")
println("Classical Slippage - Mean: $(round(mean(bps_summary_no_peers.classical_slippage), digits=2)) bps, Std: $(round(std(bps_summary_no_peers.classical_slippage), digits=2)) bps")
println("Spread Crossing    - Mean: $(round(mean(bps_summary_no_peers.spread_cross_pct) * 100, digits=1))%")

# =============================================================================
# Test 3: Return truncation examples (peer_return_truncation parameter)
# =============================================================================
println("\n" * "=" ^ 80)
println("TEST 3: Peer return truncation examples")
println("=" ^ 80)

# Example 3a: Default truncation at 2 sigma
println("\n--- Example 3a: Default truncation (2 sigma) ---")
exec_data_trunc_2 = ExecutionData(fills, metadata_df, bidask, true_covar;
                                  volume=volume_df, peer_return_truncation=2.0)
calculate_slippage!(exec_data_trunc_2)
bps_trunc_2 = get_slippage!(exec_data_trunc_2, :bps)
println("Refined Slippage - Mean: $(round(mean(bps_trunc_2.refined_slippage), digits=2)) bps, Std: $(round(std(bps_trunc_2.refined_slippage), digits=2)) bps")

# Example 3b: Tighter truncation at 1 sigma
println("\n--- Example 3b: Tighter truncation (1 sigma) ---")
exec_data_trunc_1 = ExecutionData(fills, metadata_df, bidask, true_covar;
                                  volume=volume_df, peer_return_truncation=1.0)
calculate_slippage!(exec_data_trunc_1)
bps_trunc_1 = get_slippage!(exec_data_trunc_1, :bps)
println("Refined Slippage - Mean: $(round(mean(bps_trunc_1.refined_slippage), digits=2)) bps, Std: $(round(std(bps_trunc_1.refined_slippage), digits=2)) bps")

# Example 3c: Looser truncation at 3 sigma
println("\n--- Example 3c: Looser truncation (3 sigma) ---")
exec_data_trunc_3 = ExecutionData(fills, metadata_df, bidask, true_covar;
                                  volume=volume_df, peer_return_truncation=3.0)
calculate_slippage!(exec_data_trunc_3)
bps_trunc_3 = get_slippage!(exec_data_trunc_3, :bps)
println("Refined Slippage - Mean: $(round(mean(bps_trunc_3.refined_slippage), digits=2)) bps, Std: $(round(std(bps_trunc_3.refined_slippage), digits=2)) bps")

# Example 3d: No truncation (Inf)
println("\n--- Example 3d: No truncation (Inf) ---")
exec_data_no_trunc = ExecutionData(fills, metadata_df, bidask, true_covar;
                                   volume=volume_df, peer_return_truncation=Inf)
calculate_slippage!(exec_data_no_trunc)
bps_no_trunc = get_slippage!(exec_data_no_trunc, :bps)
println("Refined Slippage - Mean: $(round(mean(bps_no_trunc.refined_slippage), digits=2)) bps, Std: $(round(std(bps_no_trunc.refined_slippage), digits=2)) bps")

# Example 3e: Select top 4 peers with 2 sigma truncation
println("\n--- Example 3e: Top 4 peers with 2 sigma truncation ---")
exec_data_top4 = ExecutionData(fills, metadata_df, bidask, true_covar;
                               volume=volume_df, num_peers=4, peer_return_truncation=2.0)
calculate_slippage!(exec_data_top4)
bps_top4 = get_slippage!(exec_data_top4, :bps)
println("Refined Slippage - Mean: $(round(mean(bps_top4.refined_slippage), digits=2)) bps, Std: $(round(std(bps_top4.refined_slippage), digits=2)) bps")
println("Number of unique peers used: $(length(unique(exec_data_top4.peers.peer)))")

# Summary comparison
println("\n" * "=" ^ 80)
println("TRUNCATION COMPARISON SUMMARY")
println("=" ^ 80)
comparison_df = DataFrame(
    truncation = ["1 sigma", "2 sigma (default)", "3 sigma", "No truncation (Inf)", "Top 4 peers (2σ)"],
    mean_bps = [
        mean(bps_trunc_1.refined_slippage),
        mean(bps_trunc_2.refined_slippage),
        mean(bps_trunc_3.refined_slippage),
        mean(bps_no_trunc.refined_slippage),
        mean(bps_top4.refined_slippage)
    ],
    std_bps = [
        std(bps_trunc_1.refined_slippage),
        std(bps_trunc_2.refined_slippage),
        std(bps_trunc_3.refined_slippage),
        std(bps_no_trunc.refined_slippage),
        std(bps_top4.refined_slippage)
    ]
)
comparison_df.mean_bps = round.(comparison_df.mean_bps, digits=2)
comparison_df.std_bps = round.(comparison_df.std_bps, digits=2)
println(comparison_df)

# =============================================================================
# Test 4: Summary statistics and visualization
# =============================================================================
println("\n" * "=" ^ 80)
println("TEST 4: Summary statistics and execution visualization")
println("=" ^ 80)

# Print summary statistics
println("\n--- Summary Statistics (All executions with peers) ---")
print_slippage_summary(exec_data_with_peers; unit=:bps)

# Plot first execution
first_execution = first(unique(fills.execution_name))
println("\n--- Plotting execution: $first_execution ---")
plot = plot_execution_markout(exec_data_with_peers, first_execution; window_before=100, window_after=100)
println("Execution plot created (view with: plot |> display)")

# For demonstration - plot a few different executions
println("\n--- Creating plots for first 3 executions ---")
for exec_name in first(unique(fills.execution_name), 3)
    p = plot_execution_markout(exec_data_with_peers, exec_name; window_before=50, window_after=50)
    println("Created plot for: $exec_name")
    # Uncomment to display: p |> display
end
