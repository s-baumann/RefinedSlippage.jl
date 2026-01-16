# Examples

## Basic Usage with Classical Slippage

To calculate classical slippage without peer adjustments:

```julia
using RefinedSlippage, DataFrames

# Fill data: time, quantity, price for each execution
fills = DataFrame(
    time = [1.0, 2.0, 3.0],
    quantity = [100, 150, 100],
    price = [101.0, 101.5, 102.0],
    execution_name = ["buy_AAPL", "buy_AAPL", "buy_AAPL"],
    asset = [:AAPL, :AAPL, :AAPL]
)

# Metadata for each execution
metadata = DataFrame(
    execution_name = ["buy_AAPL"],
    arrival_price = [100.0],
    side = ["buy"],
    desired_quantity = [350]
)

# Top of book data
tob = DataFrame(
    time = [1.0, 2.0, 3.0],
    symbol = [:AAPL, :AAPL, :AAPL],
    bid_price = [99.5, 100.5, 101.0],
    ask_price = [100.5, 101.5, 102.0]
)

# Create ExecutionData and calculate slippage
exec_data = ExecutionData(fills, metadata, tob)
add_slippage!(exec_data)

# View results
print_slippage_summary(exec_data)
```

## Refined Slippage with Peer Assets

To use refined slippage with automatic peer selection:

```julia
using RefinedSlippage, DataFrames, HighFrequencyCovariance

# Create covariance matrix from time series data
# (assuming you have TimeseriesData from HighFrequencyCovariance.jl)
covar = CovarianceMatrix(timeseries_data)

# ExecutionData with peer-based adjustment
exec_data = ExecutionData(
    fills,
    metadata,
    tob,
    covar;
    num_peers = 5,                    # Use top 5 correlated peers
    peer_return_truncation = 2.0      # Truncate peer returns at ±2σ
)

add_slippage!(exec_data)

# Summary now includes both classical and refined slippage
print_slippage_summary(exec_data, unit=:bps)
```

## Visualization

To plot execution markout for a specific execution:

```julia
# Create plot showing price path and cumulative slippage
plot = plot_execution_markout(
    exec_data,
    "buy_AAPL",
    window_before = 5.0,    # Show 5 time units before first fill
    window_after = 10.0     # Show 10 time units after last fill
)

# Save or display the plot
using VegaLite
save("execution_markout.png", plot)
```

The plot shows:
- Top panel: bid/ask prices, arrival price, counterfactual price (if peers available), and fill points
- Bottom panel: cumulative classical and refined slippage over the execution

## Manual Peer Weights

If you want to specify peer weights directly:

```julia
# Define custom peer weights
peer_weights = Dict(:SPY => 0.6, :QQQ => 0.4)

# Optional: provide volatilities for return truncation
vols = Dict(:AAPL => 0.02, :SPY => 0.015, :QQQ => 0.025)

exec_data = ExecutionData(fills, metadata, tob, peer_weights, vols)
add_slippage!(exec_data)
```
