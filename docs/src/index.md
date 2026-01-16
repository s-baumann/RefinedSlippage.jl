## Overview

RefinedSlippage.jl measures execution slippage - the difference between arrival price and actual fill prices. It computes both classical slippage (raw price impact) and refined slippage (market-adjusted using peer assets).

## Classical vs Refined Slippage

Classical slippage measures the direct cost of execution relative to the arrival price:

$$\text{Classical Slippage} = \frac{\sum (p_i - p_0) q_i}{\sum q_i \cdot p_0}$$

where $p_i$ is the fill price, $p_0$ is the arrival price, and $q_i$ is the fill quantity.

Refined slippage adjusts for market movements by constructing a counterfactual price from correlated peer assets:

$$\text{Refined Slippage} = \frac{\sum (p_i - \hat{p}_i) q_i}{\sum q_i \cdot p_0}$$

where $\hat{p}_i$ is the counterfactual price based on peer asset returns.

The peer weights are determined by bilateral correlations from a covariance matrix. Returns from peer assets can be truncated at a specified number of standard deviations to reduce the impact of outliers.

## Main Components

* `ExecutionData` - Container for execution fills, metadata, bid/ask data, and optional covariance matrix for peer selection
* `calculate_slippage!` - Calculates classical and refined slippage for all executions
* `print_slippage_summary` - Prints summary statistics
* `plot_execution_markout` - Visualizes execution price paths and cumulative slippage

## Peer Selection

When a covariance matrix is provided, the package selects peer assets with the highest bilateral correlation to the traded asset. The `num_peers` parameter controls how many peers to use.

Peer returns can also be truncated at `peer_return_truncation` standard deviations (default 2.0) so your counterfactual price and refined slippage is not too impacted by extreme events.
