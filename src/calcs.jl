
"""
    add_slippage!(exec_data::ExecutionData)

Calculate slippage metrics and store results in `exec_data.summary` and `exec_data.fill_returns`.

If `exec_data.peers` is provided, calculates both classical and refined slippage.
If `exec_data.peers` is missing, calculates only classical slippage.

Summary is stored as a Dict with keys `:bps`, `:pct`, `:usd` for different units.
Use `get_slippage(exec_data, :bps)` to retrieve the desired format.

# Returns
- `exec_data`: The modified ExecutionData with `summary` and `fill_returns` populated.
"""
function add_slippage!(exec_data::ExecutionData)
    fills = exec_data.fills
    metadata = exec_data.metadata
    tob = exec_data.tob
    peers = exec_data.peers
    has_peers = !ismissing(peers)

    # Join fills with metadata and tob to get arrival_price, side, and bid/ask at fill time
    fills_with_meta = innerjoin(
        fills,
        metadata[:, [:execution_name, :arrival_price, :side]],
        on = :execution_name
    )

    # Get bid/ask at each fill time for spreadcrossing calculation
    fills_with_tob = innerjoin(
        fills_with_meta,
        tob[:, [:time, :symbol, :bid_price, :ask_price]],
        on = [:time, :asset => :symbol]
    )

    # Calculate spreadcrossing proportion for each fill
    # For buy: 1 = at bid (best), 0 = at ask (worst)
    # For sell: 1 = at ask (best), 0 = at bid (worst)
    fills_with_tob[!, :spread_cross] = map(eachrow(fills_with_tob)) do row
        spread = row.ask_price - row.bid_price
        if spread <= 0
            return 0.5  # No spread, assume mid
        end
        if row.side == "buy"
            # For buy: at bid is best (1), at ask is worst (0)
            prop = (row.ask_price - row.price) / spread
        else
            # For sell: at ask is best (1), at bid is worst (0)
            prop = (row.price - row.bid_price) / spread
        end
        return clamp(prop, 0.0, 1.0)
    end

    if has_peers
        # Full refined slippage calculation
        mid_prices = DataFrame(
            time = tob.time,
            symbol = tob.symbol,
            mid_price = (tob.bid_price .+ tob.ask_price) ./ 2
        )

        first_fill_times = combine(groupby(fills, :execution_name), :time => minimum => :first_fill_time)

        base_prices = innerjoin(
            first_fill_times,
            mid_prices,
            on = [:first_fill_time => :time]
        )
        rename!(base_prices, :symbol => :peer, :mid_price => :base_price)

        fills_ext = leftjoin(fills, first_fill_times, on=:execution_name)
        fill_peer_prices = innerjoin(
            fills_ext[:, [:time, :execution_name, :asset, :quantity, :price, :first_fill_time]],
            mid_prices,
            on = :time
        )
        rename!(fill_peer_prices, :symbol => :peer, :mid_price => :peer_price)

        fill_peer_prices = innerjoin(
            fill_peer_prices,
            base_prices[:, [:execution_name, :peer, :base_price]],
            on = [:execution_name, :peer]
        )

        fill_peer_prices[!, :peer_return] = log.(fill_peer_prices.peer_price ./ fill_peer_prices.base_price)

        # Apply return truncation if vols provided and truncation not disabled
        if !ismissing(exec_data.vols) && !isinf(exec_data.peer_return_truncation)
            fill_peer_prices = leftjoin(
                fill_peer_prices,
                exec_data.vols[:, [:asset, :volatility]],
                on = :peer => :asset
            )

            # Truncate returns at peer_return_truncation * volatility
            fill_peer_prices[!, :peer_return] = Float64[
                ismissing(row.volatility) ? row.peer_return :
                clamp(row.peer_return, -exec_data.peer_return_truncation * row.volatility,
                      exec_data.peer_return_truncation * row.volatility)
                for row in eachrow(fill_peer_prices)
            ]

            # Remove volatility column (no longer needed)
            select!(fill_peer_prices, Not(:volatility))
        end

        fill_peer_weighted = innerjoin(
            fill_peer_prices,
            peers,
            on = [:execution_name, :peer]
        )

        counterfactual_returns = combine(
            groupby(fill_peer_weighted, [:time, :execution_name, :asset, :quantity, :price]),
            [:peer_return, :weight] => ((r, w) -> sum(r .* w)) => :counterfactual_return
        )

        fills_with_counterfactual = innerjoin(
            counterfactual_returns,
            metadata[:, [:execution_name, :arrival_price, :side]],
            on = :execution_name
        )

        fills_with_counterfactual[!, :counterfactual_price] =
            fills_with_counterfactual.arrival_price .* exp.(fills_with_counterfactual.counterfactual_return)

        # Add spread_cross to fills_with_counterfactual
        fills_with_counterfactual = innerjoin(
            fills_with_counterfactual,
            fills_with_tob[:, [:time, :execution_name, :spread_cross]],
            on = [:time, :execution_name]
        )

        # Create wide format fill_returns with peer prices
        peer_prices_wide = unstack(
            fill_peer_prices[:, [:time, :execution_name, :asset, :quantity, :price, :peer, :peer_price]],
            [:time, :execution_name, :asset, :quantity, :price],
            :peer,
            :peer_price
        )

        fill_returns = innerjoin(
            peer_prices_wide,
            fills_with_counterfactual[:, [:time, :execution_name, :counterfactual_price, :side, :arrival_price, :spread_cross]],
            on = [:time, :execution_name]
        )

        peer_cols = setdiff(names(peer_prices_wide), ["time", "execution_name", "asset", "quantity", "price"])
        col_order = [:time, :quantity, :price, :execution_name, :asset, :arrival_price, :side, :counterfactual_price, :spread_cross, Symbol.(peer_cols)...]
        fill_returns = fill_returns[:, col_order]

        # Calculate both classical and refined slippage
        summary_base = combine(groupby(fills_with_counterfactual, :execution_name)) do df
            total_qty = sum(df.quantity)
            arrival_price = df.arrival_price[1]
            side = df.side[1]
            side_sign = side == "buy" ? -1 : 1

            classical_slippage = side_sign * sum((df.price .- arrival_price) .* df.quantity) / (total_qty * arrival_price)
            refined_slippage = side_sign * sum((df.price .- df.counterfactual_price) .* df.quantity) / (total_qty * arrival_price)
            avg_spread_cross = sum(df.spread_cross .* df.quantity) / total_qty

            DataFrame(
                side = side,
                classical_slippage = classical_slippage,
                refined_slippage = refined_slippage,
                spread_cross_pct = avg_spread_cross,
                total_quantity = total_qty,
                arrival_price = arrival_price
            )
        end
    else
        # Classical slippage only (no peers)
        fill_returns = fills_with_tob[:, [:time, :quantity, :price, :execution_name, :asset, :arrival_price, :side, :spread_cross]]

        summary_base = combine(groupby(fills_with_tob, :execution_name)) do df
            total_qty = sum(df.quantity)
            arrival_price = df.arrival_price[1]
            side = df.side[1]
            side_sign = side == "buy" ? -1 : 1

            classical_slippage = side_sign * sum((df.price .- arrival_price) .* df.quantity) / (total_qty * arrival_price)
            avg_spread_cross = sum(df.spread_cross .* df.quantity) / total_qty

            DataFrame(
                side = side,
                classical_slippage = classical_slippage,
                spread_cross_pct = avg_spread_cross,
                total_quantity = total_qty,
                arrival_price = arrival_price
            )
        end
    end

    # Create summary dict with different units
    summary_bps = copy(summary_base)
    summary_pct = copy(summary_base)
    summary_usd = copy(summary_base)

    # Convert slippage to different units
    # bps: multiply by 10000
    summary_bps[!, :classical_slippage] = summary_base.classical_slippage .* 10000
    if has_peers
        summary_bps[!, :refined_slippage] = summary_base.refined_slippage .* 10000
    end

    # pct: multiply by 100
    summary_pct[!, :classical_slippage] = summary_base.classical_slippage .* 100
    if has_peers
        summary_pct[!, :refined_slippage] = summary_base.refined_slippage .* 100
    end

    # usd: slippage * arrival_price * total_quantity
    summary_usd[!, :classical_slippage] = summary_base.classical_slippage .* summary_base.arrival_price .* summary_base.total_quantity
    if has_peers
        summary_usd[!, :refined_slippage] = summary_base.refined_slippage .* summary_base.arrival_price .* summary_base.total_quantity
    end

    summary = Dict{Symbol,DataFrame}(
        :bps => summary_bps,
        :pct => summary_pct,
        :usd => summary_usd
    )

    exec_data.fill_returns = fill_returns
    exec_data.summary = summary
    return exec_data
end

"""
    get_slippage(exec_data::ExecutionData, unit::Symbol=:bps)

Retrieve slippage summary in the specified unit.

# Arguments
- `exec_data`: ExecutionData with slippage already calculated via `calculate_slippage!`
- `unit`: One of `:bps` (basis points), `:pct` (percentage points), or `:usd` (dollar value)

# Returns
- DataFrame with slippage metrics in the requested unit
"""
function get_slippage!(exec_data::ExecutionData, unit::Symbol=:bps)
    if ismissing(exec_data.summary)
        calculate_slippage!(exec_data)
    end
    if !(unit in [:bps, :pct, :usd])
        error("unit must be one of :bps, :pct, or :usd")
    end
    return exec_data.summary[unit]
end