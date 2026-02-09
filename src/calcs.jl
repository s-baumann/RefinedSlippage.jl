
# Convert time difference to hours for volatility scaling in truncation.
# DateTime periods are auto-converted; numeric times use exec_data.time_to_hours.
function _duration_hours(t, t0, time_to_hours::Float64)
    dt = t - t0
    return dt isa Dates.Period ? Dates.value(Dates.Millisecond(dt)) / 3_600_000.0 : Float64(dt) * time_to_hours
end

# Find the index of the closest value in a sorted vector using binary search.
function _closest_sorted(sorted_vec::AbstractVector, val)
    n = length(sorted_vec)
    k = searchsortedlast(sorted_vec, val)
    if k == 0
        return 1
    elseif k >= n
        return n
    else
        return abs(sorted_vec[k] - val) <= abs(sorted_vec[k+1] - val) ? k : k + 1
    end
end

"""
    calculate_slippage!(exec_data::ExecutionData)

Calculate slippage metrics and store results in `exec_data.summary_bps`, `exec_data.summary_pct`,
`exec_data.summary_usd`, and `exec_data.fill_returns`.

If `exec_data.peers` is provided, calculates both classical and refined slippage.
If `exec_data.volume` is provided, calculates vs_vwap slippage (fill VWAP vs market VWAP).
If `exec_data.peers` is missing, calculates only classical slippage.

Use `get_slippage!(exec_data, :bps)` to retrieve the desired format.

# Returns
- `exec_data`: The modified ExecutionData with summary and `fill_returns` populated.
"""
function calculate_slippage!(exec_data::ExecutionData)
    fills = exec_data.fills
    metadata = exec_data.metadata
    tob = exec_data.tob
    peers = exec_data.peers
    volume = exec_data.volume
    has_peers = !ismissing(peers)
    has_volume = !ismissing(volume)

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

    # Vectorized spread crossing calculation
    # For buy: 1 = at bid (best), 0 = at ask (worst)
    # For sell: 1 = at ask (best), 0 = at bid (worst)
    spreads = fills_with_tob.ask_price .- fills_with_tob.bid_price
    safe_spreads = ifelse.(spreads .<= 0, 1.0, spreads)
    is_buy = fills_with_tob.side .== "buy"
    raw_prop = ifelse.(is_buy,
        (fills_with_tob.ask_price .- fills_with_tob.price) ./ safe_spreads,
        (fills_with_tob.price .- fills_with_tob.bid_price) ./ safe_spreads)
    fills_with_tob[!, :spread_cross] = ifelse.(spreads .<= 0, 0.5, clamp.(raw_prop, 0.0, 1.0))

    # Compute mid-prices once (shared by VWAP and peers)
    tob_times = tob.time
    tob_symbols = tob.symbol
    tob_mid_prices = (tob.bid_price .+ tob.ask_price) ./ 2
    mid_prices = DataFrame(time = tob_times, symbol = tob_symbols, mid_price = tob_mid_prices)

    # Calculate VWAP metrics if volume data is provided
    if has_volume
        # Pre-group and sort TOB mids by asset for binary search
        tob_by_asset = Dict{eltype(tob_symbols), Tuple{Vector{eltype(tob_times)}, Vector{Float64}}}()
        for gdf in groupby(mid_prices, :symbol)
            sym = gdf.symbol[1]
            perm = sortperm(gdf.time)
            tob_by_asset[sym] = (gdf.time[perm], Float64.(gdf.mid_price[perm]))
        end

        # Pre-group and sort volume by asset
        vol_by_asset = Dict{eltype(volume.symbol), Tuple{Vector{eltype(volume.time_from)}, Vector{eltype(volume.time_to)}, Vector{Float64}}}()
        for gdf in groupby(volume, :symbol)
            sym = gdf.symbol[1]
            perm = sortperm(gdf.time_from)
            vol_by_asset[sym] = (gdf.time_from[perm], gdf.time_to[perm], Float64.(gdf.volume[perm]))
        end

        # Pre-compute execution start times as Dict for O(1) lookup
        exec_start = Dict{Tuple{eltype(fills.execution_name), eltype(fills.asset)}, eltype(fills.time)}()
        for gdf in groupby(fills, [:execution_name, :asset])
            exec_start[(gdf.execution_name[1], gdf.asset[1])] = minimum(gdf.time)
        end

        # Compute market VWAP for each fill using pre-grouped data and binary search
        n_fills = nrow(fills_with_tob)
        market_vwaps = Vector{Union{Missing, Float64}}(missing, n_fills)

        ft_asset = fills_with_tob.asset
        ft_exec = fills_with_tob.execution_name
        ft_time = fills_with_tob.time

        for i in 1:n_fills
            asset = ft_asset[i]
            start_time = get(exec_start, (ft_exec[i], asset), nothing)
            isnothing(start_time) && continue

            haskey(vol_by_asset, asset) || continue
            vol_from, vol_to, vol_vol = vol_by_asset[asset]

            haskey(tob_by_asset, asset) || continue
            asset_tob_times, asset_tob_prices = tob_by_asset[asset]

            fill_time = ft_time[i]

            # Binary search: only check intervals starting at or before fill_time
            last_idx = searchsortedlast(vol_from, fill_time)
            last_idx == 0 && continue

            total_value = 0.0
            total_vol = 0.0

            for j in 1:last_idx
                vol_to[j] < start_time && continue

                interval_mid = (vol_from[j] + vol_to[j]) / 2
                closest_k = _closest_sorted(asset_tob_times, interval_mid)
                total_value += asset_tob_prices[closest_k] * vol_vol[j]
                total_vol += vol_vol[j]
            end

            if total_vol > 0
                market_vwaps[i] = total_value / total_vol
            end
        end

        fills_with_tob[!, :market_vwap] = market_vwaps
    end

    if has_peers
        # Full refined slippage calculation
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

            # Truncate returns at peer_return_truncation * volatility * sqrt(duration)
            # Uses direct column indexing instead of eachrow for performance
            pp_time = fill_peer_prices.time
            pp_fft = fill_peer_prices.first_fill_time
            pp_vol = fill_peer_prices.volatility
            pp_ret = fill_peer_prices.peer_return
            tth = exec_data.time_to_hours
            trunc = exec_data.peer_return_truncation
            n_pp = length(pp_ret)
            new_returns = Vector{Float64}(undef, n_pp)
            for i in 1:n_pp
                v = pp_vol[i]
                if ismissing(v)
                    new_returns[i] = pp_ret[i]
                else
                    dur_h = _duration_hours(pp_time[i], pp_fft[i], tth)
                    bound = trunc * v * sqrt(max(dur_h, 0.0))
                    new_returns[i] = clamp(pp_ret[i], -bound, bound)
                end
            end
            fill_peer_prices[!, :peer_return] = new_returns

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

        # Add spread_cross (and market_vwap if available) to fills_with_counterfactual
        tob_cols = [:time, :execution_name, :spread_cross]
        if has_volume
            push!(tob_cols, :market_vwap)
        end
        fills_with_counterfactual = innerjoin(
            fills_with_counterfactual,
            fills_with_tob[:, tob_cols],
            on = [:time, :execution_name]
        )

        # Create wide format fill_returns with peer prices
        peer_prices_wide = unstack(
            fill_peer_prices[:, [:time, :execution_name, :asset, :quantity, :price, :peer, :peer_price]],
            [:time, :execution_name, :asset, :quantity, :price],
            :peer,
            :peer_price
        )

        counterfactual_cols = [:time, :execution_name, :counterfactual_price, :side, :arrival_price, :spread_cross]
        if has_volume
            push!(counterfactual_cols, :market_vwap)
        end
        fill_returns = innerjoin(
            peer_prices_wide,
            fills_with_counterfactual[:, counterfactual_cols],
            on = [:time, :execution_name]
        )

        peer_cols = setdiff(names(peer_prices_wide), ["time", "execution_name", "asset", "quantity", "price"])
        col_order = [:time, :quantity, :price, :execution_name, :asset, :arrival_price, :side, :counterfactual_price, :spread_cross]
        if has_volume
            push!(col_order, :market_vwap)
        end
        append!(col_order, Symbol.(peer_cols))
        fill_returns = fill_returns[:, col_order]

        # Sort by time within each execution so that last(market_vwap) is chronologically latest
        sort!(fills_with_counterfactual, [:execution_name, :time])

        # Calculate both classical and refined slippage (and vs_vwap if volume available)
        summary_base = combine(groupby(fills_with_counterfactual, :execution_name)) do df
            total_qty = sum(df.quantity)
            arrival_price = df.arrival_price[1]
            side = df.side[1]
            side_sign = side == "buy" ? -1 : 1

            classical_slippage = side_sign * sum((df.price .- arrival_price) .* df.quantity) / (total_qty * arrival_price)
            refined_slippage = side_sign * sum((df.price .- df.counterfactual_price) .* df.quantity) / (total_qty * arrival_price)
            avg_spread_cross = sum(df.spread_cross .* df.quantity) / total_qty

            result = DataFrame(
                side = side,
                classical_slippage = classical_slippage,
                refined_slippage = refined_slippage,
                spread_cross_pct = avg_spread_cross,
                total_quantity = total_qty,
                arrival_price = arrival_price
            )

            # Add vs_vwap if volume data is available
            if has_volume && :market_vwap in propertynames(df) && !all(ismissing, df.market_vwap)
                fill_vwap = sum(df.price .* df.quantity) / total_qty
                valid_vwaps = skipmissing(df.market_vwap)
                if !isempty(valid_vwaps)
                    market_vwap = last(collect(valid_vwaps))
                    vs_vwap_slippage = side_sign * (fill_vwap - market_vwap) / arrival_price
                    result[!, :vs_vwap_slippage] .= vs_vwap_slippage
                    result[!, :fill_vwap] .= fill_vwap
                    result[!, :market_vwap] .= market_vwap
                end
            end

            result
        end
    else
        # Classical slippage only (no peers)
        fill_cols = [:time, :quantity, :price, :execution_name, :asset, :arrival_price, :side, :spread_cross]
        if has_volume
            push!(fill_cols, :market_vwap)
        end

        # Sort by time within each execution so that last(market_vwap) is chronologically latest
        sort!(fills_with_tob, [:execution_name, :time])

        fill_returns = fills_with_tob[:, fill_cols]

        summary_base = combine(groupby(fills_with_tob, :execution_name)) do df
            total_qty = sum(df.quantity)
            arrival_price = df.arrival_price[1]
            side = df.side[1]
            side_sign = side == "buy" ? -1 : 1

            classical_slippage = side_sign * sum((df.price .- arrival_price) .* df.quantity) / (total_qty * arrival_price)
            avg_spread_cross = sum(df.spread_cross .* df.quantity) / total_qty

            result = DataFrame(
                side = side,
                classical_slippage = classical_slippage,
                spread_cross_pct = avg_spread_cross,
                total_quantity = total_qty,
                arrival_price = arrival_price
            )

            # Add vs_vwap if volume data is available
            if has_volume && :market_vwap in propertynames(df) && !all(ismissing, df.market_vwap)
                fill_vwap = sum(df.price .* df.quantity) / total_qty
                valid_vwaps = skipmissing(df.market_vwap)
                if !isempty(valid_vwaps)
                    market_vwap = last(collect(valid_vwaps))
                    vs_vwap_slippage = side_sign * (fill_vwap - market_vwap) / arrival_price
                    result[!, :vs_vwap_slippage] .= vs_vwap_slippage
                    result[!, :fill_vwap] .= fill_vwap
                    result[!, :market_vwap] .= market_vwap
                end
            end

            result
        end
    end

    # Create summary dict with different units
    summary_bps = copy(summary_base)
    summary_pct = copy(summary_base)
    summary_usd = copy(summary_base)

    has_vs_vwap = :vs_vwap_slippage in propertynames(summary_base)

    # Convert slippage to different units
    # bps: multiply by 10000
    summary_bps[!, :classical_slippage] = summary_base.classical_slippage .* 10000
    if has_peers
        summary_bps[!, :refined_slippage] = summary_base.refined_slippage .* 10000
    end
    if has_vs_vwap
        summary_bps[!, :vs_vwap_slippage] = summary_base.vs_vwap_slippage .* 10000
    end

    # pct: multiply by 100
    summary_pct[!, :classical_slippage] = summary_base.classical_slippage .* 100
    if has_peers
        summary_pct[!, :refined_slippage] = summary_base.refined_slippage .* 100
    end
    if has_vs_vwap
        summary_pct[!, :vs_vwap_slippage] = summary_base.vs_vwap_slippage .* 100
    end

    # usd: slippage * arrival_price * total_quantity
    summary_usd[!, :classical_slippage] = summary_base.classical_slippage .* summary_base.arrival_price .* summary_base.total_quantity
    if has_peers
        summary_usd[!, :refined_slippage] = summary_base.refined_slippage .* summary_base.arrival_price .* summary_base.total_quantity
    end
    if has_vs_vwap
        summary_usd[!, :vs_vwap_slippage] = summary_base.vs_vwap_slippage .* summary_base.arrival_price .* summary_base.total_quantity
    end

    exec_data.fill_returns = fill_returns
    exec_data.summary_bps = summary_bps
    exec_data.summary_pct = summary_pct
    exec_data.summary_usd = summary_usd
    return exec_data
end

"""
    get_slippage!(exec_data::ExecutionData, unit::Symbol=:bps)

Retrieve slippage summary in the specified unit.

# Arguments
- `exec_data`: ExecutionData with slippage already calculated via `calculate_slippage!`
- `unit`: One of `:bps` (basis points), `:pct` (percentage points), or `:usd` (dollar value)

# Returns
- DataFrame with slippage metrics in the requested unit
"""
function get_slippage!(exec_data::ExecutionData, unit::Symbol=:bps)
    if ismissing(exec_data.summary_bps)
        calculate_slippage!(exec_data)
    end
    if unit == :bps
        return exec_data.summary_bps
    elseif unit == :pct
        return exec_data.summary_pct
    elseif unit == :usd
        return exec_data.summary_usd
    else
        error("unit must be one of :bps, :pct, or :usd")
    end
end
