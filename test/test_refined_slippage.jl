using Dates, Random, DataFrames, LinearAlgebra
using HighFrequencyCovariance

function manually_calculate_classical_slippage(fills::DataFrame, metadata::DataFrame)
    total_qty = sum(fills.quantity)
    arrival_price = metadata.arrival_price[1]
    side = metadata.side[1]

    side_sign = side == "buy" ? -1 : 1

    total_notional_at_arrival = total_qty * arrival_price

    price_change = sum((fill.price - arrival_price) * fill.quantity for fill in eachrow(fills))

    slippage_fraction = price_change / total_notional_at_arrival

    classical_slippage = side_sign * slippage_fraction

    return classical_slippage * 10000.0  # in bps
end


"""
    manually_calculate_refined_slippage(fills::DataFrame, metadata::DataFrame, tob::DataFrame,
                                         peers::DataFrame, covar_matrix::Matrix{Float64},
                                         covar_labels::Vector{Symbol})

Calculate refined slippage manually (without using the package).
Returns slippage in basis points.

The refined slippage calculation:
1. For each fill, calculate the counterfactual return using peer returns and weights
2. counterfactual_price = arrival_price * exp(counterfactual_return)
3. refined_slippage = side_sign * sum((fill_price - counterfactual_price) * qty) / (total_qty * arrival_price)
"""
function manually_calculate_refined_slippage(fills::DataFrame, metadata::DataFrame, tob::DataFrame,
                                              peers::DataFrame, covar_matrix::AbstractMatrix{Float64},
                                              covar_labels::Vector{Symbol})
    total_qty = sum(fills.quantity)
    arrival_price = metadata.arrival_price[1]
    side = metadata.side[1]
    traded_asset = fills.asset[1]

    side_sign = side == "buy" ? -1 : 1

    # Get first fill time (base time for peer returns)
    first_fill_time = minimum(fills.time)

    # Calculate mid prices from TOB
    tob_with_mid = copy(tob)
    tob_with_mid[!, :mid_price] = (tob.bid_price .+ tob.ask_price) ./ 2

    # Get base prices for peers at first fill time
    base_prices = Dict{Symbol, Float64}()
    for peer in unique(peers.peer)
        base_row = filter(r -> r.time == first_fill_time && r.symbol == peer, tob_with_mid)
        if nrow(base_row) > 0
            base_prices[peer] = base_row.mid_price[1]
        end
    end

    # Calculate counterfactual prices for each fill
    total_refined_cost = 0.0

    for fill in eachrow(fills)
        fill_time = fill.time
        fill_qty = fill.quantity
        fill_price = fill.price

        # Calculate peer returns from base time to fill time
        peer_returns = Dict{Symbol, Float64}()
        for peer in unique(peers.peer)
            peer_row_at_fill = filter(r -> r.time == fill_time && r.symbol == peer, tob_with_mid)
            if nrow(peer_row_at_fill) > 0 && haskey(base_prices, peer)
                peer_price_at_fill = peer_row_at_fill.mid_price[1]
                peer_returns[peer] = log(peer_price_at_fill / base_prices[peer])
            end
        end

        # Calculate counterfactual return using peer weights
        counterfactual_return = 0.0
        for peer_row in eachrow(peers)
            peer = peer_row.peer
            weight = peer_row.weight
            if haskey(peer_returns, peer)
                counterfactual_return += weight * peer_returns[peer]
            end
        end

        # Calculate counterfactual price
        counterfactual_price = arrival_price * exp(counterfactual_return)

        # Accumulate refined cost
        total_refined_cost += (fill_price - counterfactual_price) * fill_qty
    end

    total_notional_at_arrival = total_qty * arrival_price
    refined_slippage = side_sign * total_refined_cost / total_notional_at_arrival

    return refined_slippage * 10000.0  # in bps
end

"""
    manually_calculate_spread_cross_pct(fills::DataFrame, metadata::DataFrame, tob::DataFrame)

Calculate spread crossing percentage manually (without using the package).
Returns the quantity-weighted average spread crossing proportion.

For buy orders: 1 = at bid (best), 0 = at ask (worst)
For sell orders: 1 = at ask (best), 0 = at bid (worst)
"""
function manually_calculate_spread_cross_pct(fills::DataFrame, metadata::DataFrame, tob::DataFrame)
    side = metadata.side[1]
    total_qty = sum(fills.quantity)

    weighted_spread_cross = 0.0

    for fill in eachrow(fills)
        fill_time = fill.time
        fill_price = fill.price
        fill_qty = fill.quantity
        fill_asset = fill.asset

        # Get TOB at fill time for this asset
        tob_row = filter(r -> r.time == fill_time && r.symbol == fill_asset, tob)
        if nrow(tob_row) == 0
            continue
        end

        bid_price = tob_row.bid_price[1]
        ask_price = tob_row.ask_price[1]
        spread = ask_price - bid_price

        if spread <= 0
            prop = 0.5  # No spread, assume mid
        elseif side == "buy"
            # For buy: at bid is best (1), at ask is worst (0)
            prop = (ask_price - fill_price) / spread
        else
            # For sell: at ask is best (1), at bid is worst (0)
            prop = (fill_price - bid_price) / spread
        end

        # Clamp to [0, 1]
        prop = clamp(prop, 0.0, 1.0)

        weighted_spread_cross += prop * fill_qty
    end

    return weighted_spread_cross / total_qty
end

"""
    get_peer_weights_from_covariance(covar_matrix::Matrix{Float64}, covar_labels::Vector{Symbol},
                                      traded_asset::Symbol, peer_assets::Vector{Symbol})

Calculate peer weights from covariance matrix using the conditional mean formula:
weights = Σ₁₂ * Σ₂₂⁻¹
"""
function get_peer_weights_from_covariance(covar_matrix::AbstractMatrix{Float64}, covar_labels::Vector{Symbol},
                                           traded_asset::Symbol, peer_assets::Vector{Symbol})
    asset_index = findfirst(==(traded_asset), covar_labels)
    peer_indices = [findfirst(==(p), covar_labels) for p in peer_assets]

    sigma12 = covar_matrix[asset_index:asset_index, peer_indices]
    sigma22 = covar_matrix[peer_indices, peer_indices]

    weights = sigma12 / sigma22
    return weights[:]
end

@testset "Refined Slippage" begin
    Random.seed!(42)

    @testset "Basic refined slippage with multiple fills" begin
        # Generate simple correlated returns
        n_obs = 100
        assets = [:AAPL, :SPY, :QQQ]

        # Create price data with known correlation
        times = collect(1.0:100.0)
        base_returns = randn(n_obs) .* 0.01

        prices = Dict{Symbol, Vector{Float64}}()
        for (i, asset) in enumerate(assets)
            noise = randn(n_obs) .* 0.003
            prices[asset] = cumprod(1.0 .+ base_returns .+ noise) .* (100.0 + i*20)
        end

        # Build time series for covariance matrix
        ts_data = DataFrame()
        for (i, t) in enumerate(times)
            for asset in assets
                push!(ts_data, (Time=t, Name=asset, Value=log(prices[asset][i])))
            end
        end

        sdf = SortedDataFrame(ts_data, :Time, :Name, :Value, Second(1))
        covar = estimate_covariance(sdf)
        covar_matrix = HighFrequencyCovariance.covariance(covar, Hour(1))
        covar_labels = covar.labels

        # Create execution data with multiple fills at different times
        fill_times = [10.0, 25.0, 40.0, 55.0]
        fill_qtys = [100, 75, 150, 50]
        fill_prices = [prices[:AAPL][Int(t)] * (1 + 0.005*randn()) for t in fill_times]

        fills = DataFrame(
            time = fill_times,
            quantity = fill_qtys,
            price = fill_prices,
            execution_name = fill("test_basic", length(fill_times)),
            asset = fill(:AAPL, length(fill_times))
        )

        arrival_price = (prices[:AAPL][Int(fill_times[1])] * 0.999 + prices[:AAPL][Int(fill_times[1])] * 1.001) / 2
        metadata = DataFrame(
            execution_name = ["test_basic"],
            arrival_price = [arrival_price],
            side = ["buy"],
            desired_quantity = [sum(fill_qtys)]
        )

        # Create TOB for all times and all assets
        tob_rows = []
        for t in times
            for asset in assets
                push!(tob_rows, (
                    time = t,
                    symbol = asset,
                    bid_price = prices[asset][Int(t)] * 0.999,
                    ask_price = prices[asset][Int(t)] * 1.001
                ))
            end
        end
        tob = DataFrame(tob_rows)

        exec_data = ExecutionData(fills, metadata, tob, covar)
        add_slippage!(exec_data)

        summary = exec_data.summary[:bps]

        # Calculate expected values manually
        expected_classical = manually_calculate_classical_slippage(fills, metadata)
        expected_spread_cross = manually_calculate_spread_cross_pct(fills, metadata, tob)
        expected_refined = manually_calculate_refined_slippage(fills, metadata, tob, exec_data.peers, covar_matrix, covar_labels)

        # Verify results match manual calculations
        @test summary.classical_slippage[1] ≈ expected_classical atol=0.1
        @test summary.spread_cross_pct[1] ≈ expected_spread_cross atol=0.001
        @test summary.refined_slippage[1] ≈ expected_refined atol=0.1
    end

    @testset "Degenerate covariance with multiple fills" begin
        # When peer correlations are near zero, refined slippage should be close to classical

        Random.seed!(123)
        n_obs = 100
        assets = [:AAPL, :SPY, :QQQ]
        times = collect(1.0:100.0)

        # Create independent price series (low correlation)
        prices = Dict{Symbol, Vector{Float64}}()
        for (i, asset) in enumerate(assets)
            # Completely independent returns for each asset
            returns = randn(n_obs) .* 0.01
            prices[asset] = cumprod(1.0 .+ returns) .* (100.0 + i*20)
        end

        ts_data = DataFrame()
        for (i, t) in enumerate(times)
            for asset in assets
                push!(ts_data, (Time=t, Name=asset, Value=log(prices[asset][i])))
            end
        end

        sdf = SortedDataFrame(ts_data, :Time, :Name, :Value, Second(1))
        covar = estimate_covariance(sdf)
        covar_matrix = HighFrequencyCovariance.covariance(covar, Hour(1))
        covar_labels = covar.labels

        # Multiple fills
        fill_times = [10.0, 30.0, 50.0, 70.0]
        fill_qtys = [80, 120, 60, 100]
        fill_prices = [prices[:AAPL][Int(t)] for t in fill_times]

        fills = DataFrame(
            time = fill_times,
            quantity = fill_qtys,
            price = fill_prices,
            execution_name = fill("test_degen", length(fill_times)),
            asset = fill(:AAPL, length(fill_times))
        )

        arrival_price = prices[:AAPL][1]
        metadata = DataFrame(
            execution_name = ["test_degen"],
            arrival_price = [arrival_price],
            side = ["sell"],
            desired_quantity = [sum(fill_qtys)]
        )

        tob_rows = []
        for t in times
            for asset in assets
                push!(tob_rows, (
                    time = t,
                    symbol = asset,
                    bid_price = prices[asset][Int(t)] * 0.995,
                    ask_price = prices[asset][Int(t)] * 1.005
                ))
            end
        end
        tob = DataFrame(tob_rows)

        exec_data = ExecutionData(fills, metadata, tob, covar)
        add_slippage!(exec_data)

        summary = exec_data.summary[:bps]

        # Calculate expected values manually
        expected_classical = manually_calculate_classical_slippage(fills, metadata)
        expected_spread_cross = manually_calculate_spread_cross_pct(fills, metadata, tob)
        expected_refined = manually_calculate_refined_slippage(fills, metadata, tob, exec_data.peers, covar_matrix, covar_labels)

        @test summary.classical_slippage[1] ≈ expected_classical atol=0.1
        @test summary.spread_cross_pct[1] ≈ expected_spread_cross atol=0.001
        @test summary.refined_slippage[1] ≈ expected_refined atol=0.1
    end

    @testset "High correlation manual calculation verification" begin
        # Create highly correlated assets where peer movement explains most of asset movement

        Random.seed!(456)
        n_obs = 100
        assets = [:AAPL, :SPY, :QQQ]
        times = collect(1.0:100.0)

        # Create prices where peers are highly correlated with AAPL
        base_returns = randn(n_obs) .* 0.02
        prices = Dict{Symbol, Vector{Float64}}()
        prices[:AAPL] = cumprod(1.0 .+ base_returns) .* 100.0
        prices[:SPY] = cumprod(1.0 .+ base_returns .+ randn(n_obs) .* 0.002) .* 200.0
        prices[:QQQ] = cumprod(1.0 .+ base_returns .+ randn(n_obs) .* 0.002) .* 150.0

        ts_data = DataFrame()
        for (i, t) in enumerate(times)
            for asset in assets
                push!(ts_data, (Time=t, Name=asset, Value=log(prices[asset][i])))
            end
        end

        sdf = SortedDataFrame(ts_data, :Time, :Name, :Value, Second(1))
        covar = estimate_covariance(sdf)
        covar_matrix = HighFrequencyCovariance.covariance(covar, Hour(1))
        covar_labels = covar.labels

        # Multiple fills with realistic price variation
        fill_times = Float64.([10, 30, 33, 80, 90])
        fill_qtys = [100, 50, 75, 100, 25]
        # Add some execution noise to fill prices
        fill_prices = (1.0 .+ 0.02 * randn(5)) .* [prices[:AAPL][Int(t)] for t in fill_times]

        fills = DataFrame(
            time = fill_times,
            quantity = fill_qtys,
            price = fill_prices,
            execution_name = fill("test_highcorr", length(fill_times)),
            asset = fill(:AAPL, length(fill_times))
        )

        arrival_price = prices[:AAPL][5]  # Arrival at time 5
        metadata = DataFrame(
            execution_name = ["test_highcorr"],
            arrival_price = [arrival_price],
            side = ["buy"],
            desired_quantity = [sum(fill_qtys)]
        )

        tob_rows = []
        for t in times
            for asset in assets
                push!(tob_rows, (
                    time = t,
                    symbol = asset,
                    bid_price = prices[asset][Int(t)] * 0.999,
                    ask_price = prices[asset][Int(t)] * 1.001
                ))
            end
        end
        tob = DataFrame(tob_rows)

        exec_data = ExecutionData(fills, metadata, tob, covar)
        add_slippage!(exec_data)

        summary = exec_data.summary[:bps]

        # Calculate expected values manually
        expected_classical = manually_calculate_classical_slippage(fills, metadata)
        expected_spread_cross = manually_calculate_spread_cross_pct(fills, metadata, tob)
        expected_refined = manually_calculate_refined_slippage(fills, metadata, tob, exec_data.peers, covar_matrix, covar_labels)

        @test summary.classical_slippage[1] ≈ expected_classical atol=0.1
        @test summary.spread_cross_pct[1] ≈ expected_spread_cross atol=0.001
        @test summary.refined_slippage[1] ≈ expected_refined atol=0.1

        # With high correlation, refined slippage should have smaller magnitude than classical
        # (peers explain most of the price movement)
        @test abs(summary.refined_slippage[1]) <= abs(summary.classical_slippage[1]) + 50.0
    end

    @testset "num_peers parameter with multiple fills" begin
        Random.seed!(789)
        n_obs = 100
        assets = [:AAPL, :SPY, :QQQ, :IWM, :DIA]
        times = collect(1.0:100.0)

        prices = Dict{Symbol, Vector{Float64}}()
        base_returns = randn(n_obs) .* 0.01
        for (i, asset) in enumerate(assets)
            noise = randn(n_obs) .* 0.005
            prices[asset] = cumprod(1.0 .+ base_returns .+ noise) .* (100.0 + i*10)
        end

        ts_data = DataFrame()
        for (i, t) in enumerate(times)
            for asset in assets
                push!(ts_data, (Time=t, Name=asset, Value=log(prices[asset][i])))
            end
        end

        sdf = SortedDataFrame(ts_data, :Time, :Name, :Value, Second(1))
        covar = estimate_covariance(sdf)
        covar_matrix = HighFrequencyCovariance.covariance(covar, Hour(1))
        covar_labels = covar.labels

        # Multiple fills
        fill_times = [10.0, 25.0, 50.0, 75.0]
        fill_qtys = [100, 80, 120, 60]
        fill_prices = [prices[:AAPL][Int(t)] * (1 + 0.003*randn()) for t in fill_times]

        fills = DataFrame(
            time = fill_times,
            quantity = fill_qtys,
            price = fill_prices,
            execution_name = fill("test_numpeers", length(fill_times)),
            asset = fill(:AAPL, length(fill_times))
        )

        arrival_price = prices[:AAPL][1]
        metadata = DataFrame(
            execution_name = ["test_numpeers"],
            arrival_price = [arrival_price],
            side = ["buy"],
            desired_quantity = [sum(fill_qtys)]
        )

        tob_rows = []
        for t in times
            for asset in assets
                push!(tob_rows, (
                    time = t,
                    symbol = asset,
                    bid_price = prices[asset][Int(t)] * 0.995,
                    ask_price = prices[asset][Int(t)] * 1.005
                ))
            end
        end
        tob = DataFrame(tob_rows)

        # Test with num_peers = 2 (should only use top 2 most correlated)
        exec_data = ExecutionData(fills, metadata, tob, covar; num_peers=2)

        # Should have exactly 2 peers per execution
        @test nrow(exec_data.peers) == 2

        add_slippage!(exec_data)

        summary = exec_data.summary[:bps]

        # Calculate expected values manually
        expected_classical = manually_calculate_classical_slippage(fills, metadata)
        expected_spread_cross = manually_calculate_spread_cross_pct(fills, metadata, tob)
        expected_refined = manually_calculate_refined_slippage(fills, metadata, tob, exec_data.peers, covar_matrix, covar_labels)

        @test summary.classical_slippage[1] ≈ expected_classical atol=0.1
        @test summary.spread_cross_pct[1] ≈ expected_spread_cross atol=0.001
        @test summary.refined_slippage[1] ≈ expected_refined atol=0.1
    end

    @testset "Sell side with multiple fills" begin
        # Test sell side to ensure sign conventions are correct

        Random.seed!(999)
        n_obs = 100
        assets = [:AAPL, :SPY, :QQQ]
        times = collect(1.0:100.0)

        base_returns = randn(n_obs) .* 0.015
        prices = Dict{Symbol, Vector{Float64}}()
        for (i, asset) in enumerate(assets)
            noise = randn(n_obs) .* 0.003
            prices[asset] = cumprod(1.0 .+ base_returns .+ noise) .* (100.0 + i*30)
        end

        ts_data = DataFrame()
        for (i, t) in enumerate(times)
            for asset in assets
                push!(ts_data, (Time=t, Name=asset, Value=log(prices[asset][i])))
            end
        end

        sdf = SortedDataFrame(ts_data, :Time, :Name, :Value, Second(1))
        covar = estimate_covariance(sdf)
        covar_matrix = HighFrequencyCovariance.covariance(covar, Hour(1))
        covar_labels = covar.labels

        # Multiple fills for sell order
        fill_times = [15.0, 35.0, 60.0, 85.0]
        fill_qtys = [200, 150, 100, 150]
        fill_prices = [prices[:AAPL][Int(t)] * (1 - 0.003*randn()) for t in fill_times]

        fills = DataFrame(
            time = fill_times,
            quantity = fill_qtys,
            price = fill_prices,
            execution_name = fill("test_sell", length(fill_times)),
            asset = fill(:AAPL, length(fill_times))
        )

        arrival_price = prices[:AAPL][10]
        metadata = DataFrame(
            execution_name = ["test_sell"],
            arrival_price = [arrival_price],
            side = ["sell"],
            desired_quantity = [sum(fill_qtys)]
        )

        tob_rows = []
        for t in times
            for asset in assets
                push!(tob_rows, (
                    time = t,
                    symbol = asset,
                    bid_price = prices[asset][Int(t)] * 0.998,
                    ask_price = prices[asset][Int(t)] * 1.002
                ))
            end
        end
        tob = DataFrame(tob_rows)

        exec_data = ExecutionData(fills, metadata, tob, covar)
        add_slippage!(exec_data)

        summary = exec_data.summary[:bps]

        # Calculate expected values manually
        expected_classical = manually_calculate_classical_slippage(fills, metadata)
        expected_spread_cross = manually_calculate_spread_cross_pct(fills, metadata, tob)
        expected_refined = manually_calculate_refined_slippage(fills, metadata, tob, exec_data.peers, covar_matrix, covar_labels)

        @test summary.classical_slippage[1] ≈ expected_classical atol=0.1
        @test summary.spread_cross_pct[1] ≈ expected_spread_cross atol=0.001
        @test summary.refined_slippage[1] ≈ expected_refined atol=0.1
    end
end
