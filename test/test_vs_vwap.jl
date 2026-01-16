

function manually_calculate_vs_vwap_slippage(fills::DataFrame, metadata::DataFrame, volume::DataFrame, tob::DataFrame)
    side = metadata.side[1]
    arrival_price = metadata.arrival_price[1]
    side_sign = side == "buy" ? -1 : 1

    # Calculate fill VWAP
    fill_value = sum(fills.price .* fills.quantity)
    fill_qty = sum(fills.quantity)
    fill_vwap = fill_value / fill_qty

    # Calculate market VWAP over execution window using TOB mid-prices
    asset = fills.asset[1]
    start_time = minimum(fills.time)
    end_time = maximum(fills.time)

    # Get TOB mid-prices for this asset
    tob_asset = filter(r -> r.symbol == asset, tob)
    tob_asset[!, :mid_price] = (tob_asset.bid_price .+ tob_asset.ask_price) ./ 2

    # Filter volume intervals that overlap execution window
    vol_subset = filter(r -> r.symbol == asset &&
                             r.time_from <= end_time &&
                             r.time_to >= start_time, volume)

    # Calculate VWAP using mid-prices at interval midpoints
    total_value = 0.0
    total_vol = 0.0
    for vol_row in eachrow(vol_subset)
        interval_mid = (vol_row.time_from + vol_row.time_to) / 2
        time_diffs = abs.(tob_asset.time .- interval_mid)
        closest_idx = argmin(time_diffs)
        mid_price = tob_asset.mid_price[closest_idx]
        total_value += mid_price * vol_row.volume
        total_vol += vol_row.volume
    end

    market_vwap = total_value / total_vol

    # vs_vwap slippage
    vs_vwap = side_sign * (fill_vwap - market_vwap) / arrival_price

    return vs_vwap * 10000.0  # in bps
end

@testset "Vs VWAP Slippage" begin

    @testset "Buy side - fills above market VWAP (negative slippage)" begin
        # Our fills are at 101 and 102 (avg 101.5)
        # Market VWAP uses TOB mid-prices: mid at t=1 is 100.5, mid at t=2 is 101.5
        # With equal volumes: market VWAP = (100.5*500 + 101.5*500)/1000 = 101.0
        # Fill VWAP = 101.5, so we paid more than market -> negative slippage

        fills = DataFrame(
            time = [1.0, 2.0],
            quantity = [100, 100],
            price = [101.0, 102.0],
            execution_name = ["test_buy", "test_buy"],
            asset = [:AAPL, :AAPL]
        )

        metadata = DataFrame(
            execution_name = ["test_buy"],
            arrival_price = [100.0],
            side = ["buy"],
            desired_quantity = [200]
        )

        tob = DataFrame(
            time = [1.0, 2.0],
            symbol = [:AAPL, :AAPL],
            bid_price = [100.0, 101.0],
            ask_price = [101.0, 102.0]
        )

        # Market volume data (interval-based)
        volume = DataFrame(
            time_from = [0.5, 1.5],
            time_to = [1.5, 2.5],
            symbol = [:AAPL, :AAPL],
            volume = [500, 500]
        )

        exec_data = ExecutionData(fills, metadata, tob; volume=volume)
        calculate_slippage!(exec_data)

        summary = exec_data.summary[:bps]

        expected_bps = manually_calculate_vs_vwap_slippage(fills, metadata, volume, tob)

        @test :vs_vwap_slippage in propertynames(summary)
        @test summary.vs_vwap_slippage[1] ≈ expected_bps atol=0.1
    end

    @testset "Buy side - fills below market VWAP (positive slippage)" begin
        # Our fills are at 99 and 100 (avg 99.5)
        # Market VWAP from TOB mid-prices will be higher
        # We paid less than market, so positive slippage for buyer

        fills = DataFrame(
            time = [1.0, 2.0],
            quantity = [100, 100],
            price = [99.0, 100.0],
            execution_name = ["test_buy_good", "test_buy_good"],
            asset = [:AAPL, :AAPL]
        )

        metadata = DataFrame(
            execution_name = ["test_buy_good"],
            arrival_price = [100.0],
            side = ["buy"],
            desired_quantity = [200]
        )

        tob = DataFrame(
            time = [1.0, 2.0],
            symbol = [:AAPL, :AAPL],
            bid_price = [100.0, 101.0],
            ask_price = [101.0, 102.0]
        )

        volume = DataFrame(
            time_from = [0.5, 1.5],
            time_to = [1.5, 2.5],
            symbol = [:AAPL, :AAPL],
            volume = [500, 500]
        )

        exec_data = ExecutionData(fills, metadata, tob; volume=volume)
        calculate_slippage!(exec_data)

        summary = exec_data.summary[:bps]

        expected_bps = manually_calculate_vs_vwap_slippage(fills, metadata, volume, tob)

        @test summary.vs_vwap_slippage[1] ≈ expected_bps atol=0.1
        @test summary.vs_vwap_slippage[1] > 0  # positive slippage (good for buyer)
    end

    @testset "Sell side - fills below market VWAP (negative slippage)" begin
        # For sells, selling below market VWAP is bad

        fills = DataFrame(
            time = [1.0, 2.0],
            quantity = [100, 100],
            price = [99.0, 100.0],
            execution_name = ["test_sell", "test_sell"],
            asset = [:AAPL, :AAPL]
        )

        metadata = DataFrame(
            execution_name = ["test_sell"],
            arrival_price = [100.0],
            side = ["sell"],
            desired_quantity = [200]
        )

        tob = DataFrame(
            time = [1.0, 2.0],
            symbol = [:AAPL, :AAPL],
            bid_price = [100.0, 101.0],
            ask_price = [101.0, 102.0]
        )

        volume = DataFrame(
            time_from = [0.5, 1.5],
            time_to = [1.5, 2.5],
            symbol = [:AAPL, :AAPL],
            volume = [500, 500]
        )

        exec_data = ExecutionData(fills, metadata, tob; volume=volume)
        calculate_slippage!(exec_data)

        summary = exec_data.summary[:bps]

        expected_bps = manually_calculate_vs_vwap_slippage(fills, metadata, volume, tob)

        @test summary.vs_vwap_slippage[1] ≈ expected_bps atol=0.1
        @test summary.vs_vwap_slippage[1] < 0  # negative slippage (bad for seller)
    end

    @testset "Weighted volume calculation" begin
        # Test that market VWAP correctly weights by volume
        # TOB mid is constant at 100, so VWAP should be 100 regardless of weights

        fills = DataFrame(
            time = [1.0, 2.0],
            quantity = [100, 100],
            price = [100.0, 100.0],
            execution_name = ["test_weighted", "test_weighted"],
            asset = [:AAPL, :AAPL]
        )

        metadata = DataFrame(
            execution_name = ["test_weighted"],
            arrival_price = [100.0],
            side = ["buy"],
            desired_quantity = [200]
        )

        tob = DataFrame(
            time = [1.0, 2.0],
            symbol = [:AAPL, :AAPL],
            bid_price = [99.5, 99.5],
            ask_price = [100.5, 100.5]
        )

        # Unequal volumes - but TOB mid is constant so VWAP = 100
        volume = DataFrame(
            time_from = [0.5, 1.5],
            time_to = [1.5, 2.5],
            symbol = [:AAPL, :AAPL],
            volume = [900, 100]
        )

        exec_data = ExecutionData(fills, metadata, tob; volume=volume)
        calculate_slippage!(exec_data)

        summary = exec_data.summary[:bps]

        expected_bps = manually_calculate_vs_vwap_slippage(fills, metadata, volume, tob)

        @test summary.vs_vwap_slippage[1] ≈ expected_bps atol=0.1
        @test summary.vs_vwap_slippage[1] ≈ 0.0 atol=0.1
    end

    @testset "No volume data - no vs_vwap calculated" begin
        # Without volume data, vs_vwap should not be in the summary

        fills = DataFrame(
            time = [1.0, 2.0],
            quantity = [100, 100],
            price = [101.0, 102.0],
            execution_name = ["test_no_vol", "test_no_vol"],
            asset = [:AAPL, :AAPL]
        )

        metadata = DataFrame(
            execution_name = ["test_no_vol"],
            arrival_price = [100.0],
            side = ["buy"],
            desired_quantity = [200]
        )

        tob = DataFrame(
            time = [1.0, 2.0],
            symbol = [:AAPL, :AAPL],
            bid_price = [99.5, 100.5],
            ask_price = [101.5, 102.5]
        )

        exec_data = ExecutionData(fills, metadata, tob)
        calculate_slippage!(exec_data)

        summary = exec_data.summary[:bps]

        @test !(:vs_vwap_slippage in propertynames(summary))
    end

    @testset "Fill returns includes market_vwap" begin
        # Check that fill_returns has the market_vwap column

        fills = DataFrame(
            time = [1.0, 2.0],
            quantity = [100, 100],
            price = [101.0, 102.0],
            execution_name = ["test_fr", "test_fr"],
            asset = [:AAPL, :AAPL]
        )

        metadata = DataFrame(
            execution_name = ["test_fr"],
            arrival_price = [100.0],
            side = ["buy"],
            desired_quantity = [200]
        )

        tob = DataFrame(
            time = [1.0, 2.0],
            symbol = [:AAPL, :AAPL],
            bid_price = [99.5, 100.5],
            ask_price = [100.5, 101.5]
        )

        volume = DataFrame(
            time_from = [0.5, 1.5],
            time_to = [1.5, 2.5],
            symbol = [:AAPL, :AAPL],
            volume = [500, 500]
        )

        exec_data = ExecutionData(fills, metadata, tob; volume=volume)
        calculate_slippage!(exec_data)

        fill_returns = exec_data.fill_returns

        @test :market_vwap in propertynames(fill_returns)
        @test nrow(fill_returns) == 2

        # First fill at t=1: only interval [0.5,1.5] overlaps, mid at t=1 is 100.0
        @test fill_returns.market_vwap[1] ≈ 100.0 atol=0.1
        # Second fill at t=2: both intervals overlap, mids are 100.0 and 101.0, equal vols -> 100.5
        @test fill_returns.market_vwap[2] ≈ 100.5 atol=0.1
    end

    @testset "Unit conversions for vs_vwap" begin
        fills = DataFrame(
            time = [1.0, 2.0],
            quantity = [100, 100],
            price = [102.0, 102.0],  # Fill VWAP = 102
            execution_name = ["test_units", "test_units"],
            asset = [:AAPL, :AAPL]
        )

        metadata = DataFrame(
            execution_name = ["test_units"],
            arrival_price = [100.0],
            side = ["buy"],
            desired_quantity = [200]
        )

        # TOB mid at t=1: 100, mid at t=2: 101
        tob = DataFrame(
            time = [1.0, 2.0],
            symbol = [:AAPL, :AAPL],
            bid_price = [99.5, 100.5],
            ask_price = [100.5, 101.5]
        )

        volume = DataFrame(
            time_from = [0.5, 1.5],
            time_to = [1.5, 2.5],
            symbol = [:AAPL, :AAPL],
            volume = [500, 500]
        )

        exec_data = ExecutionData(fills, metadata, tob; volume=volume)
        calculate_slippage!(exec_data)

        # Fill VWAP = 102, Market VWAP = (100*500 + 101*500)/1000 = 100.5
        # vs_vwap = -1 * (102 - 100.5) / 100 = -0.015 = -150 bps
        expected_bps = manually_calculate_vs_vwap_slippage(fills, metadata, volume, tob)

        @test exec_data.summary[:bps].vs_vwap_slippage[1] ≈ expected_bps atol=0.1
        @test exec_data.summary[:pct].vs_vwap_slippage[1] ≈ expected_bps / 100 atol=0.01
        @test exec_data.summary[:usd].vs_vwap_slippage[1] ≈ expected_bps / 10000 * 100 * 200 atol=0.1

        print_slippage_summary(exec_data)
        plot_execution_markout(exec_data; "test_units")
    end
end
