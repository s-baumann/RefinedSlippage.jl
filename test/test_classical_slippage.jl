

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

@testset "Classical Slippage" begin

    @testset "Buy side - price increase (negative slippage)" begin
        # Simple buy execution: arrival at 100, filled at 101 and 102
        # Buy side: higher prices = worse, so slippage should be negative

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
            bid_price = [99.5, 100.5],
            ask_price = [100.5, 101.5]
        )

        exec_data = ExecutionData(fills, metadata, tob)
        calculate_slippage!(exec_data)

        summary = exec_data.summary_bps

        # Manual calculation:
        # Fill 1: price = 101, arrival = 100, qty = 100
        # Fill 2: price = 102, arrival = 100, qty = 100
        # Total notional at arrival = 200 * 100 = 20000
        # Cost relative to arrival = (101-100)*100 + (102-100)*100 = 100 + 200 = 300
        # Classical slippage (fraction) = 300 / 20000 = 0.015 = 1.5%
        # For buy: side_sign = -1, so slippage = -1 * 0.015 = -0.015
        # In bps: -0.015 * 10000 = -150 bps
        expected_bps = manually_calculate_classical_slippage(fills, metadata)

        @test summary.classical_slippage[1] ≈ expected_bps atol=0.1

        # Verify other units
        summary_pct = exec_data.summary_pct
        @test summary_pct.classical_slippage[1] ≈ -1.5 atol=0.01

        summary_usd = exec_data.summary_usd
        # USD = slippage_fraction * arrival_price * total_qty = -0.015 * 100 * 200 = -300
        @test summary_usd.classical_slippage[1] ≈ -300.0 atol=0.1
    end

    @testset "Sell side - price decrease (negative slippage)" begin
        # Simple sell execution: arrival at 100, filled at 99 and 98
        # Sell side: lower prices = worse, so slippage should be negative

        fills_sell = DataFrame(
            time = [1.0, 2.0],
            quantity = [100, 100],
            price = [99.0, 98.0],
            execution_name = ["test_sell", "test_sell"],
            asset = [:AAPL, :AAPL]
        )

        metadata_sell = DataFrame(
            execution_name = ["test_sell"],
            arrival_price = [100.0],
            side = ["sell"],
            desired_quantity = [200]
        )

        tob = DataFrame(
            time = [1.0, 2.0],
            symbol = [:AAPL, :AAPL],
            bid_price = [98.5, 97.5],
            ask_price = [99.5, 98.5]
        )

        exec_data_sell = ExecutionData(fills_sell, metadata_sell, tob)
        calculate_slippage!(exec_data_sell)

        summary_sell = exec_data_sell.summary_bps

        # Manual calculation:
        expected_bps = manually_calculate_classical_slippage(fills_sell, metadata_sell)

        @test summary_sell.classical_slippage[1] ≈ expected_bps atol=0.1
    end

    @testset "Buy side - price decrease (positive slippage)" begin
        # Buy execution where prices fell (good for buyer)
        # Arrival at 100, filled at 99 and 98

        fills = DataFrame(
            time = [1.0, 2.0],
            quantity = [100, 100],
            price = [99.0, 98.0],
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
            bid_price = [98.5, 97.5],
            ask_price = [99.5, 98.5]
        )

        exec_data = ExecutionData(fills, metadata, tob)
        calculate_slippage!(exec_data)

        summary = exec_data.summary_bps

        # Manual calculation:
        expected_bps = manually_calculate_classical_slippage(fills, metadata)

        @test summary.classical_slippage[1] ≈ expected_bps atol=0.1
    end

    @testset "Sell side - price increase (positive slippage)" begin
        # Sell execution where prices rose (good for seller)
        # Arrival at 100, filled at 101 and 102

        fills = DataFrame(
            time = [1.0, 2.0],
            quantity = [100, 100],
            price = [101.0, 102.0],
            execution_name = ["test_sell_good", "test_sell_good"],
            asset = [:AAPL, :AAPL]
        )

        metadata = DataFrame(
            execution_name = ["test_sell_good"],
            arrival_price = [100.0],
            side = ["sell"],
            desired_quantity = [200]
        )

        tob = DataFrame(
            time = [1.0, 2.0],
            symbol = [:AAPL, :AAPL],
            bid_price = [100.5, 101.5],
            ask_price = [101.5, 102.5]
        )

        exec_data = ExecutionData(fills, metadata, tob)
        calculate_slippage!(exec_data)

        summary = exec_data.summary_bps

        # Manual calculation:
        expected_bps = manually_calculate_classical_slippage(fills, metadata)

        @test summary.classical_slippage[1] ≈ expected_bps atol=0.1
    end

    @testset "Weighted average with different quantities" begin
        # Test that different fill quantities are handled correctly

        fills = DataFrame(
            time = [1.0, 2.0],
            quantity = [300, 100],  # 3:1 ratio
            price = [101.0, 104.0],
            execution_name = ["test_weighted", "test_weighted"],
            asset = [:AAPL, :AAPL]
        )

        metadata = DataFrame(
            execution_name = ["test_weighted"],
            arrival_price = [100.0],
            side = ["buy"],
            desired_quantity = [400]
        )

        tob = DataFrame(
            time = [1.0, 2.0],
            symbol = [:AAPL, :AAPL],
            bid_price = [100.5, 103.5],
            ask_price = [101.5, 104.5]
        )

        exec_data = ExecutionData(fills, metadata, tob)
        calculate_slippage!(exec_data)

        summary = exec_data.summary_bps

        # Manual calculation:
        expected_bps = manually_calculate_classical_slippage(fills, metadata)

        @test summary.classical_slippage[1] ≈ expected_bps atol=0.1
    end

    @testset "Spreadcrossing proportion" begin
        # Test spread crossing calculation

        fills = DataFrame(
            time = [1.0],
            quantity = [100],
            price = [100.5],  # Exactly at mid of 100-101 spread
            execution_name = ["test_spread"],
            asset = [:AAPL]
        )

        metadata = DataFrame(
            execution_name = ["test_spread"],
            arrival_price = [100.5],
            side = ["buy"],
            desired_quantity = [100]
        )

        tob = DataFrame(
            time = [1.0],
            symbol = [:AAPL],
            bid_price = [100.0],
            ask_price = [101.0]
        )

        exec_data = ExecutionData(fills, metadata, tob)
        calculate_slippage!(exec_data)

        summary = exec_data.summary_bps

        # For buy at mid: (ask - price) / spread = (101 - 100.5) / 1 = 0.5
        @test summary.spread_cross_pct[1] ≈ 0.5 atol=0.01

        # Test buy at bid (best case = 1.0)
        fills_at_bid = DataFrame(
            time = [1.0],
            quantity = [100],
            price = [100.0],
            execution_name = ["test_bid"],
            asset = [:AAPL]
        )
        metadata_bid = DataFrame(
            execution_name = ["test_bid"],
            arrival_price = [100.0],
            side = ["buy"],
            desired_quantity = [100]
        )

        exec_data_bid = ExecutionData(fills_at_bid, metadata_bid, tob)
        calculate_slippage!(exec_data_bid)
        @test exec_data_bid.summary_bps.spread_cross_pct[1] ≈ 1.0 atol=0.01

        # Test buy at ask (worst case = 0.0)
        fills_at_ask = DataFrame(
            time = [1.0],
            quantity = [100],
            price = [101.0],
            execution_name = ["test_ask"],
            asset = [:AAPL]
        )
        metadata_ask = DataFrame(
            execution_name = ["test_ask"],
            arrival_price = [101.0],
            side = ["buy"],
            desired_quantity = [100]
        )

        exec_data_ask = ExecutionData(fills_at_ask, metadata_ask, tob)
        calculate_slippage!(exec_data_ask)
        @test exec_data_ask.summary_bps.spread_cross_pct[1] ≈ 0.0 atol=0.01
    end

    @testset "Multiple executions" begin
        # Test that multiple executions are handled independently

        fills = DataFrame(
            time = [1.0, 2.0, 1.0, 2.0],
            quantity = [100, 100, 200, 200],
            price = [101.0, 102.0, 99.0, 98.0],
            execution_name = ["exec1", "exec1", "exec2", "exec2"],
            asset = [:AAPL, :AAPL, :AAPL, :AAPL]
        )

        metadata = DataFrame(
            execution_name = ["exec1", "exec2"],
            arrival_price = [100.0, 100.0],
            side = ["buy", "sell"],
            desired_quantity = [200, 400]
        )

        tob = DataFrame(
            time = [1.0, 2.0],
            symbol = [:AAPL, :AAPL],
            bid_price = [98.0, 97.0],
            ask_price = [102.0, 103.0]
        )

        exec_data = ExecutionData(fills, metadata, tob)
        calculate_slippage!(exec_data)

        summary = exec_data.summary_bps

        @test nrow(summary) == 2

        # exec1 (buy): -150 bps
        exec1_row = summary[summary.execution_name .== "exec1", :]
        @test exec1_row.classical_slippage[1] ≈ manually_calculate_classical_slippage(fills[1:2, :], metadata[[1], :]) atol=0.1

        # exec2 (sell): price went down, bad for seller
        exec2_row = summary[summary.execution_name .== "exec2", :]
        @test exec2_row.classical_slippage[1] ≈ manually_calculate_classical_slippage(fills[3:4, :], metadata[[2], :]) atol=0.1
    end
end
