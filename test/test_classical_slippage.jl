@testset "Classical Slippage" begin
    # Simple buy execution: arrival at 100, filled at 101 and 102
    # Buy side: higher prices = worse, so slippage should be positive

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
    add_slippage!(exec_data)

    summary = exec_data.summary[:bps]

    # Expected calculation:
    # Cost = (101-100)*100 + (102-100)*100 = 100 + 200 = 300
    # Total notional = 100*100 + 100*100 = 20000
    # Slippage fraction = 300/20000 = 0.015 = 1.5% = 150 bps
    # But buy side has sign flip: -1 * 150 = -150 bps
    # Wait, let me recalculate: side_sign for buy is -1
    # slippage = -1 * (sum((price - arrival_price) * quantity)) / (total_qty * arrival_price)
    # = -1 * 300 / (200 * 100) = -300/20000 = -0.015 = -150 bps

    @test summary.classical_slippage[1] ≈ -150.0 atol=0.1

    # Test sell side
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

    exec_data_sell = ExecutionData(fills_sell, metadata_sell, tob)
    add_slippage!(exec_data_sell)

    summary_sell = exec_data_sell.summary[:bps]

    # Expected calculation for sell:
    # Cost = (99-100)*100 + (98-100)*100 = -100 + -200 = -300
    # side_sign for sell is +1
    # slippage = 1 * -300 / (200 * 100) = -300/20000 = -0.015 = -150 bps

    @test summary_sell.classical_slippage[1] ≈ -150.0 atol=0.1
end
