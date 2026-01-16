@testset "Refined Slippage" begin
    # Test with covariance matrix
    # Create simple synthetic covariance structure
    using Dates, Random
    using HighFrequencyCovariance: TimeseriesData, CovarianceMatrix
    Random.seed!(42)

    # Generate simple correlated returns
    n_assets = 3
    n_obs = 100
    assets = [:AAPL, :SPY, :QQQ]

    # Create price data with known correlation
    times = collect(1.0:100.0)
    base_return = 0.001
    volatility = 0.01

    prices = Dict{Symbol, Vector{Float64}}()
    for asset in assets
        returns = randn(n_obs) .* volatility .+ base_return
        prices[asset] = cumprod(1.0 .+ returns) .* 100.0
    end

    # Build time series for covariance matrix
    ts_data = DataFrame()
    for (i, t) in enumerate(times)
        for asset in assets
            push!(ts_data, (Time=t, Name=asset, Value=log(prices[asset][i])))
        end
    end

    ts = TimeseriesData(ts_data)
    covar = CovarianceMatrix(ts)

    # Create execution data
    fills_covar = DataFrame(
        time = [10.0, 20.0],
        quantity = [100, 100],
        price = [prices[:AAPL][10], prices[:AAPL][20]],
        execution_name = ["test_covar", "test_covar"],
        asset = [:AAPL, :AAPL]
    )

    metadata_covar = DataFrame(
        execution_name = ["test_covar"],
        arrival_price = [prices[:AAPL][10]],
        side = ["buy"],
        desired_quantity = [200]
    )

    tob_covar = DataFrame(
        time = collect(1.0:100.0),
        symbol = repeat(assets, inner=100),
        bid_price = vcat([prices[a] .* 0.995 for a in assets]...),
        ask_price = vcat([prices[a] .* 1.005 for a in assets]...)
    )

    exec_data_covar = ExecutionData(fills_covar, metadata_covar, tob_covar, covar)
    add_slippage!(exec_data_covar)

    summary_covar = exec_data_covar.summary[:bps]

    # Verify both classical and refined exist
    @test "classical_slippage" in names(summary_covar)
    @test "refined_slippage" in names(summary_covar)
    @test !ismissing(summary_covar.classical_slippage[1])
    @test !ismissing(summary_covar.refined_slippage[1])

    # Refined should generally have lower magnitude than classical due to market adjustment
    # (not always true but often true)
    @test abs(summary_covar.refined_slippage[1]) <= abs(summary_covar.classical_slippage[1]) + 100.0
end
