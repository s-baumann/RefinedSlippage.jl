using StableRNGs, Distributions, Dates, Statistics

# =============================================================================
# Regression Test for RefinedSlippage.jl
#
# This test pins specific numerical outputs to guard against unintended
# changes during refactoring or optimization. Unlike the other test files
# which verify against manual recalculations, these assert hardcoded values.
# Any numerical drift should cause a failure.
# =============================================================================

# =============================================================================
# Deterministic synthetic data generation
# =============================================================================
const REG_NUM_EXECUTIONS = 3
const REG_FILLS_PER_EXECUTION = 5
const REG_TICKS_BETWEEN_EXECUTIONS = 200
const REG_DIMS = 4

reg_ticks = (REG_NUM_EXECUTIONS + 2) * REG_TICKS_BETWEEN_EXECUTIONS * REG_DIMS
reg_brownian_corr_matrix = Hermitian(0.5 .+ 0.5 * I(REG_DIMS))
reg_rng = StableRNG(12345)

reg_ts, reg_true_covar, _, _ = HighFrequencyCovariance.generate_random_path(
    REG_DIMS, reg_ticks; syncronous=true, brownian_corr_matrix=reg_brownian_corr_matrix,
    vol_dist=Distributions.Uniform(0.0005, 0.001),
    micro_noise_dist=Distributions.Uniform(0, 0.0000000001),
)
reg_assets = reg_true_covar.labels

# Build bidask DataFrame
reg_bidask = copy(reg_ts.df)
reg_bidask[!, :Value] .= exp.(reg_bidask.Value)
rename!(reg_bidask, Dict(:Time => :time, :Name => :symbol, :Value => :bid_price))
reg_bidask[:, :ask_price] = reg_bidask[:, :bid_price] .* (1.0025 .+ (0.0025 .* rand(reg_rng, size(reg_bidask, 1))))

# Build volume data
reg_volume_times = unique(reg_bidask.time[1:50:end])
reg_n_intervals = length(reg_volume_times) - 1
reg_volume_dfs = DataFrame[]
for asset in reg_assets
    push!(reg_volume_dfs, DataFrame(
        time_from=reg_volume_times[1:end-1],
        time_to=reg_volume_times[2:end],
        symbol=fill(asset, reg_n_intervals),
        volume=rand(reg_rng, 50000:200000, reg_n_intervals)
    ))
end
reg_volume_df = reduce(vcat, reg_volume_dfs)

# Generate executions
function generate_regression_executions(bidask, assets, num_executions, fills_per_exec, ticks_between, rng)
    allfills = DataFrame[]
    metadata = DataFrame[]
    unique_times = sort(unique(bidask.time))

    for exec_idx in 1:num_executions
        asset = assets[mod1(exec_idx, length(assets))]
        start_tick = 1 + (exec_idx - 1) * ticks_between
        end_tick = min(start_tick + ticks_between - 1, length(unique_times))

        if end_tick <= start_tick + fills_per_exec
            @warn "Not enough ticks for execution $exec_idx, skipping"
            continue
        end

        exec_times = unique_times[start_tick:end_tick]
        subframe = bidask[(bidask.symbol .== asset) .& (bidask.time .>= exec_times[1]) .& (bidask.time .<= exec_times[end]), :]

        if nrow(subframe) < fills_per_exec
            @warn "Not enough data for execution $exec_idx, skipping"
            continue
        end

        fill_indices = round.(Int, range(1, nrow(subframe), length=fills_per_exec))
        fill_times = subframe.time[fill_indices]
        fill_prices = [subframe.bid_price[i] + rand(rng) * (subframe.ask_price[i] - subframe.bid_price[i])
                       for i in fill_indices]
        fill_quantities = rand(rng, 100:500, fills_per_exec)

        exec_name = "RegExec_$(exec_idx)_$(asset)"

        push!(allfills, DataFrame(
            time=fill_times,
            quantity=fill_quantities,
            price=fill_prices,
            execution_name=fill(exec_name, fills_per_exec),
            asset=fill(asset, fills_per_exec)
        ))

        arrival_price = (subframe.bid_price[1] + subframe.ask_price[1]) / 2
        side = exec_idx % 2 == 1 ? "buy" : "sell"  # deterministic alternating sides

        push!(metadata, DataFrame(
            execution_name=[exec_name],
            arrival_price=[arrival_price],
            side=[side],
            desired_quantity=[sum(fill_quantities)]
        ))
    end

    return reduce(vcat, allfills), reduce(vcat, metadata)
end

reg_fills, reg_metadata = generate_regression_executions(
    reg_bidask, reg_assets, REG_NUM_EXECUTIONS, REG_FILLS_PER_EXECUTION,
    REG_TICKS_BETWEEN_EXECUTIONS, reg_rng
)

# =============================================================================
# Tests
# =============================================================================
@testset "Regression Tests" begin

    # =========================================================================
    # Scenario 1: Classical slippage only (no peers, no volume)
    # =========================================================================
    @testset "Scenario 1: Classical only" begin
        ed1 = ExecutionData(reg_fills, reg_metadata, reg_bidask)
        calculate_slippage!(ed1)
        s1 = get_slippage!(ed1, :bps)

        @test nrow(s1) == REG_NUM_EXECUTIONS
        @test nrow(ed1.fill_returns) == REG_NUM_EXECUTIONS * REG_FILLS_PER_EXECUTION

        s1_sorted = sort(s1, :execution_name)
        @test s1_sorted.classical_slippage[1] ≈  74.20823611026483  atol=1e-8
        @test s1_sorted.classical_slippage[2] ≈ -234.56787992451746 atol=1e-8
        @test s1_sorted.classical_slippage[3] ≈  108.60770871663813 atol=1e-8

        @test s1_sorted.spread_cross_pct[1] ≈ 0.7242058483949932 atol=1e-8
        @test s1_sorted.spread_cross_pct[2] ≈ 0.6252315810793809 atol=1e-8
        @test s1_sorted.spread_cross_pct[3] ≈ 0.4485365324877124 atol=1e-8
    end

    # =========================================================================
    # Scenario 2: Classical + vs_vwap (no peers, with volume)
    # =========================================================================
    @testset "Scenario 2: Classical + vs_vwap" begin
        ed2 = ExecutionData(reg_fills, reg_metadata, reg_bidask; volume=reg_volume_df)
        calculate_slippage!(ed2)
        s2 = get_slippage!(ed2, :bps)

        @test nrow(s2) == REG_NUM_EXECUTIONS
        @test nrow(ed2.fill_returns) == REG_NUM_EXECUTIONS * REG_FILLS_PER_EXECUTION

        s2_sorted = sort(s2, :execution_name)

        # Classical should be identical to scenario 1
        @test s2_sorted.classical_slippage[1] ≈  74.20823611026483  atol=1e-8
        @test s2_sorted.classical_slippage[2] ≈ -234.56787992451746 atol=1e-8
        @test s2_sorted.classical_slippage[3] ≈  108.60770871663813 atol=1e-8

        # vs_vwap values
        @test s2_sorted.vs_vwap_slippage[1] ≈   8.192012327442896  atol=1e-8
        @test s2_sorted.vs_vwap_slippage[2] ≈ -14.240285092136787  atol=1e-8
        @test s2_sorted.vs_vwap_slippage[3] ≈  -1.127719356173398  atol=1e-8

        # Spread cross unchanged
        @test s2_sorted.spread_cross_pct[1] ≈ 0.7242058483949932 atol=1e-8
        @test s2_sorted.spread_cross_pct[2] ≈ 0.6252315810793809 atol=1e-8
        @test s2_sorted.spread_cross_pct[3] ≈ 0.4485365324877124 atol=1e-8
    end

    # =========================================================================
    # Scenario 3: Refined slippage via covariance auto-peer selection
    # =========================================================================
    @testset "Scenario 3: Refined (covariance auto-peers)" begin
        ed3 = ExecutionData(reg_fills, reg_metadata, reg_bidask, reg_true_covar;
                            volume=reg_volume_df, num_peers=2, peer_return_truncation=2.0)
        calculate_slippage!(ed3)
        s3 = get_slippage!(ed3, :bps)

        @test nrow(s3) == REG_NUM_EXECUTIONS
        @test nrow(ed3.fill_returns) == REG_NUM_EXECUTIONS * REG_FILLS_PER_EXECUTION

        s3_sorted = sort(s3, :execution_name)

        # Classical (same as above)
        @test s3_sorted.classical_slippage[1] ≈  74.20823611026483  atol=1e-8
        @test s3_sorted.classical_slippage[2] ≈ -234.56787992451746 atol=1e-8
        @test s3_sorted.classical_slippage[3] ≈  108.60770871663813 atol=1e-8

        # Refined slippage
        @test s3_sorted.refined_slippage[1] ≈  55.767844688274636  atol=1e-8
        @test s3_sorted.refined_slippage[2] ≈ -159.53135337009036  atol=1e-8
        @test s3_sorted.refined_slippage[3] ≈  35.17679787088012   atol=1e-8

        # vs_vwap (same as scenario 2)
        @test s3_sorted.vs_vwap_slippage[1] ≈   8.192012327442896  atol=1e-8
        @test s3_sorted.vs_vwap_slippage[2] ≈ -14.240285092136787  atol=1e-8
        @test s3_sorted.vs_vwap_slippage[3] ≈  -1.127719356173398  atol=1e-8

        # Spread cross
        @test s3_sorted.spread_cross_pct[1] ≈ 0.7242058483949932 atol=1e-8
        @test s3_sorted.spread_cross_pct[2] ≈ 0.6252315810793809 atol=1e-8
        @test s3_sorted.spread_cross_pct[3] ≈ 0.4485365324877124 atol=1e-8

        # Spot-check fill_returns intermediate values
        fr3 = sort(ed3.fill_returns, [:execution_name, :time])
        @test fr3.counterfactual_price[1] ≈ 1.0047076786569864 atol=1e-8
        @test fr3.counterfactual_price[3] ≈ 1.000831112809567  atol=1e-8

        # Spot-check peer prices in fill_returns
        @test fr3[1, :asset_3] ≈ 1.0029120148976016 atol=1e-8
        @test fr3[1, :asset_4] ≈ 1.0056008408615158 atol=1e-8
        @test fr3[3, :asset_2] ≈ 0.9914262061051844 atol=1e-8
        @test fr3[3, :asset_4] ≈ 1.0141012759668986 atol=1e-8

        # Check last row (execution 3)
        @test fr3[end, :counterfactual_price] ≈ 0.9781941218156145 atol=1e-8

        # Verify auto-selected peer weights
        peers3 = sort(ed3.peers, [:execution_name, :peer])
        exec1_peers = filter(r -> r.execution_name == "RegExec_1_asset_1", peers3)
        @test nrow(exec1_peers) == 2
    end

    # =========================================================================
    # Scenario 4: Refined slippage via user-defined peers
    # =========================================================================
    @testset "Scenario 4: Refined (user-defined peers)" begin
        exec_names = unique(reg_fills.execution_name)
        exec_assets = [unique(reg_fills[reg_fills.execution_name .== en, :asset])[1] for en in exec_names]
        peer_rows = []
        for (en, ea) in zip(exec_names, exec_assets)
            available_peers = setdiff(reg_assets, [ea])
            for (i, p) in enumerate(available_peers[1:min(2, length(available_peers))])
                push!(peer_rows, (execution_name=en, peer=p, weight=i == 1 ? 0.6 : 0.4))
            end
        end
        reg_user_peers = DataFrame(peer_rows)

        ed4 = ExecutionData(reg_fills, reg_metadata, reg_bidask, reg_user_peers; volume=reg_volume_df)
        calculate_slippage!(ed4)
        s4 = get_slippage!(ed4, :bps)

        @test nrow(s4) == REG_NUM_EXECUTIONS
        @test nrow(ed4.fill_returns) == REG_NUM_EXECUTIONS * REG_FILLS_PER_EXECUTION

        s4_sorted = sort(s4, :execution_name)

        # Classical (same as all scenarios)
        @test s4_sorted.classical_slippage[1] ≈  74.20823611026483  atol=1e-8
        @test s4_sorted.classical_slippage[2] ≈ -234.56787992451746 atol=1e-8
        @test s4_sorted.classical_slippage[3] ≈  108.60770871663813 atol=1e-8

        # Refined slippage (different from scenario 3 due to different peer weights)
        @test s4_sorted.refined_slippage[1] ≈  23.93076888880192  atol=1e-8
        @test s4_sorted.refined_slippage[2] ≈ -228.8554296744592  atol=1e-8
        @test s4_sorted.refined_slippage[3] ≈   9.964721575328648 atol=1e-8

        # vs_vwap (same as scenario 2 & 3)
        @test s4_sorted.vs_vwap_slippage[1] ≈   8.192012327442896  atol=1e-8
        @test s4_sorted.vs_vwap_slippage[2] ≈ -14.240285092136787  atol=1e-8
        @test s4_sorted.vs_vwap_slippage[3] ≈  -1.127719356173398  atol=1e-8

        # Spread cross
        @test s4_sorted.spread_cross_pct[1] ≈ 0.7242058483949932 atol=1e-8
        @test s4_sorted.spread_cross_pct[2] ≈ 0.6252315810793809 atol=1e-8
        @test s4_sorted.spread_cross_pct[3] ≈ 0.4485365324877124 atol=1e-8

        # Spot-check counterfactual prices (different from scenario 3)
        fr4 = sort(ed4.fill_returns, [:execution_name, :time])
        @test fr4.counterfactual_price[1] ≈ 1.0047076786569864 atol=1e-8
        @test fr4.counterfactual_price[3] ≈ 0.9946776157662661 atol=1e-8
        @test fr4[end, :counterfactual_price] ≈ 0.9721081938671712 atol=1e-8
    end

    # =========================================================================
    # Scenario 5: Unit conversions (pct, usd) for classical-only
    # =========================================================================
    @testset "Scenario 5: Unit conversions" begin
        ed = ExecutionData(reg_fills, reg_metadata, reg_bidask)
        calculate_slippage!(ed)

        s_pct = sort(get_slippage!(ed, :pct), :execution_name)
        @test s_pct.classical_slippage[1] ≈  0.7420823611026482  atol=1e-8
        @test s_pct.classical_slippage[2] ≈ -2.3456787992451744  atol=1e-8
        @test s_pct.classical_slippage[3] ≈  1.0860770871663812  atol=1e-8

        s_usd = sort(get_slippage!(ed, :usd), :execution_name)
        @test s_usd.classical_slippage[1] ≈  10.922686149697551  atol=1e-8
        @test s_usd.classical_slippage[2] ≈ -30.629082355005053  atol=1e-8
        @test s_usd.classical_slippage[3] ≈   7.872459043402604  atol=1e-8
    end

    # =========================================================================
    # Scenario 6: Unit conversions (pct, usd) for full refined + vs_vwap
    # =========================================================================
    @testset "Scenario 6: Unit conversions (refined)" begin
        ed = ExecutionData(reg_fills, reg_metadata, reg_bidask, reg_true_covar;
                           volume=reg_volume_df, num_peers=2, peer_return_truncation=2.0)
        calculate_slippage!(ed)

        s_pct = sort(get_slippage!(ed, :pct), :execution_name)
        @test s_pct.classical_slippage[1] ≈  0.7420823611026482   atol=1e-8
        @test s_pct.refined_slippage[1]   ≈  0.5576784468827464   atol=1e-8
        @test s_pct.vs_vwap_slippage[1]   ≈  0.08192012327442896  atol=1e-8
        @test s_pct.refined_slippage[2]   ≈ -1.5953135337009035   atol=1e-8
        @test s_pct.vs_vwap_slippage[3]   ≈ -0.011277193561733982 atol=1e-8

        s_usd = sort(get_slippage!(ed, :usd), :execution_name)
        @test s_usd.classical_slippage[1] ≈  10.922686149697551   atol=1e-8
        @test s_usd.refined_slippage[1]   ≈   8.208450930837355   atol=1e-8
        @test s_usd.vs_vwap_slippage[1]   ≈   1.2057796314435643  atol=1e-8
        @test s_usd.refined_slippage[2]   ≈ -20.831065882295103   atol=1e-8
        @test s_usd.vs_vwap_slippage[3]   ≈  -0.08174304152838996 atol=1e-8
    end

    # =========================================================================
    # Scenario 7: Fill-level intermediate values (classical path)
    # =========================================================================
    @testset "Scenario 7: Fill-level intermediates" begin
        ed = ExecutionData(reg_fills, reg_metadata, reg_bidask; volume=reg_volume_df)
        calculate_slippage!(ed)
        fr = sort(ed.fill_returns, [:execution_name, :time])

        # Per-fill spread crossing
        @test fr.spread_cross[1]  ≈ 0.6784930448753796  atol=1e-8
        @test fr.spread_cross[3]  ≈ 0.949102730873323   atol=1e-8
        @test fr.spread_cross[5]  ≈ 0.4631389267271433  atol=1e-8
        @test fr.spread_cross[8]  ≈ 0.7939382831477751  atol=1e-8
        @test fr.spread_cross[11] ≈ 0.28613268356948884 atol=1e-8
        @test fr.spread_cross[15] ≈ 0.18642679992993194 atol=1e-8

        # Per-fill market VWAP
        @test fr.market_vwap[1]  ≈ 1.0100263246587815  atol=1e-8
        @test fr.market_vwap[5]  ≈ 0.9980749779619325  atol=1e-8
        @test fr.market_vwap[6]  ≈ 0.9865776841311816  atol=1e-8
        @test fr.market_vwap[10] ≈ 0.9696253812881216  atol=1e-8
        @test fr.market_vwap[15] ≈ 0.9847506780043614  atol=1e-8

        # Per-fill prices
        @test fr.price[1]  ≈ 1.0041204349280801  atol=1e-8
        @test fr.price[5]  ≈ 0.9822837828304508  atol=1e-8
        @test fr.price[11] ≈ 0.9965328999791389  atol=1e-8
        @test fr.price[15] ≈ 0.9712463449018847  atol=1e-8

        # Summary fill_vwap and market_vwap
        s = sort(get_slippage!(ed, :bps), :execution_name)
        @test s.fill_vwap[1]   ≈ 0.997251920193029   atol=1e-8
        @test s.fill_vwap[2]   ≈ 0.9682134994489284  atol=1e-8
        @test s.fill_vwap[3]   ≈ 0.9848629624020653  atol=1e-8
        @test s.market_vwap[1] ≈ 0.9980749779619325  atol=1e-8
        @test s.market_vwap[2] ≈ 0.9696253812881216  atol=1e-8
        @test s.market_vwap[3] ≈ 0.9847506780043614  atol=1e-8
    end

    # =========================================================================
    # Scenario 8: get_slippage! auto-triggers calculate_slippage!
    # =========================================================================
    @testset "Scenario 8: Auto-calculation via get_slippage!" begin
        ed = ExecutionData(reg_fills, reg_metadata, reg_bidask)
        @test ismissing(ed.summary_bps)

        s = sort(get_slippage!(ed, :bps), :execution_name)
        @test !ismissing(ed.summary_bps)
        @test s.classical_slippage[1] ≈  74.20823611026483  atol=1e-8
        @test s.classical_slippage[2] ≈ -234.56787992451746 atol=1e-8
        @test s.classical_slippage[3] ≈  108.60770871663813 atol=1e-8
    end

    # =========================================================================
    # Scenario 9: Refined with truncation disabled (peer_return_truncation=Inf)
    # =========================================================================
    @testset "Scenario 9: Refined (truncation disabled)" begin
        ed = ExecutionData(reg_fills, reg_metadata, reg_bidask, reg_true_covar;
                           volume=reg_volume_df, num_peers=2, peer_return_truncation=Inf)
        calculate_slippage!(ed)
        s = sort(get_slippage!(ed, :bps), :execution_name)

        @test s.classical_slippage[1] ≈  74.20823611026483  atol=1e-8
        @test s.classical_slippage[2] ≈ -234.56787992451746 atol=1e-8
        @test s.classical_slippage[3] ≈  108.60770871663813 atol=1e-8

        @test s.refined_slippage[1] ≈  55.767844688274636  atol=1e-8
        @test s.refined_slippage[2] ≈ -159.53135337009036  atol=1e-8
        @test s.refined_slippage[3] ≈  35.17679787088012   atol=1e-8

        @test s.vs_vwap_slippage[1] ≈   8.192012327442896  atol=1e-8
        @test s.vs_vwap_slippage[2] ≈ -14.240285092136787  atol=1e-8
        @test s.vs_vwap_slippage[3] ≈  -1.127719356173398  atol=1e-8

        fr = sort(ed.fill_returns, [:execution_name, :time])
        @test fr.counterfactual_price[1]   ≈ 1.0047076786569864 atol=1e-8
        @test fr.counterfactual_price[3]   ≈ 1.000831112809567  atol=1e-8
        @test fr.counterfactual_price[end] ≈ 0.9781941218156145 atol=1e-8
    end

    # =========================================================================
    # Scenario 10: Time-scaled truncation (tight bounds that actually bite)
    # =========================================================================
    @testset "Scenario 10: Time-scaled truncation" begin
        # Small time_to_hours + tight threshold makes duration_hours small,
        # so truncation bounds = threshold * vol * sqrt(duration_hours) are tight
        ed = ExecutionData(reg_fills, reg_metadata, reg_bidask, reg_true_covar;
                           volume=reg_volume_df, num_peers=2,
                           peer_return_truncation=0.1, time_to_hours=0.0001)
        calculate_slippage!(ed)
        s = sort(get_slippage!(ed, :bps), :execution_name)

        # Classical and vs_vwap unaffected by peer return truncation
        @test s.classical_slippage[1] ≈  74.20823611026483  atol=1e-8
        @test s.classical_slippage[2] ≈ -234.56787992451746 atol=1e-8
        @test s.classical_slippage[3] ≈  108.60770871663813 atol=1e-8
        @test s.vs_vwap_slippage[1]   ≈   8.192012327442896 atol=1e-8

        # Refined slippage differs from scenario 9 (Inf) due to truncation
        @test s.refined_slippage[1] ≈  72.66485228643131  atol=1e-8
        @test s.refined_slippage[2] ≈ -231.06740920084363 atol=1e-8
        @test s.refined_slippage[3] ≈  104.19060184179426 atol=1e-8

        # Verify truncation moved refined closer to classical (less peer adjustment)
        s_inf = sort(get_slippage!(ExecutionData(reg_fills, reg_metadata, reg_bidask, reg_true_covar;
                     volume=reg_volume_df, num_peers=2, peer_return_truncation=Inf), :bps), :execution_name)
        for i in 1:3
            @test abs(s.refined_slippage[i] - s.classical_slippage[i]) <
                  abs(s_inf.refined_slippage[i] - s_inf.classical_slippage[i])
        end

        # First fill counterfactual unaffected (duration=0, return=0, no truncation)
        fr = sort(ed.fill_returns, [:execution_name, :time])
        @test fr.counterfactual_price[1] ≈ 1.0047076786569864 atol=1e-8
        # Last fill counterfactual differs due to truncation
        @test fr.counterfactual_price[end] ≈ 0.9949320368653808 atol=1e-8
    end
end
