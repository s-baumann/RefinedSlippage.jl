using VegaLite, Statistics

"""
    print_slippage_summary(exec_data::ExecutionData; unit::Symbol=:bps)

Print a formatted summary of slippage statistics across all executions.

# Arguments
- `exec_data`: ExecutionData with slippage already calculated
- `unit`: One of `:bps` (basis points), `:pct` (percentage), or `:usd` (dollar value)

# Output
Prints mean and standard deviation of classical slippage (and refined slippage if peers available).
"""
function print_slippage_summary(exec_data::ExecutionData; unit::Symbol=:bps)
    if ismissing(exec_data.summary)
        error("Slippage not yet calculated. Run calculate_slippage!(exec_data) first.")
    end

    summary_df = exec_data.summary[unit]
    has_refined = "refined_slippage" in names(summary_df)

    # Get unit label
    unit_label = if unit == :bps
        "bps"
    elseif unit == :pct
        "%"
    elseif unit == :usd
        "USD"
    else
        string(unit)
    end

    println("="^80)
    println("SLIPPAGE SUMMARY ($(summary_df |> nrow) executions)")
    println("="^80)

    # Classical slippage stats
    class_mean = mean(summary_df.classical_slippage)
    class_std = std(summary_df.classical_slippage)
    class_var = var(summary_df.classical_slippage)

    println("\nClassical Slippage:")
    println("  Mean:              $(round(class_mean, digits=2)) $unit_label")
    println("  Std Dev:           $(round(class_std, digits=2)) $unit_label")
    println("  Variance:          $(round(class_var, digits=2)) $(unit_label)^2")

    # Refined slippage stats (if available)
    if has_refined
        ref_mean = mean(summary_df.refined_slippage)
        ref_std = std(summary_df.refined_slippage)
        ref_var = var(summary_df.refined_slippage)

        println("\nRefined Slippage:")
        println("  Mean:              $(round(ref_mean, digits=2)) $unit_label")
        println("  Std Dev:           $(round(ref_std, digits=2)) $unit_label")
        println("  Variance:          $(round(ref_var, digits=2)) $(unit_label)^2")

        # Comparison
        mean_diff = ref_mean - class_mean
        std_reduction = (1 - ref_std / class_std) * 100

        println("\nComparison:")
        println("  Mean difference:   $(round(mean_diff, digits=2)) $unit_label")
        println("  Std Dev reduction: $(round(std_reduction, digits=1))%")
    end

    # Spread crossing
    avg_spread_cross = mean(summary_df.spread_cross_pct) * 100
    println("\nSpread Crossing:     $(round(avg_spread_cross, digits=1))%")
    println("="^80)
end


"""
    plot_execution_markout(exec_data::ExecutionData, execution_name::String;
                          window_before::Real=0, window_after::Real=0)

Create a two-panel VegaLite visualization for a single execution:
- Top panel: Price markout showing bid/ask lines and fill prices
- Bottom panel: Cumulative classical and refined slippage over time

# Arguments
- `exec_data`: ExecutionData with slippage already calculated
- `execution_name`: Name of the execution to visualize
- `window_before`: Time to show before first fill (default: 0)
- `window_after`: Time to show after last fill (default: 0)

# Returns
VegaLite plot specification
"""
function plot_execution_markout(exec_data::ExecutionData, execution_name::String;
                                window_before::Real=0, window_after::Real=0)
    if ismissing(exec_data.fill_returns)
        error("Slippage not yet calculated. Run calculate_slippage!(exec_data) first.")
    end

    # Get execution data
    fills = exec_data.fills[exec_data.fills.execution_name .== execution_name, :]
    if nrow(fills) == 0
        error("Execution '$execution_name' not found in data")
    end

    metadata = exec_data.metadata[exec_data.metadata.execution_name .== execution_name, :]
    asset = fills.asset[1]
    side = metadata.side[1]
    arrival_price = metadata.arrival_price[1]

    # Get time range
    first_fill_time = minimum(fills.time)
    last_fill_time = maximum(fills.time)
    time_start = first_fill_time - window_before
    time_end = last_fill_time + window_after

    # Get bid/ask data for the time window
    tob_exec = exec_data.tob[
        (exec_data.tob.symbol .== asset) .&
        (exec_data.tob.time .>= time_start) .&
        (exec_data.tob.time .<= time_end),
        :]

    # Prepare price markout data
    bid_data = DataFrame(
        time = tob_exec.time,
        price = tob_exec.bid_price,
        series = fill("Bid", nrow(tob_exec))
    )

    ask_data = DataFrame(
        time = tob_exec.time,
        price = tob_exec.ask_price,
        series = fill("Ask", nrow(tob_exec))
    )

    arrival_data = DataFrame(
        time = tob_exec.time,
        price = fill(arrival_price, nrow(tob_exec)),
        series = fill("Arrival Price", nrow(tob_exec))
    )

    fill_dots = DataFrame(
        time = fills.time,
        price = fills.price,
        quantity = fills.quantity
    )

    # Calculate cumulative slippage over time
    sorted_fills = sort(fills, :time)
    cum_qty = cumsum(sorted_fills.quantity)
    total_qty = sum(sorted_fills.quantity)
    side_sign = side == "buy" ? -1 : 1

    # Classical slippage cumulative
    cum_classical_cost = cumsum((sorted_fills.price .- arrival_price) .* sorted_fills.quantity)
    cum_classical_slippage = side_sign .* cum_classical_cost ./ (cum_qty .* arrival_price) .* 10000

    has_refined = "counterfactual_price" in names(exec_data.fill_returns)

    if has_refined
        # Get counterfactual prices
        fill_returns_exec = exec_data.fill_returns[
            exec_data.fill_returns.execution_name .== execution_name, :]
        fill_returns_sorted = sort(fill_returns_exec, :time)

        # Create counterfactual price line data
        counterfactual_data = DataFrame(
            time = fill_returns_sorted.time,
            price = fill_returns_sorted.counterfactual_price,
            series = fill("Counterfactual Price", nrow(fill_returns_sorted))
        )

        price_lines = vcat(bid_data, ask_data, arrival_data, counterfactual_data)

        # Refined slippage cumulative
        cum_refined_cost = cumsum((fill_returns_sorted.price .- fill_returns_sorted.counterfactual_price) .*
                                   fill_returns_sorted.quantity)
        cum_refined_slippage = side_sign .* cum_refined_cost ./ (cum_qty .* arrival_price) .* 10000

        slippage_data = vcat(
            DataFrame(
                time = sorted_fills.time,
                slippage = cum_classical_slippage,
                type = fill("Classical", length(cum_classical_slippage))
            ),
            DataFrame(
                time = sorted_fills.time,
                slippage = cum_refined_slippage,
                type = fill("Refined", length(cum_refined_slippage))
            )
        )
    else
        price_lines = vcat(bid_data, ask_data, arrival_data)

        slippage_data = DataFrame(
            time = sorted_fills.time,
            slippage = cum_classical_slippage,
            type = fill("Classical", length(cum_classical_slippage))
        )
    end

    # Get summary statistics for this execution
    summary_df = exec_data.summary[:bps]
    exec_summary = summary_df[summary_df.execution_name .== execution_name, :]

    final_classical = cum_classical_slippage[end]
    final_refined = has_refined ? cum_refined_slippage[end] : missing
    spread_cross_pct = nrow(exec_summary) > 0 ? exec_summary[1, :spread_cross_pct] * 100 : missing

    # Create top panel: Price markout
    price_domains = has_refined ?
        ["Bid", "Ask", "Arrival Price", "Counterfactual Price"] :
        ["Bid", "Ask", "Arrival Price"]
    price_colors = has_refined ?
        ["#006400", "#d62728", "#1f77b4", "#ff7f0e"] :
        ["#006400", "#d62728", "#1f77b4"]
    price_dashes = has_refined ?
        [[0], [0], [5, 5], [2, 2]] :
        [[0], [0], [5, 5]]

    price_chart = @vlplot(
        width=600,
        height=250,
        title="Execution Markout: $execution_name ($(side) $(asset))"
    ) +
    @vlplot(
        data=price_lines,
        mark={:line, strokeWidth=1.5},
        encoding={
            x={field=:time, type=:quantitative, title="Time"},
            y={field=:price, type=:quantitative, title="Price",
               scale={zero=false}},
            color={field=:series, type=:nominal, legend=nothing,
                  scale={
                      domain=price_domains,
                      range=price_colors
                  }},
            strokeDash={field=:series, type=:nominal,
                       scale={
                           domain=price_domains,
                           range=price_dashes
                       }}
        }
    ) +
    @vlplot(
        data=fill_dots,
        mark={:point, filled=true, color="#000000"},
        encoding={
            x={field=:time, type=:quantitative},
            y={field=:price, type=:quantitative},
            size={field=:quantity, type=:quantitative, scale={range=[50, 400]}, legend=nothing},
            tooltip=[
                {field=:time, type=:quantitative},
                {field=:price, type=:quantitative},
                {field=:quantity, type=:quantitative}
            ]
        }
    )

    # Create text for summary statistics (to be placed below the chart)
    stats_text_combined = if has_refined && !ismissing(spread_cross_pct)
        "Classical: $(round(final_classical, digits=1)) bps  |  Refined: $(round(final_refined, digits=1)) bps  |  Spread Cross: $(round(spread_cross_pct, digits=1))%"
    elseif !ismissing(spread_cross_pct)
        "Classical: $(round(final_classical, digits=1)) bps  |  Spread Cross: $(round(spread_cross_pct, digits=1))%"
    else
        "Classical: $(round(final_classical, digits=1)) bps"
    end

    # Create bottom panel: Cumulative slippage with legend
    slippage_chart = @vlplot(
        width=600,
        height=200,
        title={text=stats_text_combined, fontSize=11, anchor=:start, dx=10},
        data=slippage_data,
        mark={:line, strokeWidth=2, point=true},
        encoding={
            x={field=:time, type=:quantitative, title="Time"},
            y={field=:slippage, type=:quantitative, title="Cumulative Slippage (bps)"},
            color={
                field=:type,
                type=:nominal,
                title="Slippage Type",
                scale={
                    domain=has_refined ? ["Classical", "Refined"] : ["Classical"],
                    range=has_refined ? ["#8B4513", "#000000"] : ["#8B4513"]
                },
                legend={
                    orient="right",
                    titleFontSize=12,
                    labelFontSize=11
                }
            },
            tooltip=[
                {field=:time, type=:quantitative},
                {field=:slippage, type=:quantitative, title="Slippage (bps)"},
                {field=:type, type=:nominal}
            ]
        }
    )

    # Combine panels vertically
    return [price_chart; slippage_chart]
end