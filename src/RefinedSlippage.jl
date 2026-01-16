module RefinedSlippage

    using LinearAlgebra, HighFrequencyCovariance, DataFrames, Dates, TimeZones, VegaLite, Statistics

    include("structs.jl")
    export ExecutionData
    include("calcs.jl")
    export add_slippage!, get_slippage!
    include("present_metrics.jl")
    export print_slippage_summary, plot_execution_markout


end # module RefinedSlippage
