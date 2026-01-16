using Test
using RefinedSlippage
using DataFrames
using LinearAlgebra
using HighFrequencyCovariance

@testset "RefinedSlippage Tests" begin
    include("test_classical_slippage.jl")
    include("test_refined_slippage.jl")
    include("test_vs_vwap.jl")
end
