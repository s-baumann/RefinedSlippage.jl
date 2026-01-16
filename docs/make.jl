using Documenter, RefinedSlippage

makedocs(
    format = Documenter.HTML(),
    sitename = "RefinedSlippage",
    modules = [RefinedSlippage],
    pages = Any["Overview" => "index.md",
                "Examples" => "Examples.md",
                "API" => "api.md"]
)

deploydocs(
    repo   = "github.com/s-baumann/RefinedSlippage.jl.git",
    devbranch = "main",
    target = "build",
    deps   = nothing,
    make   = nothing
)
