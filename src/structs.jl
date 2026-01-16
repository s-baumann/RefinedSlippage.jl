const namemap = Dict{Symbol,Symbol}(
    :time => :time,
    :quantity => :quantity,
    :price => :price,
    :execution_name => :execution_name,
    :asset => :asset,
    :side => :side,
    :desired_quantity => :desired_quantity,
    :arrival_price => :arrival_price,
    :symbol => :symbol,
    :bid_price => :bid_price,
    :ask_price => :ask_price,
    :time_from => :time_from,
    :time_to => :time_to,
    :volume => :volume,
    :peer => :peer,
    :weight => :weight
)

function validate_column_existance(df::DataFrame, cols::Vector{Symbol}, df_name::String, namemap::Dict{Symbol,Symbol}=namemap)
    for col in cols
        col2 = namemap[col]
        if !(col2 in Symbol.(names(df)))
            error("$df_name must have a column representing $(col) which should have the name $(col2). If it has a different name change namemap.")
        end
    end
end


"""
    ExecutionData

    Container for all data needed for execution analysis.

    # Fields
    - `fills::DataFrame`: Fill data with columns `:time`, `:quantity`, `:price`, `:execution_name`, `:asset`
    - `metadata::DataFrame`: Execution metadata with columns `:execution_name`, `:side`, `:desired_quantity`, (optional: `:arrival_price`)
    - `tob::DataFrame`: Top-of-book prices with columns `:time`, `:symbol`, `:bid_price`, `:ask_price`
    - `volume::Union{Missing,DataFrame}`: Optional market volume data with columns `:time_from`, `:time_to`, `:symbol`, `:volume` for vs_vwap calculation. Market VWAP is estimated using TOB mid-prices weighted by volume.
    - `peers::Union{Missing,DataFrame}`: Optional peer weights with columns `:execution_name`, `:peer`, `:weight`
    - `vols::Union{Missing,DataFrame}`: Optional volatilities with columns `:asset`, `:volatility` (hourly vol)
    - `peer_return_truncation::Float64`: Truncation threshold for peer returns (in multiples of volatility). Default 2.0, use Inf to disable.
    - `fill_returns::Union{Missing,DataFrame}`: Computed by `calculate_slippage!`, contains fill-level data with counterfactual prices
    - `summary::Union{Missing,Dict{Symbol,DataFrame}}`: Computed by `calculate_slippage!`, contains execution-level slippage summary

    # Constructors
    ```julia
    # Without peers (classical slippage only)
    data = ExecutionData(fills_df, metadata_df, bidask_df; volume=volume_df)

    # With user-provided peers (optional vols for truncation)
    data = ExecutionData(fills_df, metadata_df, bidask_df, peers_df; volume=volume_df, vols=vols_df, peer_return_truncation=2.0)

    # With automatic peer calculation from covariance matrix
    data = ExecutionData(fills_df, metadata_df, bidask_df, covar; volume=volume_df, num_peers=4, peer_return_truncation=2.0)
    ```
"""
mutable struct ExecutionData
    fills::DataFrame
    metadata::DataFrame
    tob::DataFrame
    volume::Union{Missing,DataFrame}
    peers::Union{Missing,DataFrame}
    vols::Union{Missing,DataFrame}
    peer_return_truncation::Float64
    fill_returns::Union{Missing,DataFrame}
    summary::Union{Missing,Dict{Symbol,DataFrame}}
end

# Constructor without peers (classical slippage only)
function ExecutionData(fills::DataFrame, metadata::DataFrame, tob::DataFrame;
                        volume::Union{Missing,DataFrame}=missing,
                        namemap::Dict{Symbol,Symbol}=namemap)
    # Validate fills columns
    validate_column_existance(fills, [:time, :quantity, :price, :execution_name, :asset], "fills", namemap)
    # Validate metadata columns
    validate_column_existance(metadata, [:execution_name, :side, :desired_quantity], "metadata", namemap)
    # Validate tob columns
    validate_column_existance(tob, [:time, :symbol, :bid_price, :ask_price], "tob", namemap)
    # Validate volume if provided
    if !ismissing(volume)
        validate_column_existance(volume, [:time_from, :time_to, :symbol, :volume], "volume", namemap)
    end
    ExecutionData(fills, metadata, tob, volume, missing, missing, Inf, missing, missing)
end

# Constructor with user-provided peers DataFrame
function ExecutionData(fills::DataFrame, metadata::DataFrame, tob::DataFrame, peers::DataFrame;
                        volume::Union{Missing,DataFrame}=missing,
                        vols::Union{Missing,DataFrame}=missing,
                        peer_return_truncation::Float64=2.0,
                        namemap::Dict{Symbol,Symbol}=namemap)
    # Validate fills columns
    validate_column_existance(fills, [:time, :quantity, :price, :execution_name, :asset], "fills", namemap)
    # Validate metadata columns
    validate_column_existance(metadata, [:execution_name, :side, :desired_quantity], "metadata", namemap)
    # Validate tob columns
    validate_column_existance(tob, [:time, :symbol, :bid_price, :ask_price], "tob", namemap)
    # Validate peers
    validate_column_existance(peers, [:execution_name, :peer, :weight], "peers", namemap)
    # Validate volume if provided
    if !ismissing(volume)
        validate_column_existance(volume, [:time_from, :time_to, :symbol, :volume], "volume", namemap)
    end
    # If vols not provided, truncation won't occur (set to Inf internally)
    effective_truncation = ismissing(vols) ? Inf : peer_return_truncation
    ExecutionData(fills, metadata, tob, volume, peers, vols, effective_truncation, missing, missing)
end

# Constructor with covariance matrix for automatic peer weight calculation
function ExecutionData(fills::DataFrame, metadata::DataFrame, tob::DataFrame, covar::HighFrequencyCovariance.CovarianceMatrix;
                        volume::Union{Missing,DataFrame}=missing,
                        num_peers::Union{Int,Nothing}=nothing,
                        peer_return_truncation::Float64=2.0,
                        namemap::Dict{Symbol,Symbol}=namemap)
    # Validate fills columns
    validate_column_existance(fills, [:time, :quantity, :price, :execution_name, :asset], "fills", namemap)
    # Validate metadata columns
    validate_column_existance(metadata, [:execution_name, :side, :desired_quantity], "metadata", namemap)
    # Validate tob columns
    validate_column_existance(tob, [:time, :symbol, :bid_price, :ask_price], "tob", namemap)
    # Validate volume if provided
    if !ismissing(volume)
        validate_column_existance(volume, [:time_from, :time_to, :symbol, :volume], "volume", namemap)
    end

    # Extract covariance matrix and correlation matrix
    covar_matrix = HighFrequencyCovariance.covariance(covar, Dates.Hour(1))
    covariance_labels = covar.labels

    # Extract volatilities (sqrt of diagonal of covariance matrix)
    volatilities = sqrt.(diag(covar_matrix))
    vols_df = DataFrame(asset = covariance_labels, volatility = volatilities)

    # Calculate correlation matrix for peer selection
    vol_diag = Diagonal(volatilities)
    vol_diag_inv = Diagonal(1.0 ./ volatilities)
    corr_matrix = vol_diag_inv * covar_matrix * vol_diag_inv

    # Function to get peer weights
    function get_peer_weights(covar_matrix, covariance_labels, asset::Symbol, conditioning_assets::Vector{Symbol})
        asset_index = findfirst(asset .== covariance_labels)
        conditioning_indices = map(x -> findfirst(==(x), covariance_labels), conditioning_assets)
        sigma12 = covar_matrix[asset_index:asset_index, conditioning_indices]
        sigma22 = covar_matrix[conditioning_indices, conditioning_indices]
        weights = sigma12 / sigma22
        return weights[:]
    end

    # Function to select top N peers by correlation
    function select_top_peers(corr_matrix, covariance_labels, asset::Symbol, n_peers::Union{Int,Nothing})
        asset_index = findfirst(asset .== covariance_labels)
        other_indices = setdiff(1:length(covariance_labels), [asset_index])

        if isnothing(n_peers) || n_peers >= length(other_indices)
            # Use all peers
            return covariance_labels[other_indices]
        end

        # Get absolute correlations with other assets
        correlations = abs.(corr_matrix[asset_index, other_indices])

        # Sort by correlation (descending) and take top N
        sorted_indices = sortperm(correlations, rev=true)
        top_indices = other_indices[sorted_indices[1:n_peers]]

        return covariance_labels[top_indices]
    end

    assets = covariance_labels
    execution_assets = unique(fills[!, [:execution_name, :asset]])
    peer_rows = []

    for row in eachrow(execution_assets)
        exec_name = row.execution_name
        traded_asset = row.asset

        # Select peers (top N by correlation if num_peers specified)
        peer_assets = select_top_peers(corr_matrix, covariance_labels, traded_asset, num_peers)

        if length(peer_assets) > 0
            weights = get_peer_weights(covar_matrix, covariance_labels, traded_asset, peer_assets)
            for (i, peer) in enumerate(peer_assets)
                push!(peer_rows, (
                    execution_name = exec_name,
                    peer = peer,
                    weight = weights[i]
                ))
            end
        end
    end

    peers_df = DataFrame(peer_rows)
    ExecutionData(fills, metadata, tob, volume, peers_df, vols_df, peer_return_truncation, missing, missing)
end
