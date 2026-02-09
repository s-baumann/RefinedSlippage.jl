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

# Rename columns from user-provided names (via namemap) to standard internal names.
# For the default namemap (identity), returns the original DataFrame with no copy.
function standardize_columns(df::DataFrame, namemap::Dict{Symbol,Symbol}, cols::Vector{Symbol})
    renames = Pair{Symbol,Symbol}[]
    for col in cols
        mapped = namemap[col]
        if mapped != col
            push!(renames, mapped => col)
        end
    end
    isempty(renames) && return df
    return rename(df, renames...)
end


"""
    ExecutionData

    Container for all data needed for execution analysis.

    # Fields
    - `fills::DataFrame`: Fill data with columns `:time`, `:quantity`, `:price`, `:execution_name`, `:asset`
    - `metadata::DataFrame`: Execution metadata with columns `:execution_name`, `:side`, `:desired_quantity`, `:arrival_price`
    - `tob::DataFrame`: Top-of-book prices with columns `:time`, `:symbol`, `:bid_price`, `:ask_price`
    - `volume::Union{Missing,DataFrame}`: Optional market volume data with columns `:time_from`, `:time_to`, `:symbol`, `:volume` for vs_vwap calculation. Market VWAP is estimated using TOB mid-prices weighted by volume.
    - `peers::Union{Missing,DataFrame}`: Optional peer weights with columns `:execution_name`, `:peer`, `:weight`
    - `vols::Union{Missing,DataFrame}`: Optional volatilities with columns `:asset`, `:volatility` (hourly vol)
    - `peer_return_truncation::Float64`: Truncation threshold for peer returns (in multiples of volatility scaled by `sqrt(duration)`). Default 2.0, use Inf to disable.
    - `time_to_hours::Float64`: Conversion factor from the time column's units to hours. Used to scale volatility for truncation: `bound = threshold * hourly_vol * sqrt(duration_hours)`. Default 1.0 (time is in hours). Ignored for DateTime time columns (auto-converted).
    - `fill_returns::Union{Missing,DataFrame}`: Computed by `calculate_slippage!`, contains fill-level data with counterfactual prices
    - `summary_bps::Union{Missing,DataFrame}`: Computed by `calculate_slippage!`, slippage summary in basis points. `spread_cross_pct` is a proportion (0.0–1.0), not a percentage.
    - `summary_pct::Union{Missing,DataFrame}`: Computed by `calculate_slippage!`, slippage summary in percentage points
    - `summary_usd::Union{Missing,DataFrame}`: Computed by `calculate_slippage!`, slippage summary in USD

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
    time_to_hours::Float64
    fill_returns::Union{Missing,DataFrame}
    summary_bps::Union{Missing,DataFrame}
    summary_pct::Union{Missing,DataFrame}
    summary_usd::Union{Missing,DataFrame}
end

# Constructor without peers (classical slippage only)
function ExecutionData(fills::DataFrame, metadata::DataFrame, tob::DataFrame;
                        volume::Union{Missing,DataFrame}=missing,
                        namemap::Dict{Symbol,Symbol}=namemap)
    fills_cols = [:time, :quantity, :price, :execution_name, :asset]
    meta_cols = [:execution_name, :side, :desired_quantity, :arrival_price]
    tob_cols = [:time, :symbol, :bid_price, :ask_price]
    vol_cols = [:time_from, :time_to, :symbol, :volume]
    validate_column_existance(fills, fills_cols, "fills", namemap)
    validate_column_existance(metadata, meta_cols, "metadata", namemap)
    validate_column_existance(tob, tob_cols, "tob", namemap)
    if !ismissing(volume)
        validate_column_existance(volume, vol_cols, "volume", namemap)
    end
    fills_std = standardize_columns(fills, namemap, fills_cols)
    meta_std = standardize_columns(metadata, namemap, meta_cols)
    tob_std = standardize_columns(tob, namemap, tob_cols)
    vol_std = ismissing(volume) ? missing : standardize_columns(volume, namemap, vol_cols)
    ExecutionData(fills_std, meta_std, tob_std, vol_std, missing, missing, Inf, 1.0, missing, missing, missing, missing)
end

# Constructor with user-provided peers DataFrame
function ExecutionData(fills::DataFrame, metadata::DataFrame, tob::DataFrame, peers::DataFrame;
                        volume::Union{Missing,DataFrame}=missing,
                        vols::Union{Missing,DataFrame}=missing,
                        peer_return_truncation::Float64=2.0,
                        time_to_hours::Float64=1.0,
                        namemap::Dict{Symbol,Symbol}=namemap)
    fills_cols = [:time, :quantity, :price, :execution_name, :asset]
    meta_cols = [:execution_name, :side, :desired_quantity, :arrival_price]
    tob_cols = [:time, :symbol, :bid_price, :ask_price]
    peers_cols = [:execution_name, :peer, :weight]
    vol_cols = [:time_from, :time_to, :symbol, :volume]
    validate_column_existance(fills, fills_cols, "fills", namemap)
    validate_column_existance(metadata, meta_cols, "metadata", namemap)
    validate_column_existance(tob, tob_cols, "tob", namemap)
    validate_column_existance(peers, peers_cols, "peers", namemap)
    if !ismissing(volume)
        validate_column_existance(volume, vol_cols, "volume", namemap)
    end
    fills_std = standardize_columns(fills, namemap, fills_cols)
    meta_std = standardize_columns(metadata, namemap, meta_cols)
    tob_std = standardize_columns(tob, namemap, tob_cols)
    peers_std = standardize_columns(peers, namemap, peers_cols)
    vol_std = ismissing(volume) ? missing : standardize_columns(volume, namemap, vol_cols)
    vols_std = ismissing(vols) ? missing : standardize_columns(vols, namemap, [:asset])
    # If vols not provided, truncation won't occur (set to Inf internally)
    effective_truncation = ismissing(vols) ? Inf : peer_return_truncation
    ExecutionData(fills_std, meta_std, tob_std, vol_std, peers_std, vols_std, effective_truncation, time_to_hours, missing, missing, missing, missing)
end

# Constructor with covariance matrix for automatic peer weight calculation
function ExecutionData(fills::DataFrame, metadata::DataFrame, tob::DataFrame, covar::HighFrequencyCovariance.CovarianceMatrix;
                        volume::Union{Missing,DataFrame}=missing,
                        num_peers::Union{Int,Nothing}=nothing,
                        peer_return_truncation::Float64=2.0,
                        time_to_hours::Float64=1.0,
                        namemap::Dict{Symbol,Symbol}=namemap)
    fills_cols = [:time, :quantity, :price, :execution_name, :asset]
    meta_cols = [:execution_name, :side, :desired_quantity, :arrival_price]
    tob_cols = [:time, :symbol, :bid_price, :ask_price]
    vol_cols = [:time_from, :time_to, :symbol, :volume]
    validate_column_existance(fills, fills_cols, "fills", namemap)
    validate_column_existance(metadata, meta_cols, "metadata", namemap)
    validate_column_existance(tob, tob_cols, "tob", namemap)
    if !ismissing(volume)
        validate_column_existance(volume, vol_cols, "volume", namemap)
    end
    fills_std = standardize_columns(fills, namemap, fills_cols)
    meta_std = standardize_columns(metadata, namemap, meta_cols)
    tob_std = standardize_columns(tob, namemap, tob_cols)
    vol_std = ismissing(volume) ? missing : standardize_columns(volume, namemap, vol_cols)

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

    execution_assets = unique(fills_std[!, [:execution_name, :asset]])
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
    ExecutionData(fills_std, meta_std, tob_std, vol_std, peers_df, vols_df, peer_return_truncation, time_to_hours, missing, missing, missing, missing)
end
