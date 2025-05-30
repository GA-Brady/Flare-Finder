using HTTP
using JSON3
using DataFrames
using CSV
using Tables
using DataFrames

global const MAST_BASE_URL = "https://mast.stsci.edu/api/v0"
global const CAOM_SEARCH_URL = "$MAST_BASE_URL/invoke"

function set_minmax(x::Float64, tol::Float64)
    min = x - tol
    max = x + tol
    return min, max
end

function set_filters(parameters::Dict)
    return [Dict("paramName" => p, "values" => v) for (p, v) in parameters]
end

function mast_query(request::Dict)
    try
        headers = Dict("Content-type" => "application/x-www-form-urlencoded",
                    "Accept" => "text/plain")
        body = "request=" * JSON3.write(request)
        response = HTTP.post(CAOM_SEARCH_URL, headers, body)

        if response.status == 200
            data = JSON3.read(response.body)
            return data
            #=
            if haskey(data, :data) && !isempty(data.data)
                df = DataFrame(data.data)
                println("Request contains $(nrow(df)) rows")
                return df
            else
                println("Empty datafield")
                return nothing
            end
            =#
        else
            printstyled("HTTP response failed with status $response.status \n"; color =:red)
            return nothing
        end 
    catch e
        printstyled("Error executing search: $e \n"; color=:red)
        return nothing
    end
end

function HST_COS_search(ra, dec, tol)
    print("Searching for HST spectra...")

    filts = set_filters(Dict( 
    "obs_collection" => ["HST"],
    "wavelength_region" => ["UV", "NUV", "FUV"],
    "dataproduct_type" => ["spectrum"],
    "instrument_name" => ["COS/FUV", "COS/NUV"]))

    params = Dict("columns"=>"*", "filters"=> filts, "position"=>"$ra, $dec, $tol")
    request = Dict("service" => "Mast.Caom.Filtered.Position",
                "format"=>"json",
                "params" => params)
    return mast_query(request)
end

function main()

    test_mdwarf = "GJ 176"
    println("Querying MAST for $test_mdwarf s...")

    params = Dict("input" => test_mdwarf, "format"=>"json")
    resolver_request = Dict("service" => "Mast.Name.Lookup",
    "params" => params)

    pos_data = mast_query(resolver_request).resolvedCoordinate[1]
    target_ra = pos_data.ra; target_dec = pos_data.decl

    printstyled("Target $test_mdwarf positioned at RA: $target_ra, DEC: $target_dec \n"; color=:green)

    println(HST_COS_search(target_ra, target_dec, .5))
end

# Execute the search
if abspath(PROGRAM_FILE) == @__FILE__
    results = main()
end

