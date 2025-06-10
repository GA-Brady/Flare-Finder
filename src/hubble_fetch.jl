module Queries

using CSV
using DataFrames
using FITSIO
using HTTP
using JSON3
using Tables
using Tar
using CodecZlib
using FITSIO
using Logging

export mast_query, HST_COS_count, HST_COS_search, download_request, get_CAOM_products, obsid_lister, target_bundler, targz_download

global const MAST_BASE_URL = "https://mast.stsci.edu/api/v0"
global const CAOM_SEARCH_URL = "$MAST_BASE_URL/invoke"
# Field definitions: (name, start, end, type)
const FIELDS = [
    ("Gaia", 1, 19, Int64),
    ("SDSS", 21, 29, Int64),
    ("Teff", 30, 34, Int64),
    ("Fe_H", 36, 40, Float64),
    ("Mg_H", 42, 46, Float64),
    ("Al_H", 48, 52, Float64),
    ("Si_H", 54, 58, Float64),
    ("C_H", 60, 64, Float64),
    ("O_H", 66, 70, Float64),
    ("Ca_H", 72, 76, Float64),
    ("Ti_H", 78, 82, Float64),
    ("Cr_H", 84, 88, Float64),
    ("N_H", 90, 94, Float64),
    ("Ni_H", 96, 100, Float64),
    ("Chi2", 102, 106, Int64),
    ("TempAgree", 108, 112, String),
    ("e_Teff", 114, 117, Float64),
    ("e_Fe_H", 119, 122, Float64),
    ("e_Mg_H", 124, 127, Float64),
    ("e_Al_H", 129, 132, Float64),
    ("e_Si_H", 134, 137, Float64),
    ("e_C_H", 139, 142, Float64),
    ("e_O_H", 144, 147, Float64),
    ("e_Ca_H", 149, 152, Float64),
    ("e_Ti_H", 154, 157, Float64),
    ("e_Cr_H", 159, 162, Float64),
    ("e_N_H", 164, 167, Float64),
    ("e_Ni_H", 169, 172, Float64)
]

struct APIResult{T}
    success::Bool
    data::Union{T, Nothing}
    error_type::Symbol
    message::String
    should_retry::Bool
    retry_after::Union{Integer, Nothing} # seconds
end

#=
function handle_api_response(response::HTTP.Response, expected_type::Type=Any)
    status = response.status

    if 200 <= status <= 299
        try
            data = JSON3.read(response.body, expected_type)

        catch
        end
    end
end
=#

function parse_mrt(data::String)
    lines = split(strip(data), '\n')
    lines = filter(l -> !isempty(strip(l)), lines)
    
    # Initialize columns in order
    columns = []
    for (name, _, _, type) in FIELDS
        if type == String
            push!(columns, name => String[])
        else
            push!(columns, name => Union{type, Missing}[])
        end
    end
    
    # Parse each line
    for line in lines
        for (i, (name, start_pos, end_pos, type)) in enumerate(FIELDS)
            if length(line) >= end_pos
                raw = strip(line[start_pos:end_pos])
                
                if isempty(raw)
                    push!(columns[i][2], missing)
                else
                    try
                        if type == String
                            push!(columns[i][2], raw)
                        else
                            push!(columns[i][2], parse(type, raw))
                        end
                    catch
                        push!(columns[i][2], missing)
                    end
                end
            else
                push!(columns[i][2], missing)
            end
        end
    end
    
    return columns
end

function set_minmax(x::Float64, tol::Float64)
    tol = abs(tol)
    min = x - tol
    max = x + tol
    return min, max
end

function set_filters(parameters::Dict)
    return [Dict("paramName" => p, "values" => v) for (p, v) in parameters]
end

function mast_query(request::Dict)
    haskey(request, "service") ? printstyled("using $(request["service"])\n"; color=:yellow) : nothing
    try
        headers = Dict("Content-type" => "application/x-www-form-urlencoded",
                    "Accept" => "text/plain")
        body = "request=" * JSON3.write(request)
        response = HTTP.post(CAOM_SEARCH_URL, headers, body)

        if response.status == 200
            data = JSON3.read(response.body)
            return data
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
    print("Searching for HST spectra ")

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

function HST_COS_count(ra, dec, tol)
    print("Searching for HST spectra ")

    filts = set_filters(Dict( 
    "obs_collection" => ["HST"],
    "wavelength_region" => ["UV", "NUV", "FUV"],
    "dataproduct_type" => ["spectrum"],
    "instrument_name" => ["COS/FUV", "COS/NUV"]))

    params = Dict("columns"=>"COUNT_BIG(*)", "filters"=> filts, "position"=>"$ra, $dec, $tol")
    request = Dict("service" => "Mast.Caom.Filtered.Position",
                "format"=>"json",
                "params" => params)

    response = mast_query(request)
    counts = response.data[1].Column1
    printstyled("$counts HST COS/NUV || COS/FUV observations found"; color=:green)
    return counts
end

function SDSS_crossmatch(ra::Float64, dec::Float64, tol::Float64)
    print("Querying MAST for SDSS cross-match using RA: $ra; DEC: $dec ")

    crossmatch_input = Dict(
        "fields" => [
            Dict("name" => "ra", "type" => "float"),
            Dict("name" => "dec", "type" => "float")
        ],
        "data" => [
            Dict("ra" => ra, "dec" => dec)
        ]
    )
    
    request = Dict(
        "service" => "Mast.Sdss.Crossmatch",
        "data" => crossmatch_input,
        "params" => Dict(
            "raColumn" => "ra",
            "decColumn" => "dec",
            "radius" => tol
        ),
        "format" => "json",
        "pagesize" => 1000,
        "page" => 1
    )

    response = mast_query(request) 
    return response
end

function GAIA_DR3_crossmatch(ra::Float64, dec::Float64, tol::Float64)
    print("Querying MAST for GAIA cross-match using RA: $ra; DEC: $dec ")

    crossmatch_input = Dict(
        "fields" => [
            Dict("name" => "ra", "type" => "float"),
            Dict("name" => "dec", "type" => "float")
        ],
        "data" => [
            Dict("ra" => ra, "dec" => dec)
        ]
    )
    
    request = Dict(
        "service" => "Mast.GaiaDR3.Crossmatch",
        "data" => crossmatch_input,
        "params" => Dict(
            "raColumn" => "ra",
            "decColumn" => "dec",
            "radius" => tol
        ),
        "format" => "json",
        "pagesize" => 1000,
        "page" => 1
    )

    response = mast_query(request) 
    return response
end

function GAIA_DR3_finder(ra::Union{Float64, Nothing}, dec::Union{Float64,Nothing})
    #=
    Since RA & DEC measurements are not absolute, this function implores an iterative approach
    to finding potential GAIA DR3 names from RA, DEC coordinates. 
    =#
    ra_missing = isnothing(ra)
    dec_missing = isnothing(dec)

    if ra_missing || dec_missing
        (ra_missing && dec_missing) ? (printstyled("RA and Dec missing\n"; color=:red); return nothing) : nothing
        ra_missing ? (printstyled("RA missing\n"; color=:red); return nothing) : nothing
        dec_missing ? (printstyled("Dec missing\n"; color=:red); return nothing) : nothing
    end
    
    tol = .05
    count = 0
    JSON_data = []
    max_iterations = 10
    iteration = 0
    
    # looping until non-zero results returned
    while count == 0 && iteration < max_iterations
        response = GAIA_DR3_crossmatch(ra, dec, tol)
        
        if !isnothing(response) 
            JSON_data = response.data
            count = length(JSON_data)

            tol += .1
            iteration += 1
        else
            printstyled("MAST Query failed"; color=:red)
            iteration += 1
        end
    end

    if iteration >= max_iterations
        printstyled("Maximum iterations exceeded without finding matches\n"; color =:red)
        return nothing
    end

    println("$(length(JSON_data)) potential cross-matches found within $(tol)ᵒ")
    println("Attempting to minimize distance to RA: $ra, DEC: $dec")
    println("")
    candidate_list = DataFrame(id = Int64[], score = Float64[], ra = Float64[], ra_err = Float64[], dec=Float64[], dec_err = Float64[])

    for candidate in JSON_data
        s_id = candidate.MatchID
        
        s_ra = candidate.MatchRA
        ra_err = candidate.ra_error

        s_dec = candidate.MatchDEC
        dec_err = candidate.dec_error
        
        if !(ra_err == 0 || dec_err == 0)
            score = sqrt(abs(((s_ra-ra)/ra_err)^2+((s_dec-dec)/dec_err)^2))
        else 
            score = 0
        end

        push!(candidate_list, [s_id, score, s_ra, ra_err, s_dec, dec_err])
    end
    sort!(candidate_list, [:score], rev=[true])
    println(candidate_list)
    println("")

    return candidate_list
end

function Behmard_metallicity(df::Union{DataFrame, Nothing})
    if isnothing(df)
        printstyled("Candidate DataFrame missing\n"; color=:red)
        return nothing
    end

    printstyled("Checking Behmard source list for match\n"; color=:yellow)
    sort!(df, [:score], rev=[true])

    for candidate in df.id
        found, _, metallicity_data = GAIA_exists_in_file("data/apjadaf1ft2_mrt.txt", candidate)
        if found
            metallicity_cols = parse_mrt(metallicity_data)
            return true, nothing, metallicity_cols
        end
    end

    printstyled("Metallicity data not found in Behmard\n"; color =:red)
    return false, nothing, nothing
end

function GAIA_exists_in_file(filename::String, target_integer::Int)
    open(filename, "r") do file
        row_number = 1
        while !eof(file)
            line = readline(file)
            
            if length(line) >= 19
                substring = line[1:19]
                if occursin(string(target_integer), substring)
                    printstyled("Metallicity data for GAIA ID:$target_integer in row $row_number\n"; color=:green)
                    return (found=true, row_number=row_number, content=line)
                end
            end
            row_number += 1
        end
        
        println("Metallicity data for GAIA ID:$target_integer not found in file")
        return (found=false, row_number=nothing, content=nothing)
    end
end

function mast_name_lookup(target::AbstractString)
    print("Querying MAST for $target ")

    params = Dict("input" => target, "format"=>"json")
    resolver_request = Dict("service" => "Mast.Name.Lookup",
    "params" => params)

    pos_data = mast_query(resolver_request).resolvedCoordinate

    if isempty(pos_data)
        printstyled("$target not found in MAST database. \n"; color=:red)
        return nothing, nothing
    else
        coords = pos_data[1]
        target_ra = coords.ra; target_dec = coords.decl
        printstyled("$target found at RA: $target_ra; DEC: $target_dec \n"; color = :green)
        return target_ra, target_dec
    end
end

function create_empty_viable_targets_df()
    # Create base columns
    base_columns = [
        :Name => String[],
        :RA => Float64[],
        :Dec => Float64[]
    ]
    
    # Add metallicity columns
    metallicity_columns = []
    for (name, _, _, type) in FIELDS
        if type == String
            push!(metallicity_columns, Symbol(name) => String[])
        else
            push!(metallicity_columns, Symbol(name) => Union{type, Missing}[])
        end
    end
    
    # Combine all columns
    all_columns = vcat(base_columns, metallicity_columns)
    
    return DataFrame(all_columns)
end

function append_target_data!(df::DataFrame, name::String, ra::Float64, dec::Float64, metallicity_cols)
    # Create new row data
    new_row = Dict{Symbol, Any}()
    new_row[:Name] = name
    new_row[:RA] = ra
    new_row[:Dec] = dec
    
    # Add metallicity data
    for (col_name, col_data) in metallicity_cols
        # Take first value if multiple entries, or missing if empty
        if !isempty(col_data)
            new_row[Symbol(col_name)] = col_data[1]
        else
            new_row[Symbol(col_name)] = missing
        end
    end
    
    # Append to DataFrame
    push!(df, new_row)
end

function get_target_list()
    # function which returns viable targets from the Melbourne list
    viable_targets = create_empty_viable_targets_df()
    target_list = CSV.read("data/target_list.csv", DataFrame).Target
    total_targets = length(target_list)

    for (i, t) in enumerate(target_list)
        printstyled("Target $i/$total_targets \n"; color =:cyan)

        try
            target = String(t)
            ra, dec = mast_name_lookup(target)
            GAIA_list = GAIA_DR3_finder(ra, dec)
            found, _, cols = Behmard_metallicity(GAIA_list)

            if found
                append_target_data!(viable_targets, target, ra, dec, cols)
            end

        catch e
            printstyled("Error processing target $i/$total_targets: $e \n"; color=:red)
        end
        
    end

    println(viable_targets)

    output_file = "data/viable_targets_with_metallicity.csv"
    CSV.write(output_file, viable_targets)
    println("Results saved to: $(output_file)")
end

function download_request(request, filepath, download_type="file")
    printstyled("Getting data products \n"; color=:yellow)
    try
        request_url="https://mast.stsci.edu/api/v0.1/Download/" * download_type
        response = HTTP.post(request_url, ["Content-Type" => "application/json"], body=request)

        if response.status == 200
            println("Download successfully executed")
            open(filepath, "w") do file
                write(file, response.body)
            end
        else
            printstyled("Download failed. Server response $(response.status) \n"; color=:red)
        end

        return response
    catch e
        printstyled("Error: $e encountered attempting to download products \n"; color=:red)
    end
end

function get_CAOM_products(obsid::Integer)
    request = Dict("service" => "Mast.Caom.Products",
    "params" => Dict("obsid"=>obsid), "format" => "json")

    return mast_query(request)
end

# directly copied from main.jl which provide basic funcitonality
function obsid_lister(ra::Union{Float64, Integer}, dec::Union{Float64, Integer}, tol::Union{Float64, Integer})
    results = HST_COS_search(ra, dec, tol)["data"]

    obs_list = []
    num_rows = length(results)

    if isempty(results)
        return nothing
    end

    for i in 1:num_rows
        row = results[i]
        obsid = row["obsid"]
        push!(obs_list, obsid)
    end

    return obs_list
end

function target_bundler(obsid::Integer)
    # Function overloader to make sure that they correct type is passed
    return target_bundle_downloader([obsid], download_path)
end

function target_bundler(obsid_list::Vector{Any})
    println("Getting CAOM products")
    
    required_products = "CORRTAG_A CORRTAG_B X1DSUM" # required data products for Splittag
    product_URLs = []
    count = 0

    for obsid in obsid_list
        result = get_CAOM_products(obsid).data
        
        for available_product in result
            raw_desc = available_product["productSubGroupDescription"]
            desc = !isnothing(raw_desc) ? String(raw_desc) : "Shkeebert Ruiz"
            
            if occursin(desc, required_products)
                url = available_product["dataURI"]
                push!(product_URLs, url)
                count += 1
            end
        end

    end
    println("Found $count observations")
    return product_URLs
end

function targz_download(uri::String, download_path::String)
    # targz_download overloader for type checking
    return targz_download([uri], download_path)
end

function targz_download(uri_list::Vector{Any}, download_path::String)
    println("Downloading uris")
    payload = JSON3.write(uri_list)
    download_request(payload, download_path, "bundle.tar.gz")
end

function working_targz_download()    
    form_data = []

    # Or even more explicit:
    for uri in uri_list
        push!(form_data, uri)
    end

    data = JSON3.write(form_data)
    println("Data: $data")

    file_name = "test" # would be the observation ID or something in actual implementation
    extension = ".tar.gz"
    download_path = "$file_name$extension"
    download_request(data, download_path, "bundle$extension")
end

function untarzipper()
    # Open the tar.gz file as a stream
    io = GzipDecompressorStream(open("test.tar.gz", "r"))

    # Extract the archive to a directory named "output"
    Tar.extract(io, "output")

    # Close the stream
    close(io)
end

end