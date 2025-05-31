using HTTP
using JSON3
using DataFrames
using CSV
using Tables
using DataFrames

global const MAST_BASE_URL = "https://mast.stsci.edu/api/v0"
global const CAOM_SEARCH_URL = "$MAST_BASE_URL/invoke"

# Field definitions: (name, start, end, type)
const FIELDS = [
    ("Gaia", 1, 19, Int64),
    ("SDSS", 21, 29, Int64),
    ("Teff", 31, 34, Int64),
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
    
    return DataFrame(columns)
end

function set_minmax(x::Float64, tol::Float64)
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
    print("Querying MAST for GAIA DR3 cross-match using RA: $ra, DEC: $dec; TOL: $tol ")
    
    request = Dict(
        "service" => "Mast.Catalogs.GaiaDR3.Cone",
        "params" => Dict(
            "ra" => ra,
            "dec" => dec,
            "radius" => tol
        ),
        "format" => "json",
        "pagesize" => 1000,
        "page" => 1
    )

    response = mast_query(request) 
    return response
end

function GAIA_DR3_finder(ra::Float64, dec::Float64)
    #=
    Since RA & DEC measurements are not absolute, this function implores an iterative approach
    to finding potential GAIA DR3 names from RA, DEC coordinates. 
    =#
    tol = .002 # base tolerance suggested by MAST
    count = 0
    JSON_data = []
    
    # looping until non-zero results returned
    while count == 0
        response = GAIA_DR3_crossmatch(ra, dec, tol)
        JSON_data = response.data
        count = length(JSON_data)
        tol += .01
    end

    println("$(length(JSON_data)) potential cross-matches found within $(tol)ᵒ")
    println("Attempting to minimize distance to RA: $ra, DEC: $dec")
    println("")
    candidate_list = DataFrame(id = Int64[], score = Float64[], ra = Float64[], ra_err = Float64[], dec=Float64[], dec_err = Float64[])

    for candidate in JSON_data
        s_id = candidate.source_id
        
        s_ra = candidate.ra
        ra_err = candidate.ra_error

        s_dec = candidate.dec
        dec_err = candidate.dec_error

        score = sqrt(((s_ra-ra)/ra_err)^2+((s_dec-dec)/dec_err)^2)
        push!(candidate_list, [s_id, score, s_ra, ra_err, s_dec, dec_err])
    end
    sort!(candidate_list, [:score], rev=[true])
    println(candidate_list)
    println("")

    return candidate_list
end

function Behmard_metallicity(df::DataFrame)
    printstyled("Checking Behmard source list for match\n"; color=:yellow)
    sort!(df, [:score], rev=[true])

    for candidate in df.id
        found, _, metallicity_data = GAIA_exists_in_file("data/apjadaf1ft2_mrt.txt", candidate)
        if found
            metallicity_df = parse_mrt(metallicity_data)
            return metallicity_df
        end
    end

    print("Metallicity data not found in Behmard")
    return nothing
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

function mast_name_lookup(target::String)
    print("Querying MAST for $target ")

    params = Dict("input" => target, "format"=>"json")
    resolver_request = Dict("service" => "Mast.Name.Lookup",
    "params" => params)

    pos_data = mast_query(resolver_request).resolvedCoordinate

    if isempty(pos_data)
        printstyled("$target not found in MAST database. \n"; color=:red)
        return missing, missing
    else
        coords = pos_data[1]
        target_ra = coords.ra; target_dec = coords.decl
        printstyled("$target found at RA: $target_ra; DEC: $target_dec \n"; color = :green)
        return target_ra, target_dec
    end
end

function main()
    test_mdwarf = "GJ 176"
    ra, dec = mast_name_lookup(test_mdwarf)
    # HST_count = HST_COS_count(ra, dec, .002) #.002 is MAST's default cross match value
    GAIA_list = GAIA_DR3_finder(ra, dec)
    print(Behmard_metallicity(GAIA_list))
end

# Execute the search
if abspath(PROGRAM_FILE) == @__FILE__
    results = main()
end

