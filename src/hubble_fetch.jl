using HTTP
using JSON3
using DataFrames
using CSV
using Tables
using DataFrames

global const MAST_BASE_URL = "https://mast.stsci.edu/api/v0"
global const CAOM_SEARCH_URL = "$MAST_BASE_URL/invoke"

function mast_query(request::Dict)
    headers = Dict("Content-type" => "application/x-www-form-urlencoded",
                "Accept" => "text/plain")
    body = "request=" * JSON3.write(request)
    resp = HTTP.post(CAOM_SEARCH_URL, headers, body)
    return resp
end

function main()
    println("Querying MAST for COS observations of M-dwarf stars...")
    test_mdwarf = "GJ 176"

    params = Dict("input" => test_mdwarf, "format"=>"json")
    resolver_request = Dict("service" => "Mast.Name.Lookup",
    "params" => params)

    print(mast_query(resolver_request))
end

# Execute the search
if abspath(PROGRAM_FILE) == @__FILE__
    results = main()
end

