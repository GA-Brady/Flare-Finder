using HTTP
using JSON3
using DataFrames
using CSV
using Tables
using DataFrames

global const MAST_BASE_URL = "https://mast.stsci.edu/api/v0"
global const CAOM_SEARCH_URL = "$MAST_BASE_URL/invoke"

function get_gaia_coords(gaia_id)
    query = """
    SELECT source_id, ra, dec
    FROM gaiadr3.gaia_source
    WHERE source_id = $gaia_id
    """
    url = "https://gea.esac.esa.int/tap-server/tap/sync"
    response = HTTP.post(url, [
        "Content-Type" => "application/x-www-form-urlencoded"
    ], "REQUEST=doQuery&LANG=ADQL&FORMAT=csv&QUERY=$(replace(query, '\n' => " "))")

    return response.body
end

function main()
    print(get_gaia_coords([421503353090268416, 421535342004177920]))
end

if abspath(PROGRAM_FILE) == @__FILE__
    results = main()
end