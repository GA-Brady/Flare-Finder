using HTTP

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

    df = CSV.read(IOBuffer(response.body), DataFrame)
    isempty(df) && return nothing
    return df[!, [:ra, :dec]][1, :]
end

get_gaia_coords(421503353090268416)