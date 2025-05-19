using HTTP
using JSON3
using DataFrames
using CSV


url = "https://mast.stsci.edu/api/v0.1/portal/MashupQuery"
headers = ["Content-Type" => "application/json"]

query_payload = Dict(
    "service" => "Mast.Caom.Cone",
    "params" => Dict("ra" => 10.684, "dec" => 41.269, "radius" => 0.01),
    "format" => "json",
    "pagesize" => 10
)

body = JSON3.write(query_payload)
response = HTTP.post(url, headers, body)

println(String(response.body))

fetch_hst_time_tag_metadata()