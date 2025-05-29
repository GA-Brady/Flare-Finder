using DataFrames
using CSV

home_dir = pwd()
data_dir = "$home_dir/data"
src_dir  = "$home_dir/source"
#=
    This program takes Katherine Melbourne's 2020 M-dwarf list of COS spectra,
    cleans the data for program identifiers and footnotes,
    then returns a cleaned data list for use with MAST queries.
=#

function clean(s::Union{AbstractString, Missing})
    #=
        Takes in a target name, s, and replaces it with missing if it does not fit
        the search criteria.
    =#
    name = s
    if !ismissing(s)
        if contains(s, "GO") 
            name = missing
        elseif contains(s, "AR")
            name = missing
        elseif contains(s, "^")
            name = missing
        end
    end
    return name
end

melbourne_df = CSV.read("$data_dir/melbourne_2020.txt", DataFrame)
target_list_dirty = vcat([melbourne_df[!, col] for col in names(melbourne_df)]...)
target_list_clean = []

for target in target_list_dirty
    !ismissing(clean(target)) ? push!(target_list_clean, target) : nothing
end

target_list = DataFrame(Target = collect(target_list_clean))
CSV.write("$data_dir/target_list.csv", target_list)