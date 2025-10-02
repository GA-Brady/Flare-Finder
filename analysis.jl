### A Pluto.jl notebook ###
# v0.20.13

using Markdown
using InteractiveUtils

# ╔═╡ d885e346-293d-4bfc-8e90-1ba3626deb12
begin
	using Pkg
	Pkg.activate(".")
end

# ╔═╡ 495c25db-1e24-4cc0-bba4-4a8262c8d895
begin
	ENV["JULIA_CONDAPKG_BACKEND"] = "Null" # sets the .CondaPkg to Null
	ENV["JULIA_PYTHONCALL_EXE"] = "/Users/ga-brady/conda/envs/cos_analysis_env/bin/python" # path to python environment
	
	using PythonCall;
	costools = pyimport("costools")
	println(costools.__version__)
end

# ╔═╡ aea19ad1-629c-4651-8769-510ac9d79ef6
begin
	include("src/hubble_fetch.jl")
	using .Queries
end;

# ╔═╡ 1ea5447e-1fce-4bae-b642-fc0c27884025
begin
	obsids = obsid_lister(70.7323959199788, 18.9581655408456, .02)
    urls = target_bundler(obsids)
    targz_download(urls, "temp/test.tar.gz")
end

# ╔═╡ 2c94e665-feb4-41b0-93d3-13ba86acf285


# ╔═╡ Cell order:
# ╠═d885e346-293d-4bfc-8e90-1ba3626deb12
# ╠═495c25db-1e24-4cc0-bba4-4a8262c8d895
# ╠═aea19ad1-629c-4651-8769-510ac9d79ef6
# ╠═1ea5447e-1fce-4bae-b642-fc0c27884025
# ╠═2c94e665-feb4-41b0-93d3-13ba86acf285
