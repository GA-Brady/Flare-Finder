include("src/hubble_fetch.jl")
using .Queries
using DataFrames
using CSV
using HTTP
using JSON3
using Tar
using CodecZlib
using FITSIO
using Plots

function main()
    #=
    f = FITS("/Users/ga-brady/Repos/MHD-flares/code/output/MAST_2025-06-09T1515/HST/lc2601hsq/lc2601hsq_x1d.fits")
    data = DataFrame(f[2])
    print(names(data))

    wvln_A = data[!, "WAVELENGTH"][1]
    flux_A = data[!, "FLUX"][1]

    p1 = plot(wvln_A, flux_A)
    savefig(p1, "figures/test.png")
    =#

    obsids = obsid_lister(70.7323959199788, 18.9581655408456, .02)
    urls = target_bundler(obsids)
    targz_download(urls, "temp/test.tar.gz")
end

if abspath(PROGRAM_FILE) == @__FILE__
    results = main()
end
