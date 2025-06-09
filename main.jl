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

function download_files_lister()
    results = HST_COS_search(70.7323959199788, 18.9581655408456, .02)
    println("Getting CAOM products")
    results_2 = get_CAOM_products(24845146).data
    
    required_products = "CORRTAG_A CORRTAG_B X1DSUM"
    product_URLs = []

    file_name = "test" # would be the observation ID or something in actual implementation
    extension = ".tar.gz"
    download_path = "$file_name$extension"
    count = 0
    
    for available_product in results_2
        raw_desc = available_product["productSubGroupDescription"]
        desc = !isnothing(raw_desc) ? String(raw_desc) : "Shkeebert Ruiz"
        
        if occursin(desc, required_products)
            url = ("uri", available_product["dataURI"])
            push!(product_URLs, url)
            count += 1
        end
    end

    println("Downloading $count products")
    url_list = DataFrame(product_URLs)
    CSV.write("data/downloaded_files.csv", url_list)

    download_request(product_URLs, download_path, "bundle")
end

function working_targz_download()
    uri_list = CSV.read("data/downloaded_files.csv", DataFrame)[!, 2]
    
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

function main()
    f = FITS("/Users/ga-brady/Repos/MHD-flares/code/output/MAST_2025-06-09T1515/HST/lc2601hsq/lc2601hsq_x1d.fits")
    data = DataFrame(f[2])
    print(names(data))

    wvln_A = data[!, "WAVELENGTH"][1]
    flux_A = data[!, "FLUX"][1]

    p1 = plot(wvln_A, flux_A)
    savefig(p1, "figures/test.png")

end

if abspath(PROGRAM_FILE) == @__FILE__
    results = main()
end
