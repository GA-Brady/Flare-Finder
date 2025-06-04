include("src/hubble_fetch.jl")
using .Queries
using DataFrames
using CSV

function main()
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

if abspath(PROGRAM_FILE) == @__FILE__
    results = main()

end
