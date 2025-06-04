function hello_world()
    printstyled("Hello World! \n"; color=:red)
    # execute in terminal using julia --project=. -e "include(\"src/test.jl\"); hello_world()"
end

function main()
    hello_world()
    print(contains("CORRTAG_ACORRTAG_B", "CORRTAG_B"))
end

if abspath(PROGRAM_FILE) == @__FILE__
    results = main()
end