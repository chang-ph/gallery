# This script is used to import models from
# https://github.com/StatisticalRethinkingJulia/TuringModels.jl.

# The cloned TuringModels.jl repository folder path
SRC_DIR =  ARGS[1]

function script_name(path::String)
    name = splitdir(path)[2]
    name = splitext(name)[1]
    return name
end

function titlize(path::String)
    name = script_name(path)
    words = split(name, "-")
    return join(map(uppercasefirst, words), " ")
end

function cp_project_file()
    proj = joinpath(SRC_DIR, "Project.toml")
    content = read(proj, String)
    lines = split(content, "\n")

    open("Project.toml", "w") do io
        for line in lines[6:end]
            println(io, line)
        end
    end
end

REG_DATA = r"""(\w+) = joinpath\(.*project_root, "data", "(.+)"\)"""
REG_M1 = r"""model = (.+);?$"""
REG_M2 = r"""chns = sample\((.+), NUTS\(\), \d+\)"""
REG_M3 = r"""^(\w+) = sample\($"""

function cp_data_file(lines)
    for line in lines
        m = match(REG_DATA, line)
        m == nothing && continue
        path = joinpath(SRC_DIR, "data", m[2])
        cp(path, "data.csv")
        return true
    end
    @warn "No data files found."
    return false
end

function model_func(mline, has_data)
    mline = replace(mline, r"(\W)df(\W)"=>s"\1_input\2")
    mline = rstrip(mline, [',', ';', ' '])
    arg_line = has_data ? "_input == nothing && (_input = df)" : ""
    return """
function model(_input)
    $(arg_line)
    _model = $(mline)
    return _model
end
"""
end

function cp_main_script(lines)
    new_lines = ["# auto-generated\n"]
    has_data = false

    for (idx, line) in enumerate(lines)
        m = match(r"(using|import)\s+TuringModels", line)
        if m !== nothing
            continue
        end

        m = match(REG_DATA, line)
        if m !== nothing
            has_data = true
            push!(new_lines, """$(m[1]) = joinpath(@__DIR__, "data.csv") """)
            continue
        end

        m = match(REG_M1, line)
        if m !== nothing
            push!(new_lines, model_func(m[1], has_data))
            break
        end

        m = match(REG_M2, line)
        if m !== nothing
            push!(new_lines, model_func(m[1], has_data))
            break
        end

        m = match(REG_M3, line)
        if m !== nothing
            push!(new_lines, model_func(lines[idx + 1], has_data))
            break
        end

        push!(new_lines, line)
    end
    open("main.jl", "w") do io
        for line in new_lines
            println(io, line)
        end
    end
end

function main()
    dest_dir = length(ARGS) > 1 ? ARGS[2] : joinpath($(pwd()), "imported-models")
    isdir(dest_dir) || mkpath(dest_dir)

    println("Using source [$SRC_DIR]...")
    println("Working under [$dest_dir]...")
    path = joinpath(SRC_DIR, "scripts")
    for file in readdir(path; join=true)
        println("-> Dealing with [$file]...")
        sname = script_name(file)
        if in(sname,["spatial-autocorrelation-oceanic",
                     "multivariate-chimpanzees-priors"])
            @warn "ignore $(sname)"
            continue
        end
        title = titlize(file)
        content = read(file, String)
        lines = split(content, "\n")
        dir = joinpath(dest_dir, sname)
        mkdir(dir)
        cd(dir) do
            files = ["main.jl"]
            cp_project_file()
            cp_data_file(lines) && push!(files, "data.csv")
            cp_main_script(lines)
        end
        println("<- Done with model [$dir].\n")
    end
    println("Done with srouce tree [$dest_dir].")
end


main()
