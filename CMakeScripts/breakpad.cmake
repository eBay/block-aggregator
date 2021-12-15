
#See https://chromium.googlesource.com/breakpad/breakpad/+/master/docs/linux_starter_guide.md#producing-symbols-for-your-application

set(sym_file ${CMAKE_ARGV3}) #arguments starting with index 0 are: cmake -P this-script-path sym-file-arg
get_filename_component(base_dir ${sym_file} DIRECTORY)
get_filename_component(prog_name ${sym_file} NAME_WE)

#The first line of the sym file has this format: 'MODULE mac x86_64 6C3385E0493F309987A7918271841FD50 MonstorDatabase'
#The forth field is the digest that needs to be captured
execute_process(COMMAND bash "-c"  "head -n1 ${sym_file} | cut -d ' ' -f 4" OUTPUT_VARIABLE digest OUTPUT_STRIP_TRAILING_WHITESPACE)

set(sym_path ${base_dir}/symbols/${prog_name}/${digest})
file(REMOVE_RECURSE ${sym_path})
file(MAKE_DIRECTORY ${sym_path})
file(RENAME ${sym_file} ${sym_path}/${prog_name}.sym)
