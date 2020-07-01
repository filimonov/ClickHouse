add_executable (convert-alter-partitioning-key main.cpp)
target_link_libraries(convert-alter-partitioning-key PRIVATE clickhouse_aggregate_functions dbms clickhouse_parsers boost::program_options)