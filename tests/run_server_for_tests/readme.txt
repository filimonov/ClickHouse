CLICKHOUSE_BINDIR=$(realpath ../../build-vscode/clang_8_Debug/programs) ./run_server_for_tests/run_here.sh
./clickhouse-test -b ../build-vscode/clang_8_Debug/programs/clickhouse --no-stateful --zookeeper mutations_with_nondeterministic_functions_zookeeper.sh
















# mkdir users.d
# mkdir config.d
# ln -s ../../../../programs/server/users.xml users.xml
# ln -s ../../../../programs/server/config.xml config.xml

# ln -s ../../../../config/zookeeper.xml ./config.d/;
# ln -s ../../../../config/listen.xml ./config.d/;
# ln -s ../../../../config/part_log.xml ./config.d/;
# ln -s ../../../../config/text_log.xml ./config.d/;
# ln -s ../../../../config/metric_log.xml ./config.d/;
# ln -s ../../../../config/query_masking_rules.xml ./config.d/;
# ln -s ../../../../config/log_queries.xml ./users.d/;
# ln -s ../../../../config/readonly.xml ./users.d/;
# ln -s ../../../../config/access_management.xml ./users.d/;
# ln -s ../../../config/ints_dictionary.xml ./;
# ln -s ../../../config/strings_dictionary.xml ./;
# ln -s ../../../config/decimals_dictionary.xml ./;
# ln -s ../../../../config/macros.xml ./config.d/;
# ln -s ../../../../config/disks.xml ./config.d/;
# ln -s ../../../../config/secure_ports.xml ./config.d/;
# ln -s ../../../../config/clusters.xml ./config.d/;
# ln -s ../../../config/server.key ./;
# ln -s ../../../config/server.crt ./;
# ln -s ../../../config/dhparam.pem ./;

#ln -s /usr/share/clickhouse-test/config/polymorphic_parts.xml /etc/clickhouse-server/config.d/; fi; \
#    ln -sf /usr/share/clickhouse-test/config/client_config.xml /etc/clickhouse-client/config.xml;
