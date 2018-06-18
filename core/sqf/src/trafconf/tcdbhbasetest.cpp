#include <assert.h>
#include <iostream>
#include <string.h>
#include <vector>

#include <thrift/transport/TSocket.h>
#include <thrift/transport/TBufferTransports.h>
#include <thrift/protocol/TBinaryProtocol.h>

#include "gen-cpp/Hbase.h"

#include "tcdbhbase.h"


using namespace apache::thrift;
using namespace apache::thrift::protocol;
using namespace apache::thrift::transport;
using namespace apache::hadoop::hbase::thrift;

typedef std::vector<std::string> StrVec;

void deleteTable()
{
    int thriftPort = 9090;
    char *th = getenv("TRAF_HOME");
    if (th != NULL)
    {
        char sqlscr[PATH_MAX];
        sprintf(sqlscr, "%s/sql/scripts/sw_ports", th);
        FILE *file = fopen(sqlscr, "r");
        const char *thrStr = "MY_HBASE_REGIONSERVER_THRIFT_PORT_NUM=";
        int thrStrLen = (int) strlen(thrStr);
        if (file != NULL)
        {
            char line[100];
            for (;;)
            {
                char *s = fgets(line, sizeof(line), file);
                if (s == NULL)
                    break;
                char *eq = strstr(s, "=");
                if (eq != NULL)
                {
                    int strLen = (int) (eq - s);
                    strLen++;
                    if (strLen == thrStrLen)
                    {
                        if (memcmp(s, thrStr, thrStrLen) == 0)
                        {
                            thriftPort = atoi(&eq[1]);
                        }
                    }
                }
            }
            fclose(file);
        }
    }
    std::shared_ptr<TSocket> socket(new TSocket("localhost", thriftPort));
    std::shared_ptr<TTransport> transport(new TBufferedTransport(socket));
    std::shared_ptr<TProtocol> protocol(new TBinaryProtocol(transport));
    HbaseClient client(protocol);
    try
    {
        transport->open();
        std::string t("trfconf");

        // Scan all tables, look for the trfconf table and delete it.
        StrVec tables;
        client.getTableNames(tables);
        for (StrVec::const_iterator it = tables.begin(); it != tables.end(); ++it)
        {
            if (t == *it)
            {
                if (client.isTableEnabled(*it))
                {
                    client.disableTable(*it);
                }
                client.deleteTable(*it);
            }
        }
        transport->close();
    } catch (const TException &tx)
    {
        std::cerr << "ERROR: " << tx.what() << std::endl;
    }
}

void usage(char *cmd)
{
    printf("%s: usage [ delete | read ]\n", cmd);
}

int main(int argc, char** argv)
{
    CTcdbHbase tcdbHbase;
    int rc;
    bool readDb = false;
    bool deleteDb = false;

    if (argc > 1)
    {
        if (strcmp(argv[1], "read") == 0)
            readDb = true;
        else if (strcmp(argv[1], "delete") == 0)
            deleteDb = true;
        else
        {
            printf("unknown command=%s\n", argv[1]);
            usage(argv[0]);
            return 1;
        }
    }

    try
    {
        if (deleteDb)
        {
            printf("deleting DB\n");
            fflush(stdout);
            deleteTable();
            return 0;
        }
        else if (!readDb)
        {
            printf("deleting DB\n");
            deleteTable();
        }

        // not initialized
        rc = tcdbHbase.AddLNodeData(0, 0, 0, 0, 0, 0);
        assert(rc == TCNOTINIT);
        rc = tcdbHbase.AddNameServer("nodeName");
        assert(rc == TCNOTINIT);
        rc = tcdbHbase.AddPNodeData("name", 0, 0, 0);
        assert(rc == TCNOTINIT);
        rc = tcdbHbase.AddSNodeData("name", 0, 0, 0, 0);
        assert(rc == TCNOTINIT);
        rc = tcdbHbase.AddRegistryPersistentData("keyName", "valueName");
        assert(rc == TCNOTINIT);
        rc = tcdbHbase.AddRegistryKey("key");
        assert(rc == TCNOTINIT);
        rc = tcdbHbase.AddRegistryProcess("name");
        assert(rc == TCNOTINIT);
        rc = tcdbHbase.AddRegistryClusterData("key", "dataValue");
        assert(rc == TCNOTINIT);
        rc = tcdbHbase.AddRegistryProcessData("procName", "key", "dataValue");
        assert(rc == TCNOTINIT);
        rc = tcdbHbase.AddUniqueString(0, 0, "uniqStr");
        assert(rc == TCNOTINIT);
        rc = tcdbHbase.DeleteNameServer("nodeName");
        assert(rc == TCNOTINIT);
        rc = tcdbHbase.DeleteNodeData(0);
        assert(rc == TCNOTINIT);
        rc = tcdbHbase.DeleteUniqueString(0);
        assert(rc == TCNOTINIT);
        rc = tcdbHbase.GetNameServer("nodeName");
        assert(rc == TCNOTINIT);
        int count0;
        char *nodeNames00[1];
        rc = tcdbHbase.GetNameServers(&count0, 0, nodeNames00);
        assert(rc == TCNOTINIT);
        TcNodeConfiguration_t nodeConfig0;
        rc = tcdbHbase.GetNode(0, nodeConfig0);
        assert(rc == TCNOTINIT);
        rc = tcdbHbase.GetNode("name", nodeConfig0);
        assert(rc == TCNOTINIT);
        TcNodeConfiguration_t nodeConfig00[1];
        rc = tcdbHbase.GetNodes(count0, 0, nodeConfig00);
        assert(rc == TCNOTINIT);
        TcPhysicalNodeConfiguration_t pnodeConfig0;
        rc = tcdbHbase.GetPNode(0, pnodeConfig0);
        assert(rc == TCNOTINIT);
        rc = tcdbHbase.GetPNode("name", pnodeConfig0);
        assert(rc == TCNOTINIT);
        TcPhysicalNodeConfiguration_t pnodeConfig00[1];
        rc = tcdbHbase.GetSNodes(count0, 0, pnodeConfig00);
        assert(rc == TCNOTINIT);
        TcPersistConfiguration_t persistConfig0;
        rc = tcdbHbase.GetPersistProcess("persistPrefix", persistConfig0);
        assert(rc == TCNOTINIT);
        rc = tcdbHbase.GetPersistProcessKeys("persistProcessKeys");
        assert(rc == TCNOTINIT);
        TcRegistryConfiguration_t registryConfig00[1];
        rc = tcdbHbase.GetRegistryClusterSet(count0, 0, registryConfig00);
        assert(rc == TCNOTINIT);
        rc = tcdbHbase.GetRegistryProcessSet(count0, 0, registryConfig00);
        assert(rc == TCNOTINIT);
        rc = tcdbHbase.GetUniqueString(0, 0, "uniqStr");
        assert(rc == TCNOTINIT);
        int id0;
        rc = tcdbHbase.GetUniqueStringId(0, "uniqStr", id0);
        assert(rc == TCNOTINIT);
        rc = tcdbHbase.GetUniqueStringIdMax(0, id0);
        assert(rc == TCNOTINIT);

        rc = tcdbHbase.Initialize();
        assert(rc == TCSUCCESS);

        if (!readDb)
        {
            rc = tcdbHbase.AddPNodeData("nap043.esgyn.local", 0, -1, -1);
            assert(rc == TCSUCCESS);
            rc = tcdbHbase.AddPNodeData("nap044.esgyn.local", 1, -1, -1);
            assert(rc == TCSUCCESS);
            rc = tcdbHbase.AddPNodeData("nap045.esgyn.local", 2, -1, -1);
            assert(rc == TCSUCCESS);
            rc = tcdbHbase.AddPNodeData("nap046.esgyn.local", 3, -1, -1);
            assert(rc == TCSUCCESS);
            rc = tcdbHbase.AddPNodeData("nap047.esgyn.local", 4, -1, -1);
            assert(rc == TCSUCCESS);
            rc = tcdbHbase.AddPNodeData("nap048.esgyn.local", 5, -1, -1);
            assert(rc == TCSUCCESS);
            rc = tcdbHbase.AddPNodeData("nap049.esgyn.local", 6, -1, -1);
            assert(rc == TCSUCCESS);
            rc = tcdbHbase.AddPNodeData("nap050.esgyn.local", 7, -1, -1);
            assert(rc == TCSUCCESS);
            rc = tcdbHbase.AddPNodeData("nap051.esgyn.local", 8, -1, -1);
            assert(rc == TCSUCCESS);
            rc = tcdbHbase.AddPNodeData("nap052.esgyn.local", 9, -1, -1);
            assert(rc == TCSUCCESS);
            rc = tcdbHbase.AddPNodeData("nap053.esgyn.local", 10, -1, -1);
            assert(rc == TCSUCCESS);
            rc = tcdbHbase.AddPNodeData("nap054.esgyn.local", 11, -1, -1);
            assert(rc == TCSUCCESS);
    
            rc = tcdbHbase.AddLNodeData(0, 0, 0, 7, 2, 39);
            assert(rc == TCSUCCESS);
            rc = tcdbHbase.AddLNodeData(1, 1, 0, 7, 2, 39);
            assert(rc == TCSUCCESS);
            rc = tcdbHbase.AddLNodeData(2, 2, 0, 7, 2, 39);
            assert(rc == TCSUCCESS);
            rc = tcdbHbase.AddLNodeData(3, 3, 0, 7, 2, 39);
            assert(rc == TCSUCCESS);
            rc = tcdbHbase.AddLNodeData(4, 4, 0, 7, 2, 39);
            assert(rc == TCSUCCESS);
            rc = tcdbHbase.AddLNodeData(5, 5, 0, 7, 2, 39);
            assert(rc == TCSUCCESS);
            rc = tcdbHbase.AddLNodeData(6, 6, 0, 7, 2, 39);
            assert(rc == TCSUCCESS);
            rc = tcdbHbase.AddLNodeData(7, 7, 0, 7, 2, 39);
            assert(rc == TCSUCCESS);
            rc = tcdbHbase.AddLNodeData(8, 8, 0, 7, 2, 39);
            assert(rc == TCSUCCESS);
            rc = tcdbHbase.AddLNodeData(9, 9, 0, 7, 2, 39);
            assert(rc == TCSUCCESS);
            rc = tcdbHbase.AddLNodeData(10, 10, 0, 7, 2, 39);
            assert(rc == TCSUCCESS);
            rc = tcdbHbase.AddLNodeData(11, 11, 0, 7, 2, 39);
            assert(rc == TCSUCCESS);
    
            for (int i = 0; i < 1; i++)
            {
                std::string nodeName = "nap099.esgyn.local";
                rc = tcdbHbase.AddSNodeData(nodeName.c_str()
                                           ,i        // pnid
                                           ,i        // spnid
                                           ,0        // firstCore
                                           ,7);      // lastCore
                assert(rc == TCSUCCESS);
            }
    
            rc = tcdbHbase.AddRegistryProcessData("$TM0", "DTM_INCARNATION_NUM", "0");
            assert(rc == TCSUCCESS);
            rc = tcdbHbase.AddRegistryProcessData("$TM0", "DTM_NEXT_SEQNUM_BLOCK", "10001");
            assert(rc == TCSUCCESS);
            rc = tcdbHbase.AddRegistryProcessData("$TM1", "DTM_INCARNATION_NUM", "0");
            assert(rc == TCSUCCESS);
            rc = tcdbHbase.AddRegistryProcessData("$TM1", "DTM_NEXT_SEQNUM_BLOCK", "10001");
            assert(rc == TCSUCCESS);
            rc = tcdbHbase.AddRegistryProcessData("$TM2", "DTM_INCARNATION_NUM", "0");
            assert(rc == TCSUCCESS);
            rc = tcdbHbase.AddRegistryProcessData("$TM2", "DTM_NEXT_SEQNUM_BLOCK", "10001");
            assert(rc == TCSUCCESS);
            rc = tcdbHbase.AddRegistryProcessData("$TM3", "DTM_INCARNATION_NUM", "0");
            assert(rc == TCSUCCESS);
            rc = tcdbHbase.AddRegistryProcessData("$TM3", "DTM_NEXT_SEQNUM_BLOCK", "10001");
            assert(rc == TCSUCCESS);
            rc = tcdbHbase.AddRegistryProcessData("$TM4", "DTM_INCARNATION_NUM", "0");
            assert(rc == TCSUCCESS);
            rc = tcdbHbase.AddRegistryProcessData("$TM4", "DTM_NEXT_SEQNUM_BLOCK", "10001");
            assert(rc == TCSUCCESS);
            rc = tcdbHbase.AddRegistryProcessData("$TM5", "DTM_INCARNATION_NUM", "0");
            assert(rc == TCSUCCESS);
            rc = tcdbHbase.AddRegistryProcessData("$TM5", "DTM_NEXT_SEQNUM_BLOCK", "10001");
            assert(rc == TCSUCCESS);
            rc = tcdbHbase.AddRegistryProcessData("$TM6", "DTM_INCARNATION_NUM", "0");
            assert(rc == TCSUCCESS);
            rc = tcdbHbase.AddRegistryProcessData("$TM6", "DTM_NEXT_SEQNUM_BLOCK", "10001");
            assert(rc == TCSUCCESS);
            rc = tcdbHbase.AddRegistryProcessData("$TM7", "DTM_INCARNATION_NUM", "0");
            assert(rc == TCSUCCESS);
            rc = tcdbHbase.AddRegistryProcessData("$TM7", "DTM_NEXT_SEQNUM_BLOCK", "10001");
            assert(rc == TCSUCCESS);
            rc = tcdbHbase.AddRegistryProcessData("$TM8", "DTM_INCARNATION_NUM", "0");
            assert(rc == TCSUCCESS);
            rc = tcdbHbase.AddRegistryProcessData("$TM8", "DTM_NEXT_SEQNUM_BLOCK", "10001");
            assert(rc == TCSUCCESS);
            rc = tcdbHbase.AddRegistryProcessData("$TM9", "DTM_INCARNATION_NUM", "0");
            assert(rc == TCSUCCESS);
            rc = tcdbHbase.AddRegistryProcessData("$TM9", "DTM_NEXT_SEQNUM_BLOCK", "10001");
            assert(rc == TCSUCCESS);
            rc = tcdbHbase.AddRegistryProcessData("$TM10", "DTM_INCARNATION_NUM", "0");
            assert(rc == TCSUCCESS);
            rc = tcdbHbase.AddRegistryProcessData("$TM10", "DTM_NEXT_SEQNUM_BLOCK", "10001");
            assert(rc == TCSUCCESS);
            rc = tcdbHbase.AddRegistryProcessData("$TM11", "DTM_INCARNATION_NUM", "0");
            assert(rc == TCSUCCESS);
            rc = tcdbHbase.AddRegistryProcessData("$TM11", "DTM_NEXT_SEQNUM_BLOCK", "10001");
            assert(rc == TCSUCCESS);
    
            rc = tcdbHbase.AddRegistryClusterData("SQ_MBTYPE", "64");
            assert(rc == TCSUCCESS);
            rc = tcdbHbase.AddRegistryClusterData("MY_NODES", "2");
            assert(rc == TCSUCCESS);
            rc = tcdbHbase.AddRegistryClusterData("JAVA_HOME", "/usr/lib/jvm/java-1.8.0-openjdk-1.8.0.131-0.b11.el6_9.x86_64");
            assert(rc == TCSUCCESS);
            rc = tcdbHbase.AddRegistryClusterData("MY_CLUSTER_ID", "0");
            assert(rc == TCSUCCESS);
            rc = tcdbHbase.AddRegistryClusterData("TRAF_FOUNDATION_READY", "1");
            assert(rc == TCSUCCESS);
            rc = tcdbHbase.AddRegistryClusterData("DTM_RUN_MODE", "2");
            assert(rc == TCSUCCESS);
            rc = tcdbHbase.AddRegistryClusterData("SQ_AUDITSVC_READY", "1");
            assert(rc == TCSUCCESS);
            rc = tcdbHbase.AddRegistryClusterData("DTM_TLOG_PER_TM", "1");
            assert(rc == TCSUCCESS);
            rc = tcdbHbase.AddRegistryClusterData("TRAF_TM_LOCKED", "0");
            assert(rc == TCSUCCESS);
            rc = tcdbHbase.AddRegistryClusterData("DTM_RECOVERING_TX_COUNT", "0");
            assert(rc == TCSUCCESS);
            rc = tcdbHbase.AddRegistryClusterData("SQ_TXNSVC_READY", "1");
            assert(rc == TCSUCCESS);
    
            rc = tcdbHbase.AddRegistryPersistentData("LOB_PROCESS_NAME", "$ZLOBSRV%nid+");
            assert(rc == TCSUCCESS);
            rc = tcdbHbase.AddRegistryPersistentData("LOB_PROCESS_TYPE", "PERSIST");
            assert(rc == TCSUCCESS);
            rc = tcdbHbase.AddRegistryPersistentData("LOB_PROGRAM_NAME", "mxlobsrvr");
            assert(rc == TCSUCCESS);
            rc = tcdbHbase.AddRegistryPersistentData("LOB_PROGRAM_ARGS", "");
            assert(rc == TCSUCCESS);
            rc = tcdbHbase.AddRegistryPersistentData("LOB_REQUIRES_DTM", "N");
            assert(rc == TCSUCCESS);
            rc = tcdbHbase.AddRegistryPersistentData("LOB_STDOUT", "stdout_ZLOBSRV%nid");
            assert(rc == TCSUCCESS);
            rc = tcdbHbase.AddRegistryPersistentData("LOB_PERSIST_RETRIES", "10,60");
            assert(rc == TCSUCCESS);
            rc = tcdbHbase.AddRegistryPersistentData("LOB_PERSIST_ZONES", "%zid");
            assert(rc == TCSUCCESS);
            rc = tcdbHbase.AddRegistryPersistentData("PERSIST_PROCESS_KEYS", "DTM,TMID,SSCP,SSMP,PSD,WDG,CMON,NMON,TNFY,LOB");
            assert(rc == TCSUCCESS);
            char persistProcessKeys[100];
            persistProcessKeys[0] = '\0';
            rc = tcdbHbase.GetPersistProcessKeys(persistProcessKeys);
            assert(rc == TCSUCCESS);
            std::cout << "persist process keys=" << persistProcessKeys << std::endl;
    
            rc = tcdbHbase.AddUniqueString(0, 1,  "shell");
            assert(rc == TCSUCCESS);
            rc = tcdbHbase.AddUniqueString(0, 2,  "pstartd");
            assert(rc == TCSUCCESS);
            rc = tcdbHbase.AddUniqueString(0, 3,  "sqwatchdog");
            assert(rc == TCSUCCESS);
            rc = tcdbHbase.AddUniqueString(0, 4,  "/opt/trafodion/esgynDB-2.5.0/sql/scripts/idtmsrv");
            assert(rc == TCSUCCESS);
            rc = tcdbHbase.AddUniqueString(0, 5,  "/opt/trafodion/esgynDB-2.5.0/sql/scripts/tm");
            assert(rc == TCSUCCESS);
            rc = tcdbHbase.AddUniqueString(0, 6,  "/opt/trafodion/esgynDB-2.5.0/sql/scripts/service_monitor");
            assert(rc == TCSUCCESS);
            rc = tcdbHbase.AddUniqueString(0, 7,  "/opt/trafodion/esgynDB-2.5.0/sql/scripts/mxsscp");
            assert(rc == TCSUCCESS);
            rc = tcdbHbase.AddUniqueString(0, 8,  "/opt/trafodion/esgynDB-2.5.0/sql/scripts/mxssmp");
            assert(rc == TCSUCCESS);
            rc = tcdbHbase.AddUniqueString(0, 9,  "/opt/trafodion/esgynDB-2.5.0/sql/scripts/run_command");
            assert(rc == TCSUCCESS);
            rc = tcdbHbase.AddUniqueString(0, 10, "/opt/trafodion/esgynDB-2.5.0/sql/scripts/mxosrvr");
            assert(rc == TCSUCCESS);
            rc = tcdbHbase.AddUniqueString(0, 11, "/opt/trafodion/esgynDB-2.5.0/sql/scripts/tdm_arkesp");
            assert(rc == TCSUCCESS);
            rc = tcdbHbase.AddUniqueString(0, 12, "/opt/trafodion/esgynDB-2.5.0/sql/scripts/tdm_arkcmp");
            assert(rc == TCSUCCESS);
            rc = tcdbHbase.AddUniqueString(0, 13, "/opt/trafodion/esgynDB-2.5.0/sql/scripts/traf_notify");
            assert(rc == TCSUCCESS);
            rc = tcdbHbase.AddUniqueString(0, 14, "/opt/trafodion/esgynDB-2.5.0/sql/scripts/mxlobsrvr");
            assert(rc == TCSUCCESS);
            rc = tcdbHbase.AddUniqueString(1, 1,  "shell");
            assert(rc == TCSUCCESS);
            rc = tcdbHbase.AddUniqueString(1, 2,  "pstartd");
            assert(rc == TCSUCCESS);
            rc = tcdbHbase.AddUniqueString(1, 3,  "sqwatchdog");
            assert(rc == TCSUCCESS);
            rc = tcdbHbase.AddUniqueString(1, 4,  "/opt/trafodion/esgynDB-2.5.0/sql/scripts/idtmsrv");
            assert(rc == TCSUCCESS);
            rc = tcdbHbase.AddUniqueString(1, 5,  "/opt/trafodion/esgynDB-2.5.0/sql/scripts/tm");
            assert(rc == TCSUCCESS);
            rc = tcdbHbase.AddUniqueString(1, 6,  "/opt/trafodion/esgynDB-2.5.0/sql/scripts/service_monitor");
            assert(rc == TCSUCCESS);
            rc = tcdbHbase.AddUniqueString(1, 7,  "/opt/trafodion/esgynDB-2.5.0/sql/scripts/mxsscp");
            assert(rc == TCSUCCESS);
            rc = tcdbHbase.AddUniqueString(1, 8,  "/opt/trafodion/esgynDB-2.5.0/sql/scripts/mxssmp");
            assert(rc == TCSUCCESS);
            rc = tcdbHbase.AddUniqueString(1, 9,  "/opt/trafodion/esgynDB-2.5.0/sql/scripts/run_command");
            assert(rc == TCSUCCESS);
            rc = tcdbHbase.AddUniqueString(1, 10, "/opt/trafodion/esgynDB-2.5.0/sql/scripts/mxosrvr");
            assert(rc == TCSUCCESS);
            rc = tcdbHbase.AddUniqueString(1, 11, "/opt/trafodion/esgynDB-2.5.0/sql/scripts/tdm_arkesp");
            assert(rc == TCSUCCESS);
            rc = tcdbHbase.AddUniqueString(1, 12, "/opt/trafodion/esgynDB-2.5.0/sql/scripts/tdm_arkcmp");
            assert(rc == TCSUCCESS);
            rc = tcdbHbase.AddUniqueString(1, 13, "/opt/trafodion/esgynDB-2.5.0/sql/scripts/traf_notify");
            assert(rc == TCSUCCESS);
            rc = tcdbHbase.AddUniqueString(1, 14, "/opt/trafodion/esgynDB-2.5.0/sql/scripts/mxlobsrvr");
            assert(rc == TCSUCCESS);
            rc = tcdbHbase.AddUniqueString(2, 1,  "shell");
            assert(rc == TCSUCCESS);
            rc = tcdbHbase.AddUniqueString(2, 2,  "pstartd");
            assert(rc == TCSUCCESS);
            rc = tcdbHbase.AddUniqueString(2, 3,  "sqwatchdog");
            assert(rc == TCSUCCESS);
            rc = tcdbHbase.AddUniqueString(2, 4,  "/opt/trafodion/esgynDB-2.5.0/sql/scripts/idtmsrv");
            assert(rc == TCSUCCESS);
            rc = tcdbHbase.AddUniqueString(2, 5,  "/opt/trafodion/esgynDB-2.5.0/sql/scripts/tm");
            assert(rc == TCSUCCESS);
            rc = tcdbHbase.AddUniqueString(2, 6,  "/opt/trafodion/esgynDB-2.5.0/sql/scripts/service_monitor");
            assert(rc == TCSUCCESS);
            rc = tcdbHbase.AddUniqueString(2, 7,  "/opt/trafodion/esgynDB-2.5.0/sql/scripts/mxsscp");
            assert(rc == TCSUCCESS);
            rc = tcdbHbase.AddUniqueString(2, 8,  "/opt/trafodion/esgynDB-2.5.0/sql/scripts/mxssmp");
            assert(rc == TCSUCCESS);
            rc = tcdbHbase.AddUniqueString(2, 9,  "/opt/trafodion/esgynDB-2.5.0/sql/scripts/run_command");
            assert(rc == TCSUCCESS);
            rc = tcdbHbase.AddUniqueString(2, 10, "/opt/trafodion/esgynDB-2.5.0/sql/scripts/mxosrvr");
            assert(rc == TCSUCCESS);
            rc = tcdbHbase.AddUniqueString(2, 11, "/opt/trafodion/esgynDB-2.5.0/sql/scripts/tdm_arkesp");
            assert(rc == TCSUCCESS);
            rc = tcdbHbase.AddUniqueString(2, 12, "/opt/trafodion/esgynDB-2.5.0/sql/scripts/tdm_arkcmp");
            assert(rc == TCSUCCESS);
            rc = tcdbHbase.AddUniqueString(2, 13, "/opt/trafodion/esgynDB-2.5.0/sql/scripts/traf_notify");
            assert(rc == TCSUCCESS);
            rc = tcdbHbase.AddUniqueString(2, 14, "/opt/trafodion/esgynDB-2.5.0/sql/scripts/mxlobsrvr");
            assert(rc == TCSUCCESS);
    
            for (int i = 0; i < 2; i++)
            {
                std::string nodeName = "n" + std::to_string(i);
                rc = tcdbHbase.AddNameServer(nodeName.c_str());
                assert(rc == TCSUCCESS);
            }
    
            std::cout << "Added 10 pnodes" << std::endl;
            std::cout << "Added 10 lnodes" << std::endl;
            std::cout << "Added 1 snodes" << std::endl;
            std::cout << "Added 5 mreg" << std::endl;
            std::cout << "Added 8 mrpd" << std::endl;
            std::cout << "Added 3 mrus" << std::endl;
            std::cout << "Added 2 mrns" << std::endl;
    
            std::cout << "delete node data pNid=5" << std::endl;
            rc = tcdbHbase.DeleteNodeData(5);
            assert(rc == TCSUCCESS);
            std::cout << "delete mrus nid=1" << std::endl;
            rc = tcdbHbase.DeleteUniqueString(1);
            assert(rc == TCSUCCESS);
        }

        std::cout << "GetNode(1)" << std::endl;
        TcNodeConfiguration_t nodeConfig1;
        rc = tcdbHbase.GetNode(1, nodeConfig1);
        if (!readDb)
            assert(rc == TCSUCCESS);
        std::cout << "GetNode(nap053.esgyn.local)" << std::endl;
        TcNodeConfiguration_t nodeConfig2;
        rc = tcdbHbase.GetNode("nap053.esgyn.local", nodeConfig2);
        if (!readDb)
            assert(rc == TCSUCCESS);
        std::cout << "GetPNode(2)" << std::endl;
        TcPhysicalNodeConfiguration_t pnodeConfig1;
        rc = tcdbHbase.GetPNode(2, pnodeConfig1);
        if (!readDb)
            assert(rc == TCSUCCESS);
        std::cout << "GetPNode(nap053.esgyn.local)" << std::endl;
        TcPhysicalNodeConfiguration_t pnodeConfig2;
        rc = tcdbHbase.GetPNode("nap053.esgyn.local", pnodeConfig2);
        if (!readDb)
            assert(rc == TCSUCCESS);
        std::cout << "GetSNodes()" << std::endl;
        TcPhysicalNodeConfiguration_t spareNodeConfig[20];
        int snConfigCnt = -1;
        rc = tcdbHbase.GetSNodes(snConfigCnt
                                ,20 // max
                                ,spareNodeConfig);
        assert(rc == TCSUCCESS);
        std::cout << "GetPersistProcess()" << std::endl;
        TcPersistConfiguration_t persistConfig1;
        rc = tcdbHbase.GetPersistProcess("LOB_", persistConfig1);
        if (!readDb)
            assert(rc == TCSUCCESS);
        std::cout << "GetRegistryClusterSet()" << std::endl;
        TcRegistryConfiguration_t registryConfig1[20];
        int regConfigCnt1 = -1;
        rc = tcdbHbase.GetRegistryClusterSet(regConfigCnt1
                                            ,20 // max
                                            ,registryConfig1);
        assert(rc == TCSUCCESS);
        std::cout << "GetRegistryProcessSet()" << std::endl;
        TcRegistryConfiguration_t registryConfig2[20];
        int regConfigCnt2 = -1;
        rc = tcdbHbase.GetRegistryProcessSet(regConfigCnt2
                                            ,20 // max
                                            ,registryConfig2);
        assert(rc == TCSUCCESS);
        for (int inx = 0; inx < regConfigCnt2; inx++)
        {
            TcRegistryConfiguration_t *reg = &registryConfig2[inx];
            printf("s=%s, k=%s, v=%s\n",
                   reg->scope, reg->key, reg->value);
        }
    
        TcNodeConfiguration_t nodeConfigs[20];
        int configCnt = 0;
        rc = tcdbHbase.GetNodes(configCnt
                               ,20 // max
                               ,nodeConfigs);
        assert(rc == TCSUCCESS);
        std::cout << "GetNameServers()" << std::endl;
        int nsCnt = 0;
        char *nodeNames[10];
        rc = tcdbHbase.GetNameServers(&nsCnt, 10, nodeNames);
        assert(rc == TCSUCCESS);
        for (int inx = 0; inx < nsCnt; inx++)
            std::cout << "ns=" << nodeNames[inx] << std::endl;
        char uniqStr1[100];
        rc = tcdbHbase.GetUniqueString(0, 1, uniqStr1);
        assert(rc == TCSUCCESS);
        std::cout << "uniqStr=" << uniqStr1 << std::endl;
        const char *uniqStr2 = "sqwatchdog";
        int uniqId1;
        rc = tcdbHbase.GetUniqueStringId(0, uniqStr2, uniqId1);
        assert(rc == TCSUCCESS);
        std::cout << "id=" << uniqId1 << std::endl;
        int uniqId2;
        rc = tcdbHbase.GetUniqueStringIdMax(0, uniqId2);
        assert(rc == TCSUCCESS);
        std::cout << "id=" << uniqId2 << std::endl;

        // negative!
        std::cout << "DeleteNameServer(99)" << std::endl;
        rc = tcdbHbase.DeleteNameServer("99");
        assert(rc != TCSUCCESS);
        std::cout << "DeleteNodeData(99)" << std::endl;
        rc = tcdbHbase.DeleteNodeData(99);
        assert(rc != TCSUCCESS);
        std::cout << "DeleteUniqueString(99)" << std::endl;
        rc = tcdbHbase.DeleteUniqueString(1);
        if (!readDb)
            assert(rc != TCSUCCESS);
        std::cout << "GetNode(99)" << std::endl;
        TcNodeConfiguration_t nodeConfig3;
        rc = tcdbHbase.GetNode(99, nodeConfig3);
        assert(rc != TCSUCCESS);
        std::cout << "GetNode('99')" << std::endl;
        TcNodeConfiguration_t nodeConfig4;
        rc = tcdbHbase.GetNode("99", nodeConfig4);
        assert(rc != TCSUCCESS);
        std::cout << "GetPNode(99)" << std::endl;
        TcPhysicalNodeConfiguration_t pnodeConfig3;
        rc = tcdbHbase.GetPNode(99, pnodeConfig3);
        assert(rc != TCSUCCESS);
        std::cout << "GetPNode('99')" << std::endl;
        TcPhysicalNodeConfiguration_t pnodeConfig4;
        rc = tcdbHbase.GetPNode("99", pnodeConfig4);
        assert(rc != TCSUCCESS);
        std::cout << "GetPersistProcess()" << std::endl;
        TcPersistConfiguration_t persistConfig2;
        rc = tcdbHbase.GetPersistProcess("ZZZ_", persistConfig2);
        assert(rc != TCSUCCESS);
        char uniqStr3[100];
        rc = tcdbHbase.GetUniqueString(99, 1, uniqStr3);
        assert(rc != TCSUCCESS);
        int uniqId3;
        rc = tcdbHbase.GetUniqueStringId(99, "zzz", uniqId3);
        assert(rc != TCSUCCESS);

    } catch (const TException &tx)
    {
        std::cerr << "ERROR: " << tx.what() << std::endl;
    }
}
