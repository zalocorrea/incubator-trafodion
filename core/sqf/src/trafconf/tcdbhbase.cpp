///////////////////////////////////////////////////////////////////////////////
//
// @@@ START COPYRIGHT @@@
//
// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
//
// @@@ END COPYRIGHT @@@
// 
///////////////////////////////////////////////////////////////////////////////

#ifdef USE_HBASE

#include <iostream>
#include <limits.h>
#include <string.h>
#include <vector>

#include <thrift/transport/TSocket.h>
#include <thrift/transport/TBufferTransports.h>
#include <thrift/protocol/TBinaryProtocol.h>

#include "gen-cpp/Hbase.h"

#include "tcdbhbase.h"
#include "tclog.h"
#include "tctrace.h"

using namespace apache::thrift;
using namespace apache::thrift::protocol;
using namespace apache::thrift::transport;
using namespace apache::hadoop::hbase::thrift;

typedef std::map<std::string,TCell>            CellMap;
typedef std::vector<TCell>                     CellVec;
typedef std::map<std::string,ColumnDescriptor> ColMap;
typedef std::vector<ColumnDescriptor>          ColVec;
typedef std::map<std::string,std::string>      StrMap;
typedef std::vector<std::string>               StrVec;

// A little kludgy!
// These would normally be in the class CTcdbHbase definition
// but it gets messy
static apache::hadoop::hbase::thrift::HbaseClient *client_    = NULL;
static std::shared_ptr<TTransport>                 transport_ = NULL;

class ScannerMgr {
public:
    ScannerMgr(int scanner);
    ~ScannerMgr();

private:
    int scanner_;
};

    
CTcdbHbase::CTcdbHbase()
: CTcdbStore( TCDBHBASE )
, inited_(false)
{
    const char method_name[] = "CTcdbHbase::CTcdbHbase";
    TRACE_ENTRY;

    memcpy(&eyecatcher_, "TCHB", 4);
    config_ = new CTcdbHbaseConfig();

    TRACE_EXIT;
}

CTcdbHbase::~CTcdbHbase()
{
    const char method_name[] = "CTcdbHbase::~CTcdbHbase";
    TRACE_ENTRY;

    memcpy(&eyecatcher_, "tchb", 4);
    delete config_;

    TRACE_EXIT;
}

int CTcdbHbase::AddLNodeData( int         nid
                            , int         pnid
                            , int         firstCore
                            , int         lastCore
                            , int         processors
                            , int         roles )
{
    const char method_name[] = "CTcdbHbase::AddLNodeData";
    TRACE_ENTRY;

    if ( !IsInitialized() )  
    {
        if (TcTraceSettings & (TC_TRACE_REGISTRY | TC_TRACE_REQUEST | TC_TRACE_INIT))
        {
            trace_printf( "%s@%d Database is not initialized for access!\n"
                        , method_name, __LINE__);
        }
        TRACE_EXIT;
        return( TCNOTINIT );
    }
    
    if (TcTraceSettings & (TC_TRACE_NODE | TC_TRACE_REQUEST))
    {
        trace_printf( "%s@%d inserting into LNODE values (lNid=%d, pNid=%d, "
                      "processors=%d, roles=%d, firstCore=%d, lastCore=%d)\n"
                     , method_name, __LINE__
                     , nid
                     , pnid
                     , processors
                     , roles
                     , firstCore
                     , lastCore );
    }

    // A bit of cheating going on here
    // We know that an AddLNodeData is preceeded by an AddPNodeData
    // so we fetch the pnode data and add it to our lnode data
    std::string startRow = config_->keyPN(pnid);
    std::string stopRow = config_->keyPN(pnid + 1);
    std::string nodeName;
    std::string excFirstCore;
    std::string excLastCore;
    int scanner = client_->scannerOpenWithStop(config_->tbl_, startRow, stopRow, config_->tblPnodeCols_, config_->attr_);
    ScannerMgr scanMgr(scanner);
    
    // scan pnode table for pnid, then extract data for it
    try
    {
        while ( true )
        {
            std::vector<TRowResult> value;
            client_->scannerGet(value, scanner);
            if (value.size() == 0)
                break;
            config_->tsPnode_.addScan(value.size());
            for (size_t i = 0; i < value.size(); i++)
            {
                for (CellMap::const_iterator it = value[i].columns.begin();
                     it != value[i].columns.end(); ++it)
                {
                    std::string key = it->first;
                    std::string value = it->second.value;
                    if (key == config_->tblPnodeColNodeName_)
                        nodeName = value;
                     else if (key == config_->tblPnodeColExcFirstCore_)
                        excFirstCore = value;
                     else if (key == config_->tblPnodeColExcLastCore_)
                        excLastCore = value;
                }
            }
        }
    }
    catch ( const IOError &ioe )
    {
        char buf[TC_LOG_BUF_SIZE];
        snprintf( buf, sizeof(buf)
                , "[%s] scanner IOError: %s\n"
                , method_name, ioe.what() );
        TcLogWrite( HBASE_DB_ACCESS_ERROR, TC_LOG_ERR, buf );
        TRACE_EXIT;
        return( TCDBOPERROR );
    }

    std::vector<Mutation> mutations;
    mutations.push_back(Mutation());
    mutations.back().column = config_->tblLnodeColLnid_;
    mutations.back().value = config_->encInt(nid);
    mutations.push_back(Mutation());
    mutations.back().column = config_->tblLnodeColPnid_;
    mutations.back().value = config_->encInt(pnid);
    mutations.push_back(Mutation());
    mutations.back().column = config_->tblLnodeColProcessors_;
    mutations.back().value = config_->encInt(processors);
    mutations.push_back(Mutation());
    mutations.back().column = config_->tblLnodeColRoles_;
    mutations.back().value = config_->encInt(roles);
    mutations.push_back(Mutation());
    mutations.back().column = config_->tblLnodeColFirstCore_;
    mutations.back().value = config_->encInt(firstCore);
    mutations.push_back(Mutation());
    mutations.back().column = config_->tblLnodeColLastCore_;
    mutations.back().value = config_->encInt(lastCore);

    // adding pnode column here
    mutations.push_back(Mutation());
    mutations.back().column = config_->tblLnodeColPNodeName_;
    mutations.back().value = nodeName;
    mutations.push_back(Mutation());
    mutations.back().column = config_->tblLnodeColPExcFirstCore_;
    mutations.back().value = excFirstCore;
    mutations.push_back(Mutation());
    mutations.back().column = config_->tblLnodeColPExcLastCore_;
    mutations.back().value = excLastCore;

    std::string row = config_->keyLN(nid);
    // tableName, row, mutations, attributes
    client_->mutateRow(config_->tbl_, row, mutations, config_->attr_);
    config_->tsLnode_.addInsert(1);

    TRACE_EXIT;
    return( TCSUCCESS );
}

int CTcdbHbase::AddNameServer( const char *nodeName )
{
    const char method_name[] = "CTcdbHbase::AddNameServer";
    TRACE_ENTRY;

    if ( !IsInitialized() )  
    {
        if (TcTraceSettings & (TC_TRACE_REGISTRY | TC_TRACE_REQUEST | TC_TRACE_INIT))
        {
            trace_printf( "%s@%d Database is not initialized for access!\n"
                        , method_name, __LINE__);
        }
        TRACE_EXIT;
        return( TCNOTINIT );
    }
    
//  if (TcTraceSettings & (TC_TRACE_NAMESERVER | TC_TRACE_REQUEST))
    if (TcTraceSettings & (TC_TRACE_REQUEST))
    {
        trace_printf( "%s@%d inserting into MRNS values (node=%s)\n"
                     , method_name, __LINE__
                     , nodeName );
    }

    std::vector<Mutation> mutations;
    mutations.clear();
    mutations.push_back(Mutation());
    mutations.back().column = config_->tblMrnsColKeyName_;
    mutations.back().value = nodeName;
    mutations.push_back(Mutation());
    mutations.back().column = config_->tblMrnsColValueName_;
    mutations.back().value = nodeName;
    std::string row = config_->keyMRNS(nodeName);
    // tableName, row, mutations, attributes
    client_->mutateRow(config_->tbl_, row, mutations, config_->attr_);
    config_->tsMrns_.addInsert(1);

    TRACE_EXIT;
    return( TCSUCCESS );
}

int CTcdbHbase::AddPNodeData( const char *name
                            , int         pnid
                            , int         excludedFirstCore
                            , int         excludedLastCore )
{
    const char method_name[] = "CTcdbHbase::AddPNodeData";
    TRACE_ENTRY;

    if ( !IsInitialized() )  
    {
        if (TcTraceSettings & (TC_TRACE_REGISTRY | TC_TRACE_REQUEST | TC_TRACE_INIT))
        {
            trace_printf( "%s@%d Database is not initialized for access!\n"
                        , method_name, __LINE__);
        }
        TRACE_EXIT;
        return( TCNOTINIT );
    }

    if (TcTraceSettings & (TC_TRACE_NODE | TC_TRACE_REQUEST))
    {
        trace_printf( "%s@%d inserting into PNODE values (pNid=%d, "
                      "nodeName=%s, excFirstCore=%d, excLastCore=%d)\n"
                     , method_name, __LINE__
                     , pnid
                     , name
                     , excludedFirstCore
                     , excludedLastCore );
    }

    std::vector<Mutation> mutations;
    mutations.push_back(Mutation());
    mutations.back().column = config_->tblPnodeColPnid_;
    mutations.back().value = config_->encInt(pnid);
    mutations.push_back(Mutation());
    mutations.back().column = config_->tblPnodeColNodeName_;
    mutations.back().value = name;
    mutations.push_back(Mutation());
    mutations.back().column = config_->tblPnodeColExcFirstCore_;
    mutations.back().value = config_->encInt(excludedFirstCore);
    mutations.push_back(Mutation());
    mutations.back().column = config_->tblPnodeColExcLastCore_;
    mutations.back().value = config_->encInt(excludedLastCore);
    std::string row = config_->keyPN(pnid);
    // tableName, row, mutations, attributes
    client_->mutateRow(config_->tbl_, row, mutations, config_->attr_);
    config_->tsPnode_.addInsert(1);

    TRACE_EXIT;
    return( TCSUCCESS );
}

int CTcdbHbase::AddRegistryClusterData( const char *key
                                      , const char *dataValue )
{
    const char method_name[] = "CTcdbHbase::AddRegistryClusterData";
    TRACE_ENTRY;

    if ( !IsInitialized() )  
    {
        if (TcTraceSettings & (TC_TRACE_REGISTRY | TC_TRACE_REQUEST | TC_TRACE_INIT))
        {
            trace_printf( "%s@%d Database is not initialized for access!\n"
                        , method_name, __LINE__);
        }
        TRACE_EXIT;
        return( TCNOTINIT );
    }

    if (TcTraceSettings & (TC_TRACE_REGISTRY | TC_TRACE_REQUEST))
    {
        trace_printf( "%s@%d inserting into MRCD values (key=%s)\n",
                      method_name, __LINE__, key );
    }

    std::vector<Mutation> mutations;
    mutations.push_back(Mutation());
    mutations.back().column = config_->tblMrcdColKeyName_;
    mutations.back().value = key;
    mutations.push_back(Mutation());
    mutations.back().column = config_->tblMrcdColDataValue_;
    mutations.back().value = dataValue;
    std::string row = config_->keyMRCD(key);
    // tableName, row, mutations, attributes
    client_->mutateRow(config_->tbl_, row, mutations, config_->attr_);
    config_->tsMrcd_.addInsert(1);

    TRACE_EXIT;
    return( TCSUCCESS );
}

int CTcdbHbase::AddRegistryKey( const char * /*key*/ )
{
    const char method_name[] = "CTcdbHbase::AddRegistryKey";
    TRACE_ENTRY;

    if ( !IsInitialized() )  
    {
        if (TcTraceSettings & (TC_TRACE_REGISTRY | TC_TRACE_REQUEST | TC_TRACE_INIT))
        {
            trace_printf( "%s@%d Database is not initialized for access!\n"
                        , method_name, __LINE__);
        }
        TRACE_EXIT;
        return( TCNOTINIT );
    }

    if (TcTraceSettings & (TC_TRACE_REGISTRY | TC_TRACE_REQUEST))
    {
        trace_printf( "%s@%d NOTHING TO DO\n",
                      method_name, __LINE__ );
    }
    TRACE_EXIT;
    return( TCSUCCESS );
}

int CTcdbHbase::AddRegistryPersistentData( const char *keyName
                                         , const char *valueName )
{
    const char method_name[] = "CTcdbHbase::AddRegistryPersistentData";
    TRACE_ENTRY;

    if ( !IsInitialized() )  
    {
        if (TcTraceSettings & (TC_TRACE_REGISTRY | TC_TRACE_REQUEST | TC_TRACE_INIT))
        {
            trace_printf( "%s@%d Database is not initialized for access!\n"
                        , method_name, __LINE__);
        }
        TRACE_EXIT;
        return( TCNOTINIT );
    }

    if (TcTraceSettings & (TC_TRACE_REGISTRY | TC_TRACE_REQUEST))
    {
        trace_printf( "%s@%d inserting into MRPD values (key=%s, value=%s)\n",
                      method_name, __LINE__, keyName, valueName );
    }

    std::vector<Mutation> mutations;
    mutations.push_back(Mutation());
    mutations.back().column = config_->tblMrpdColKeyName_;
    mutations.back().value = keyName;
    mutations.push_back(Mutation());
    mutations.back().column = config_->tblMrpdColValueName_;
    mutations.back().value = valueName;
    std::string row = config_->keyMRPD(keyName);
    // tableName, row, mutations, attributes
    client_->mutateRow(config_->tbl_, row, mutations, config_->attr_);
    config_->tsMrpd_.addInsert(1);

    TRACE_EXIT;
    return( TCSUCCESS );
}

int CTcdbHbase::AddRegistryProcess( const char * /*name*/ )
{
    const char method_name[] = "CTcdbHbase::AddRegistryProcess";
    TRACE_ENTRY;

    if ( !IsInitialized() )  
    {
        if (TcTraceSettings & (TC_TRACE_REGISTRY | TC_TRACE_REQUEST | TC_TRACE_INIT))
        {
            trace_printf( "%s@%d Database is not initialized for access!\n"
                        , method_name, __LINE__);
        }
        TRACE_EXIT;
        return( TCNOTINIT );
    }

    if (TcTraceSettings & (TC_TRACE_REGISTRY | TC_TRACE_REQUEST))
    {
        trace_printf( "%s@%d NOTHING TO DO\n",
                      method_name, __LINE__ );
    }
    TRACE_EXIT;
    return( TCSUCCESS );
}

int CTcdbHbase::AddRegistryProcessData( const char *procName
                                      , const char *key
                                      , const char *dataValue )
{
    const char method_name[] = "CTcdbHbase::AddRegistryProcessData";
    TRACE_ENTRY;

    if ( !IsInitialized() )  
    {
        if (TcTraceSettings & (TC_TRACE_REGISTRY | TC_TRACE_REQUEST | TC_TRACE_INIT))
        {
            trace_printf( "%s@%d Database is not initialized for access!\n"
                        , method_name, __LINE__);
        }
        TRACE_EXIT;
        return( TCNOTINIT );
    }

    if (TcTraceSettings & (TC_TRACE_REGISTRY | TC_TRACE_REQUEST))
    {
        trace_printf( "%s@%d inserting into MREG values (key=%s, proc=%s)\n",
                      method_name, __LINE__, key, procName );
    }

    std::vector<Mutation> mutations;
    mutations.push_back(Mutation());
    mutations.back().column = config_->tblMregColKeyName_;
    mutations.back().value = key;
    mutations.push_back(Mutation());
    mutations.back().column = config_->tblMregColProcName_;
    mutations.back().value = procName;
    mutations.push_back(Mutation());
    mutations.back().column = config_->tblMregColDataValue_;
    mutations.back().value = dataValue;
    std::string row = config_->keyMR(procName, key);
    // tableName, row, mutations, attributes
    client_->mutateRow(config_->tbl_, row, mutations, config_->attr_);
    config_->tsMreg_.addInsert(1);

    TRACE_EXIT;
    return( TCSUCCESS );
}

int CTcdbHbase::AddSNodeData( const char *name
                            , int         pNid
                            , int         spNid
                            , int         firstCore
                            , int         lastCore )
{
    const char method_name[] = "CTcdbHbase::AddSNodeData";
    TRACE_ENTRY;

    if ( !IsInitialized() )  
    {
        if (TcTraceSettings & (TC_TRACE_REGISTRY | TC_TRACE_REQUEST | TC_TRACE_INIT))
        {
            trace_printf( "%s@%d Database is not initialized for access!\n"
                        , method_name, __LINE__);
        }
        TRACE_EXIT;
        return( TCNOTINIT );
    }

    if (TcTraceSettings & (TC_TRACE_REGISTRY | TC_TRACE_REQUEST))
    {
        trace_printf( "%s@%d inserting into SNODE values (name=%s, pNid=%d, "
                      "spNid=%d, firstCore=%d, lastCore=%d)\n",
                      method_name, __LINE__, name, pNid, spNid, firstCore, lastCore );
    }

    std::vector<Mutation> mutations;
    mutations.push_back(Mutation());
    mutations.back().column = config_->tblSnodeColPnid_;
    mutations.back().value = config_->encInt(pNid);
    mutations.push_back(Mutation());
    mutations.back().column = config_->tblSnodeColSpnid_;
    mutations.back().value = config_->encInt(spNid);
    mutations.push_back(Mutation());
    mutations.back().column = config_->tblSnodeColNodeName_;
    mutations.back().value = name;
    mutations.push_back(Mutation());
    mutations.back().column = config_->tblSnodeColFirstCore_;
    mutations.back().value = config_->encInt(firstCore);
    mutations.push_back(Mutation());
    mutations.back().column = config_->tblSnodeColLastCore_;
    mutations.back().value = config_->encInt(lastCore);
    std::string row = config_->keySN(pNid, spNid);
    // tableName, row, mutations, attributes
    client_->mutateRow(config_->tbl_, row, mutations, config_->attr_);
    config_->tsSnode_.addInsert(1);

    TRACE_EXIT;
    return( TCSUCCESS );
}

int CTcdbHbase::AddUniqueString( int nid
                               , int id
                               , const char *uniqStr )
{
    const char method_name[] = "CTcdbHbase::AddUniqueString";
    TRACE_ENTRY;

    if ( !IsInitialized() )  
    {
        if (TcTraceSettings & (TC_TRACE_REGISTRY | TC_TRACE_REQUEST | TC_TRACE_INIT))
        {
            trace_printf( "%s@%d Database is not initialized for access!\n"
                        , method_name, __LINE__);
        }
        TRACE_EXIT;
        return( TCNOTINIT );
    }

    if (TcTraceSettings & (TC_TRACE_REGISTRY | TC_TRACE_REQUEST))
    {
        trace_printf( "%s@%d inserting into MRUS values (nid=%d, id=%d)\n",
                      method_name, __LINE__, nid, id );
    }

    std::vector<Mutation> mutations;
    mutations.push_back(Mutation());
    mutations.back().column = config_->tblMrusColNid_;
    mutations.back().value = config_->encInt(nid);
    mutations.push_back(Mutation());
    mutations.back().column = config_->tblMrusColId_;
    mutations.back().value = config_->encInt(id);
    mutations.push_back(Mutation());
    mutations.back().column = config_->tblMrusColDataValue_;
    mutations.back().value = uniqStr;
    std::string row = config_->keyMRUS(nid, id);
    // tableName, row, mutations, attributes
    client_->mutateRow(config_->tbl_, row, mutations, config_->attr_);
    config_->tsMrus_.addInsert(1);

    TRACE_EXIT;
    return( TCSUCCESS );
}

int CTcdbHbase::Close( void )
{
    const char method_name[] = "CTcdbHbase::Close";
    TRACE_ENTRY;

    if ( !IsInitialized() )  
    {
        if (TcTraceSettings & (TC_TRACE_REGISTRY | TC_TRACE_REQUEST | TC_TRACE_INIT))
        {
            trace_printf( "%s@%d Database is not initialized for access!\n"
                        , method_name, __LINE__ );
        }
        TRACE_EXIT;
        return( TCNOTINIT );
    }

    try
    {
        transport_->close();
        delete client_;
        client_ = NULL;
        transport_ = NULL;
    }
    catch ( const TException &tx )
    {
        char buf[TC_LOG_BUF_SIZE];
        snprintf( buf, sizeof(buf),
                  "[%s] error encountered: %s\n"
                , method_name,  tx.what() );
        TcLogWrite( HBASE_DB_ACCESS_ERROR, TC_LOG_CRIT, buf );
        TRACE_EXIT;
        return( TCDBOPERROR );
    }

    TRACE_EXIT;
    return( TCSUCCESS );
}

int CTcdbHbase::DeleteData( void )
{
    const char method_name[] = "CTcdbHbase::DeleteData";
    TRACE_ENTRY;

    if ( !IsInitialized() )  
    {
        if (TcTraceSettings & (TC_TRACE_REGISTRY | TC_TRACE_REQUEST | TC_TRACE_INIT))
        {
            trace_printf( "%s@%d Database is not initialized for access!\n"
                        , method_name, __LINE__ );
        }
        TRACE_EXIT;
        return( TCNOTINIT );
    }

    try
    {
        // Scan all tables, look for the trfconf table and delete it.
        StrVec tables;
        client_->getTableNames(tables);
        for (StrVec::const_iterator it = tables.begin(); it != tables.end(); ++it)
        {
            if (TcTraceSettings & (TC_TRACE_NODE | TC_TRACE_REQUEST))
            {
                trace_printf( "%s@%d scanning table=%s for delete\n"
                             , method_name, __LINE__, it->c_str() );
            }
            if (config_->tbl_ == *it)
            {
                if (TcTraceSettings & (TC_TRACE_NODE | TC_TRACE_REQUEST))
                {
                    trace_printf( "%s@%d deleting table=%s\n"
                                 , method_name, __LINE__, it->c_str() );
                }
                if (client_->isTableEnabled(*it))
                {
                    client_->disableTable(*it);
                }
                client_->deleteTable(*it);
            }
        }
    }
    catch ( const TException &tx )
    {
        char buf[TC_LOG_BUF_SIZE];
        snprintf( buf, sizeof(buf),
                  "[%s] error encountered: %s\n"
                , method_name,  tx.what() );
        TcLogWrite( HBASE_DB_ACCESS_ERROR, TC_LOG_CRIT, buf );
        TRACE_EXIT;
        return( TCDBOPERROR );
    }

    TRACE_EXIT;
    return( TCSUCCESS );
}

int CTcdbHbase::DeleteNameServer( const char *nodeName )
{
    const char method_name[] = "CTcdbHbase::DeleteNameServer";
    TRACE_ENTRY;

    if ( !IsInitialized() )  
    {
        if (TcTraceSettings & (TC_TRACE_REGISTRY | TC_TRACE_REQUEST | TC_TRACE_INIT))
        {
            trace_printf( "%s@%d Database is not initialized for access!\n"
                        , method_name, __LINE__);
        }
        TRACE_EXIT;
        return( TCNOTINIT );
    }

    if (TcTraceSettings & (TC_TRACE_NODE | TC_TRACE_REQUEST))
    {
        trace_printf( "%s@%d delete from MRNS, values (nodeName=%s)\n"
                     , method_name, __LINE__
                     , nodeName );
    }

    // "delete from monRegNameServer where monRegNameServer.keyName = ?";
    StrVec columnNames;
    columnNames.push_back(config_->tblMrnsColKeyName_);
    std::string startRow = config_->keyMRNS(nodeName);
    std::string stopRow = startRow;
    int scanner = client_->scannerOpenWithStop(config_->tbl_, startRow, stopRow, columnNames, config_->attr_);
    ScannerMgr scanMgr(scanner);

    // scan mrns table for nodeName, then delete it found
    try
    {
        bool found = false;
        while ( !found )
        {
            std::vector<TRowResult> value;
            client_->scannerGet(value, scanner);
            if (value.size() == 0)
                break;
            config_->tsMrus_.addScan(value.size());
            for (size_t i = 0; i < value.size(); i++)
            {
                client_->deleteAllRow(config_->tbl_, value[i].row, config_->attr_);
                config_->tsMrns_.addDelete(1);
                found = true;
                break;
            }
        }
        if ( !found )
        {
            char buf[TC_LOG_BUF_SIZE];
            snprintf( buf, sizeof(buf)
                    , "[%s] failed, nodeName=%s, does not exist\n"
                    , method_name, nodeName );
            TcLogWrite( HBASE_DB_ACCESS_ERROR, TC_LOG_ERR, buf );
            TRACE_EXIT;
            return( TCDBNOEXIST );
        }
    }
    catch ( const IOError &ioe )
    {
        char buf[TC_LOG_BUF_SIZE];
        snprintf( buf, sizeof(buf)
                , "[%s] scanner IOError: %s\n"
                , method_name, ioe.what() );
        TcLogWrite( HBASE_DB_ACCESS_ERROR, TC_LOG_ERR, buf );
        TRACE_EXIT;
        return( TCDBOPERROR );
    }

    TRACE_EXIT;
    return( TCSUCCESS );
}

int CTcdbHbase::DeleteNameServerData()
{
    const char method_name[] = "CTcdbHbase::DeleteNameServerData";
    TRACE_ENTRY;

    if ( !IsInitialized() )  
    {
        if (TcTraceSettings & (TC_TRACE_REGISTRY | TC_TRACE_REQUEST | TC_TRACE_INIT))
        {
            trace_printf( "%s@%d Database is not initialized for access!\n"
                        , method_name, __LINE__);
        }
        TRACE_EXIT;
        return( TCNOTINIT );
    }

    if (TcTraceSettings & (TC_TRACE_NODE | TC_TRACE_REQUEST))
    {
        trace_printf( "%s@%d delete from MRNS\n"
                     , method_name, __LINE__ );
    }

    // "delete from monRegNameServer";
    StrVec columnNames;
    columnNames.push_back(config_->tblMrnsColKeyName_);
    std::string startRow = config_->keyMRNS(config_->keyStrMin_);
    std::string stopRow = config_->keyMRNS(config_->keyStrMax_);
    int scanner = client_->scannerOpenWithStop(config_->tbl_, startRow, stopRow, columnNames, config_->attr_);
    ScannerMgr scanMgr(scanner);

    // scan mrns table
    try
    {
        while ( true )
        {
            std::vector<TRowResult> value;
            client_->scannerGet(value, scanner);
            if (value.size() == 0)
                break;
            config_->tsMrus_.addScan(value.size());
            for (size_t i = 0; i < value.size(); i++)
            {
                client_->deleteAllRow(config_->tbl_, value[i].row, config_->attr_);
                config_->tsMrns_.addDelete(1);
            }
        }
    }
    catch ( const IOError &ioe )
    {
        char buf[TC_LOG_BUF_SIZE];
        snprintf( buf, sizeof(buf)
                , "[%s] scanner IOError: %s\n"
                , method_name, ioe.what() );
        TcLogWrite( HBASE_DB_ACCESS_ERROR, TC_LOG_ERR, buf );
        TRACE_EXIT;
        return( TCDBOPERROR );
    }

    TRACE_EXIT;
    return( TCSUCCESS );
}

int CTcdbHbase::DeleteNodeData( int pnid )
{
    const char method_name[] = "CTcdbHbase::DeleteNodeData";
    TRACE_ENTRY;

    if ( !IsInitialized() )  
    {
        if (TcTraceSettings & (TC_TRACE_REGISTRY | TC_TRACE_REQUEST | TC_TRACE_INIT))
        {
            trace_printf( "%s@%d Database is not initialized for access!\n"
                        , method_name, __LINE__);
        }
        TRACE_EXIT;
        return( TCNOTINIT );
    }

    if (TcTraceSettings & (TC_TRACE_NODE | TC_TRACE_REQUEST))
    {
        trace_printf( "%s@%d delete from lnode, pnode values (pNid=%d)\n"
                     , method_name, __LINE__ , pnid );
    }

    std::string pnidStr = config_->encInt(pnid);
    StrVec columnNames;
    columnNames.push_back(config_->tblLnodeColPnid_);
    std::string startRow = config_->keyLN(0);
    std::string stopRow = config_->keyLN(config_->keyIntMax_);
    int scanner = client_->scannerOpenWithStop(config_->tbl_, startRow, stopRow, columnNames, config_->attr_);
    ScannerMgr scanMgr(scanner);

    // scan lnode table for pnid, then delete it found
    try
    {
        bool found = false;
        while ( !found )
        {
            std::vector<TRowResult> value;
            client_->scannerGet(value, scanner);
            if (value.size() == 0)
                break;
            config_->tsLnode_.addScan(value.size());
            for (size_t i = 0; i < value.size(); i++)
            {
                for (CellMap::const_iterator it = value[i].columns.begin();
                     it != value[i].columns.end(); ++it)
                {
                    if (it->second.value == pnidStr)
                    {
                        client_->deleteAllRow(config_->tbl_, value[i].row, config_->attr_);
                        config_->tsLnode_.addDelete(1);
                        i = value.size();
                        found = true;
                        break;
                    }
                }
            }
        }
        if ( !found )
        {
            char buf[TC_LOG_BUF_SIZE];
            snprintf( buf, sizeof(buf)
                    , "[%s] failed, pnid=%d, does not exist\n"
                    , method_name, pnid );
            TcLogWrite( HBASE_DB_ACCESS_ERROR, TC_LOG_ERR, buf );
            TRACE_EXIT;
            return( TCDBNOEXIST );
        }
    }
    catch ( const IOError &ioe )
    {
        char buf[TC_LOG_BUF_SIZE];
        snprintf( buf, sizeof(buf)
                , "[%s] scanner IOError: %s\n"
                , method_name, ioe.what() );
        TcLogWrite( HBASE_DB_ACCESS_ERROR, TC_LOG_ERR, buf );
        TRACE_EXIT;
        return( TCDBOPERROR );
    }

    // pnode table delete a lot simpler!
    std::string row = config_->keyPN(pnid);
    client_->deleteAllRow(config_->tbl_, row, config_->attr_);
    config_->tsPnode_.addDelete(1);

    TRACE_EXIT;
    return( TCSUCCESS );
}

int CTcdbHbase::DeleteRegistryPersistentData()
{
    const char method_name[] = "CTcdbHbase::DeleteRegistryPersistentData";
    TRACE_ENTRY;

    if ( !IsInitialized() )  
    {
        if (TcTraceSettings & (TC_TRACE_REGISTRY | TC_TRACE_REQUEST | TC_TRACE_INIT))
        {
            trace_printf( "%s@%d Database is not initialized for access!\n"
                        , method_name, __LINE__);
        }
        TRACE_EXIT;
        return( TCNOTINIT );
    }

    // "delete from monRegPersistData"
    int ret = TCSUCCESS;
    std::string startRow = config_->keyMRPD(config_->keyStrMin_);
    std::string stopRow = config_->keyMRPD(config_->keyStrMax_);
    int scanner = client_->scannerOpenWithStop(config_->tbl_, startRow, stopRow, config_->tblMrpdCols_, config_->attr_);
    ScannerMgr scanMgr(scanner);

    // scan mrpd table
    try
    {
        while ( ret == TCSUCCESS )
        {
            std::vector<TRowResult> value;
            client_->scannerGet(value, scanner);
            if (value.size() == 0)
                break;
            config_->tsMrpd_.addScan(value.size());
            for (size_t i = 0; i < value.size(); i++)
            {
                client_->deleteAllRow(config_->tbl_, value[i].row, config_->attr_);
            }
        }
    }
    catch ( const IOError &ioe )
    {
        char buf[TC_LOG_BUF_SIZE];
        snprintf( buf, sizeof(buf)
                , "[%s] scanner IOError: %s\n"
                , method_name, ioe.what() );
        TcLogWrite( HBASE_DB_ACCESS_ERROR, TC_LOG_ERR, buf );
        TRACE_EXIT;
        return( TCDBOPERROR );
    }

    TRACE_EXIT;
    return( TCSUCCESS );
}

int CTcdbHbase::DeleteUniqueString( int nid )
{
    const char method_name[] = "CTcdbHbase::DeleteUniqueString";
    TRACE_ENTRY;

    if ( !IsInitialized() )  
    {
        if (TcTraceSettings & (TC_TRACE_REGISTRY | TC_TRACE_REQUEST | TC_TRACE_INIT))
        {
            trace_printf( "%s@%d Database is not initialized for access!\n"
                        , method_name, __LINE__);
        }
        TRACE_EXIT;
        return( TCNOTINIT );
    }

    if (TcTraceSettings & (TC_TRACE_REGISTRY | TC_TRACE_REQUEST))
    {
        trace_printf( "%s@%d delete from MRUS, values (nid=%d)\n",
                      method_name, __LINE__, nid );
    }

    StrVec columnNames;
    columnNames.push_back(config_->tblMrusColNid_);
    std::string startRow = config_->keyMRUS(nid, 0);
    std::string stopRow = config_->keyMRUS(nid + 1, 0);
    int scanner = client_->scannerOpenWithStop(config_->tbl_, startRow, stopRow, columnNames, config_->attr_);
    ScannerMgr scanMgr(scanner);

    // scan mrus table for nid, then delete it found
    try
    {
        bool found = false;
        while ( true )
        {
            std::vector<TRowResult> value;
            client_->scannerGet(value, scanner);
            if (value.size() == 0)
                break;
            config_->tsMrus_.addScan(value.size());
            for (size_t i = 0; i < value.size(); i++)
            {
                client_->deleteAllRow(config_->tbl_, value[i].row, config_->attr_);
                config_->tsMrus_.addDelete(1);
                found = true;
            }
        }
        if ( !found )
        {
            char buf[TC_LOG_BUF_SIZE];
            snprintf( buf, sizeof(buf)
                    , "[%s] failed, nid=%d, does not exist\n"
                    , method_name, nid );
            TcLogWrite( HBASE_DB_ACCESS_ERROR, TC_LOG_ERR, buf );
            TRACE_EXIT;
            return( TCDBNOEXIST );
        }
    }
    catch ( const IOError &ioe )
    {
        char buf[TC_LOG_BUF_SIZE];
        snprintf( buf, sizeof(buf)
                , "[%s] scanner IOError: %s\n"
                , method_name, ioe.what() );
        TcLogWrite( HBASE_DB_ACCESS_ERROR, TC_LOG_ERR, buf );
        TRACE_EXIT;
        return( TCDBOPERROR );
    }

    TRACE_EXIT;
    return( TCSUCCESS );
}

int CTcdbHbase::GetNameServer( const char *nodeName )
{
    const char method_name[] = "CTcdbHbase::GetNameServer";
    TRACE_ENTRY;

    if ( !IsInitialized() )  
    {
        if (TcTraceSettings & (TC_TRACE_REGISTRY | TC_TRACE_REQUEST | TC_TRACE_INIT))
        {
            trace_printf( "%s@%d Database is not initialized for access!\n"
                        , method_name, __LINE__);
        }
        TRACE_EXIT;
        return( TCNOTINIT );
    }
    nodeName = nodeName; // touch
    return( TCNOTINIT );

    TRACE_EXIT;
    return( TCSUCCESS );
}

int CTcdbHbase::GetNameServers( int *count, int max, char **nodeNames )
{
    const char method_name[] = "CTcdbHbase::GetNameServers";
    TRACE_ENTRY;

    if ( !IsInitialized() )  
    {
        if (TcTraceSettings & (TC_TRACE_REGISTRY | TC_TRACE_REQUEST | TC_TRACE_INIT))
        {
            trace_printf( "%s@%d Database is not initialized for access!\n"
                        , method_name, __LINE__);
        }
        TRACE_EXIT;
        return( TCNOTINIT );
    }
    
    int nodeCount = 0;
    const char *nodename = NULL;
    // "select p.keyName"
    // " from monRegNameServer p";
    int ret = TCSUCCESS;
    std::string startRow = config_->keyMRNS(config_->keyStrMin_);
    std::string stopRow = config_->keyMRNS(config_->keyStrMax_);
    int scanner = client_->scannerOpenWithStop(config_->tbl_, startRow, stopRow, config_->tblMrnsCols_, config_->attr_);
    ScannerMgr scanMgr(scanner);

    // scan mrns table
    try
    {
        while ( ret == TCSUCCESS )
        {
            std::vector<TRowResult> value;
            client_->scannerGet(value, scanner);
            if (value.size() == 0)
                break;
            if ( TcTraceSettings & (TC_TRACE_NODE | TC_TRACE_REQUEST) )
            {
                trace_printf("%s@%d hbase_row_count=%lu\n",
                             method_name, __LINE__, value.size());
            }
            config_->tsMrns_.addScan(value.size());
            if (max == 0)
            {
                nodeCount += (int) value.size();
                break;
            }
            for (size_t i = 0; i < value.size(); i++)
            {
                if ( TcTraceSettings & (TC_TRACE_NODE | TC_TRACE_REQUEST) )
                {
                    trace_printf("%s@%d hbase_column_count=%lu\n",
                                 method_name, __LINE__, value[i].columns.size());
                }
                for (CellMap::const_iterator it = value[i].columns.begin();
                     it != value[i].columns.end(); ++it)
                {
                    std::string key = it->first;
                    std::string value = it->second.value;
                    if (key == config_->tblMrnsColValueName_)
                        nodename = value.c_str();
                }
                if (nodeCount < max)
                {
                    if (nodename)
                    {
                        nodeNames[nodeCount] = new char[strlen(nodename)+1];
                        strcpy(nodeNames[nodeCount], nodename);
                    }
                    else
                        nodeNames[nodeCount] = NULL;
                    nodeCount++;
                }
                else
                {
                    *count = nodeCount;
                    ret = TCDBTRUNCATE;
                    break;
                }
            }
        }
        if (ret == TCSUCCESS)
            *count = nodeCount;
    }
    catch ( const IOError &ioe )
    {
        char buf[TC_LOG_BUF_SIZE];
        snprintf( buf, sizeof(buf)
                , "[%s] scanner IOError: %s\n"
                , method_name, ioe.what() );
        TcLogWrite( HBASE_DB_ACCESS_ERROR, TC_LOG_ERR, buf );
        TRACE_EXIT;
        return( TCDBOPERROR );
    }

    TRACE_EXIT;
    return( TCSUCCESS );
}

int CTcdbHbase::GetNode( int nid
                       , TcNodeConfiguration_t &nodeConfig )
{
    const char method_name[] = "CTcdbHbase::GetNode";
    TRACE_ENTRY;

    if ( !IsInitialized() )  
    {
        if (TcTraceSettings & (TC_TRACE_REGISTRY | TC_TRACE_REQUEST | TC_TRACE_INIT))
        {
            trace_printf( "%s@%d Database is not initialized for access!\n"
                        , method_name, __LINE__);
        }
        TRACE_EXIT;
        return( TCNOTINIT );
    }

    // "select p.pNid, l.lNid, p.nodeName, l.firstCore, l.lastCore,"
    // " p.excFirstCore, p.excLastCore, l.processors, l.roles"
    // "  from pnode p, lnode l where p.pNid = l.pNid"
    // "   and l.lNid = ?";
    int firstcore = -1;
    int lastcore = -1;
    int excfirstcore = -1;
    int exclastcore = -1;
    int lnid = -1;
    int pnid = -1;
    int processors = 0;
    int roles = -1;
    const char *nodename = NULL;
    std::string startRow = config_->keyLN(nid);
    std::string stopRow = config_->keyLN(nid + 1);
    int scanner = client_->scannerOpenWithStop(config_->tbl_, startRow, stopRow, config_->tblLnodeCols_, config_->attr_);
    ScannerMgr scanMgr(scanner);

    // scan lnode table for nid
    try
    {
        while ( true )
        {
            std::vector<TRowResult> value;
            client_->scannerGet(value, scanner);
            if (value.size() == 0)
                break;
            if ( TcTraceSettings & (TC_TRACE_NODE | TC_TRACE_REQUEST) )
            {
                trace_printf("%s@%d hbase_row_count=%lu\n",
                             method_name, __LINE__, value.size());
            }
            config_->tsLnode_.addScan(value.size());
            for (size_t i = 0; i < value.size(); i++)
            {
                if ( TcTraceSettings & (TC_TRACE_NODE | TC_TRACE_REQUEST) )
                {
                    trace_printf("%s@%d hbase_column_count=%lu\n",
                                 method_name, __LINE__, value[i].columns.size());
                }
                for (CellMap::const_iterator it = value[i].columns.begin();
                     it != value[i].columns.end(); ++it)
                {
                    std::string key = it->first;
                    std::string value = it->second.value;
                    if (key == config_->tblLnodeColPnid_)
                        pnid = config_->decInt(value);
                    else if (key == config_->tblLnodeColLnid_)
                        lnid = config_->decInt(value);
                    else if (key == config_->tblLnodeColFirstCore_)
                        firstcore = config_->decInt(value);
                    else if (key == config_->tblLnodeColLastCore_)
                        lastcore = config_->decInt(value);
                    else if (key == config_->tblLnodeColProcessors_)
                        processors = config_->decInt(value);
                    else if (key == config_->tblLnodeColRoles_)
                        roles = config_->decInt(value);
                    else if (key == config_->tblLnodeColPNodeName_)
                        nodename = value.c_str();
                    else if (key == config_->tblLnodeColPExcFirstCore_)
                        excfirstcore = config_->decInt(value);
                    else if (key == config_->tblLnodeColPExcLastCore_)
                        exclastcore = config_->decInt(value);
                }
            }
        }
        if ( nodename == NULL )
        {
            if ( TcTraceSettings & (TC_TRACE_NODE | TC_TRACE_REQUEST) )
            {
                trace_printf("%s@%d Finished processing logical nodes.\n",
                             method_name, __LINE__);
            }
            TRACE_EXIT;
            return( TCDBNOEXIST );
        }
    }
    catch ( const IOError &ioe )
    {
        char buf[TC_LOG_BUF_SIZE];
        snprintf( buf, sizeof(buf)
                , "[%s] scanner IOError: %s\n"
                , method_name, ioe.what() );
        TcLogWrite( HBASE_DB_ACCESS_ERROR, TC_LOG_ERR, buf );
        TRACE_EXIT;
        return( TCDBOPERROR );
    }

    SetLNodeData( lnid
                , pnid
                , nodename
                , excfirstcore
                , exclastcore
                , firstcore
                , lastcore
                , processors
                , roles 
                , nodeConfig );

    TRACE_EXIT;
    return( TCSUCCESS );
}

int CTcdbHbase::GetNode( const char *name
                       , TcNodeConfiguration_t &nodeConfig )
{
    const char method_name[] = "CTcdbHbase::GetNode";
    TRACE_ENTRY;

    if ( !IsInitialized() )  
    {
        if (TcTraceSettings & (TC_TRACE_REGISTRY | TC_TRACE_REQUEST | TC_TRACE_INIT))
        {
            trace_printf( "%s@%d Database is not initialized for access!\n"
                        , method_name, __LINE__);
        }
        TRACE_EXIT;
        return( TCNOTINIT );
    }

    // "select p.pNid, l.lNid, p.nodeName, l.firstCore, l.lastCore,"
    //      " p.excFirstCore, p.excLastCore, l.processors, l.roles"
    //      "  from pnode p, lnode l where p.pNid = l.pNid"
    //      "   and p.nodeName = ?";
    int firstcore = -1;
    int lastcore = -1;
    int excfirstcore = -1;
    int exclastcore = -1;
    int lnid = -1;
    int pnid = -1;
    int processors = 0;
    int roles = -1;
    const char *nodename = NULL;
    std::string startRow = config_->keyLN(0);
    std::string stopRow = config_->keyLN(config_->keyIntMax_);
    int scanner = client_->scannerOpenWithStop(config_->tbl_, startRow, stopRow, config_->tblLnodeCols_, config_->attr_);
    ScannerMgr scanMgr(scanner);

    // scan lnode table for nid
    try
    {
        while ( true )
        {
            std::vector<TRowResult> value;
            client_->scannerGet(value, scanner);
            if (value.size() == 0)
                break;
            if ( TcTraceSettings & (TC_TRACE_NODE | TC_TRACE_REQUEST) )
            {
                trace_printf("%s@%d hbase_row_count=%lu\n",
                             method_name, __LINE__, value.size());
            }
            config_->tsLnode_.addScan(value.size());
            for (size_t i = 0; i < value.size(); i++)
            {
                CellMap::const_iterator lit = value[i].columns.find(config_->tblLnodeColPNodeName_);
                if ((lit != value[i].columns.end()) && (lit->second.value == name))
                {
                    if ( TcTraceSettings & (TC_TRACE_NODE | TC_TRACE_REQUEST) )
                    {
                        trace_printf("%s@%d hbase_column_count=%lu\n",
                                     method_name, __LINE__, value[i].columns.size());
                    }
                    for (CellMap::const_iterator it = value[i].columns.begin();
                         it != value[i].columns.end(); ++it)
                    {
                        std::string key = it->first;
                        std::string value = it->second.value;
                        if ( TcTraceSettings & (TC_TRACE_NODE | TC_TRACE_REQUEST) )
                        {
                            trace_printf("%s@%d column %s is %s\n",
                                         method_name, __LINE__, key.c_str(), value.c_str());
                        }
                        if (key == config_->tblLnodeColPnid_)
                            pnid = config_->decInt(value);
                        else if (key == config_->tblLnodeColLnid_)
                            lnid = config_->decInt(value);
                        else if (key == config_->tblLnodeColFirstCore_)
                            firstcore = config_->decInt(value);
                        else if (key == config_->tblLnodeColLastCore_)
                            lastcore = config_->decInt(value);
                        else if (key == config_->tblLnodeColProcessors_)
                            processors = config_->decInt(value);
                        else if (key == config_->tblLnodeColRoles_)
                            roles = config_->decInt(value);
                        else if (key == config_->tblLnodeColPNodeName_)
                            nodename = value.c_str();
                        else if (key == config_->tblLnodeColPExcFirstCore_)
                            excfirstcore = config_->decInt(value);
                        else if (key == config_->tblLnodeColPExcLastCore_)
                            exclastcore = config_->decInt(value);
                    }
                    break;
                }
            }
        }
        if ( nodename == NULL )
        {
            if ( TcTraceSettings & (TC_TRACE_NODE | TC_TRACE_REQUEST) )
            {
                trace_printf("%s@%d Finished processing logical nodes.\n",
                             method_name, __LINE__);
            }
            TRACE_EXIT;
            return( TCDBNOEXIST );
        }
    }
    catch ( const IOError &ioe )
    {
        char buf[TC_LOG_BUF_SIZE];
        snprintf( buf, sizeof(buf)
                , "[%s] scanner IOError: %s\n"
                , method_name, ioe.what() );
        TcLogWrite( HBASE_DB_ACCESS_ERROR, TC_LOG_ERR, buf );
        TRACE_EXIT;
        return( TCDBOPERROR );
    }

    SetLNodeData( lnid
                , pnid
                , nodename
                , excfirstcore
                , exclastcore
                , firstcore
                , lastcore
                , processors
                , roles 
                , nodeConfig );

    TRACE_EXIT;
    return( TCSUCCESS );
}

int CTcdbHbase::GetNodes( int &count
                        , int max
                        , TcNodeConfiguration_t nodeConfig[] )
{
    const char method_name[] = "CTcdbHbase::GetNodes";
    TRACE_ENTRY;

    if ( !IsInitialized() )  
    {
        if (TcTraceSettings & (TC_TRACE_REGISTRY | TC_TRACE_REQUEST | TC_TRACE_INIT))
        {
            trace_printf( "%s@%d Database is not initialized for access!\n"
                        , method_name, __LINE__);
        }
        TRACE_EXIT;
        return( TCNOTINIT );
    }

    // "select p.pNid, l.lNid, p.nodeName, l.firstCore, l.lastCore,"
    // " p.excFirstCore, p.excLastCore, l.processors, l.roles"
    // "  from pnode p, lnode l where p.pNid = l.pNid";
    int  firstcore = -1;
    int  lastcore = -1;
    int  excfirstcore = -1;
    int  exclastcore = -1;
    int  lnid = -1;
    int  pnid = -1;
    int  processors = 0;
    int  roles = -1;
    const char *nodename = NULL;
    int nodeCount = 0;
    int ret = TCSUCCESS;
    std::string startRow = config_->keyLN(0);
    std::string stopRow = config_->keyLN(config_->keyIntMax_);
    int scanner = client_->scannerOpenWithStop(config_->tbl_, startRow, stopRow, config_->tblLnodeCols_, config_->attr_);
    ScannerMgr scanMgr(scanner);

    // scan lnode table
    try
    {
        while ( ret == TCSUCCESS )
        {
            std::vector<TRowResult> value;
            client_->scannerGet(value, scanner);
            if (value.size() == 0)
                break;
            if ( TcTraceSettings & (TC_TRACE_NODE | TC_TRACE_REQUEST) )
            {
                trace_printf("%s@%d hbase_row_count=%lu\n",
                             method_name, __LINE__, value.size());
            }
            config_->tsLnode_.addScan(value.size());
            if (max == 0)
            {
                nodeCount += (int) value.size();
                break;
            }
            for (size_t i = 0; i < value.size(); i++)
            {
                if ( TcTraceSettings & (TC_TRACE_NODE | TC_TRACE_REQUEST) )
                {
                    trace_printf("%s@%d hbase_column_count=%lu\n",
                                 method_name, __LINE__, value[i].columns.size());
                }
                for (CellMap::const_iterator it = value[i].columns.begin();
                     it != value[i].columns.end(); ++it)
                {
                    std::string key = it->first;
                    std::string value = it->second.value;
                    if (key == config_->tblLnodeColPnid_)
                        pnid = config_->decInt(value);
                    else if (key == config_->tblLnodeColLnid_)
                        lnid = config_->decInt(value);
                    else if (key == config_->tblLnodeColFirstCore_)
                        firstcore = config_->decInt(value);
                    else if (key == config_->tblLnodeColLastCore_)
                        lastcore = config_->decInt(value);
                    else if (key == config_->tblLnodeColProcessors_)
                        processors = config_->decInt(value);
                    else if (key == config_->tblLnodeColRoles_)
                        roles = config_->decInt(value);
                    else if (key == config_->tblLnodeColPNodeName_)
                        nodename = value.c_str();
                    else if (key == config_->tblLnodeColPExcFirstCore_)
                        excfirstcore = config_->decInt(value);
                    else if (key == config_->tblLnodeColPExcLastCore_)
                        exclastcore = config_->decInt(value);
                }
                if (nodeCount < max)
                {
                    SetLNodeData( lnid
                                , pnid
                                , nodename
                                , excfirstcore
                                , exclastcore
                                , firstcore
                                , lastcore
                                , processors
                                , roles 
                                , nodeConfig[nodeCount] );
                    nodeCount++;
                }
                else
                {
                    count = nodeCount;
                    ret = TCDBTRUNCATE;
                    break;
                }
            }
        }
        if (ret == TCSUCCESS)
            count = nodeCount;
    }
    catch ( const IOError &ioe )
    {
        char buf[TC_LOG_BUF_SIZE];
        snprintf( buf, sizeof(buf)
                , "[%s] scanner IOError: %s\n"
                , method_name, ioe.what() );
        TcLogWrite( HBASE_DB_ACCESS_ERROR, TC_LOG_ERR, buf );
        TRACE_EXIT;
        return( TCDBOPERROR );
    }

    TRACE_EXIT;
    return( TCSUCCESS );
}

int CTcdbHbase::GetPNode( int pnid
                        , TcPhysicalNodeConfiguration_t &pnodeConfig )
{
    const char method_name[] = "CTcdbHbase::GetPNode";
    TRACE_ENTRY;

    if ( !IsInitialized() )  
    {
        if (TcTraceSettings & (TC_TRACE_REGISTRY | TC_TRACE_REQUEST | TC_TRACE_INIT))
        {
            trace_printf( "%s@%d Database is not initialized for access!\n"
                        , method_name, __LINE__);
        }
        TRACE_EXIT;
        return( TCNOTINIT );
    }

    // "select p.pNid, p.nodeName, p.excFirstCore, p.excLastCore"
    // "  from pnode p where p.pNid = ?";
    int excfirstcore = -1;
    int exclastcore = -1;
    const char *nodename = NULL;
    std::string startRow = config_->keyPN(pnid);
    std::string stopRow = config_->keyPN(pnid + 1);
    int scanner = client_->scannerOpenWithStop(config_->tbl_, startRow, stopRow, config_->tblPnodeCols_, config_->attr_);
    ScannerMgr scanMgr(scanner);

    // scan lnode table for nid
    try
    {
        while ( true )
        {
            std::vector<TRowResult> value;
            client_->scannerGet(value, scanner);
            if (value.size() == 0)
                break;
            if ( TcTraceSettings & (TC_TRACE_NODE | TC_TRACE_REQUEST) )
            {
                trace_printf("%s@%d hbase_row_count=%lu\n",
                             method_name, __LINE__, value.size());
            }
            config_->tsPnode_.addScan(value.size());
            for (size_t i = 0; i < value.size(); i++)
            {
                if ( TcTraceSettings & (TC_TRACE_NODE | TC_TRACE_REQUEST) )
                {
                    trace_printf("%s@%d hbase_column_count=%lu\n",
                                 method_name, __LINE__, value[i].columns.size());
                }
                for (CellMap::const_iterator it = value[i].columns.begin();
                     it != value[i].columns.end(); ++it)
                {
                    std::string key = it->first;
                    std::string value = it->second.value;
                    if (key == config_->tblPnodeColNodeName_)
                        nodename = value.c_str();
                    else if (key == config_->tblPnodeColExcFirstCore_)
                        excfirstcore = config_->decInt(value);
                    else if (key == config_->tblPnodeColExcLastCore_)
                        exclastcore = config_->decInt(value);
                }
            }
        }
        if ( nodename == NULL )
        {
            if ( TcTraceSettings & (TC_TRACE_NODE | TC_TRACE_REQUEST) )
            {
                trace_printf("%s@%d Finished processing physical nodes.\n",
                             method_name, __LINE__);
            }
            TRACE_EXIT;
            return( TCDBNOEXIST );
        }
    }
    catch ( const IOError &ioe )
    {
        char buf[TC_LOG_BUF_SIZE];
        snprintf( buf, sizeof(buf)
                , "[%s] scanner IOError: %s\n"
                , method_name, ioe.what() );
        TcLogWrite( HBASE_DB_ACCESS_ERROR, TC_LOG_ERR, buf );
        TRACE_EXIT;
        return( TCDBOPERROR );
    }

    SetPNodeData( pnid
                , nodename
                , excfirstcore
                , exclastcore
                , pnodeConfig );

    TRACE_EXIT;
    return( TCSUCCESS );
}

int CTcdbHbase::GetPNode( const char *name
                        , TcPhysicalNodeConfiguration_t &pnodeConfig )
{
    const char method_name[] = "CTcdbHbase::GetPNode";
    TRACE_ENTRY;

    if ( !IsInitialized() )  
    {
        if (TcTraceSettings & (TC_TRACE_REGISTRY | TC_TRACE_REQUEST | TC_TRACE_INIT))
        {
            trace_printf( "%s@%d Database is not initialized for access!\n"
                        , method_name, __LINE__);
        }
        TRACE_EXIT;
        return( TCNOTINIT );
    }

    // "select p.pNid, p.nodeName, p.excFirstCore, p.excLastCore"
    // "  from pnode p where p.nodeName = ?";
    int pnid = -1;
    int excfirstcore = -1;
    int exclastcore = -1;
    const char *nodename = NULL;
    std::string startRow = config_->keyPN(0);
    std::string stopRow = config_->keyPN(config_->keyIntMax_);
    int scanner = client_->scannerOpenWithStop(config_->tbl_, startRow, stopRow, config_->tblPnodeCols_, config_->attr_);
    ScannerMgr scanMgr(scanner);

    // scan pnode table for name
    try
    {
        while ( true )
        {
            std::vector<TRowResult> value;
            client_->scannerGet(value, scanner);
            if (value.size() == 0)
                break;
            config_->tsPnode_.addScan(value.size());
            for (size_t i = 0; i < value.size(); i++)
            {
                CellMap::const_iterator lit = value[i].columns.find(config_->tblPnodeColNodeName_);
                if ((lit != value[i].columns.end()) && (lit->second.value == name))
                {
                    if ( TcTraceSettings & (TC_TRACE_NODE | TC_TRACE_REQUEST) )
                    {
                        trace_printf("%s@%d hbase_column_count=%lu\n",
                                     method_name, __LINE__, value[i].columns.size());
                    }
                    for (CellMap::const_iterator it = value[i].columns.begin();
                         it != value[i].columns.end(); ++it)
                    {
                        std::string key = it->first;
                        std::string value = it->second.value;
                        if (key == config_->tblPnodeColPnid_)
                            pnid = config_->decInt(value);
                        else if (key == config_->tblPnodeColNodeName_)
                            nodename = value.c_str();
                        else if (key == config_->tblPnodeColExcFirstCore_)
                            excfirstcore = config_->decInt(value);
                        else if (key == config_->tblPnodeColExcLastCore_)
                            exclastcore = config_->decInt(value);
                    }
                }
            }
        }
        if ( nodename == NULL )
        {
            if ( TcTraceSettings & (TC_TRACE_NODE | TC_TRACE_REQUEST) )
            {
                trace_printf("%s@%d Finished processing physical nodes.\n",
                             method_name, __LINE__);
            }
            TRACE_EXIT;
            return( TCDBNOEXIST );
        }
    }
    catch ( const IOError &ioe )
    {
        char buf[TC_LOG_BUF_SIZE];
        snprintf( buf, sizeof(buf)
                , "[%s] scanner IOError: %s\n"
                , method_name, ioe.what() );
        TcLogWrite( HBASE_DB_ACCESS_ERROR, TC_LOG_ERR, buf );
        TRACE_EXIT;
        return( TCDBOPERROR );
    }

    SetPNodeData( pnid
                , nodename
                , excfirstcore
                , exclastcore
                , pnodeConfig );

    TRACE_EXIT;
    return( TCSUCCESS );
}

int CTcdbHbase::GetPersistProcess( const char *persistPrefix
                                 , TcPersistConfiguration_t &persistConfig )
{
    const char method_name[] = "CTcdbHbase::GetPersistProcess";
    TRACE_ENTRY;

    if ( !IsInitialized() )  
    {
        if (TcTraceSettings & (TC_TRACE_REGISTRY | TC_TRACE_REQUEST | TC_TRACE_INIT))
        {
            trace_printf( "%s@%d Database is not initialized for access!\n"
                        , method_name, __LINE__);
        }
        TRACE_EXIT;
        return( TCNOTINIT );
    }

    // "select p.keyName, p.valueName"
    // " from monRegPersistData p"
    // "  where p.keyName like ?";
    std::string startRow = config_->keyMRPD(persistPrefix);
    int startLen = (int) startRow.length();
    char *start = new char[startLen + 1];
    strcpy(start, startRow.c_str());
    // change "LOB_" to "LOC_" for stop row
    start[startLen - 2]++;
    std::string stopRow(start);
    delete [] start;
    int scanner = client_->scannerOpenWithStop(config_->tbl_, startRow, stopRow, config_->tblMrpdCols_, config_->attr_);
    ScannerMgr scanMgr(scanner);

    try
    {
        bool found = false;
        while ( true )
        {
            std::vector<TRowResult> value;
            client_->scannerGet(value, scanner);
            if (value.size() == 0)
                break;
            found = true;
            config_->tsMrpd_.addScan(value.size());
            for (size_t i = 0; i < value.size(); i++)
            {
                std::string persistKey;
                std::string persistValue;
                for (CellMap::const_iterator it = value[i].columns.begin();
                     it != value[i].columns.end(); ++it)
                {
                    std::string key = it->first;
                    std::string value = it->second.value;
                    if (key == config_->tblMrpdColKeyName_)
                        persistKey = value;
                    else if (key == config_->tblMrpdColValueName_)
                        persistValue = value;
                }
                int rs = SetPersistProcessData( persistKey.c_str()
                                              , persistValue.c_str()
                                              , persistConfig );
                if ( rs != TCSUCCESS )
                {
                    char buf[TC_LOG_BUF_SIZE];
                    snprintf( buf, sizeof(buf)
                            , "[%s], Error: Invalid persist key value in "
                              "configuration, key=%s, value=%s\n"
                            , method_name, persistKey.c_str(), persistValue.c_str() );
                    TcLogWrite( HBASE_DB_ACCESS_ERROR, TC_LOG_CRIT, buf );
                    TRACE_EXIT;
                    return( TCDBOPERROR );
                }
            }
        }
        if ( !found )
        {
            char buf[TC_LOG_BUF_SIZE];
            snprintf( buf, sizeof(buf)
                    , "[%s] failed, persistPrefix=%s, does not exist\n"
                    , method_name, persistPrefix );
            TcLogWrite( HBASE_DB_ACCESS_ERROR, TC_LOG_ERR, buf );
            TRACE_EXIT;
            return( TCDBNOEXIST );
        }
    }
    catch ( const IOError &ioe )
    {
        char buf[TC_LOG_BUF_SIZE];
        snprintf( buf, sizeof(buf)
                , "[%s] scanner IOError: %s\n"
                , method_name, ioe.what() );
        TcLogWrite( HBASE_DB_ACCESS_ERROR, TC_LOG_ERR, buf );
        TRACE_EXIT;
        return( TCDBOPERROR );
    }

    TRACE_EXIT;
    return( TCSUCCESS );
}

int CTcdbHbase::GetPersistProcessKeys( const char *persistProcessKeys )
{
    const char method_name[] = "CTcdbHbase::GetPersistProcessKeys";
    TRACE_ENTRY;

    if ( !IsInitialized() )  
    {
        if (TcTraceSettings & (TC_TRACE_REGISTRY | TC_TRACE_REQUEST | TC_TRACE_INIT))
        {
            trace_printf( "%s@%d Database is not initialized for access!\n"
                        , method_name, __LINE__);
        }
        TRACE_EXIT;
        return( TCNOTINIT );
    }

    StrVec columnNames;
    columnNames.push_back(config_->tblMrpdColValueName_);
    std::string startRow = config_->keyMRPD("PERSIST_PROCESS_KEYS");
    std::string stopRow(startRow);
    int scanner = client_->scannerOpenWithStop(config_->tbl_, startRow, stopRow, columnNames, config_->attr_);
    ScannerMgr scanMgr(scanner);

    try
    {
        while ( true )
        {
            std::vector<TRowResult> value;
            client_->scannerGet(value, scanner);
            if (value.size() == 0)
                break;
            config_->tsMrpd_.addScan(value.size());
            for (size_t i = 0; i < value.size(); i++)
            {
                for (CellMap::const_iterator it = value[i].columns.begin();
                     it != value[i].columns.end(); ++it)
                {
                    strcpy((char *) persistProcessKeys, it->second.value.c_str());
                }
            }
        }
    }
    catch ( const IOError &ioe )
    {
        char buf[TC_LOG_BUF_SIZE];
        snprintf( buf, sizeof(buf)
                , "[%s] scanner IOError: %s\n"
                , method_name, ioe.what() );
        TcLogWrite( HBASE_DB_ACCESS_ERROR, TC_LOG_ERR, buf );
        TRACE_EXIT;
        return( TCDBOPERROR );
    }

    TRACE_EXIT;
    return( TCSUCCESS );
}

int CTcdbHbase::GetRegistryClusterSet( int &count
                                     , int max
                                     , TcRegistryConfiguration_t registryConfig[] )
{
    const char method_name[] = "CTcdbHbase::GetRegistryClusterSet";
    TRACE_ENTRY;

    if ( !IsInitialized() )  
    {
        if (TcTraceSettings & (TC_TRACE_REGISTRY | TC_TRACE_REQUEST | TC_TRACE_INIT))
        {
            trace_printf( "%s@%d Database is not initialized for access!\n"
                        , method_name, __LINE__);
        }
        TRACE_EXIT;
        return( TCNOTINIT );
    }

    // "select k.keyName, d.dataValue "
    // " from monRegKeyName k, monRegClusterData d "
    // " where k.keyId = d.keyId";
    int ret = TCSUCCESS;
    int entryNum = 0;
    std::string startRow = config_->keyMRCD(config_->keyStrMin_);
    std::string stopRow = config_->keyMRCD(config_->keyStrMax_);
    int scanner = client_->scannerOpenWithStop(config_->tbl_, startRow, stopRow, config_->tblMrcdCols_, config_->attr_);
    ScannerMgr scanMgr(scanner);

    // scan mrcd table
    try
    {
        while ( ret == TCSUCCESS )
        {
            std::vector<TRowResult> value;
            client_->scannerGet(value, scanner);
            if (value.size() == 0)
                break;
            config_->tsMrcd_.addScan(value.size());
            if (max == 0)
            {
                entryNum += (int) value.size();
                break;
            }
            for (size_t i = 0; i < value.size(); i++)
            {
                std::string clusterKey;
                std::string clusterValue;
                for (CellMap::const_iterator it = value[i].columns.begin();
                     it != value[i].columns.end(); ++it)
                {
                    std::string key = it->first;
                    std::string value = it->second.value;
                    if (key == config_->tblMrcdColKeyName_)
                        clusterKey = value;
                    else if (key == config_->tblMrcdColDataValue_)
                        clusterValue = value;
                }
                if ( entryNum < max )
                {
                    const char *group = "CLUSTER";
                    strncpy( registryConfig[entryNum].scope, group, TC_REGISTRY_KEY_MAX );
                    strncpy( registryConfig[entryNum].key, (const char *) clusterKey.c_str(), TC_REGISTRY_KEY_MAX );
                    strncpy( registryConfig[entryNum].value, (const char *) clusterValue.c_str(), TC_REGISTRY_VALUE_MAX );
                    entryNum++;
                }
                else
                {
                    count = entryNum;
                    ret = TCDBTRUNCATE;
                    break;
                }
            }
        }
        if (ret == TCSUCCESS)
            count = entryNum;
    }
    catch ( const IOError &ioe )
    {
        char buf[TC_LOG_BUF_SIZE];
        snprintf( buf, sizeof(buf)
                , "[%s] scanner IOError: %s\n"
                , method_name, ioe.what() );
        TcLogWrite( HBASE_DB_ACCESS_ERROR, TC_LOG_ERR, buf );
        TRACE_EXIT;
        return( TCDBOPERROR );
    }

    TRACE_EXIT;
    return( TCSUCCESS );
}

int CTcdbHbase::GetRegistryProcessSet( int &count
                                     , int max
                                     , TcRegistryConfiguration_t registryConfig[] )
{
    const char method_name[] = "CTcdbHbase::GetRegistryProcessSet";
    TRACE_ENTRY;

    if ( !IsInitialized() )  
    {
        if (TcTraceSettings & (TC_TRACE_REGISTRY | TC_TRACE_REQUEST | TC_TRACE_INIT))
        {
            trace_printf( "%s@%d Database is not initialized for access!\n"
                        , method_name, __LINE__);
        }
        TRACE_EXIT;
        return( TCNOTINIT );
    }

    // "select p.procName, k.keyName, d.dataValue"
    // " from monRegProcName p, monRegKeyName k, monRegProcData d"
    // " where p.procId = d.procId"
    // "   and k.keyId = d.keyId";
    int ret = TCSUCCESS;
    int entryNum = 0;
    std::string startRow = config_->keyMR(config_->keyStrMin_, "");
    std::string stopRow = config_->keyMR(config_->keyStrMax_, "");
    int scanner = client_->scannerOpenWithStop(config_->tbl_, startRow, stopRow, config_->tblMregCols_, config_->attr_);
    ScannerMgr scanMgr(scanner);

    // scan mreg table
    try
    {
        while ( ret == TCSUCCESS )
        {
            std::vector<TRowResult> value;
            client_->scannerGet(value, scanner);
            if (value.size() == 0)
                break;
            config_->tsMreg_.addScan(value.size());
            if (max == 0)
            {
                entryNum += (int) value.size();
                break;
            }
            for (size_t i = 0; i < value.size(); i++)
            {
                std::string processProcess;
                std::string processKey;
                std::string processValue;
                for (CellMap::const_iterator it = value[i].columns.begin();
                     it != value[i].columns.end(); ++it)
                {
                    std::string key = it->first;
                    std::string value = it->second.value;
                    if (key == config_->tblMregColProcName_)
                        processProcess = value;
                    else if (key == config_->tblMregColKeyName_)
                        processKey = value;
                    else if (key == config_->tblMregColDataValue_)
                        processValue = value;
                }
                if ( entryNum < max )
                {
                    strncpy( registryConfig[entryNum].scope, (const char *) processProcess.c_str(), TC_REGISTRY_KEY_MAX );
                    strncpy( registryConfig[entryNum].key, (const char *) processKey.c_str(), TC_REGISTRY_KEY_MAX );
                    strncpy( registryConfig[entryNum].value, (const char *) processValue.c_str(), TC_REGISTRY_VALUE_MAX );
                    entryNum++;
                }
                else
                {
                    count = entryNum;
                    ret = TCDBTRUNCATE;
                    break;
                }
            }
        }
        if (ret == TCSUCCESS)
            count = entryNum;
    }
    catch ( const IOError &ioe )
    {
        char buf[TC_LOG_BUF_SIZE];
        snprintf( buf, sizeof(buf)
                , "[%s] scanner IOError: %s\n"
                , method_name, ioe.what() );
        TcLogWrite( HBASE_DB_ACCESS_ERROR, TC_LOG_ERR, buf );
        TRACE_EXIT;
        return( TCDBOPERROR );
    }

    TRACE_EXIT;
    return( TCSUCCESS );
}

int CTcdbHbase::GetSNodes( int &count
                         , int max
                         , TcPhysicalNodeConfiguration_t spareNodeConfig[] )
{
    const char method_name[] = "CTcdbHbase::GetSNodes";
    TRACE_ENTRY;

    if ( !IsInitialized() )  
    {
        if (TcTraceSettings & (TC_TRACE_REGISTRY | TC_TRACE_REQUEST | TC_TRACE_INIT))
        {
            trace_printf( "%s@%d Database is not initialized for access!\n"
                        , method_name, __LINE__);
        }
        TRACE_EXIT;
        return( TCNOTINIT );
    }

    int excfirstcore = -1;
    int exclastcore = -1;
    int pnid = -1;
    int snodeCount = 0;
    const char *nodename = NULL;
    std::string startRow = config_->keySN(0, 0);
    std::string stopRow = config_->keySN(config_->keyIntMax_, 0);
    int scanner = client_->scannerOpenWithStop(config_->tbl_, startRow, stopRow, config_->tblSnodeCols_, config_->attr_);
    ScannerMgr scanMgr(scanner);

    // scan snode table
    try
    {
        while ( true )
        {
            std::vector<TRowResult> value;
            client_->scannerGet(value, scanner);
            if (value.size() == 0)
                break;
            config_->tsSnode_.addScan(value.size());
            if (max == 0)
            {
                snodeCount += (int) value.size();
                continue;
            }
            for (size_t i = 0; i < value.size(); i++)
            {
                if ( snodeCount < max )
                {
                    for (CellMap::const_iterator it = value[i].columns.begin();
                         it != value[i].columns.end(); ++it)
                    {
                        std::string key = it->first;
                        std::string value = it->second.value;
                        if (key == config_->tblSnodeColPnid_)
                            pnid = config_->decInt(value);
                        else if (key == config_->tblSnodeColNodeName_)
                            nodename = value.c_str();
                        else if (key == config_->tblSnodeColFirstCore_)
                            excfirstcore = config_->decInt(value);
                        else if (key == config_->tblSnodeColLastCore_)
                            exclastcore = config_->decInt(value);
                    }
                    if ( GetSNodeData( pnid
                                     , nodename
                                     , excfirstcore
                                     , exclastcore
                                     , spareNodeConfig[snodeCount] ) )
                    {
                        char buf[TC_LOG_BUF_SIZE];
                        snprintf( buf, sizeof(buf)
                                , "[%s], Error: Invalid node configuration\n"
                                , method_name );
                        TcLogWrite( HBASE_DB_ACCESS_ERROR, TC_LOG_CRIT, buf );
                        TRACE_EXIT;
                        return( TCDBOPERROR );
                    }
                    snodeCount++;
                }
            }
        }
        count = snodeCount;
    }
    catch ( const IOError &ioe )
    {
        char buf[TC_LOG_BUF_SIZE];
        snprintf( buf, sizeof(buf)
                , "[%s] scanner IOError: %s\n"
                , method_name, ioe.what() );
        TcLogWrite( HBASE_DB_ACCESS_ERROR, TC_LOG_ERR, buf );
        TRACE_EXIT;
        return( TCDBOPERROR );
    }

    TRACE_EXIT;
    return( TCSUCCESS );
}

int CTcdbHbase::GetSNodeData( int pnid
                            , const char *nodename
                            , int excfirstcore
                            , int exclastcore 
                            , TcPhysicalNodeConfiguration_t &spareNodeConfig )
{
    const char method_name[] = "CTcdbHbase::GetSNodeData";
    TRACE_ENTRY;

    if ( TcTraceSettings & (TC_TRACE_NODE | TC_TRACE_REQUEST) )
    {
        trace_printf( "%s@%d pnid=%d, name=%s, excluded cores=(%d:%d)\n"
                    , method_name, __LINE__
                    , pnid
                    , nodename
                    , excfirstcore
                    , exclastcore );
    }

    std::string pnidStr = config_->encInt(pnid);
    spareNodeConfig.pnid = pnid;
    strncpy( spareNodeConfig.node_name
           , nodename
           , sizeof(spareNodeConfig.node_name) );
    spareNodeConfig.excluded_first_core = excfirstcore;
    spareNodeConfig.excluded_last_core = exclastcore;

    int spareCount = 0;
    std::string startRow = config_->keySN(pnid, 0);
    std::string stopRow = config_->keySN(pnid, config_->keyIntMax_);
    int scanner = client_->scannerOpenWithStop(config_->tbl_, startRow, stopRow, config_->tblSnodeCols_, config_->attr_);
    ScannerMgr scanMgr(scanner);

    // scan snode table
    try
    {
        while ( true )
        {
            std::vector<TRowResult> value;
            client_->scannerGet(value, scanner);
            if (value.size() == 0)
                break;
            config_->tsSnode_.addScan(value.size());
            for (size_t i = 0; i < value.size(); i++)
            {
                CellMap::const_iterator lit = value[i].columns.find(config_->tblSnodeColPnid_);
                if ((lit != value[i].columns.end()) && (lit->second.value == pnidStr))
                {
                    int spnid = -1;
                    for (CellMap::const_iterator it = value[i].columns.begin();
                         it != value[i].columns.end(); ++it)
                    {
                        std::string key = it->first;
                        std::string value = it->second.value;
                        if (key == config_->tblSnodeColSpnid_)
                            spnid = config_->decInt(value);
                    }
                    spareNodeConfig.spare_pnid[spareCount] = spnid;
                    spareCount++;
                }
            }
        }
    }
    catch ( const IOError &ioe )
    {
        char buf[TC_LOG_BUF_SIZE];
        snprintf( buf, sizeof(buf)
                , "[%s] scanner IOError: %s\n"
                , method_name, ioe.what() );
        TcLogWrite( HBASE_DB_ACCESS_ERROR, TC_LOG_ERR, buf );
        TRACE_EXIT;
        return( TCDBOPERROR );
    }

    TRACE_EXIT;
    return( TCSUCCESS );
}

int CTcdbHbase::GetUniqueString( int nid, int id, const char *uniqStr )
{
    const char method_name[] = "CTcdbHbase::GetUniqueString";
    TRACE_ENTRY;

    if ( !IsInitialized() )  
    {
        if (TcTraceSettings & (TC_TRACE_REGISTRY | TC_TRACE_REQUEST | TC_TRACE_INIT))
        {
            trace_printf( "%s@%d Database is not initialized for access!\n"
                        , method_name, __LINE__);
        }
        TRACE_EXIT;
        return( TCNOTINIT );
    }

    // "select dataValue from monRegUniqueStrings where nid = ? and id = ?";
    int ret = TCSUCCESS;
    std::string startRow = config_->keyMRUS(nid, id);
    std::string stopRow = startRow;
    int scanner = client_->scannerOpenWithStop(config_->tbl_, startRow, stopRow, config_->tblMrusCols_, config_->attr_);
    ScannerMgr scanMgr(scanner);

    // scan mrus table
    try
    {
        bool found = false;
        while ( (ret == TCSUCCESS) && (!found) )
        {
            std::vector<TRowResult> value;
            client_->scannerGet(value, scanner);
            if (value.size() == 0)
                break;
            if ( TcTraceSettings & (TC_TRACE_NODE | TC_TRACE_REQUEST) )
            {
                trace_printf("%s@%d hbase_row_count=%lu\n",
                             method_name, __LINE__, value.size());
            }
            config_->tsMrus_.addScan(value.size());
            for (size_t i = 0; i < value.size(); i++)
            {
                if ( TcTraceSettings & (TC_TRACE_NODE | TC_TRACE_REQUEST) )
                {
                    trace_printf("%s@%d hbase_column_count=%lu\n",
                                 method_name, __LINE__, value[i].columns.size());
                }
                for (CellMap::const_iterator it = value[i].columns.begin();
                     it != value[i].columns.end(); ++it)
                {
                    std::string key = it->first;
                    std::string value = it->second.value;
                    if (key == config_->tblMrusColDataValue_)
                    {
                        strcpy((char *) uniqStr, value.c_str());
                        i = value.size();
                        found = true;
                        break;
                    }
                }
            }
        }
        if ( !found )
        {
            char buf[TC_LOG_BUF_SIZE];
            snprintf( buf, sizeof(buf)
                    , "[%s] failed, (nid=%d, id=%d), does not exist\n"
                    , method_name, nid, id );
            TcLogWrite( HBASE_DB_ACCESS_ERROR, TC_LOG_ERR, buf );
            TRACE_EXIT;
            return( TCDBNOEXIST );
        }
    }
    catch ( const IOError &ioe )
    {
        char buf[TC_LOG_BUF_SIZE];
        snprintf( buf, sizeof(buf)
                , "[%s] scanner IOError: %s\n"
                , method_name, ioe.what() );
        TcLogWrite( HBASE_DB_ACCESS_ERROR, TC_LOG_ERR, buf );
        TRACE_EXIT;
        return( TCDBOPERROR );
    }

    TRACE_EXIT;
    return( TCSUCCESS );
}

int CTcdbHbase::GetUniqueStringId( int nid
                                 , const char *uniqStr
                                 , int &id )
{
    const char method_name[] = "CTcdbHbase::GetUniqueStringId";
    TRACE_ENTRY;

    if ( !IsInitialized() )  
    {
        if (TcTraceSettings & (TC_TRACE_REGISTRY | TC_TRACE_REQUEST | TC_TRACE_INIT))
        {
            trace_printf( "%s@%d Database is not initialized for access!\n"
                        , method_name, __LINE__);
        }
        TRACE_EXIT;
        return( TCNOTINIT );
    }

    if (TcTraceSettings & (TC_TRACE_REGISTRY | TC_TRACE_REQUEST))
    {
        trace_printf( "%s@%d Getting unique string id: nid=%d string=%s\n"
                    , method_name, __LINE__, nid, uniqStr);
    }

    // "select id from monRegUniqueStrings where nid = ? and dataValue = ?";
    int ret = TCSUCCESS;
    std::string startRow = config_->keyMRUS(nid, 0);
    std::string stopRow = config_->keyMRUS(nid + 1, 0);
    int scanner = client_->scannerOpenWithStop(config_->tbl_, startRow, stopRow, config_->tblMrusCols_, config_->attr_);
    ScannerMgr scanMgr(scanner);

    // scan mrus table
    try
    {
        bool found = false;
        while ( (ret == TCSUCCESS) && (!found) )
        {
            std::vector<TRowResult> value;
            client_->scannerGet(value, scanner);
            if (value.size() == 0)
                break;
            if ( TcTraceSettings & (TC_TRACE_NODE | TC_TRACE_REQUEST) )
            {
                trace_printf("%s@%d hbase_row_count=%lu\n",
                             method_name, __LINE__, value.size());
            }
            config_->tsMrus_.addScan(value.size());
            for (size_t i = 0; i < value.size(); i++)
            {
                if ( TcTraceSettings & (TC_TRACE_NODE | TC_TRACE_REQUEST) )
                {
                    trace_printf("%s@%d hbase_column_count=%lu\n",
                                 method_name, __LINE__, value[i].columns.size());
                }
                CellMap::const_iterator lit1 = value[i].columns.find(config_->tblMrusColDataValue_);
                if ((lit1 != value[i].columns.end()) && (lit1->second.value == uniqStr))
                {
                    CellMap::const_iterator lit2 = value[i].columns.find(config_->tblMrusColId_);
                    if (lit2 != value[i].columns.end())
                    {
                        std::string value = lit2->second.value;
                        id = config_->decInt(value);
                        i = value.size(); // setup for exit
                        found = true; // setup for exit
                        break;
                    }
                }
            }
        }
        if ( !found )
        {
            char buf[TC_LOG_BUF_SIZE];
            snprintf( buf, sizeof(buf)
                    , "[%s] failed, (nid=%d, uniq=%s), does not exist\n"
                    , method_name, nid, uniqStr );
            TcLogWrite( HBASE_DB_ACCESS_ERROR, TC_LOG_ERR, buf );
            TRACE_EXIT;
            return( TCDBNOEXIST );
        }
    }
    catch ( const IOError &ioe )
    {
        char buf[TC_LOG_BUF_SIZE];
        snprintf( buf, sizeof(buf)
                , "[%s] scanner IOError: %s\n"
                , method_name, ioe.what() );
        TcLogWrite( HBASE_DB_ACCESS_ERROR, TC_LOG_ERR, buf );
        TRACE_EXIT;
        return( TCDBOPERROR );
    }

    TRACE_EXIT;
    return( TCSUCCESS );
}

int CTcdbHbase::GetUniqueStringIdMax( int nid, int &id )
{
    const char method_name[] = "CTcdbHbase::GetUniqueStringIdMax";
    TRACE_ENTRY;
    
    if ( !IsInitialized() )  
    {
        if (TcTraceSettings & (TC_TRACE_REGISTRY | TC_TRACE_REQUEST | TC_TRACE_INIT))
        {
            trace_printf( "%s@%d Database is not initialized for access!\n"
                        , method_name, __LINE__);
        }
        TRACE_EXIT;
        return( TCNOTINIT );
    }

    // "select max(id) from monRegUniqueStrings where nid=?";
    int ret = TCSUCCESS;
    int maxId = 0;
    std::string startRow = config_->keyMRUS(nid, 0);
    std::string stopRow = config_->keyMRUS(nid + 1, 0);
    int scanner = client_->scannerOpenWithStop(config_->tbl_, startRow, stopRow, config_->tblMrusCols_, config_->attr_);
    ScannerMgr scanMgr(scanner);

    // scan mrus table
    try
    {
        while ( ret == TCSUCCESS )
        {
            std::vector<TRowResult> value;
            client_->scannerGet(value, scanner);
            if (value.size() == 0)
                break;
            if ( TcTraceSettings & (TC_TRACE_NODE | TC_TRACE_REQUEST) )
            {
                trace_printf("%s@%d hbase_row_count=%lu\n",
                             method_name, __LINE__, value.size());
            }
            config_->tsMrus_.addScan(value.size());
            for (size_t i = 0; i < value.size(); i++)
            {
                if ( TcTraceSettings & (TC_TRACE_NODE | TC_TRACE_REQUEST) )
                {
                    trace_printf("%s@%d hbase_column_count=%lu\n",
                                 method_name, __LINE__, value[i].columns.size());
                }
                CellMap::const_iterator lit = value[i].columns.find(config_->tblMrusColId_);
                if (lit != value[i].columns.end())
                {
                    std::string value = lit->second.value;
                    int tid = config_->decInt(value);
                    if (tid > maxId)
                        maxId = tid;
                }
            }
        }
        id = maxId;
    }
    catch ( const IOError &ioe )
    {
        char buf[TC_LOG_BUF_SIZE];
        snprintf( buf, sizeof(buf)
                , "[%s] scanner IOError: %s\n"
                , method_name, ioe.what() );
        TcLogWrite( HBASE_DB_ACCESS_ERROR, TC_LOG_ERR, buf );
        TRACE_EXIT;
        return( TCDBOPERROR );
    }

    TRACE_EXIT;
    return( TCSUCCESS );
}

int CTcdbHbase::Initialize( void )
{
    const char method_name[] = "CTcdbHbase::Initialize";
    TRACE_ENTRY;

    if ( IsInitialized() )
    {
        // Already initialized
        TRACE_EXIT;
        return( TCALREADYINIT );
    }

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

    if (TcTraceSettings & (TC_TRACE_REGISTRY | TC_TRACE_REQUEST | TC_TRACE_INIT))
    {
        trace_printf( "%s@%d connecting to thrift server %s:%d\n"
                    , method_name, __LINE__, "localhost", thriftPort );
    }
    // Connection to the Thrift Server
    std::shared_ptr<TSocket> socket(new TSocket("localhost", thriftPort));
    std::shared_ptr<TTransport> transport(new TBufferedTransport(socket));
    std::shared_ptr<TProtocol> protocol(new TBinaryProtocol(transport));
    transport_ = transport;

    // Create the Hbase client
    client_ = new HbaseClient(protocol);
    inited_ = true;

    try
    {
        transport->open();
        if (TcTraceSettings & (TC_TRACE_REGISTRY | TC_TRACE_REQUEST | TC_TRACE_INIT))
        {
            trace_printf( "%s@%d connect OK\n", method_name, __LINE__ );
            trace_printf( "%s@%d creating Hbase table=%s\n", method_name, __LINE__, config_->tbl_.c_str());
        }

        // Create the table
        ColVec columns;
        columns.push_back(ColumnDescriptor());
        columns.back().name = config_->tblPnodeCf_; // physical node
        columns.back().maxVersions = 1;
        columns.push_back(ColumnDescriptor());
        columns.back().name = config_->tblLnodeCf_; // logical node
        columns.back().maxVersions = 1;
        columns.push_back(ColumnDescriptor());
        columns.back().name = config_->tblSnodeCf_; // spare node
        columns.back().maxVersions = 1;
        columns.push_back(ColumnDescriptor());
        columns.back().name = config_->tblMregCf_;  // monitor registry
        columns.back().maxVersions = 1;
        columns.push_back(ColumnDescriptor());
        columns.back().name = config_->tblMrcdCf_;  // monitor registry cluster data
        columns.back().maxVersions = 1;
        columns.push_back(ColumnDescriptor());
        columns.back().name = config_->tblMrpdCf_;  // monitor registry persistent data
        columns.back().maxVersions = 1;
        columns.push_back(ColumnDescriptor());
        columns.back().name = config_->tblMrusCf_;  // monitor registry unique strings
        columns.back().maxVersions = 1;
        columns.push_back(ColumnDescriptor());
        columns.back().name = config_->tblMrnsCf_;  // monitor registry name server
        columns.back().maxVersions = 1;
        try
        {
            client_->createTable(config_->tbl_, columns);
        }
        catch ( const AlreadyExists &ae )
        {
            if (TcTraceSettings & (TC_TRACE_REGISTRY | TC_TRACE_REQUEST | TC_TRACE_INIT))
            {
                trace_printf( "%s@%d create Hbase table=%s already exists\n", method_name, __LINE__, ae.what() );
            }
        }
    }
    catch ( const TException &tx )
    {
        char buf[TC_LOG_BUF_SIZE];
        snprintf( buf, sizeof(buf),
                  "[%s] error encountered: %s\n"
                , method_name,  tx.what() );
        TcLogWrite( HBASE_DB_ACCESS_ERROR, TC_LOG_CRIT, buf );
        transport_->close();
        delete client_;
        transport_ = NULL;
        client_ = NULL;
        return( TCDBOPERROR );
    }

    TRACE_EXIT;
    return( TCSUCCESS );
}

void CTcdbHbase::SetLNodeData( int nid
                             , int pnid
                             , const char *nodename
                             , int excfirstcore
                             , int exclastcore
                             , int firstcore
                             , int lastcore
                             , int processors
                             , int roles 
                             , TcNodeConfiguration_t &nodeConfig )
                                 
{
    const char method_name[] = "CTcdbHbase::SetLNodeData";
    TRACE_ENTRY;

    if ( TcTraceSettings & (TC_TRACE_NODE | TC_TRACE_REQUEST) )
    {
        trace_printf( "%s@%d nid=%d, pnid=%d, name=%s, excluded cores=(%d:%d),"
                      " cores=(%d:%d), processors=%d, roles=%d\n"
                    , method_name, __LINE__
                    , nid
                    , pnid
                    , nodename
                    , excfirstcore
                    , exclastcore
                    , firstcore
                    , lastcore
                    , processors
                    , roles );
    }

    nodeConfig.nid  = nid;
    nodeConfig.pnid = pnid;
    strncpy( nodeConfig.node_name
           , nodename
           , sizeof(nodeConfig.node_name) );
    nodeConfig.excluded_first_core = excfirstcore;
    nodeConfig.excluded_last_core = exclastcore;
    nodeConfig.first_core = firstcore;
    nodeConfig.last_core = lastcore;
    nodeConfig.processors = processors;
    nodeConfig.roles  = roles;

    TRACE_EXIT;
}

int CTcdbHbase::SetPersistProcessData( const char       *persistkey
                                     , const char       *persistvalue
                                     , TcPersistConfiguration_t &persistConfig )
{
    const char method_name[] = "CTcdbHbase::SetPersistProcessData";
    TRACE_ENTRY;

    char workValue[TC_PERSIST_KEY_MAX];
    char *pch;
    char *token1;
    char *token2;
    static const char *delimNone = "\0";
    static const char *delimComma = ",";

    if ( TcTraceSettings & (TC_TRACE_PERSIST | TC_TRACE_REQUEST) )
    {
        trace_printf( "%s@%d persistKey=%s, persistValue=%s\n"
                    , method_name, __LINE__
                    , persistkey, persistvalue );
    }
    
    strncpy( workValue, persistvalue, sizeof(workValue) );

    pch = (char *) strstr( persistkey, PERSIST_PROCESS_NAME_KEY );
    if (pch != NULL)
    {
        strncpy( persistConfig.process_name
               , workValue
               , sizeof(persistConfig.process_name) );
        goto done;
    }
    pch = (char *) strstr( persistkey, PERSIST_PROCESS_TYPE_KEY );
    if (pch != NULL)
    {
        strncpy( persistConfig.process_type
               , workValue
               , sizeof(persistConfig.process_type) );
        goto done;
    }
    pch = (char *) strstr( persistkey, PERSIST_PROGRAM_NAME_KEY );
    if (pch != NULL)
    {
        strncpy( persistConfig.program_name
               , workValue
               , sizeof(persistConfig.program_name) );
        goto done;
    }
    pch = (char *) strstr( persistkey, PERSIST_PROGRAM_ARGS_KEY );
    if (pch != NULL)
    {
        strncpy( persistConfig.program_args
               , workValue
               , sizeof(persistConfig.program_args) );
        goto done;
    }
    pch = (char *) strstr( persistkey, PERSIST_STDOUT_KEY );
    if (pch != NULL)
    {
        strncpy( persistConfig.std_out
               , workValue
               , sizeof(persistConfig.std_out) );
        goto done;
    }
    pch = (char *) strstr( persistkey, PERSIST_REQUIRES_DTM );
    if (pch != NULL)
    {
        persistConfig.requires_DTM = (strcasecmp(workValue,"Y") == 0) 
                                    ? true : false;
        goto done;
    }
    pch = (char *) strstr( persistkey, PERSIST_RETRIES_KEY );
    if (pch != NULL)
    {
        // Set retries
        token1 = strtok( workValue, delimComma );
        if (token1)
        {
            persistConfig.persist_retries = atoi(token1);
        }
        // Set time window
        token2 = strtok( NULL, delimNone );
        if (token2)
        {
            persistConfig.persist_window = atoi(token2);
        }
        goto done;
    }
    pch = (char *) strstr( persistkey, PERSIST_ZONES_KEY );
    if (pch != NULL)
    {
        strncpy( persistConfig.persist_zones
               , workValue
               , sizeof(persistConfig.persist_zones) );
        goto done;
    }
    else
    {
        TRACE_EXIT;
        return( TCDBCORRUPT );
    }

done:

    TRACE_EXIT;
    return( TCSUCCESS );
}

void CTcdbHbase::SetPNodeData( int pnid
                             , const char *nodename
                             , int excfirstcore
                             , int exclastcore
                             , TcPhysicalNodeConfiguration_t &pnodeConfig )
                                 
{
    const char method_name[] = "CTcdbHbase::SetPNodeData";
    TRACE_ENTRY;

    if ( TcTraceSettings & (TC_TRACE_NODE | TC_TRACE_REQUEST) )
    {
        trace_printf( "%s@%d pnid=%d, name=%s, excluded cores=(%d:%d)\n"
                    , method_name, __LINE__
                    , pnid
                    , nodename
                    , excfirstcore
                    , exclastcore );
    }

    pnodeConfig.pnid = pnid;
    strncpy( pnodeConfig.node_name
           , nodename
           , sizeof(pnodeConfig.node_name) );
    pnodeConfig.excluded_first_core = excfirstcore;
    pnodeConfig.excluded_last_core = exclastcore;

    TRACE_EXIT;
}

ScannerMgr::ScannerMgr(int scanner)
: scanner_(scanner)
{
}

ScannerMgr::~ScannerMgr()
{
    client_->scannerClose(scanner_);
}

#endif // USE_HBASE
