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

#ifndef TCDBHBASECONFIG_H_
#define TCDBHBASECONFIG_H_

#include <map>
#include <string>
#include <vector>


class CTcdbHbaseConfig
{
public:
    class TableStats {
    public:
        TableStats();
        ~TableStats();
        void addDelete(size_t deleteCnt);
        void addInsert(size_t insertCnt);
        void addScan(size_t scanCnt);
        void setName(std::string name);

    private:
        std::string name_;
        long        deleteCnt_;
        long        insertCnt_;
        long        scanCnt_;
    };

    CTcdbHbaseConfig( void );
    ~CTcdbHbaseConfig( void );

    int         decInt(std::string anIntStr);
    std::string encInt(int anInt);

    std::string keyLN(int lNid);
    std::string keyMR(std::string procName, std::string keyName);
    std::string keyMRCD(std::string keyName);
    std::string keyMRNS(std::string keyName);
    std::string keyMRPD(std::string keyName);
    std::string keyMRUS(int nid, int id);
    std::string keyPN(int pNid);
    std::string keySN(int pNid, int spNid);

    static std::map<std::string,std::string>  attr_;
    static const int                          keyIntMax_ = 99999;
    static std::string                        keyStrMin_;
    static std::string                        keyStrMax_;
    static std::string                        tbl_;
    static std::string                        tblLnodeCf_;
    static std::string                        tblLnodeColLnid_;
    static std::string                        tblLnodeColPnid_;
    static std::string                        tblLnodeColProcessors_;
    static std::string                        tblLnodeColRoles_;
    static std::string                        tblLnodeColFirstCore_;
    static std::string                        tblLnodeColLastCore_;
    static std::string                        tblLnodeColPNodeName_;
    static std::string                        tblLnodeColPExcFirstCore_;
    static std::string                        tblLnodeColPExcLastCore_;
    static std::vector<std::string>           tblLnodeCols_;
    static std::string                        tblMrcdCf_;
    static std::string                        tblMrcdColKeyName_;
    static std::string                        tblMrcdColDataValue_;
    static std::vector<std::string>           tblMrcdCols_;
    static std::string                        tblMregCf_;
    static std::string                        tblMregColKeyName_;
    static std::string                        tblMregColProcName_;
    static std::string                        tblMregColDataValue_;
    static std::vector<std::string>           tblMregCols_;
    static std::string                        tblMrnsCf_;
    static std::string                        tblMrnsColKeyName_;
    static std::string                        tblMrnsColValueName_;
    static std::vector<std::string>           tblMrnsCols_;
    static std::string                        tblMrpdCf_;
    static std::string                        tblMrpdColKeyName_;
    static std::string                        tblMrpdColValueName_;
    static std::vector<std::string>           tblMrpdCols_;
    static std::string                        tblMrusCf_;
    static std::string                        tblMrusColNid_;
    static std::string                        tblMrusColId_;
    static std::string                        tblMrusColDataValue_;
    static std::vector<std::string>           tblMrusCols_;
    static std::string                        tblPnodeCf_;
    static std::string                        tblPnodeColPnid_;
    static std::string                        tblPnodeColNodeName_;
    static std::string                        tblPnodeColExcFirstCore_;
    static std::string                        tblPnodeColExcLastCore_;
    static std::vector<std::string>           tblPnodeCols_;
    static std::string                        tblSnodeCf_;
    static std::string                        tblSnodeColPnid_;
    static std::string                        tblSnodeColSpnid_;
    static std::string                        tblSnodeColNodeName_;
    static std::string                        tblSnodeColFirstCore_;
    static std::string                        tblSnodeColLastCore_;
    static std::vector<std::string>           tblSnodeCols_;
    TableStats                                tsLnode_;
    TableStats                                tsMrcd_;
    TableStats                                tsMreg_;
    TableStats                                tsMrns_;
    TableStats                                tsMrpd_;
    TableStats                                tsMrus_;
    TableStats                                tsPnode_;
    TableStats                                tsSnode_;
};

#endif /* TCDBHBASECONFIG_H_ */
