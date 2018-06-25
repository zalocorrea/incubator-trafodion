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

#include "tcdbhbaseconfig.h"
#if 0
#include "tctrace.h"
#endif

// initialize statics
std::map<std::string,std::string>  CTcdbHbaseConfig::attr_;
std::string                        CTcdbHbaseConfig::keyStrMin_                 (" ");
std::string                        CTcdbHbaseConfig::keyStrMax_                 ("zzzz");
std::string                        CTcdbHbaseConfig::tbl_                       ("trfconf");
std::string                        CTcdbHbaseConfig::tblLnodeCf_                ("lnode:");
std::string                        CTcdbHbaseConfig::tblMrcdCf_                 ("mrcd:");
std::string                        CTcdbHbaseConfig::tblMregCf_                 ("mreg:");
std::string                        CTcdbHbaseConfig::tblMrnsCf_                 ("mrns:");
std::string                        CTcdbHbaseConfig::tblMrpdCf_                 ("mrpd:");
std::string                        CTcdbHbaseConfig::tblMrusCf_                 ("mrus:");
std::string                        CTcdbHbaseConfig::tblPnodeCf_                ("pnode:");
std::string                        CTcdbHbaseConfig::tblSnodeCf_                ("snode:");
std::string                        CTcdbHbaseConfig::tblLnodeColLnid_           (tblLnodeCf_ + "lNid");
std::string                        CTcdbHbaseConfig::tblLnodeColPnid_           (tblLnodeCf_ + "pNid");
std::string                        CTcdbHbaseConfig::tblLnodeColProcessors_     (tblLnodeCf_ + "processors");
std::string                        CTcdbHbaseConfig::tblLnodeColRoles_          (tblLnodeCf_ + "roles");
std::string                        CTcdbHbaseConfig::tblLnodeColFirstCore_      (tblLnodeCf_ + "firstCore");
std::string                        CTcdbHbaseConfig::tblLnodeColLastCore_       (tblLnodeCf_ + "lastCore");
std::string                        CTcdbHbaseConfig::tblLnodeColPNodeName_      (tblLnodeCf_ + "PnodeName");
std::string                        CTcdbHbaseConfig::tblLnodeColPExcFirstCore_  (tblLnodeCf_ + "PexcFirstCore");
std::string                        CTcdbHbaseConfig::tblLnodeColPExcLastCore_   (tblLnodeCf_ + "PexcLastCore");
std::string                        CTcdbHbaseConfig::tblMrcdColKeyName_         (tblMrcdCf_  + "keyName");
std::string                        CTcdbHbaseConfig::tblMrcdColDataValue_       (tblMrcdCf_  + "dataValue");
std::string                        CTcdbHbaseConfig::tblMregColKeyName_         (tblMregCf_  + "keyName");
std::string                        CTcdbHbaseConfig::tblMregColProcName_        (tblMregCf_  + "procName");
std::string                        CTcdbHbaseConfig::tblMregColDataValue_       (tblMregCf_  + "dataValue");
std::string                        CTcdbHbaseConfig::tblMrnsColKeyName_         (tblMrnsCf_  + "keyName");
std::string                        CTcdbHbaseConfig::tblMrnsColValueName_       (tblMrnsCf_  + "valueName");
std::string                        CTcdbHbaseConfig::tblMrpdColKeyName_         (tblMrpdCf_  + "keyName");
std::string                        CTcdbHbaseConfig::tblMrpdColValueName_       (tblMrpdCf_  + "valueName");
std::string                        CTcdbHbaseConfig::tblMrusColNid_             (tblMrusCf_  + "nid");
std::string                        CTcdbHbaseConfig::tblMrusColId_              (tblMrusCf_  + "id");
std::string                        CTcdbHbaseConfig::tblMrusColDataValue_       (tblMrusCf_  + "dataValue");
std::string                        CTcdbHbaseConfig::tblPnodeColPnid_           (tblPnodeCf_ + "pNid");
std::string                        CTcdbHbaseConfig::tblPnodeColNodeName_       (tblPnodeCf_ + "nodeName");
std::string                        CTcdbHbaseConfig::tblPnodeColExcFirstCore_   (tblPnodeCf_ + "excFirstCore");
std::string                        CTcdbHbaseConfig::tblPnodeColExcLastCore_    (tblPnodeCf_ + "excLastCore");
std::string                        CTcdbHbaseConfig::tblSnodeColPnid_           (tblSnodeCf_ + "pNid");
std::string                        CTcdbHbaseConfig::tblSnodeColSpnid_          (tblSnodeCf_ + "spNid");
std::string                        CTcdbHbaseConfig::tblSnodeColNodeName_       (tblSnodeCf_ + "nodeName");
std::string                        CTcdbHbaseConfig::tblSnodeColFirstCore_      (tblSnodeCf_ + "firstCore");
std::string                        CTcdbHbaseConfig::tblSnodeColLastCore_       (tblSnodeCf_ + "lastCore");

std::vector<std::string>           CTcdbHbaseConfig::tblLnodeCols_;
std::vector<std::string>           CTcdbHbaseConfig::tblMrcdCols_;
std::vector<std::string>           CTcdbHbaseConfig::tblMregCols_;
std::vector<std::string>           CTcdbHbaseConfig::tblMrnsCols_;
std::vector<std::string>           CTcdbHbaseConfig::tblMrpdCols_;
std::vector<std::string>           CTcdbHbaseConfig::tblMrusCols_;
std::vector<std::string>           CTcdbHbaseConfig::tblPnodeCols_;
std::vector<std::string>           CTcdbHbaseConfig::tblSnodeCols_;

CTcdbHbaseConfig::CTcdbHbaseConfig() {
    tsLnode_.setName(tblLnodeCf_);
    tsMrcd_.setName(tblMrcdCf_);
    tsMreg_.setName(tblMregCf_);
    tsMrns_.setName(tblMrnsCf_);
    tsMrpd_.setName(tblMrpdCf_);
    tsMrus_.setName(tblMrusCf_);
    tsPnode_.setName(tblPnodeCf_);
    tsSnode_.setName(tblSnodeCf_);

    tblLnodeCols_.push_back(tblLnodeColPnid_);
    tblLnodeCols_.push_back(tblLnodeColLnid_);
    tblLnodeCols_.push_back(tblLnodeColFirstCore_);
    tblLnodeCols_.push_back(tblLnodeColLastCore_);
    tblLnodeCols_.push_back(tblLnodeColProcessors_);
    tblLnodeCols_.push_back(tblLnodeColRoles_);
    tblLnodeCols_.push_back(tblLnodeColPNodeName_);
    tblLnodeCols_.push_back(tblLnodeColPExcFirstCore_);
    tblLnodeCols_.push_back(tblLnodeColPExcLastCore_);

    tblMrcdCols_.push_back(tblMrcdColKeyName_);
    tblMrcdCols_.push_back(tblMrcdColDataValue_);

    tblMregCols_.push_back(tblMregColKeyName_);
    tblMregCols_.push_back(tblMregColProcName_);
    tblMregCols_.push_back(tblMregColDataValue_);

    tblMrnsCols_.push_back(tblMrnsColKeyName_);
    tblMrnsCols_.push_back(tblMrnsColValueName_);

    tblMrpdCols_.push_back(tblMrpdColKeyName_);
    tblMrpdCols_.push_back(tblMrpdColValueName_);

    tblMrusCols_.push_back(tblMrusColNid_);
    tblMrusCols_.push_back(tblMrusColId_);
    tblMrusCols_.push_back(tblMrusColDataValue_);

    tblPnodeCols_.push_back(tblPnodeColPnid_);
    tblPnodeCols_.push_back(tblPnodeColNodeName_);
    tblPnodeCols_.push_back(tblPnodeColExcFirstCore_);
    tblPnodeCols_.push_back(tblPnodeColExcLastCore_);

    tblSnodeCols_.push_back(tblSnodeColPnid_);
    tblSnodeCols_.push_back(tblSnodeColSpnid_);
    tblSnodeCols_.push_back(tblSnodeColNodeName_);
    tblSnodeCols_.push_back(tblSnodeColFirstCore_);
    tblSnodeCols_.push_back(tblSnodeColLastCore_);
}

CTcdbHbaseConfig::~CTcdbHbaseConfig() {
}

int CTcdbHbaseConfig::decInt(std::string anIntStr) {
    return std::stoi(anIntStr);
}

std::string CTcdbHbaseConfig::encInt(int anInt) {
    return std::to_string(anInt);
}

std::string CTcdbHbaseConfig::keyLN(int lNid) {
    char lNidS[6];
    std::sprintf(lNidS, "%05d", lNid);
    std::string lNidSS(lNidS);
    return "ln-" + lNidSS;
}

std::string CTcdbHbaseConfig::keyMR(std::string procName, std::string keyName) {
    return "mr-" + procName + "-" + keyName;
}

std::string CTcdbHbaseConfig::keyMRCD(std::string keyName) {
    return "mrcd-" + keyName;
}

std::string CTcdbHbaseConfig::keyMRNS(std::string keyName) {
    return "mrns-" + keyName;
}

std::string CTcdbHbaseConfig::keyMRPD(std::string keyName) {
    return "mrpd-" + keyName;
}

std::string CTcdbHbaseConfig::keyMRUS(int nid, int id) {
    char nidS[6];
    char idS[6];
    std::sprintf(nidS, "%05d", nid);
    std::sprintf(idS, "%05d", id);
    std::string nidSS(nidS);
    std::string idSS(idS);
    return "us-" + nidSS + '-' + idSS;
}

std::string CTcdbHbaseConfig::keyPN(int pNid) {
    char pNidS[6];
    std::sprintf(pNidS, "%05d", pNid);
    std::string pNidSS(pNidS);
    return "pn-" + pNidSS;
}

std::string CTcdbHbaseConfig::keySN(int pNid, int spNid) {
    char pNidS[6];
    char spNidS[6];
    std::sprintf(pNidS, "%05d", pNid);
    std::sprintf(spNidS, "%05d", spNid);
    std::string pNidSS(pNidS);
    std::string spNidSS(spNidS);
    return "sn-" + pNidSS + "-" + spNidSS;
}

CTcdbHbaseConfig::TableStats::TableStats()
: name_("?")
, deleteCnt_(0)
, insertCnt_(0)
, scanCnt_(0)
{
}

CTcdbHbaseConfig::TableStats::~TableStats()
{
#if 0
    const char method_name[] = "CTcdbHbaseConfig::TableStats::~TableStats";
    if (TcTraceSettings & (TC_TRACE_INIT))
    {
        trace_printf( "%s@%d table-stats: %s\n"
                    , method_name, __LINE__, name_.c_str());
        trace_printf( "%s@%d   deletes=%lu\n"
                    , method_name, __LINE__, deleteCnt_);
        trace_printf( "%s@%d   inserts=%lu\n"
                    , method_name, __LINE__, insertCnt_);
        trace_printf( "%s@%d   scans=%lu\n"
                    , method_name, __LINE__, scanCnt_);
    }
#endif
}

void CTcdbHbaseConfig::TableStats::addDelete(size_t deleteCnt)
{
    deleteCnt_ += deleteCnt;
}

void CTcdbHbaseConfig::TableStats::addInsert(size_t insertCnt)
{
    insertCnt_ += insertCnt;
}

void CTcdbHbaseConfig::TableStats::addScan(size_t scanCnt)
{
    scanCnt_ += scanCnt;
}

void CTcdbHbaseConfig::TableStats::setName(std::string name)
{
    name_ = name;
}

#endif // USE_HBASE
