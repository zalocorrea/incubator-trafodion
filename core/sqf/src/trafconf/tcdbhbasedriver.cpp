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

#include <string.h>

#include "tcdbhbase.h"
#include "tctrace.h"

CTcdbHbase          tcdbHbase;
bool                traceEnabled = false;
CTrafConfigTrace    trafConfigTrace;

int decInt(const char *anIntStr)
{
    return atoi(anIntStr);
}

int doInit()
{
   int rc = tcdbHbase.Initialize();
   if (rc != 0)
       printf("DB initialize failed rc=%d\n", rc);
   return rc;
}

int addDbLNode(char **argv, int argInx)
{
   int rc = doInit();
   if (rc == 0)
   {
       // <lNodeId> <pNodeId> <numProcessors> <roleSet> <firstCore> <lastCore>
       int lNodeId = decInt(argv[argInx++]);
       int pNodeId = decInt(argv[argInx++]);
       int numProcessors = decInt(argv[argInx++]);
       int roleSet = decInt(argv[argInx++]);
       int firstCore = decInt(argv[argInx++]);
       int lastCore = decInt(argv[argInx++]);
       rc = tcdbHbase.AddLNodeData(lNodeId, pNodeId, numProcessors, roleSet, firstCore, lastCore);
       if (rc != 0)
           printf("addDbLNode(%d,%d,%d,%d,%d,%d) failed rc=%d\n",
                  lNodeId, pNodeId, numProcessors, roleSet, firstCore, lastCore, rc);
   }
   return rc;
}

int addDbNameServer(char **argv, int argInx)
{
   int rc = doInit();
   if (rc == 0)
   {
       // <nodeName>
       char *nodeName = argv[argInx++];
       rc = tcdbHbase.AddNameServer(nodeName);
       if (rc != 0)
           printf("addDbNameServer(%s) failed rc=%d\n", nodeName, rc);
   }
   return rc;
}

int addDbPersistData(char **argv, int argInx)
{
   int rc = doInit();
   if (rc == 0)
   {
       // <keyName> <valueName>
       char *keyName = argv[argInx++];
       char *valueName = argv[argInx++];
       if (valueName == NULL)
           valueName = (char *) "";
       rc = tcdbHbase.AddRegistryPersistentData(keyName, valueName);
       if (rc != 0)
           printf("addDbPersistData(%s,%s) failed rc=%d\n",
                  keyName, valueName, rc);
   }
   return rc;
}

int addDbPNode(char **argv, int argInx)
{
   int rc = doInit();
   if (rc == 0)
   {
       // <nodeId> <nodeName> <firstExcCore> <lastExcCore>
       int nodeId = decInt(argv[argInx++]);
       char *nodeName = argv[argInx++];
       int firstExcCore = decInt(argv[argInx++]);
       int lastExcCore = decInt(argv[argInx++]);
       rc = tcdbHbase.AddPNodeData(nodeName, nodeId, firstExcCore, lastExcCore);
       if (rc != 0)
           printf("addDbPNode(%s,%d,%d,%d) failed rc=%d\n",
                  nodeName, nodeId, firstExcCore, lastExcCore, rc);
   }
   return rc;
}

int addDbUniqStr(char **argv, int argInx)
{
   int rc = doInit();
   if (rc == 0)
   {
       // <nid> <id> <str>
       int nid = decInt(argv[argInx++]);
       int id = decInt(argv[argInx++]);
       char *str = argv[4];
       rc = tcdbHbase.AddUniqueString(nid, id, str);
       if (rc != 0)
           printf("addDbUniqStr(%d,%d,%s) failed rc=%d\n",
                  nid, id, str, rc);
   }
   return rc;
}

int delDbData(char ** /*argv*/, int /*argInx*/)
{
   int rc = doInit();
   if (rc == 0)
   {
       rc = tcdbHbase.DeleteData();
       if (rc != 0)
           printf("delDbData() failed rc=%d\n", rc);
   }
   return rc;
}

int delDbNameServerData(char ** /*argv*/, int /*argInx*/)
{
   int rc = doInit();
   if (rc == 0)
   {
       rc = tcdbHbase.DeleteNameServerData();
       if (rc != 0)
           printf("delDbNameServerData() failed rc=%d\n", rc);
   }
   return rc;
}

int delDbPersistData(char ** /*argv*/, int /*argInx*/)
{
   int rc = doInit();
   if (rc == 0)
   {
       rc = tcdbHbase.DeleteRegistryPersistentData();
       if (rc != 0)
           printf("delDbPersistData() failed rc=%d\n", rc);
   }
   return rc;
}

int main(int argc, char** argv)
{

    int rc = 0;
    char *env = getenv("TC_TRACE_ENABLE");
    if ( env && *env == '1' )
    {
        traceEnabled = true;
        trafConfigTrace.TraceInit( traceEnabled, "0", NULL );
    }

    if (argc > 1)
    {
        char *cmd = argv[1];
        int argInx = 2;
        if (strcmp(cmd, "addDbLNode") == 0)
        {
            if (argc > 7)
            {
                rc = addDbLNode(argv, argInx);
            }
            else
            {
                printf("addDbLNode <lNodeId> <pNodeId> <numProcessors> <roleSet> <firstCore> <lastCore>\n");
                rc = 1;
            }
        }
        else if (strcmp(cmd, "addDbNameServer") == 0)
        {
            if (argc > 2)
            {
                rc = addDbNameServer(argv, argInx);
            }
            else
            {
                printf("addDbNameServer <nodeName>\n");
                rc = 1;
            }
        }
        else if (strcmp(cmd, "addDbPersistData") == 0)
        {
            if (argc > 2)
            {
                rc = addDbPersistData(argv, argInx);
            }
            else
            {
                printf("addDbPersistData <keyName> <valueName>\n");
                rc = 1;
            }
        }
        else if (strcmp(cmd, "addDbPNode") == 0)
        {
            if (argc > 5)
            {
                rc = addDbPNode(argv, argInx);
            }
            else
            {
                printf("addDbPNode <nodeId> <nodeName> <firstExcCore> <lastExcCore>\n");
                rc = 1;
            }
        }
        else if (strcmp(cmd, "addDbUniqStr") == 0)
        {
            if (argc > 4)
            {
                rc = addDbUniqStr(argv, argInx);
            }
            else
            {
                printf("addDbUniqStr <nid> <id> <str>\n");
                rc = 1;
            }
        }
        else if (strcmp(cmd, "delDbData") == 0)
        {
            rc = delDbData(argv, argInx);
        }
        else if (strcmp(cmd, "delDbNameServerData") == 0)
        {
            rc = delDbNameServerData(argv, argInx);
        }
        else if (strcmp(cmd, "delDbPersistData") == 0)
        {
            rc = delDbPersistData(argv, argInx);
        }
        else
        {
            printf("unknown command=%s\n", cmd);
            rc = 1;
        }
    } else {
        printf("no command\n");
        rc = 1;
    }

    return rc;
}
