/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.ipc;

import java.io.IOException;
import java.net.InetAddress;

import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.protobuf.generated.RPCProtos.RequestHeader;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.htrace.TraceInfo;

import com.google.protobuf.BlockingService;
import com.google.protobuf.Descriptors.MethodDescriptor;
import com.google.protobuf.Message;

public interface ServerCall extends RpcCallContext {

  boolean isConnectionOpen();

  String getHostAddress();

  int getRemotePort();

  InetAddress getInetAddress();

  UserGroupInformation getUser();

  long getSize();

  RequestHeader getHeader();

  String toShortString();

  String toTraceString();

  TraceInfo getTinfo();

  BlockingService getService();

  MethodDescriptor getMethodDescriptor();

  Message getParam();

  CellScanner getCellScanner();

  long getTimestamp();

  int getPriority();

  int getTimeout();

  void setResponse(Object m, final CellScanner cells, Throwable t, String errorMsg);

  void sendResponseIfReady() throws IOException;

}
