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

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHORIZATION;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.ChannelPromise;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollServerSocketChannel;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.codec.DecoderException;
import io.netty.handler.codec.ReplayingDecoder;
import io.netty.util.concurrent.GlobalEventExecutor;
import io.netty.util.internal.RecyclableArrayList;
import io.netty.util.internal.StringUtil;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CallQueueTooBigException;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.codec.Codec;
import org.apache.hadoop.hbase.conf.ConfigurationObserver;
import org.apache.hadoop.hbase.exceptions.RegionMovedException;
import org.apache.hadoop.hbase.io.BoundedByteBufferPool;
import org.apache.hadoop.hbase.io.ByteBufferInputStream;
import org.apache.hadoop.hbase.io.ByteBufferOutputStream;
import org.apache.hadoop.hbase.ipc.RpcServer.BlockingServiceAndInterface;
import org.apache.hadoop.hbase.ipc.RpcServer.Responder;
import org.apache.hadoop.hbase.monitoring.MonitoredRPCHandler;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.VersionInfo;
import org.apache.hadoop.hbase.protobuf.generated.RPCProtos.CellBlockMeta;
import org.apache.hadoop.hbase.protobuf.generated.RPCProtos.ConnectionHeader;
import org.apache.hadoop.hbase.protobuf.generated.RPCProtos.ExceptionResponse;
import org.apache.hadoop.hbase.protobuf.generated.RPCProtos.RequestHeader;
import org.apache.hadoop.hbase.protobuf.generated.RPCProtos.ResponseHeader;
import org.apache.hadoop.hbase.protobuf.generated.RPCProtos.UserInformation;
import org.apache.hadoop.hbase.regionserver.RSRpcServices;
import org.apache.hadoop.hbase.security.AccessDeniedException;
import org.apache.hadoop.hbase.security.AuthMethod;
import org.apache.hadoop.hbase.security.HBaseSaslRpcServer;
import org.apache.hadoop.hbase.security.SaslStatus;
import org.apache.hadoop.hbase.security.SaslUtil;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.security.UserProvider;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Counter;
import org.apache.hadoop.hbase.util.JVM;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod;
import org.apache.hadoop.security.authorize.PolicyProvider;
import org.apache.hadoop.util.StringUtils;
import org.apache.htrace.TraceInfo;
import org.codehaus.jackson.map.ObjectMapper;

import com.google.protobuf.BlockingService;
import com.google.protobuf.CodedInputStream;
import com.google.protobuf.Descriptors.MethodDescriptor;
import com.google.protobuf.Message;
import com.google.protobuf.ServiceException;
import com.google.protobuf.TextFormat;

public class Netty4RpcServer implements RpcServerInterface,
    ConfigurationObserver {
  public static final CallQueueTooBigException CALL_QUEUE_TOO_BIG_EXCEPTION = new CallQueueTooBigException();
  private static final Log AUDITLOG = LogFactory.getLog("SecurityLogger."
      + Server.class.getName());
  public static final byte CURRENT_VERSION = 0;
  public static final Log LOG = LogFactory.getLog(Netty4RpcServer.class);

  private static final String WARN_RESPONSE_TIME = "hbase.ipc.warn.response.time";
  private static final String WARN_RESPONSE_SIZE = "hbase.ipc.warn.response.size";
  /**
   * Minimum allowable timeout (in milliseconds) in rpc request's header. This
   * configuration exists to prevent the rpc service regarding this request as
   * timeout immediately.
   */
  private static final String MIN_CLIENT_REQUEST_TIMEOUT = "hbase.ipc.min.client.request.timeout";
  private static final int DEFAULT_MIN_CLIENT_REQUEST_TIMEOUT = 20;

  /**
   * The maximum size that we can hold in the RPC queue
   */
  private static final int DEFAULT_MAX_CALLQUEUE_SIZE = 1024 * 1024 * 1024;
  /** Default value for above params */
  private static final int DEFAULT_WARN_RESPONSE_TIME = 10000; // milliseconds
  private static final int DEFAULT_WARN_RESPONSE_SIZE = 100 * 1024 * 1024;

  private static final ObjectMapper MAPPER = new ObjectMapper();

  private final Server server;
  private final String name;
  private final List<BlockingServiceAndInterface> services;
  protected final InetSocketAddress bindAddress;
  protected final Configuration conf;
  private final RpcScheduler scheduler;
  private final boolean authorize;
  private boolean isSecurityEnabled;

  private final CountDownLatch closed = new CountDownLatch(1);
  private Channel serverChannel;
  private final ChannelGroup allChannels = new DefaultChannelGroup(GlobalEventExecutor.INSTANCE);;

  volatile boolean started = false;
  protected HBaseRPCErrorHandler errorHandler = null;
  protected MetricsHBaseServer metrics;
  protected final Counter callQueueSize = new Counter();
  private int maxQueueSize;

  private final int warnResponseTime;
  private final int warnResponseSize;

  private int slowCallLimit;
  private long incrementPeriod;
  private AtomicLong totalSlowCalls;
  private AtomicLong totalCalls;
  private UserProvider userProvider;
  private final IPCUtil ipcUtil;
  private final BoundedByteBufferPool reservoir;
  private final int minClientRequestTimeout;

  public Netty4RpcServer(final Server server, final String name,
      final List<BlockingServiceAndInterface> services,
      final InetSocketAddress bindAddress, Configuration conf,
      final RpcScheduler scheduler) throws IOException {
    if (conf.getBoolean("hbase.ipc.server.reservoir.enabled", true)) {
      this.reservoir = new BoundedByteBufferPool(conf.getInt(
          "hbase.ipc.server.reservoir.max.buffer.size", 1024 * 1024),
          conf.getInt("hbase.ipc.server.reservoir.initial.buffer.size",
              16 * 1024),
          // Make the max twice the number of handlers to be safe.
          conf.getInt("hbase.ipc.server.reservoir.initial.max", conf.getInt(
              HConstants.REGION_SERVER_HANDLER_COUNT,
              HConstants.DEFAULT_REGION_SERVER_HANDLER_COUNT) * 2),
          // By default make direct byte buffers from the buffer pool.
          conf.getBoolean("hbase.ipc.server.reservoir.direct.buffer", true));
    } else {
      reservoir = null;
    }
    this.server = server;
    this.name = name;
    this.services = services;
    this.bindAddress = bindAddress;
    this.conf = conf;
    this.metrics = new MetricsHBaseServer(name,
        new Netty4RpcServer.MetricsHBaseServerWrapperImpl(this));

    boolean useEpoll = useEpoll(conf);
    LOG.info("useEpoll: " + useEpoll);
    EventLoopGroup bossGroup = null;
    EventLoopGroup workerGroup = null;
    if (useEpoll) {
      bossGroup = new EpollEventLoopGroup(1);
      workerGroup = new EpollEventLoopGroup();
    } else {
      bossGroup = new NioEventLoopGroup(1);
      workerGroup = new NioEventLoopGroup();
    }
    ServerBootstrap bootstrap = new ServerBootstrap();
    bootstrap.group(bossGroup, workerGroup);
    if (useEpoll) {
      bootstrap.channel(EpollServerSocketChannel.class);
    } else {
      bootstrap.channel(NioServerSocketChannel.class);
    }
    //bootstrap.option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT);
    bootstrap.childOption(ChannelOption.TCP_NODELAY, true);
    bootstrap.childOption(ChannelOption.SO_KEEPALIVE, true);
    bootstrap.childOption(ChannelOption.SO_LINGER, 0);
    //bootstrap.childOption(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT);
    // bootstrap.childOption(ChannelOption.WRITE_BUFFER_HIGH_WATER_MARK, 32 * 1024);
    // bootstrap.childOption(ChannelOption.WRITE_BUFFER_LOW_WATER_MARK, 8 * 1024);
    bootstrap.childHandler(new Initializer());

    try {
      serverChannel = bootstrap.bind(this.bindAddress).sync().channel();
      LOG.info("Netty4RpcServer bind to: " + serverChannel.localAddress());
      allChannels.add(serverChannel);
    } catch (InterruptedException e) {
      throw new IOException(e);
    }
    this.maxQueueSize = this.conf.getInt("hbase.ipc.server.max.callqueue.size",
        DEFAULT_MAX_CALLQUEUE_SIZE);
    this.warnResponseTime = this.conf.getInt(WARN_RESPONSE_TIME,
        DEFAULT_WARN_RESPONSE_TIME);
    this.warnResponseSize = this.conf.getInt(WARN_RESPONSE_SIZE,
        DEFAULT_WARN_RESPONSE_SIZE);
    this.minClientRequestTimeout = conf.getInt(MIN_CLIENT_REQUEST_TIMEOUT,
        DEFAULT_MIN_CLIENT_REQUEST_TIMEOUT);
    // mark a call slow when its total time is longer than 10 secs
    this.slowCallLimit = this.conf.getInt("hbase.ipc.server.slow.call.limit",
        10 * 1000);
    this.incrementPeriod = this.conf.getLong(
        "hbase.ipc.server.metrics.inc.period", 60 * 60 * 1000L);
    this.totalSlowCalls = new AtomicLong(0);
    this.totalCalls = new AtomicLong(0);
    this.ipcUtil = new IPCUtil(this.conf);

    this.authorize = this.conf.getBoolean(HADOOP_SECURITY_AUTHORIZATION, false);
    this.userProvider = UserProvider.instantiate(this.conf);
    this.isSecurityEnabled = userProvider.isHBaseSecurityEnabled();
    if (isSecurityEnabled) {
      HBaseSaslRpcServer.init(this.conf);
    }
    this.scheduler = scheduler;
    this.scheduler.init(new RpcSchedulerContext(this));
  }

  private boolean useEpoll(Configuration conf) {
    // Config to enable native transport.
    boolean epollEnabled = conf.getBoolean("hbase.rpc.server.nativetransport",
        false);
    // Use the faster native epoll transport mechanism on linux if enabled
    if (epollEnabled && JVM.isLinux() && JVM.isAmd64()) {
      return true;
    } else {
      return false;
    }
  }

  @Override
  public void setRsRpcServices(RSRpcServices rsRpcServices) {

  }


  @Override
  public void start() {
    if (started) {
      return;
    }
    scheduler.start();
    started = true;
  }

  @Override
  public void stop() {
    LOG.info("Stopping server on " + this.bindAddress.getPort());
    allChannels.close().awaitUninterruptibly();
    serverChannel.close();
    scheduler.stop();
    closed.countDown();
  }

  @Override
  public void join() throws InterruptedException {
    closed.await();
  }

  @Override
  public InetSocketAddress getListenerAddress() {
    return ((InetSocketAddress) serverChannel.localAddress());
  }

  private void setupResponse(ByteArrayOutputStream response, Call call, Throwable t, String error)
      throws IOException {
    if (response != null) response.reset();
    call.setResponse(null, null, t, error);
  }

  class Connection {
    // If the connection header has been read or not.
    private boolean connectionHeaderRead = false;
    ConnectionHeader connectionHeader;
    /**
     * Codec the client asked use.
     */
    private Codec codec;
    /**
     * Compression codec the client asked us use.
     */
    private CompressionCodec compressionCodec;
    BlockingService service;
    protected UserGroupInformation user = null;
    private AuthMethod authMethod;
    private boolean skipInitialSaslHandshake;
    private ByteBuffer dataLengthBuffer = null;
    private ByteBuffer data;
    // Fake 'call' for failed authorization response
    private static final int AUTHORIZATION_FAILED_CALLID = -1;
    private final Call authFailedCall = new Call(AUTHORIZATION_FAILED_CALLID,
        null, null, null, null, null, this, null, 0, null, null, 0);
    private ByteArrayOutputStream authFailedResponse = new ByteArrayOutputStream();
    // Fake 'call' for SASL context setup
    private static final int SASL_CALLID = -33;
    boolean useSasl;
    private final Call saslCall = new Call(SASL_CALLID, this.service, null,
        null, null, null, this, null, 0, null, null, 0);
    // Cache the remote host & port info so that even if the socket is
    // disconnected, we can say where it used to connect to.
    protected String hostAddress;
    protected int remotePort;
    private boolean useWrap = false;
    protected Channel channel;

    Connection(Channel channel) {
      super();
      this.channel = channel;
      InetSocketAddress inetSocketAddress = ((InetSocketAddress) channel
          .remoteAddress());
      InetAddress addr = inetSocketAddress.getAddress();
      if (addr == null) {
        this.hostAddress = "*Unknown*";
      } else {
        this.hostAddress = inetSocketAddress.getAddress().getHostAddress();
      }
      this.remotePort = inetSocketAddress.getPort();
    }

    // Reads the connection header following version
    void processConnectionHeader(ByteBuffer data) throws IOException {
      this.connectionHeader = ConnectionHeader
          .parseFrom(new ByteBufferInputStream(data));
      String serviceName = connectionHeader.getServiceName();
      if (serviceName == null) {
        throw new EmptyServiceNameException();
      }
      this.service = getService(services, serviceName);
      if (this.service == null) {
        throw new UnknownServiceException(serviceName);
      }
      setupCellBlockCodecs(this.connectionHeader);
      UserGroupInformation protocolUser = createUser(connectionHeader);
      if (!useSasl) {
        user = protocolUser;
        if (user != null) {
          user.setAuthenticationMethod(AuthMethod.SIMPLE.authenticationMethod);
        }
      } else {
        // user is authenticated
        user.setAuthenticationMethod(authMethod.authenticationMethod);
        // Now we check if this is a proxy user case. If the protocol
        // user is
        // different from the 'user', it is a proxy user scenario.
        // However,
        // this is not allowed if user authenticated with DIGEST.
        if ((protocolUser != null)
            && (!protocolUser.getUserName().equals(user.getUserName()))) {
          if (authMethod == AuthMethod.DIGEST) {
            // Not allowed to doAs if token authentication is used
            throw new AccessDeniedException("Authenticated user (" + user
                + ") doesn't match what the client claims to be ("
                + protocolUser + ")");
          } else {
            // Effective user can be different from authenticated
            // user
            // for simple auth or kerberos auth
            // The user is the real user. Now we create a proxy user
            UserGroupInformation realUser = user;
            user = UserGroupInformation.createProxyUser(
                protocolUser.getUserName(), realUser);
            // Now the user is a proxy user, set Authentication
            // method Proxy.
            user.setAuthenticationMethod(AuthenticationMethod.PROXY);
          }
        }
      }
      if (connectionHeader.hasVersionInfo()) {
        AUDITLOG.info("Connection from " + this.hostAddress + " port: "
            + this.remotePort + " with version info: "
            + TextFormat.shortDebugString(connectionHeader.getVersionInfo()));
      } else {
        AUDITLOG.info("Connection from " + this.hostAddress + " port: "
            + this.remotePort + " with unknown version info");
      }
    }

    private UserGroupInformation createUser(ConnectionHeader head) {
      UserGroupInformation ugi = null;

      if (!head.hasUserInfo()) {
        return null;
      }
      UserInformation userInfoProto = head.getUserInfo();
      String effectiveUser = null;
      if (userInfoProto.hasEffectiveUser()) {
        effectiveUser = userInfoProto.getEffectiveUser();
      }
      String realUser = null;
      if (userInfoProto.hasRealUser()) {
        realUser = userInfoProto.getRealUser();
      }
      if (effectiveUser != null) {
        if (realUser != null) {
          UserGroupInformation realUserUgi = UserGroupInformation
              .createRemoteUser(realUser);
          ugi = UserGroupInformation
              .createProxyUser(effectiveUser, realUserUgi);
        } else {
          ugi = UserGroupInformation.createRemoteUser(effectiveUser);
        }
      }
      return ugi;
    }

    /**
     * Set up cell block codecs
     * 
     * @throws FatalConnectionException
     */
    private void setupCellBlockCodecs(final ConnectionHeader header)
        throws FatalConnectionException {
      // TODO: Plug in other supported decoders.
      if (!header.hasCellBlockCodecClass())
        return;
      String className = header.getCellBlockCodecClass();
      if (className == null || className.length() == 0)
        return;
      try {
        this.codec = (Codec) Class.forName(className).newInstance();
      } catch (Exception e) {
        throw new UnsupportedCellCodecException(className, e);
      }
      if (!header.hasCellBlockCompressorClass())
        return;
      className = header.getCellBlockCompressorClass();
      try {
        this.compressionCodec = (CompressionCodec) Class.forName(className)
            .newInstance();
      } catch (Exception e) {
        throw new UnsupportedCompressionCodecException(className, e);
      }
    }

    void readPreamble(ByteBuf buffer) throws IOException {
      byte[] rpcHead = { buffer.readByte(), buffer.readByte(),
          buffer.readByte(), buffer.readByte() };
      if (!Arrays.equals(HConstants.RPC_HEADER, rpcHead)) {
        doBadPreambleHandling("Expected HEADER="
            + Bytes.toStringBinary(HConstants.RPC_HEADER)
            + " but received HEADER=" + Bytes.toStringBinary(rpcHead)
            + " from " + toString());
        return;
      }
      // Now read the next two bytes, the version and the auth to use.
      int version = buffer.readByte();
      byte authbyte = buffer.readByte();
      this.authMethod = AuthMethod.valueOf(authbyte);
      if (version != CURRENT_VERSION) {
        String msg = getFatalConnectionString(version, authbyte);
        doBadPreambleHandling(msg, new WrongVersionException(msg));
        return;
      }
      if (authMethod == null) {
        String msg = getFatalConnectionString(version, authbyte);
        doBadPreambleHandling(msg, new BadAuthException(msg));
        return;
      }
      if (isSecurityEnabled && authMethod == AuthMethod.SIMPLE) {
        AccessDeniedException ae = new AccessDeniedException(
            "Authentication is required");
        setupResponse(authFailedResponse, authFailedCall, ae, ae.getMessage());
        authFailedCall.sendResponseIfReady(ChannelFutureListener.CLOSE);
        return;
      }
      if (!isSecurityEnabled && authMethod != AuthMethod.SIMPLE) {
        doRawSaslReply(SaslStatus.SUCCESS, new IntWritable(
            SaslUtil.SWITCH_TO_SIMPLE_AUTH), null, null);
        authMethod = AuthMethod.SIMPLE;
        // client has already sent the initial Sasl message and we
        // should ignore it. Both client and server should fall back
        // to simple auth from now on.
        skipInitialSaslHandshake = true;
      }
      if (authMethod != AuthMethod.SIMPLE) {
        useSasl = true;
      }
    }

    private String getFatalConnectionString(final int version,
        final byte authByte) {
      return "serverVersion=" + CURRENT_VERSION + ", clientVersion=" + version
          + ", authMethod=" + authByte + ", authSupported="
          + (authMethod != null) + " from " + toString();
    }

    private void doRawSaslReply(SaslStatus status, Writable rv, String errorClass, String error)
        throws IOException {
      ByteBufferOutputStream saslResponse = null;
      DataOutputStream out = null;
      try {
        // In my testing, have noticed that sasl messages are usually
        // in the ballpark of 100-200. That's why the initial capacity
        // is 256.
        saslResponse = new ByteBufferOutputStream(256);
        out = new DataOutputStream(saslResponse);
        out.writeInt(status.state); // write status
        if (status == SaslStatus.SUCCESS) {
          rv.write(out);
        } else {
          WritableUtils.writeString(out, errorClass);
          WritableUtils.writeString(out, error);
        }
        saslCall.setSaslTokenResponse(saslResponse.getByteBuffer());
        saslCall.sendResponseIfReady();
      } finally {
        if (saslResponse != null) {
          saslResponse.close();
        }
        if (out != null) {
          out.close();
        }
      }
    }

    private void doBadPreambleHandling(final String msg) throws IOException {
      doBadPreambleHandling(msg, new FatalConnectionException(msg));
    }

    private void doBadPreambleHandling(final String msg, final Exception e)
        throws IOException {
      LOG.warn(msg);
      Call fakeCall = new Call(-1, null, null, null, null, null, this, null,
          -1, null, null, 0);
      setupResponse(null, fakeCall, e, msg);
      // closes out the connection.
      fakeCall.sendResponseIfReady(ChannelFutureListener.CLOSE);
    }

    Object processRequest(ByteBuffer buf) throws IOException, InterruptedException {
      long totalRequestSize = buf.limit();
      int offset = 0;
      // Here we read in the header. We avoid having pb
      // do its default 4k allocation for CodedInputStream. We force it to
      // use backing array.
      CodedInputStream cis = CodedInputStream.newInstance(buf.array(), offset, buf.limit());
      int headerSize = cis.readRawVarint32();
      offset = cis.getTotalBytesRead();
      Message.Builder builder = RequestHeader.newBuilder();
      ProtobufUtil.mergeFrom(builder, cis, headerSize);
      RequestHeader header = (RequestHeader) builder.build();
      offset += headerSize;
      int id = header.getCallId();
      if (LOG.isTraceEnabled()) {
        LOG.trace("RequestHeader " + TextFormat.shortDebugString(header) + " totalRequestSize: "
            + totalRequestSize + " bytes");
      }
      // Enforcing the call queue size, this triggers a retry in the
      // client
      // This is a bit late to be doing this check - we have already read
      // in the total request.
      if ((totalRequestSize + callQueueSize.get()) > maxQueueSize) {
        final Call callTooBig = new Call(id, this.service, null, null, null,
            null, this, null, totalRequestSize, null, null, 0);
        ByteArrayOutputStream responseBuffer = new ByteArrayOutputStream();
        metrics.exception(CALL_QUEUE_TOO_BIG_EXCEPTION);
        setupResponse(responseBuffer, callTooBig, CALL_QUEUE_TOO_BIG_EXCEPTION,
          "Call queue is full on " + getListenerAddress()
              + ", is hbase.ipc.server.max.callqueue.size too small?");
        callTooBig.sendResponseIfReady();
        return null;
      }
      MethodDescriptor md = null;
      Message param = null;
      CellScanner cellScanner = null;
      try {
        if (header.hasRequestParam() && header.getRequestParam()) {
          md = this.service.getDescriptorForType().findMethodByName(header.getMethodName());
          if (md == null) throw new UnsupportedOperationException(header.getMethodName());
          builder = this.service.getRequestPrototype(md).newBuilderForType();
          cis.resetSizeCounter();
          int paramSize = cis.readRawVarint32();
          offset += cis.getTotalBytesRead();
          if (builder != null) {
            ProtobufUtil.mergeFrom(builder, cis, paramSize);
            param = builder.build();
          }
          offset += paramSize;
        }
        if (header.hasCellBlockMeta()) {
          buf.position(offset);
          cellScanner = ipcUtil.createCellScanner(this.codec, this.compressionCodec, buf);
        }
      } catch (Throwable t) {
        String msg =
            getListenerAddress() + " is unable to read call parameter from client "
                + getHostAddress();
        LOG.warn(msg, t);

        metrics.exception(t);

        // probably the hbase hadoop version does not match the running
        // hadoop version
        if (t instanceof LinkageError) {
          t = new DoNotRetryIOException(t);
        }
        // If the method is not present on the server, do not retry.
        if (t instanceof UnsupportedOperationException) {
          t = new DoNotRetryIOException(t);
        }

        final Call readParamsFailedCall = new Call(id, this.service, null,
            null, null, null, this, null, totalRequestSize, null, null, 0);
        ByteArrayOutputStream responseBuffer = new ByteArrayOutputStream();
        setupResponse(responseBuffer, readParamsFailedCall, t, msg + "; " + t.getMessage());
        readParamsFailedCall.sendResponseIfReady();
        return null;
      }

      TraceInfo traceInfo =
          header.hasTraceInfo() ? new TraceInfo(header.getTraceInfo().getTraceId(), header
              .getTraceInfo().getParentId()) : null;
      int timeout = 0;
      if (header.hasTimeout()) {
        timeout = Math.max(minClientRequestTimeout, header.getTimeout());
      }
      Call call = new Call(id, this.service, md, header, param, cellScanner,
          this, null, totalRequestSize, traceInfo, RpcServer.getRemoteIp(),
          timeout);
//      if (!scheduler.dispatch(new CallRunner(Netty4RpcServer.this, call))) {
//        callQueueSize.add(-1 * call.getSize());
//
//        ByteArrayOutputStream responseBuffer = new ByteArrayOutputStream();
//        metrics.exception(CALL_QUEUE_TOO_BIG_EXCEPTION);
//        InetSocketAddress address = getListenerAddress();
//        setupResponse(responseBuffer, call, CALL_QUEUE_TOO_BIG_EXCEPTION, "Call queue is full on "
//            + (address != null ? address : "(channel closed)") + ", too many items queued ?");
//        call.sendResponseIfReady();
//      }
      return new CallRunner(Netty4RpcServer.this, call);
    }

    private Object process(ByteBuffer buf) throws IOException,
        InterruptedException {
      if (connectionHeaderRead) {
        return processRequest(buf);
      } else {
        processConnectionHeader(buf);
        this.connectionHeaderRead = true;
        return null;
      }
    }

    public void close() {
      // disposeSasl();
    }

    public boolean isConnectionOpen() {
      return channel.isOpen();
    }

    public String getHostAddress() {
      return hostAddress;
    }

    public int getRemotePort() {
      return remotePort;
    }

    VersionInfo getVersionInfo() {
      if (connectionHeader.hasVersionInfo()) {
        return connectionHeader.getVersionInfo();
      }
      return null;
    }
  }

  /**
   * Datastructure that holds all necessary to a method invocation and then afterward, carries the
   * result.
   */
  public class Call implements ServerCall {

    protected int id; // the client's call id
    protected BlockingService service;
    protected MethodDescriptor md;
    protected RequestHeader header;
    protected Message param; // the parameter passed
    // Optional cell data passed outside of protobufs.
    protected CellScanner cellScanner;
    protected Connection connection; // connection to client
    protected long timestamp; // the time received when response is null
    // the time served when response is not null

    protected int timeout;
    /**
     * Chain of buffers to send as response.
     */
    protected BufferChain response;
    protected boolean delayResponse;
    protected Responder responder;
    protected boolean delayReturnValue; // if the return value should be
    // set at call completion
    protected long size; // size of current call
    protected boolean isError;
    protected TraceInfo tinfo;
    private ByteBuffer cellBlock = null;

    private User user;
    private InetAddress remoteAddress;

    // set at call completion
    ByteBuf responseBB = null;

    Call(int id, final BlockingService service, final MethodDescriptor md,
        RequestHeader header, Message param, CellScanner cellScanner,
        Connection connection, Responder responder, long size, TraceInfo tinfo,
        final InetAddress remoteAddress, int timeout) {
      this.id = id;
      this.service = service;
      this.md = md;
      this.header = header;
      this.param = param;
      this.cellScanner = cellScanner;
      this.connection = connection;
      this.timestamp = System.currentTimeMillis();
      this.response = null;
      this.delayResponse = false;
      this.responder = responder;
      this.isError = false;
      this.size = size;
      this.tinfo = tinfo;
      this.user = connection.user == null ? null : userProvider
          .create(connection.user);
      this.remoteAddress = remoteAddress;
      this.timeout = timeout;
    }

    /**
     * Call is done. Execution happened and we returned results to client. It is now safe to
     * cleanup.
     */
    void done() {
    }

    protected synchronized void setSaslTokenResponse(ByteBuffer response) {
      this.response = new BufferChain(response);
    }

    public synchronized void setResponse(Object m, final CellScanner cells, Throwable t,
        String errorMsg) {
      if (this.isError) return;
      if (t != null) this.isError = true;
      BufferChain bc = null;
      try {
        ResponseHeader.Builder headerBuilder = ResponseHeader.newBuilder();
        // Presume it a pb Message. Could be null.
        Message result = (Message) m;
        // Call id.
        headerBuilder.setCallId(this.id);
        if (t != null) {
          ExceptionResponse.Builder exceptionBuilder = ExceptionResponse.newBuilder();
          exceptionBuilder.setExceptionClassName(t.getClass().getName());
          exceptionBuilder.setStackTrace(errorMsg);
          exceptionBuilder.setDoNotRetry(t instanceof DoNotRetryIOException);
          if (t instanceof RegionMovedException) {
            // Special casing for this exception. This is only one
            // carrying a payload.
            // Do this instead of build a generic system for
            // allowing exceptions carry
            // any kind of payload.
            RegionMovedException rme = (RegionMovedException) t;
            exceptionBuilder.setHostname(rme.getHostname());
            exceptionBuilder.setPort(rme.getPort());
          }
          // Set the exception as the result of the method invocation.
          headerBuilder.setException(exceptionBuilder.build());
        }
        // Pass reservoir to buildCellBlock. Keep reference to returne
        // so can add it back to the
        // reservoir when finished. This is hacky and the hack is not
        // contained but benefits are
        // high when we can avoid a big buffer allocation on each rpc.
        this.cellBlock =
            ipcUtil.buildCellBlock(this.connection.codec, this.connection.compressionCodec, cells,
              reservoir);
        if (this.cellBlock != null) {
          CellBlockMeta.Builder cellBlockBuilder = CellBlockMeta.newBuilder();
          // Presumes the cellBlock bytebuffer has been flipped so
          // limit has total size in it.
          cellBlockBuilder.setLength(this.cellBlock.limit());
          headerBuilder.setCellBlockMeta(cellBlockBuilder.build());
        }
        Message header = headerBuilder.build();

        // SHX: TODO we can save all these allocation and copies, directly get one from
        // Unpool and copy the content in there.
        // for the first three bytebuffers, the sizes are known. so they can be written into
        // final ByteBuf directly.

        // Organize the response as a set of bytebuffers rather than
        // collect it all together inside
        // one big byte array; save on allocations.
        ByteBuffer bbHeader = IPCUtil.getDelimitedMessageAsByteBuffer(header);
        ByteBuffer bbResult = IPCUtil.getDelimitedMessageAsByteBuffer(result);
        int totalSize =
            bbHeader.capacity() + (bbResult == null ? 0 : bbResult.limit())
                + (this.cellBlock == null ? 0 : this.cellBlock.limit());
        ByteBuffer bbTotalSize = ByteBuffer.wrap(Bytes.toBytes(totalSize));
        bc = new BufferChain(bbTotalSize, bbHeader, bbResult, this.cellBlock);
        if (connection.useWrap) {
          bc = wrapWithSasl(bc);
        }
      } catch (IOException e) {
        LOG.warn("Exception while creating response " + e);
      }
      this.response = bc;
      responseBB = UnpooledByteBufAllocator.DEFAULT.buffer(this.response.size());
      ByteBuffer[] buffers = this.response.getBuffers();
      for (ByteBuffer bb : buffers) {
        responseBB.writeBytes(bb);
      }
    }

    private BufferChain wrapWithSasl(BufferChain bc) throws IOException {
      return null;
    }

    @Override
    public long disconnectSince() {
      return -1L;
    }

    Connection getConnection() {
      return (Connection) this.connection;
    }

    /**
     * If we have a response, and delay is not set, then respond immediately. Otherwise, do not
     * respond to client. This is called by the RPC code in the context of the Handler thread.
     */
    public synchronized void sendResponseIfReady() throws IOException {
      getConnection().channel.writeAndFlush(this);
    }

    public synchronized void sendResponseIfReady(ChannelFutureListener listener) throws IOException {
      getConnection().channel.writeAndFlush(this).addListener(listener);
    }

    @Override
    public InetAddress getInetAddress() {
      return ((InetSocketAddress) getConnection().channel.remoteAddress()).getAddress();
    }

    @Override
    public boolean isClientCellBlockSupported() {
      return this.connection != null && this.connection.codec != null;
    }

    @Override
    public User getRequestUser() {
      return user;
    }

    @Override
    public String getRequestUserName() {
      User user = getRequestUser();
      return user == null ? null : user.getShortName();
    }

    @Override
    public InetAddress getRemoteAddress() {
      return remoteAddress;
    }

    @Override
    public VersionInfo getClientVersionInfo() {
      return connection.getVersionInfo();
    }

    @Override
    public boolean isRetryImmediatelySupported() {
      // TODO Auto-generated method stub
      return false;
    }

    @Override
    public long getResponseCellSize() {
      // TODO Auto-generated method stub
      return 0;
    }

    @Override
    public void incrementResponseCellSize(long cellSize) {
      // TODO Auto-generated method stub

    }

    @Override
    public long getResponseBlockSize() {
      // TODO Auto-generated method stub
      return 0;
    }

    @Override
    public void incrementResponseBlockSize(long blockSize) {
      // TODO Auto-generated method stub

    }

    @Override
    public boolean isConnectionOpen() {
      return connection.channel.isOpen();
    }

    @Override
    public String getHostAddress() {
      return connection.getHostAddress();
    }

    @Override
    public int getRemotePort() {
      return connection.getRemotePort();
    }

    @Override
    public UserGroupInformation getUser() {
      return connection.user;
    }

    @Override
    public long getSize() {
      return this.size;
    }

    @Override
    public RequestHeader getHeader() {
      return this.header;
    }

    @Override
    public String toShortString() {
      String serviceName = this.connection.service != null ? this.connection.service
          .getDescriptorForType().getName() : "null";
      return "callId: " + this.id + " service: " + serviceName
          + " methodName: " + ((this.md != null) ? this.md.getName() : "n/a")
          + " size: "
          + StringUtils.TraditionalBinaryPrefix.long2String(this.size, "", 1)
          + " connection: " + connection.toString();
    }

    @Override
    public String toTraceString() {
      String serviceName = this.connection.service != null ? this.connection.service
          .getDescriptorForType().getName() : "";
      String methodName = (this.md != null) ? this.md.getName() : "";
      return serviceName + "." + methodName;
    }

    @Override
    public TraceInfo getTinfo() {
      return tinfo;
    }

    @Override
    public BlockingService getService() {
      return service;
    }

    @Override
    public MethodDescriptor getMethodDescriptor() {
      return md;
    }

    @Override
    public Message getParam() {
      return param;
    }

    @Override
    public CellScanner getCellScanner() {
      return cellScanner;
    }

    @Override
    public long getTimestamp() {
      return timestamp;
    }

    @Override
    public int getPriority() {
      return this.header.getPriority();
    }

    @Override
    public int getTimeout() {
      // TODO Auto-generated method stub
      return 0;
    }
  }
  
  static BlockingService getService(
      final List<BlockingServiceAndInterface> services, final String serviceName) {
    BlockingServiceAndInterface bsasi = getServiceAndInterface(services,
        serviceName);
    return bsasi == null ? null : bsasi.getBlockingService();
  }

  static BlockingServiceAndInterface getServiceAndInterface(
      final List<BlockingServiceAndInterface> services, final String serviceName) {
    for (BlockingServiceAndInterface bs : services) {
      if (bs.getBlockingService().getDescriptorForType().getName()
          .equals(serviceName)) {
        return bs;
      }
    }
    return null;
  }

  private class Initializer extends ChannelInitializer<SocketChannel> {

    @Override
    protected void initChannel(SocketChannel channel) throws Exception {
      ChannelPipeline pipeline = channel.pipeline();
      pipeline.addLast("header", new ConnectionHeaderHandler());
      // pipeline.addLast("frameDecoder", new LengthFieldBasedFrameDecoder(Integer.MAX_VALUE, 0, 4,
      // 0, 4));
      // pipeline.addLast("decoder", new MessageDecoder());
      pipeline.addLast("decoder", new NettyProtocolDecoder());
      pipeline.addLast("encoder", new MessageEncoder());
      pipeline.addLast("schedulerHandler", new SchedulerHandler());
    }

  }
  
  public class ConnectionHeaderHandler extends ReplayingDecoder<State> {
    // If initial preamble with version and magic has been read or not.
    private boolean connectionPreambleRead = false;
    private Connection connection;

    public ConnectionHeaderHandler() {
      super(State.CHECK_PROTOCOL_VERSION);
    }

    private void readPreamble(ChannelHandlerContext ctx, ByteBuf input) throws IOException {
      if (input.readableBytes() < 6) {
        return;
      }
      connection = new Connection(ctx.channel());
      connection.readPreamble(input);
      ((NettyProtocolDecoder) ctx.pipeline().get("decoder")).setConnection(connection);
      connectionPreambleRead = true;
    }

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf byteBuf, List<Object> out)
        throws Exception {
      switch (state()) {
        case CHECK_PROTOCOL_VERSION: {
          readPreamble(ctx, byteBuf);
          if (connectionPreambleRead) {
            break;
          }
          checkpoint(State.READ_AUTH_SCHEMES);
        }
      }
      ctx.pipeline().remove(this);
    }

  }

  enum State {
    CHECK_PROTOCOL_VERSION, READ_AUTH_SCHEMES
  }

  class NettyProtocolDecoder extends ChannelInboundHandlerAdapter {

    private Connection connection;
    ByteBuf cumulation;

    void setConnection(Connection connection) {
      this.connection = connection;
    }

    /**
     * Returns the actual number of readable bytes in the internal cumulative
     * buffer of this decoder. You usually do not need to rely on this value
     * to write a decoder. Use it only when you must use it at your own risk.
     * This method is a shortcut to {@link #internalBuffer() internalBuffer().readableBytes()}.
     */
    protected int actualReadableBytes() {
      return internalBuffer().readableBytes();
    }

    /**
     * Returns the internal cumulative buffer of this decoder. You usually
     * do not need to access the internal buffer directly to write a decoder.
     * Use it only when you must use it at your own risk.
     */
    protected ByteBuf internalBuffer() {
      if (cumulation != null) {
        return cumulation;
      } else {
        return Unpooled.EMPTY_BUFFER;
      }
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
      allChannels.add(ctx.channel());
      super.channelActive(ctx);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable e) {
      LOG.warn("Unexpected exception from downstream.", e);
      allChannels.remove(ctx.channel());
      ctx.channel().close();
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
      RecyclableArrayList out = RecyclableArrayList.newInstance();
      try {
        if (msg instanceof ByteBuf) {
          ByteBuf data = (ByteBuf) msg;
          if (cumulation == null) {
            cumulation = data;
            try {
              callDecode(ctx, cumulation, out);
            } finally {
              if (cumulation != null && !cumulation.isReadable()) {
                cumulation.release();
                cumulation = null;
              }
            }
          } else {
            try {
              if (cumulation.writerIndex() > cumulation.maxCapacity() - data.readableBytes()) {
                ByteBuf oldCumulation = cumulation;
                cumulation = ctx.alloc().buffer(oldCumulation.readableBytes() + data.readableBytes());
                cumulation.writeBytes(oldCumulation);
                oldCumulation.release();
              }
              cumulation.writeBytes(data);
              callDecode(ctx, cumulation, out);
            } finally {
              if (cumulation != null) {
                if (!cumulation.isReadable()) {
                  cumulation.release();
                  cumulation = null;
                } else {
                  cumulation.discardSomeReadBytes();
                }
              }
              data.release();
            }
          }
        } else {
          out.add(msg);
        }
      } catch (DecoderException e) {
        throw e;
      } catch (Throwable t) {
        throw new DecoderException(t);
      } finally {
        if (!out.isEmpty()) {
          List<Object> results = new ArrayList<Object>();
          for (Object result : out) {
            results.add(result);
          }
          ctx.fireChannelRead(results);
        }
        out.recycle();
      }
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
      allChannels.remove(ctx.channel());
      RecyclableArrayList out = RecyclableArrayList.newInstance();
      try {
        if (cumulation != null) {
          callDecode(ctx, cumulation, out);
          decodeLast(ctx, cumulation, out);
        } else {
          decodeLast(ctx, Unpooled.EMPTY_BUFFER, out);
        }
      } catch (DecoderException e) {
        throw e;
      } catch (Exception e) {
        throw new DecoderException(e);
      } finally {
        if (cumulation != null) {
          cumulation.release();
          cumulation = null;
        }

        for (int i = 0; i < out.size(); i++) {
          ctx.fireChannelRead(out.get(i));
        }
        ctx.fireChannelInactive();
      }
    }

    @Override
    public final void handlerRemoved(ChannelHandlerContext ctx) throws Exception {
      ByteBuf buf = internalBuffer();
      int readable = buf.readableBytes();
      if (buf.isReadable()) {
        ByteBuf bytes = buf.readBytes(readable);
        buf.release();
        ctx.fireChannelRead(bytes);
      }
      cumulation = null;
      ctx.fireChannelReadComplete();
    }

    /**
     * Called once data should be decoded from the given {@link ByteBuf}. This method will call
     * {@link #decode(ChannelHandlerContext, ByteBuf, List)} as long as decoding should take place.
     *
     * @param ctx the {@link ChannelHandlerContext} which this {@link ByteToMessageDecoder} belongs to
     * @param in  the {@link ByteBuf} from which to read data
     * @param out the {@link List} to which decoded messages should be added
     */
    protected void callDecode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) {
      try {
        while (in.isReadable()) {
          int outSize = out.size();
          int oldInputLength = in.readableBytes();
          decode(ctx, in, out);

          // Check if this handler was removed before try to continue the loop.
          // If it was removed it is not safe to continue to operate on the buffer
          //
          // See https://github.com/netty/netty/issues/1664
          if (ctx.isRemoved()) {
            break;
          }

          if (outSize == out.size()) {
            if (oldInputLength == in.readableBytes()) {
              break;
            } else {
              continue;
            }
          }

          if (oldInputLength == in.readableBytes()) {
            throw new DecoderException(
                StringUtil.simpleClassName(getClass()) +
                    ".decode() did not read anything but decoded a message.");
          }
        }
      } catch (DecoderException e) {
        throw e;
      } catch (Throwable cause) {
        throw new DecoderException(cause);
      }
    }

    protected void decode(ChannelHandlerContext ctx, ByteBuf buf, List<Object> out)
        throws Exception {
      ByteBuffer data = getData(buf);
      if (data != null) {
        Object result = connection.process(data);
        if (result != null) {
          out.add(result);
        }
      }
    }

    private ByteBuffer getData(ByteBuf buf) throws Exception {
      // Make sure if the length field was received.
      if (buf.readableBytes() < 4) {
        // The length field was not received yet - return null.
        // This method will be invoked again when more packets are
        // received and appended to the buffer.
        return null;
      }
      // The length field is in the buffer.

      // Mark the current buffer position before reading the length field
      // because the whole frame might not be in the buffer yet.
      // We will reset the buffer position to the marked position if
      // there's not enough bytes in the buffer.
      buf.markReaderIndex();

      // Read the length field.
      int length = buf.readInt();
      // Make sure if there's enough bytes in the buffer.
      if (buf.readableBytes() < length) {
        // The whole bytes were not received yet - return null.
        // This method will be invoked again when more packets are
        // received and appended to the buffer.

        // Reset to the marked position to read the length field again
        // next time.
        buf.resetReaderIndex();
        return null;
      }
      // There's enough bytes in the buffer. Read it.
      // ByteBuffer data = buf.toByteBuffer(buf.readerIndex(), length);
      ByteBuffer data = ByteBuffer.allocate(length);
      buf.readBytes(data);
      data.flip();
      // buf.skipBytes(length);
      return data;
    }

    /**
     * Is called one last time when the {@link ChannelHandlerContext} goes in-active. Which means the
     * {@link #channelInactive(ChannelHandlerContext)} was triggered.
     * By default this will just call {@link #decode(ChannelHandlerContext, ByteBuf, List)} but sub-classes may
     * override this for some special cleanup operation.
     */
    protected void decodeLast(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
      decode(ctx, in, out);
    }
  }

  class MessageDecoder extends ChannelInboundHandlerAdapter {
    // If the connection header has been read or not.
    private boolean connectionHeaderRead = false;

    private Connection connection;

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
      allChannels.add(ctx.channel());
      super.channelActive(ctx);
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
      ByteBuf input = (ByteBuf) msg;
      try {
        byte[] data = new byte[input.readableBytes()];
        input.readBytes(data, 0, data.length);
        ByteBuffer buf = ByteBuffer.wrap(data);
        if (!connectionHeaderRead) {
          connection.processConnectionHeader(buf);
          connectionHeaderRead = true;
        } else {
          connection.processRequest(buf);
        }
      } finally {
        input.release();
      }
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
      allChannels.remove(ctx.channel());
      super.channelInactive(ctx);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable e) {
      LOG.warn("Unexpected exception from downstream.", e);
      allChannels.remove(ctx.channel());
      ctx.channel().close();
    }

  }
  
  class SchedulerHandler extends ChannelInboundHandlerAdapter {

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
      if (!(msg instanceof CallRunner) && !(msg instanceof List)) {
        LOG.error("receive message error,only support RequestWrapper || List");
        throw new Exception("receive message error,only support RequestWrapper || List");
      }
      if (msg instanceof List) {
        List messages = (List) msg;
        for (Object messageObject : messages) {
          handleSingleRequest(ctx, messageObject);
        }
      } else {
        handleSingleRequest(ctx, msg);
      }
    }

    private void handleSingleRequest(final ChannelHandlerContext ctx, final Object message)
        throws IOException, InterruptedException {
      CallRunner task = (CallRunner) message;
      if (!scheduler.dispatch(task)) {
        callQueueSize.add(-1 * task.getCall().getSize());
        metrics.exception(CALL_QUEUE_TOO_BIG_EXCEPTION);
        InetSocketAddress address = getListenerAddress();
        task.getCall().setResponse(
          null,
          null,
          CALL_QUEUE_TOO_BIG_EXCEPTION,
          "Call queue is full on " + (address != null ? address : "(channel closed)")
              + ", too many items queued ?");
        task.getCall().sendResponseIfReady();
      }
    }

  }

  class MessageEncoder extends ChannelOutboundHandlerAdapter {

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) {
      final Call call = (Call) msg;
//      BufferChain buf = call.response;
//      ByteBuffer[] buffers = buf.getBuffers();
//      ByteBuf encoded = ctx.alloc().buffer(buf.size());
//      for (ByteBuffer bb : buffers) {
//        encoded.writeBytes(bb);
//      }
//      ctx.write(encoded, promise).addListener(new CallWriteListener(call));
      ctx.write(call.responseBB, promise).addListener(new CallWriteListener(call));
    }

  }
  
  class CallWriteListener implements ChannelFutureListener {
    private Call call;

    CallWriteListener(Call call) {
      this.call = call;
    }

    @Override
    public void operationComplete(ChannelFuture future) throws Exception {
      if (future.isSuccess()) {
        metrics.sentBytes(call.response.size());
        // LOG.info("send response for: " + call.toShortString() + " success.");
      } else {
        // LOG.info("send response for: " + call.toShortString() + " fail.");
      }
    }

  }

  @Override
  public void setSocketSendBufSize(int size) {
  }

  @Override
  public void onConfigurationChange(Configuration conf) {
    // TODO Auto-generated method stub
  }

  @Override
  public boolean isStarted() {
    return this.started;
  }

  @Override
  public Pair<Message, CellScanner> call(BlockingService service,
      MethodDescriptor md, Message param, CellScanner cellScanner,
      long receiveTime, MonitoredRPCHandler status) throws IOException,
      ServiceException {
    return call(service, md, param, cellScanner, receiveTime, status, 0);
  }

  @Override
  public Pair<Message, CellScanner> call(BlockingService service,
      MethodDescriptor md, Message param, CellScanner cellScanner,
      long receiveTime, MonitoredRPCHandler status, int timeout)
      throws IOException, ServiceException {
    try {
      status.setRPC(md.getName(), new Object[] { param }, receiveTime);
      // TODO: Review after we add in encoded data blocks.
      status.setRPCPacket(param);
      status.resume("Servicing call");
      // get an instance of the method arg type
      long startTime = System.currentTimeMillis();
      PayloadCarryingRpcController controller = new PayloadCarryingRpcController(
          cellScanner);
      controller.setCallTimeout(timeout);
      Message result = service.callBlockingMethod(md, controller, param);
      long endTime = System.currentTimeMillis();
      int processingTime = (int) (endTime - startTime);
      int qTime = (int) (startTime - receiveTime);
      int totalTime = (int) (endTime - receiveTime);
      if (LOG.isTraceEnabled()) {
        LOG.trace(RpcServer.CurCall.get().toString() + ", response "
            + TextFormat.shortDebugString(result) + " queueTime: " + qTime
            + " processingTime: " + processingTime + " totalTime: " + totalTime);
      }
      long requestSize = param.getSerializedSize();
      long responseSize = result.getSerializedSize();
      metrics.dequeuedCall(qTime);
      metrics.processedCall(processingTime);
      metrics.totalCall(totalTime);
      metrics.receivedRequest(requestSize);
      metrics.sentResponse(responseSize);
      // log any RPC responses that are slower than the configured warn
      // response time or larger than configured warning size
      boolean tooSlow = (processingTime > warnResponseTime && warnResponseTime > -1);
      boolean tooLarge = (responseSize > warnResponseSize && warnResponseSize > -1);
      if (tooSlow || tooLarge) {
        // when tagging, we let TooLarge trump TooSmall to keep output simple
        // note that large responses will often also be slow.
        logResponse(param, md.getName(), md.getName() + "("
            + param.getClass().getName() + ")", (tooLarge ? "TooLarge"
            : "TooSlow"), status.getClient(), startTime, processingTime, qTime,
            responseSize);
      }
      return new Pair<Message, CellScanner>(result, controller.cellScanner());
    } catch (Throwable e) {
      // The above callBlockingMethod will always return a SE. Strip the SE
      // wrapper before
      // putting it on the wire. Its needed to adhere to the pb Service
      // Interface but we don't
      // need to pass it over the wire.
      if (e instanceof ServiceException) {
        if (e.getCause() == null) {
          LOG.debug("Caught a ServiceException with null cause", e);
        } else {
          e = e.getCause();
        }
      }

      // increment the number of requests that were exceptions.
      metrics.exception(e);

      if (e instanceof LinkageError)
        throw new DoNotRetryIOException(e);
      if (e instanceof IOException)
        throw (IOException) e;
      LOG.error("Unexpected throwable object ", e);
      throw new IOException(e.getMessage(), e);
    }
  }

  /**
   * Logs an RPC response to the LOG file, producing valid JSON objects for
   * client Operations.
   * 
   * @param param
   *          The parameters received in the call.
   * @param methodName
   *          The name of the method invoked
   * @param call
   *          The string representation of the call
   * @param tag
   *          The tag that will be used to indicate this event in the log.
   * @param clientAddress
   *          The address of the client who made this call.
   * @param startTime
   *          The time that the call was initiated, in ms.
   * @param processingTime
   *          The duration that the call took to run, in ms.
   * @param qTime
   *          The duration that the call spent on the queue prior to being
   *          initiated, in ms.
   * @param responseSize
   *          The size in bytes of the response buffer.
   */
  void logResponse(Message param, String methodName, String call, String tag,
      String clientAddress, long startTime, int processingTime, int qTime,
      long responseSize) throws IOException {
    // base information that is reported regardless of type of call
    Map<String, Object> responseInfo = new HashMap<String, Object>();
    responseInfo.put("starttimems", startTime);
    responseInfo.put("processingtimems", processingTime);
    responseInfo.put("queuetimems", qTime);
    responseInfo.put("responsesize", responseSize);
    responseInfo.put("client", clientAddress);
    responseInfo.put("class", server == null ? "" : server.getClass()
        .getSimpleName());
    responseInfo.put("method", methodName);
    responseInfo.put("call", call);
    responseInfo.put("param", ProtobufUtil.getShortTextFormat(param));
    LOG.warn("(response" + tag + "): "
        + MAPPER.writeValueAsString(responseInfo));
  }

  @Override
  public void setErrorHandler(HBaseRPCErrorHandler handler) {
    this.errorHandler = handler;
  }

  @Override
  public HBaseRPCErrorHandler getErrorHandler() {
    return this.errorHandler;
  }

  @Override
  public MetricsHBaseServer getMetrics() {
    return metrics;
  }

  @Override
  public void addCallSize(long diff) {
    this.callQueueSize.add(diff);
  }

  @Override
  public void refreshAuthManager(PolicyProvider pp) {
    // TODO Auto-generated method stub
  }

  @Override
  public RpcScheduler getScheduler() {
    return scheduler;
  }

  /**
   * @return The number of clients currently connected to this server.
   */
  public int getNumberOfConnections() {
    // allChannels also contains the server channel, so exclude that from
    // the count.
    return allChannels.size() - 1;
  }

  public class MetricsHBaseServerWrapperImpl implements
      MetricsHBaseServerWrapper {

    private Netty4RpcServer server;

    MetricsHBaseServerWrapperImpl(Netty4RpcServer nettyRpcServer) {
      this.server = nettyRpcServer;
    }

    private boolean isServerStarted() {
      return this.server != null && this.server.isStarted();
    }

    @Override
    public long getTotalQueueSize() {
      if (!isServerStarted()) {
        return 0;
      }
      return server.callQueueSize.get();
    }

    @Override
    public int getGeneralQueueLength() {
      if (!isServerStarted() || this.server.getScheduler() == null) {
        return 0;
      }
      return server.getScheduler().getGeneralQueueLength();
    }

    @Override
    public int getReplicationQueueLength() {
      if (!isServerStarted() || this.server.getScheduler() == null) {
        return 0;
      }
      return server.getScheduler().getReplicationQueueLength();
    }

    @Override
    public int getPriorityQueueLength() {
      if (!isServerStarted() || this.server.getScheduler() == null) {
        return 0;
      }
      return server.getScheduler().getPriorityQueueLength();
    }

    @Override
    public int getNumOpenConnections() {
      if (!isServerStarted()) {
        return 0;
      }
      return server.getNumberOfConnections();
    }

    @Override
    public int getActiveRpcHandlerCount() {
      if (!isServerStarted() || this.server.getScheduler() == null) {
        return 0;
      }
      return server.getScheduler().getActiveRpcHandlerCount();
    }

    @Override
    public long getNumGeneralCallsDropped() {
      // TODO Auto-generated method stub
      return 0;
    }

    @Override
    public long getNumLifoModeSwitches() {
      // TODO Auto-generated method stub
      return 0;
    }
  }
}
