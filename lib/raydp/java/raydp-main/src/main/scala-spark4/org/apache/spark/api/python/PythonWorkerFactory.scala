/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.api.python

import java.io.{DataInputStream, DataOutputStream, EOFException, File, InputStream}
import java.net.{InetAddress, InetSocketAddress, SocketException, StandardProtocolFamily, UnixDomainSocketAddress}
import java.net.SocketTimeoutException
import java.nio.channels._
import java.util.Arrays
import java.util.UUID
import java.util.concurrent.TimeUnit
import javax.annotation.concurrent.GuardedBy

import scala.collection.mutable
import scala.jdk.CollectionConverters._
import scala.jdk.OptionConverters._

import io.ray.api.PyActorHandle

import org.apache.spark._
import org.apache.spark.errors.SparkCoreErrors
import org.apache.spark.internal.Logging
import org.apache.spark.internal.LogKeys._
import org.apache.spark.internal.config.Python.PYTHON_FACTORY_IDLE_WORKER_MAX_POOL_SIZE
import org.apache.spark.raydp.RayPythonWorkerUtils
import org.apache.spark.security.SocketAuthHelper
import org.apache.spark.util.{RedirectThread, Utils}

case class PythonWorker(channel: SocketChannel) {

  private[this] var selectorOpt: Option[Selector] = None
  private[this] var selectionKeyOpt: Option[SelectionKey] = None

  def selector: Selector = selectorOpt.orNull
  def selectionKey: SelectionKey = selectionKeyOpt.orNull

  private def closeSelector(): Unit = {
    selectionKeyOpt.foreach(_.cancel())
    selectorOpt.foreach(_.close())
  }

  def refresh(): this.type = synchronized {
    closeSelector()
    if (channel.isBlocking) {
      selectorOpt = None
      selectionKeyOpt = None
    } else {
      val selector = Selector.open()
      selectorOpt = Some(selector)
      selectionKeyOpt =
        Some(channel.register(selector, SelectionKey.OP_READ | SelectionKey.OP_WRITE))
    }
    this
  }

  def stop(): Unit = synchronized {
    closeSelector()
    Option(channel).foreach(_.close())
  }
}

private[spark] class PythonWorkerFactory(
    pythonExec: String,
    workerModule: String,
    daemonModule: String,
    envVars: Map[String, String],
    val useDaemonEnabled: Boolean)
  extends Logging { self =>

  def this(
      pythonExec: String,
      workerModule: String,
      envVars: Map[String, String],
      useDaemonEnabled: Boolean) =
    this(pythonExec, workerModule, PythonWorkerFactory.defaultDaemonModule,
      envVars, useDaemonEnabled)

  import PythonWorkerFactory._

  // Because forking processes from Java is expensive, we prefer to launch a single Python daemon,
  // pyspark/daemon.py (by default) and tell it to fork new workers for our tasks. This daemon
  // currently only works on UNIX-based systems now because it uses signals for child management,
  // so we can also fall back to launching workers, pyspark/worker.py (by default) directly.
  private val useDaemon = {
    // This flag is ignored on Windows as it's unable to fork.
    !Utils.isWindows && useDaemonEnabled
  }

  private val conf = SparkEnv.get.conf
  private val authHelper = new SocketAuthHelper(conf)
  private val isUnixDomainSock = authHelper.isUnixDomainSock

  @GuardedBy("self")
  private var daemon: Process = null
  val daemonHost = InetAddress.getLoopbackAddress()
  @GuardedBy("self")
  private var daemonPort: Int = 0
  @GuardedBy("self")
  private val daemonWorkers = new mutable.WeakHashMap[PythonWorker, ProcessHandle]()
  @GuardedBy("self")
  private var daemonSockPath: String = _
  @GuardedBy("self")
  // Visible for testing
  private[spark] val idleWorkers = new mutable.Queue[PythonWorker]()
  @GuardedBy("self")
  private val maxIdleWorkerPoolSize =
    conf.get(PYTHON_FACTORY_IDLE_WORKER_MAX_POOL_SIZE)
  @GuardedBy("self")
  private var lastActivityNs = 0L
  new MonitorThread().start()

  // RayDP patch: simple workers are Ray Python actors, not local Processes.
  @GuardedBy("self")
  private val simpleWorkers = new mutable.WeakHashMap[PythonWorker, PyActorHandle]()

  private val pythonPath = PythonUtils.mergePythonPaths(
    PythonUtils.sparkPythonPath,
    envVars.getOrElse("PYTHONPATH", ""),
    sys.env.getOrElse("PYTHONPATH", ""))

  def create(): (PythonWorker, Option[ProcessHandle]) = {
    if (useDaemon) {
      self.synchronized {
        // Pull from idle workers until we get one that is alive, otherwise create a new one.
        while (idleWorkers.nonEmpty) {
          val worker = idleWorkers.dequeue()
          daemonWorkers.get(worker).foreach { workerHandle =>
            if (workerHandle.isAlive()) {
              try {
                return (worker.refresh(), Some(workerHandle))
              } catch {
                case _: CancelledKeyException => /* pass */
              }
            }
          }
          logWarning(log"Worker ${MDC(WORKER, worker)} " +
            log"process from idle queue is dead, discarding.")
          stopWorker(worker)
        }
      }
      createThroughDaemon()
    } else {
      createSimpleWorker(blockingMode = false)
    }
  }

  /**
   * Connect to a worker launched through pyspark/daemon.py (by default), which forks python
   * processes itself to avoid the high cost of forking from Java. This currently only works
   * on UNIX-based systems.
   */
  private def createThroughDaemon(): (PythonWorker, Option[ProcessHandle]) = {

    def createWorker(): (PythonWorker, Option[ProcessHandle]) = {
      val socketChannel = if (isUnixDomainSock) {
        SocketChannel.open(UnixDomainSocketAddress.of(daemonSockPath))
      } else {
        SocketChannel.open(new InetSocketAddress(daemonHost, daemonPort))
      }
      // These calls are blocking.
      val pid = new DataInputStream(Channels.newInputStream(socketChannel)).readInt()
      if (pid < 0) {
        throw new IllegalStateException("Python daemon failed to launch worker with code " + pid)
      }
      val processHandle = ProcessHandle.of(pid).orElseThrow(
        () => new IllegalStateException("Python daemon failed to launch worker.")
      )
      authHelper.authToServer(socketChannel)
      socketChannel.configureBlocking(false)
      val worker = PythonWorker(socketChannel)
      daemonWorkers.put(worker, processHandle)
      (worker.refresh(), Some(processHandle))
    }

    self.synchronized {
      // Start the daemon if it hasn't been started
      startDaemon()

      // Attempt to connect, restart and retry once if it fails
      try {
        createWorker()
      } catch {
        case exc: SocketException =>
          logWarning("Failed to open socket to Python daemon:", exc)
          logWarning("Assuming that daemon unexpectedly quit, attempting to restart")
          stopDaemon()
          startDaemon()
          createWorker()
      }
    }
  }

  /**
   * RayDP patch: instead of launching a local worker process via ProcessBuilder and waiting
   * for it to connect back to a ServerSocketChannel we own, we ask RayPythonWorkerUtils to
   * spawn a Ray Python actor that already owns a listening port, then connect *outbound*
   * to that port. The handshake is otherwise the same: read the PID, run authToServer.
   */
  private def createSocket(port: Int): (SocketChannel, Int) = {
    val host = "127.0.0.1"
    logInfo(s"create socket ${host} ${port}")
    var retryCount = 0
    while (retryCount < 5) {
      try {
        val socketChannel = SocketChannel.open(new InetSocketAddress(host, port))
        val pid = new DataInputStream(Channels.newInputStream(socketChannel)).readInt()
        if (pid < 0) {
          throw new IllegalStateException(
            "Python daemon failed to launch worker with code " + pid)
        }
        authHelper.authToServer(socketChannel)
        return (socketChannel, pid)
      } catch {
        case e: Exception =>
          logWarning(s"Failed to open socket to Python daemon: ${e.getMessage}", e)
          retryCount += 1
          Thread.sleep(1000)
      }
    }
    throw new IllegalStateException("Python worker failed to connect back.")
  }

  /**
   * Launch a worker as a Ray Python actor and connect to it.
   */
  private[spark] def createSimpleWorker(
      blockingMode: Boolean): (PythonWorker, Option[ProcessHandle]) = {
    // Build the env the Ray actor should run under. RayDP does NOT use UNIX domain sockets
    // (the worker process is managed by Ray, not by us, so the sock-dir contract is moot).
    val workerEnv = new java.util.HashMap[String, String]
    workerEnv.putAll(envVars.asJava)
    workerEnv.put("PYTHONPATH", pythonPath)
    workerEnv.put("PYTHONUNBUFFERED", "YES")
    workerEnv.put("PYTHON_WORKER_FACTORY_SECRET", authHelper.secret)
    if (Utils.preferIPv6) {
      workerEnv.put("SPARK_PREFER_IPV6", "True")
    }
    logInfo(s"worker python path ${pythonPath}")

    try {
      self.synchronized {
        val handle = RayPythonWorkerUtils.create(SparkEnv.get.executorId, workerEnv)
        val port = RayPythonWorkerUtils.getPort(handle)
        RayPythonWorkerUtils.start(handle)
        val (socketChannel, pid) = createSocket(port)
        if (!blockingMode) {
          socketChannel.configureBlocking(false)
        }
        val worker = PythonWorker(socketChannel)
        simpleWorkers.put(worker, handle)
        (worker.refresh(), ProcessHandle.of(pid).toScala)
      }
    } catch {
      case e: Exception =>
        throw new SparkException("Python worker failed to connect back.", e)
    }
  }

  private def startDaemon(): Unit = {
    self.synchronized {
      // Is it already running?
      if (daemon != null) {
        return
      }

      try {
        // Create and start the daemon
        val command = Arrays.asList(pythonExec, "-m", daemonModule, workerModule)
        val pb = new ProcessBuilder(command)
        val jobArtifactUUID = envVars.getOrElse("SPARK_JOB_ARTIFACT_UUID", "default")
        if (jobArtifactUUID != "default") {
          val f = new File(SparkFiles.getRootDirectory(), jobArtifactUUID)
          f.mkdir()
          pb.directory(f)
        }
        val workerEnv = pb.environment()
        workerEnv.putAll(envVars.asJava)
        workerEnv.put("PYTHONPATH", pythonPath)
        if (isUnixDomainSock) {
          workerEnv.put(
            "PYTHON_WORKER_FACTORY_SOCK_DIR",
            authHelper.sockDir)
          workerEnv.put("PYTHON_UNIX_DOMAIN_ENABLED", "True")
        } else {
          workerEnv.put("PYTHON_WORKER_FACTORY_SECRET", authHelper.secret)
        }
        if (Utils.preferIPv6) {
          workerEnv.put("SPARK_PREFER_IPV6", "True")
        }
        // This is equivalent to setting the -u flag; we use it because ipython doesn't support -u:
        workerEnv.put("PYTHONUNBUFFERED", "YES")
        daemon = pb.start()

        val in = new DataInputStream(daemon.getInputStream)
        try {
          if (isUnixDomainSock) {
            daemonSockPath = PythonWorkerUtils.readUTF(in)
          } else {
            daemonPort = in.readInt()
          }
        } catch {
          case _: EOFException if daemon.isAlive =>
            throw SparkCoreErrors.eofExceptionWhileReadPortNumberError(
              daemonModule)
          case _: EOFException =>
            throw SparkCoreErrors.
              eofExceptionWhileReadPortNumberError(daemonModule, Some(daemon.exitValue))
        }

        // test that the returned port number is within a valid range.
        // note: this does not cover the case where the port number
        // is arbitrary data but is also coincidentally within range
        val isMalformedPort = !isUnixDomainSock && (daemonPort < 1 || daemonPort > 0xffff)
        val isMalformedSockPath = isUnixDomainSock && !new File(daemonSockPath).exists()
        val errorMsg =
          if (isUnixDomainSock) daemonSockPath else f"$daemonPort (0x$daemonPort%08x)"
        if (isMalformedPort || isMalformedSockPath) {
          val exceptionMessage = f"""
            |Bad data in $daemonModule's standard output. Invalid port number/socket path:
            |  $errorMsg
            |Python command to execute the daemon was:
            |  ${command.asScala.mkString(" ")}
            |Check that you don't have any unexpected modules or libraries in
            |your PYTHONPATH:
            |  $pythonPath
            |Also, check if you have a sitecustomize.py module in your python path,
            |or in your python installation, that is printing to standard output"""
          throw new SparkException(exceptionMessage.stripMargin)
        }

        // Redirect daemon stdout and stderr
        redirectStreamsToStderr(in, daemon.getErrorStream)
      } catch {
        case e: Exception =>

          // If the daemon exists, wait for it to finish and get its stderr
          val stderr = Option(daemon)
            .flatMap { d => Utils.getStderr(d, PROCESS_WAIT_TIMEOUT_MS) }
            .getOrElse("")

          stopDaemon()

          if (stderr != "") {
            val formattedStderr = stderr.replace("\n", "\n  ")
            val errorMessage = s"""
              |Error from python worker:
              |  $formattedStderr
              |PYTHONPATH was:
              |  $pythonPath
              |$e"""

            // Append error message from python daemon, but keep original stack trace
            val wrappedException = new SparkException(errorMessage.stripMargin)
            wrappedException.setStackTrace(e.getStackTrace)
            throw wrappedException
          } else {
            throw e
          }
      }

      // Important: don't close daemon's stdin (daemon.getOutputStream) so it can correctly
      // detect our disappearance.
    }
  }

  private val workerLogCapture =
    envVars.get("PYSPARK_SPARK_SESSION_UUID").map(new PythonWorkerLogCapture(_))

  /**
   * Redirect the given streams to our stderr in separate threads.
   */
  private def redirectStreamsToStderr(stdout: InputStream, stderr: InputStream): Unit = {
    try {
      new RedirectThread(workerLogCapture.map(_.wrapInputStream(stdout)).getOrElse(stdout),
        System.err, "stdout reader for " + pythonExec).start()
      new RedirectThread(stderr, System.err, "stderr reader for " + pythonExec).start()
    } catch {
      case e: Exception =>
        logError("Exception in redirecting streams", e)
    }
  }

  /**
   * Monitor all the idle workers, kill them after timeout.
   */
  private class MonitorThread extends Thread(s"Idle Worker Monitor for $pythonExec") {

    setDaemon(true)

    override def run(): Unit = {
      while (true) {
        self.synchronized {
          if (IDLE_WORKER_TIMEOUT_NS < System.nanoTime() - lastActivityNs) {
            cleanupIdleWorkers()
            lastActivityNs = System.nanoTime()
          }
        }
        Thread.sleep(10000)
      }
    }
  }

  private def cleanupIdleWorkers(): Unit = {
    while (idleWorkers.nonEmpty) {
      val worker = idleWorkers.dequeue()
      try {
        worker.stop()
      } catch {
        case e: Exception =>
          logWarning("Failed to stop worker socket", e)
      }
    }
  }

  private def stopDaemon(): Unit = {
    self.synchronized {
      if (useDaemon) {
        cleanupIdleWorkers()

        // Request shutdown of existing daemon by sending SIGTERM
        if (daemon != null) {
          daemon.destroy()
        }

        daemon = null
        daemonPort = 0
        daemonSockPath = null
      } else {
        // RayDP patch: simple workers are Ray Python actors; kill them via Ray.
        // Drain the map so subsequent isWorkerStopped() calls report true and so
        // the WeakHashMap entries don't linger past their corresponding actors.
        simpleWorkers.values.foreach(h => h.kill())
        simpleWorkers.clear()
      }
    }
  }

  def stop(): Unit = {
    workerLogCapture.foreach(_.closeAllWriters())
    stopDaemon()
  }

  def stopWorker(worker: PythonWorker): Unit = {
    self.synchronized {
      if (useDaemon) {
        if (daemon != null) {
          daemonWorkers.get(worker).foreach { processHandle =>
            // tell daemon to kill worker by pid
            val output = new DataOutputStream(daemon.getOutputStream)
            output.writeInt(processHandle.pid().toInt)
            output.flush()
            daemon.getOutputStream.flush()
          }
        }
      } else {
        // RayDP patch: ask Ray to kill the backing Python actor. Drop the map
        // entry up front so isWorkerStopped() reflects the kill immediately.
        simpleWorkers.remove(worker).foreach(h => h.kill())
      }
    }
    worker.stop()
  }

  def releaseWorker(worker: PythonWorker): Unit = {
    if (useDaemon) {
      self.synchronized {
        lastActivityNs = System.nanoTime()
        if (maxIdleWorkerPoolSize.exists(idleWorkers.size >= _)) {
          val oldestWorker = idleWorkers.dequeue()
          try {
            stopWorker(oldestWorker)
          } catch {
            case e: Exception =>
              logWarning("Failed to stop evicted worker", e)
          }
        }
        idleWorkers.enqueue(worker)
      }
    } else {
      // RayDP non-daemon: each simple worker is backed by a Ray Python actor.
      // Pooling would require re-authenticating to the same actor, which the
      // current RayDP handshake does not support cleanly, so we tear the actor
      // down on release. `stopWorker` kills the actor, removes the bookkeeping
      // entry, and closes the socket — without this, the PyActorHandle value
      // outlived its WeakHashMap key and leaked.
      try {
        stopWorker(worker)
      } catch {
        case e: Exception =>
          logWarning("Failed to stop simple worker", e)
      }
    }
  }

  def isWorkerStopped(worker: PythonWorker): Boolean = {
    assert(!useDaemon, "isWorkerStopped() is not supported for daemon mode")
    // RayDP patch: PyActorHandle has no `isAlive`. We treat the worker as running
    // as long as its handle is still in simpleWorkers; stopWorker/stopDaemon are
    // responsible for removing it on shutdown.
    !simpleWorkers.contains(worker)
  }
}

private[spark] object PythonWorkerFactory {
  val PROCESS_WAIT_TIMEOUT_MS = 10000
  val IDLE_WORKER_TIMEOUT_NS = TimeUnit.MINUTES.toNanos(1)  // kill idle workers after 1 minute

  private[spark] val defaultDaemonModule = "pyspark.daemon"
}
