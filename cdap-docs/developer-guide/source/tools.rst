.. :author: Cask Data, Inc.
   :description: Cask Data Application Platform - Tools
         :copyright: Copyright © 2014 Cask Data, Inc.

================================================
Cask Data Application Platform - Available Tools
================================================

Tools Overview
==============
CDAP comes with a bunch of tools to make developer's life easier. These tools offers various features including,
helping to debug CDAP applications, interact with them and ingest data into them,etc:

.. list-table::
    :widths: 15 60
    :header-rows: 1

    * - Tool Name
      - Description
    * - :ref:`Test Framework<TestFramework>`
      - How you can take advantage of the test framework to test your CDAP applications before deploying.
        This makes catching bugs early and easy.
    * - :ref:`Debugging<DebugCDAP>`
      - How you can debug CDAP applications in standalone mode and app containers in distributed mode.
    * - :ref:`Transactions Debugger<TxDebugger>`
      - Snapshot and inspect the state of Transaction Manager.
    * - :ref:`Ingestion Tools<Ingest>`
      - Ways to ingest data into CDAP.

.. highlight:: java

.. _TestFramework:

Testing CDAP
============

Strategies in Testing Applications
----------------------------------

CDAP comes with a convenient way to unit test your Applications.
The base for these tests is ``TestBase``, which is packaged
separately from the API in its own artifact because it depends on the
CDAP’s runtime classes. You can include it in your test dependencies
in one of two ways:

- include all JAR files in the ``lib`` directory of the CDAP SDK installation,
  or
- include the ``cdap-unit-test`` artifact in your Maven test dependencies
  (see the ``pom.xml`` file of the *WordCount* example).

Note that for building an application, you only need to include the
CDAP API in your dependencies. For testing, however, you need the
CDAP run-time. To build your test case, extend the
``TestBase`` class.

Strategies in Testing Flows
---------------------------
Let’s write a test case for the *WordCount* example::

  public class WordCountTest extends TestBase {
    @Test
    public void testWordCount() throws Exception {


The first thing we do in this test is deploy the application,
then we’ll start the Flow and the Procedure::

      // Deploy the Application
      ApplicationManager appManager = deployApplication(WordCount.class);

      // Start the Flow and the Procedure
      FlowManager flowManager = appManager.startFlow("WordCounter");
      ProcedureManager procManager = appManager.startProcedure("RetrieveCount");

Now that the Flow is running, we can send some events to the Stream::

      // Send a few events to the Stream
      StreamWriter writer = appManager.getStreamWriter("wordStream");
      writer.send("hello world");
      writer.send("a wonderful world");
      writer.send("the world says hello");

To wait for all events to be processed, we can get a metrics observer
for the last Flowlet in the pipeline (the "word associator") and wait for
its processed count to either reach 3 or time out after 5 seconds::

      // Wait for the events to be processed, or at most 5 seconds
      RuntimeMetrics metrics = RuntimeStats.
        getFlowletMetrics("WordCount", "WordCounter", "associator");
      metrics.waitForProcessed(3, 5, TimeUnit.SECONDS);

Now we can start verifying that the processing was correct by obtaining
a client for the Procedure, and then submitting a query for the global
statistics::

      // Call the Procedure
      ProcedureClient client = procManager.getClient();

      // Query global statistics
      String response = client.query("getStats", Collections.EMPTY_MAP);

If the query fails for any reason this method would throw an exception.
In case of success, the response is a JSON string. We must deserialize
the JSON string to verify the results::

      Map<String, String> map = new Gson().fromJson(response, stringMapType);
      Assert.assertEquals("9", map.get("totalWords"));
      Assert.assertEquals("6", map.get("uniqueWords"));
      Assert.assertEquals(((double)42)/9,
        (double)Double.valueOf(map.get("averageLength")), 0.001);

Then we ask for the statistics of one of the words in the test events.
The verification is a little more complex, because we have a nested map
as a response, and the value types in the top-level map are not uniform::

      // Verify some statistics for one of the words
      response = client.query("getCount", ImmutableMap.of("word","world"));
      Map<String, Object> omap = new Gson().fromJson(response, objectMapType);
      Assert.assertEquals("world", omap.get("word"));
      Assert.assertEquals(3.0, omap.get("count"));

      // The associations are a map within the map
      Map<String, Double> assocs = (Map<String, Double>) omap.get("assocs");
      Assert.assertEquals(2.0, (double)assocs.get("hello"), 0.000001);
      Assert.assertTrue(assocs.containsKey("hello"));

Strategies in Testing MapReduce Jobs
------------------------------------
In a fashion similar to `Strategies in Testing Flows`_, we can write
unit testing for MapReduce jobs. Let's write a test case for an
application that uses MapReduce. Complete source code and test can be
found in the :ref:`Purchase Example <purchase>` included in the CDAP SDK.

The ``PurchaseTest`` class should extend from
``TestBase`` similar to `Strategies in Testing Flows`::

  public class PurchaseTest extends TestBase {
    @Test
    public void test() throws Exception {

The ``PurchaseApp`` application can be deployed using the ``deployApplication``
method from the ``TestBase`` class::

      // Deploy an Application
      ApplicationManager appManager = deployApplication(PurchaseApp.class);

The MapReduce job reads from the ``purchases`` Dataset. As a first
step, the data to the ``purchases`` should be populated by running
the ``PurchaseFlow`` and sending the data to the ``purchaseStream``
Stream::

      FlowManager flowManager = appManager.startFlow("PurchaseFlow");
      // Send data to the Stream
      sendData(appManager, now);

      // Wait for the last Flowlet to process 3 events or at most 5 seconds
      RuntimeMetrics metrics = RuntimeStats.
          getFlowletMetrics("PurchaseApp", "PurchaseFlow", "collector");
      metrics.waitForProcessed(3, 5, TimeUnit.SECONDS);

Start the MapReduce job and wait for a maximum of 60 seconds::

      // Start the MapReduce job.
      MapReduceManager mrManager = appManager.startMapReduce("PurchaseHistoryBuilder");
      mrManager.waitForFinish(60, TimeUnit.SECONDS);

We can start verifying that the MapReduce job was run correctly by
obtaining a client for the Procedure, and then submitting a query for
the counts::

      ProcedureClient client = procedureManager.getClient();

      // Verify the query.
      String response = client.query("history", ImmutableMap.of("customer", "joe"));

      // Deserialize the JSON string.
      PurchaseHistory result = GSON.fromJson(response, PurchaseHistory.class);
      Assert.assertEquals(2, result.getPurchases().size());

The assertion will verify that the correct result was received.

Strategies in Testing Spark Programs
------------------------------------
Let's write a test case for an application that uses a Spark program.
Complete source code for this test can be found at :ref:`Spark PageRank<spark-page-rank>`.

The ``SparkPageRankTest`` class should extend from
``TestBase`` similar to `Strategies in Testing Flows`::

  public class SparkPageRankTest extends TestBase {
    @Test
    public void test() throws Exception {

The ``SparkPageRankTest`` application can be deployed using the ``deployApplication``
method from the ``TestBase`` class::

  // Deploy an Application
  ApplicationManager appManager = deployApplication(SparkPageRankApp.class);

The Spark program reads from the ``backlinkURLs`` Dataset. As a first
step, data in the ``backlinkURLs`` should be populated by running
the ``BackLinkFlow`` and sending the data to the Stream ``backlinkURLStream``::

  FlowManager flowManager = appManager.startFlow("BackLinkFlow");
  // Send data to the Stream
  sendData(appManager);

  // Wait for the last Flowlet to process 4 events or at most 5 seconds
  RuntimeMetrics metrics = RuntimeStats.
      getFlowletMetrics("SparkPageRank", "BackLinkFlow", "reader");
  metrics.waitForProcessed(4, 5, TimeUnit.SECONDS);

Start the Spark program and wait for a maximum of 60 seconds::

  // Start the Spark program.
  SparkManager sparkManager = appManager.startSpark("SparkPageRankProgram");
  sparkManager.waitForFinish(60, TimeUnit.SECONDS);

We verify that the Spark program ran correctly by
obtaining a client for the Procedure, and then submitting a query for
the ranks::

  ProcedureClient client = procedureManager.getClient();

  // Verify the query.
  String response = client.query("rank", ImmutableMap.of("url", "http://example.com/page1"));
  Assert.assertEquals("1.3690036520596678", response);

The assertion will verify that the correct result was received.


Validating Test Data with SQL
-----------------------------
Often the easiest way to verify that a test produced the right data is to run a SQL query - if the data sets involved
in the test case are record-scannable as described in :ref:`Querying Datasets with SQL. <data-explore>`
This can be done using a JDBC connection obtained from the test base::


  // Obtain a JDBC connection
  Connection connection = getQueryClient();
  try {
    // Run a query over the dataset
    results = connection.prepareStatement("SELECT key FROM mytable WHERE value = '1'").executeQuery();
    Assert.assertTrue(results.next());
    Assert.assertEquals("a", results.getString(1));
    Assert.assertTrue(results.next());
    Assert.assertEquals("c", results.getString(1));
    Assert.assertFalse(results.next());

  } finally {
    results.close();
    connection.close();
  }

The JDBC connection does not implement the full JDBC functionality: it does not allow variable replacement and
will not allow you to make any changes to datasets. But it is sufficient to perform test validation: you can create
or prepare statements and execute queries, then iterate over the results set and validate its correctness.

.. _DebugCDAP:

Debugging CDAP
==============

Debugging an Application in Standalone CDAP
-------------------------------------------
Any CDAP Application can be debugged in the Standalone CDAP
by attaching a remote debugger to the CDAP JVM. To enable remote
debugging:

#. Start the Standalone CDAP with ``--enable-debug``, optionally specifying a port (default is ``5005``).

   The CDAP should confirm that the debugger port is open with a message such as
   ``Remote debugger agent started on port 5005``.

#. Deploy (for example) the *HelloWorld* Application to the CDAP by dragging and dropping the
   ``HelloWorld.jar`` file from the ``/examples/HelloWorld`` directory onto the CDAP Console.

#. Open the *HelloWorld* Application in an IDE and connect to the remote debugger.

For more information, see `Attaching a Debugger`_.

**Note:** Currently, debugging is not supported under Windows.

Debugging an Application in Distributed CDAP
--------------------------------------------

.. highlight:: console

In distributed mode, an application does not run in a single JVM. Instead, its programs
are dispersed over multiple—if not many—containers in the Hadoop cluster. There is no
single place to debug the entire application.

You can, however, debug every individual container by attaching a remote debugger to it.
This is supported for each Flowlet of a Flow and each instance of a Procedure. In order
to debug a container, you need to start the element with debugging enabled by making
an HTTP request to the element’s URL. For example, the following will start a Flow for debugging::

  POST <base-url>/apps/WordCount/flows/WordCounter/debug

Note that this URL differs from the URL for starting the Flow only by the last path
component (``debug`` instead of ``start``; see
:ref:`CDAP Client HTTP API <cdap-client-http-api>`). You can pass in
runtime arguments in the exact same way as you normally would start a Flow.

Once the Flow is running, each Flowlet will detect an available port in its container
and open that port for attaching a debugger.
To find out the address of a container’s host and the container’s debug port, you can query
the CDAP for a Procedure or Flow’s live info via HTTP::

  GET <base-url>/apps/WordCount/flows/WordCounter/live-info

The response is formatted in JSON and—pretty-printed— would look similar to this::

  {
    "app": "WordCount",
    "containers": [
      {
        "container": "container_1397069870124_0010_01_000002",
        "debugPort": 42071,
        "host": "node-1004.my.cluster.net",
        "instance": 0,
        "memory": 512,
        "name": "unique",
        "type": "flowlet",
        "virtualCores": 1
      },
      ...
      {
        "container": "container_1397069870124_0010_01_000005",
        "debugPort": 37205,
        "host": "node-1003.my.cluster.net",
        "instance": 0,
        "memory": 512,
        "name": "splitter",
        "type": "flowlet",
        "virtualCores": 1
      }
    ],
    "id": "WordCounter",
    "runtime": "distributed",
    "type": "Flow",
    "yarnAppId": "application_1397069870124_0010"
  }

You see the YARN application id and the YARN container IDs of each Flowlet. More importantly, you
can see the host name and debugging port for each Flowlet. For example, the only instance of the
splitter Flowlet is running on ``node-1003.my.cluster.net`` and the debugging port is 37205. You can now
attach your debugger to the container’s JVM (see `Attaching a Debugger`_).

The corresponding HTTP requests for the ``RetrieveCounts`` Procedure of this application would be::

  POST <base-url>/apps/WordCount/procedures/RetrieveCounts/debug
  GET <base-url>/apps/WordCount/procedures/RetrieveCounts/live-info

Analysis of the response would give you the host names and debugging ports for all instances of the Procedure.

.. highlight:: java

Attaching a Debugger
--------------------

Debugging with IntelliJ
.......................

*Note:* These instructions were developed with *IntelliJ v13.1.2.*
You may need to adjust them for your installation or version.

#. From the *IntelliJ* toolbar, select ``Run -> Edit Configurations``.
#. Click ``+`` and choose ``Remote``:

   .. image:: _images/debugging/intellij_1.png

#. Create a debug configuration by entering a name, for example, ``CDAP``.
#. Enter the host name, for example, ``localhost`` or ``node-1003.my.cluster.net``
   in the Host field.
#. Enter the debugging port, for example, ``5005`` in the Port field:

   .. image:: _images/debugging/intellij_2.png

#. To start the debugger, select ``Run -> Debug -> CDAP``.
#. Set a breakpoint in any code block, for example, a Flowlet method:

   .. image:: _images/debugging/intellij_3.png

#. Start the Flow in the Console.
#. Send an event to the Stream. The control will stop at the breakpoint
   and you can proceed with debugging.


Debugging with Eclipse
......................

*Note:* These instructions were developed with *Eclipse IDE for Java Developers v4.4.0.*
You may need to adjust them for your installation or version.

#. In Eclipse, select ``Run-> Debug`` configurations.
#. In the list on the left of the window, double-click ``Remote Java Application`` to create
   a new launch configuration.

   .. image:: _images/debugging/eclipse_1.png

#. Enter a name and project, for example, ``CDAP``.

   .. image:: _images/debugging/eclipse_2.png

#. Enter the host name, for example, ``localhost`` or ``node-1003.my.cluster.net``
   in the Port field:
#. Enter the debugging port, for example, ``5005`` in the Port field:


#. In your project, click ``Debug`` to start the debugger.

#. Set a breakpoint in any code block, for example, a Flowlet method:

   .. image:: _images/debugging/eclipse_3.png

#. Start the Flow in the Console.
#. Send an event to the Stream.
#. The control stops at the breakpoint and you can proceed with debugging.


.. _TxDebugger:

Debugging the Transaction Manager (Advanced Use)
------------------------------------------------
In this advanced use section, we will explain in depth how transactions work internally.
Transactions are introduced in the :ref:`Transaction System <transaction-system>`

A transaction is defined by an identifier, which contains the time stamp, in milliseconds,
of its creation. This identifier—also called the `write pointer`—represents the version
that this transaction will use for all of its writes. It is also used to determine
the order between transactions. A transaction with a smaller write pointer than
another transaction must have been started earlier.

The `Transaction Manager` (or TM) uses the write pointers to implement `Optimistic Concurrency Control`
by maintaining state for all transactions that could be facing concurrency issues.

Transaction Manager States
..........................
The `state` of the TM is defined by these structures and rules:

- The `in-progress set`, which contains all the write pointers of transactions
  which have neither committed nor aborted.
- The `invalid set`, which contains the write pointers of the transactions
  considered invalid, and which will never be committed. A transaction
  becomes invalid only if either it times out or, for a long-running transaction,
  it is being aborted.
- A transaction's write pointer cannot be in the `in-progress set`
  and in the `invalid set` at the same time.
- The `invalid set` and the `in-progress set` together form the `excluded set`.
  When a transaction starts, a copy of this set is given to the transaction so that
  it excludes from its reads any writes performed by transactions in that set.
- The `committing change sets`, which maps write pointers of the transactions
  which have requested to commit their writes and which have passed a first round of
  conflict check to a list of keys in which they have performed those writes.
- The `committed change sets`, which has the same structure as the `committing change sets`,
  but where the write pointers refer to transactions which are already committed and
  which have passed a second round of conflict check.


Transaction Lifecycle States
............................
Here are the states a transaction goes through in its lifecycle:

- When a transaction starts, the TM creates a new write pointer
  and saves it in the `in-progress set`.
  A copy of the current excluded set is given to the transaction,
  as well as a `read pointer`. The pointer
  is an upper bound for the version of writes the transaction is allowed to read.
  It prevents the transaction from reading committed writes performed after the transaction
  started.
- The transaction then performs writes to one or more rows, with the version of those writes
  being the write pointer of the transaction.
- When the transaction wants to commit its writes, it passes to the TM all the keys where
  those writes took place. If the transaction is not in the `excluded set`, the
  TM will use the `committed change sets` structure to detect
  a conflict. A conflict happens in cases where the transaction tries to modify a
  row which, after the start of the transaction, has been modified by one
  of the transactions present in the structure.
- If there are no conflicts, all the writes of the transaction along with its write pointer
  are stored in the `committing change sets` structure.
- The client—namely, a Dataset—can then ask the TM to commit the writes. These are retrieved from the
  `committing change sets` structure. Since the `committed change sets` structure might
  have evolved since the last conflict check, another one is performed. If the
  transaction is in the `excluded set`, the commit will fail regardless
  of conflicts.
- If the second conflict check finds no overlapping transactions, the transaction's
  write pointer is removed from the `in-progress set`, and it is placed in
  the `committed change sets` structure, along with the keys it has
  written to. The writes of this transaction will now be seen by all new transactions.
- If something went wrong in one or other of the committing steps, we distinguish
  between normal and long-running transactions:

  - For a normal transaction, the cause could be that the transaction
    was found in the excluded set or that a conflict was detected.
    The client ensures rolling back the writes the transaction has made,
    and it then asks the TM to abort the transaction.
    This will remove the transaction's write pointer from either the
    `in-progress set` or the `excluded set`, and optionally from the
    `committing change sets` structure.

  - For a long-running transaction, the only possible cause is that a conflict
    was detected. Since it is assumed that the writes will not be rolled back
    by the client, the TM aborts the transaction by storing its
    write pointer into the `excluded set`. It is the only way to
    make other transactions exclude the writes performed by this transaction.

The `committed change sets` structure determines how fast conflict detections
are performed. Fortunately, not all the committed writes need to be
remembered; only those which may create a conflict with in-progress
transactions. This is why only the writes committed after the start of the oldest,
in-progress, not-long-running transaction are stored in this structure,
and why transactions which participate in conflict detection must remain
short in duration. The older they are, the bigger the `committed change sets`
structure will be and the longer conflict detection will take.

When conflict detection takes longer, so does committing a transaction
and the transaction stays longer in the `in-progress set`. The whole transaction
system can become slow if such a situation occurs.

Dumping the Transaction Manager
...............................

.. highlight:: console

CDAP comes bundled with a script that allows you to dump the state of the internal
transaction manager into a local file to allow further investigation. If your CDAP Instance
tends to become slow, you can use this tool to detect the incriminating transactions.
This script is called ``tx-debugger`` (on Windows, it is ``tx-debugger.bat``).

To download a snapshot of the state of the TM of the CDAP, use the command::

  $ tx-debugger view --host <name> [--save <filename>]

where `name` is the host name of your CDAP instance, and the optional `filename`
specifies where the snapshot should be saved. This command will
print statistics about all the structures that define the state of the TM.

You can also load a snapshot that has already been saved locally
with the command::

  $ tx-debugger view --filename <filename>

where `filename` specifies the location where the snapshot has been saved.

Here are options that you can use with the ``tx-debugger view`` commands:

- Use the ``--ids`` option to print all the transaction write pointers
  that are stored in the different structures.
- Use the ``--transaction <writePtr>`` option to specify the write pointer
  of a transaction you would like information on. If the transaction is found
  in the committing change sets or the committed change sets
  structures, this will print the keys where the transaction has
  performed writes.

While transactions don't inform you about the tasks that launched them—whether
it was a Flowlet, a MapReduce job, etc.—you can match the time
they were started with the activity of your CDAP to track potential
issues.

If you really know what you are doing and you spot a transaction in the
in-progress set that should be in the excluded set, you can
use this command to invalidate it::

  $ tx-debugger invalidate --host <name> --transaction <writePtr>

Invalidating a transaction when we know for sure that its writes should
be invalidated is useful, because those writes will then be removed
from the concerned Tables.

.. highlight:: java

.. _Ingest:

Ingesting Data
==============

.. highlight:: console

Introduction
------------

One of the first tasks of actually working with Big Data applications is getting the data in.
As data ingestion is a fundamental issue, and as one tool often does not fit all needs,
we have assembled a set of tools and applications to assist in ingesting data into CDAP:
- Java and Python APIs for controlling and writing to Streams;
- a drop zone for bulk ingestion of files ;
- a File Tailer daemon to tail local files; and
- an Apache Flume Sink implementation for writing events received from a source.

Stream Client
-------------

The stream client is for managing Streams via external applications. The stream client is currently available in Java and Python.

Supported Actions
.................

- Create a stream with a specified *stream-name*
- Retrieve or update the TTL (time-to-live) for an existing stream with a specified *stream-name*
- Truncate an existing stream (the deletion of all events that were written to the stream)
- Write an event to an existing stream

Java API
........

Download: `Java Stream Client <https://github.com/caskdata/cdap-ingest/tree/release/1.0.0>`__

Create a StreamClient instance, specifying the fields 'host' and 'port' of the CDAP instance.
Optional configurations that can be set:

- SSL: true or false, default - false
- WriterPoolSize: max thread pool size for writing events to the stream , default - 10
- Version: CDAP instance version, used as a part of the base URI, default - 'v2'
- AuthToken: If SSL is enabled, need to specify to authenticate client requests, default - null
- APIKey: If SSL is enabled, need to specify to authenticate client requests, default - null

::

   StreamClient streamClient = new RestStreamClient.Builder("localhost", 10000)
                                                  .apiKey("apiKey")
                                                  .authToken("token")
                                                  .ssl(false)
                                                  .version("v2")
                                                  .writerPoolSize(10)
                                                  .build();


Create a new Stream with the *stream-name* "purchaseStream"::

  streamClient.create("purchaseStream");

**Note** Stream Name *<stream-name>*:

- The *stream-name* should only contain ASCII letters, digits and hyphens.
- If the Stream already exists, no error is returned, and the existing Stream remains in place.

Update TTL for the *purchaseStream*; TTL is a long value and is specified in seconds::

  streamClient.setTTL("purchaseStream", newTTL);

Get the current TTL value(seconds) for the *purchaseStream*::

  long ttl = streamClient.getTTL("purchaseStream");

Create a ``StreamWriter`` instance for writing events to the *purchaseStream*::

   StreamWriter streamWriter = streamClient.createWriter("purchaseStream");

To write new events to the Stream,use any of these five methods from the ``StreamWriter`` interface::

  ListenableFuture<Void> write(String str, Charset charset);
  ListenableFuture<Void> write(String str, Charset charset, Map<String, String> headers);
  ListenableFuture<Void> write(ByteBuffer buffer);
  ListenableFuture<Void> write(ByteBuffer buffer, Map<String, String> headers);

Example::

  streamWriter.write("New log event", Charsets.UTF_8).get();

To truncate the *purchaseStream*, use::

  streamClient.truncate("purchaseStream");

When you are finished, release all resources by calling these two methods::

  streamWriter.close();
  streamClient.close();

Putting it All Together
+++++++++++++++++++++++

::

    try {
      // Create StreamClient instance with mandatory fields 'host' and 'port'.
      StreamClient streamClient = RestStreamClient.builder("localhost", 10000).build();

      try {
        // Create a stream named "purchaseStream"
        streamClient.create("purchaseStream");

        // Create StreamWriter Instance
        StreamWriter streamWriter = streamClient.createWriter("purchaseStream");

        // Get current Stream TTL value
        long currentTTL = streamClient.getTTL("purchaseStream");
        LOG.info("Current TTL value for stream {} is: {} seconds", "purchaseStream", currentTTL);
        long newTTL = 18000;

        // Update TTL value for Stream
        streamClient.setTTL("purchaseStream", newTTL);
        LOG.info("Setting new TTL: {} seconds for stream: {}", newTTL, "purchaseStream");


        String event = "192.0.2.0 - - [09/Apr/2012:08:40:43 -0400] \"GET /NoteBook/ HTTP/1.0\" 201 809 \"-\" " +
          "\"Example v0.0.0 (www.example.org)\"";

        // Write stream event to server
        ListenableFuture<Void> future = streamWriter.write(event, null);

        Futures.addCallback(future, new FutureCallback<Void>() {
          @Override
          public void onSuccess(Void contents) {
            LOG.info("Successfully written to stream {}", "purchaseStream");
          }

          @Override
          public void onFailure(Throwable throwable) {
            LOG.error("Exception while writing to stream", throwable);
          }
        });
      } finally {
        // Releasing all resources
        streamWriter.close();
        streamClient.close();
      }
    } catch (Exception e) {
      LOG.error("Exception while writing to stream", e);
    }

Also look at :ref:`Note on Stream Client <note_stream_client>`

You can refer to  :ref:`Authentication Client Usage for Java. <AuthClientJava>`


Python API
..........

Download: `Python Stream Client <https://github.com/caskdata/cdap-ingest/tree/release/1.0.0>`__

Usage
+++++

To use the Stream Client Python API, include these imports in your
Python script:

::

        from config import Config
        from streamclient import StreamClient

Configuring and Creating a Stream
+++++++++++++++++++++++++++++++++

For Creating a ``StreamClient`` instance you would need a ``config`` object:

You can create the ``config`` object by manually configuring the config options or you can read the config options
from an existing file.

1. Creating ``config`` object and configuring it manually
::

  def createStreamClient():
    config = Config()
    config.host = localhost
    config.port = 10000
    config.ssl = False
    streamClient = StreamClient(config)

2. using an existing configuration file in JSON format :ref:`Configuration-Json <JsonConfig>` to create a ``config`` object
::

   def createStreamClient():
    config = Config.read_from_file('/path/to/config.json')
    streamClient = StreamClient(config)


3. Once we have configured the stream client, we can create a stream by calling create with a stream-name
::

  streamClient.create("purchaseStream");

:ref:`Note on Stream Name <StreamName>`

Updating Time-to-Live
+++++++++++++++++++++

Update TTL for the "purchaseStream"; ``newTTL`` is a long value specified in seconds::

  streamClient.set_ttl("purchaseStream", newTTL)

Get the current TTL value for the "purchaseStream"::

  ttl = streamClient.get_ttl("purchaseStream")

Writing Events to Stream
++++++++++++++++++++++++

Create a ``StreamWriter`` instance for writing events to the Stream.

Once you have a ``StreamWriter`` instance you can write events to the stream using the ``write()`` method

Putting it All Together
+++++++++++++++++++++++
::

  def createStreamClient():
    config = Config.read_from_file('/path/to/config.json')
    streamClient = StreamClient(config)
    streamWriter = streamClient.create_writer("purchaseStream")
    streamPromise = streamWriter.write("New log Event") #async
    streamPromise.onResponse(onOKHandler, onErrorHandler)

  def onOkHandler(httpResponse): #will be executed after successful write to stream
    ...
    parse response
    return "Success"
    ...

  def onErrorHandler(httpResponse): #will be executed if stream write fails
    ...
    parse response
    return "Failure"
    ...

.. _JsonConfig:

Config file structure in JSON format::

  {
    hostname: 'localhost',    - CDAP Instance hostname
    port: 10000,              - CDAP instance port
    SSL: false                - is SSL enabled
  }

.. _StreamName:

Stream name:
  -  The name can only contain ASCII letters, digits and hyphens.
  -  If the stream already exists, no error is returned, and the existing stream remains in place.

Also look at: :ref:`Note on Stream Client <note_stream_client>`

You can refer to  - :ref:`Authentication Client Usage for Python <AuthClientPython>`


.. _note_stream_client:

Note on Stream Client
.....................

All methods from the ``StreamClient`` and ``StreamWriter`` throw
exceptions using response code analysis from the CDAP instance. These
exceptions help determine if the request was processed successfully or
not.

.. list-table::
    :widths: 40 60
    :header-rows: 1

    * - Response Code
      - Exception
    * - ``400 Bad Request``
      - javax.ws.rs.BadRequestException
    * - ``401 Unauthorized``
      - javax.ws.rs.NotAuthorizedException
    * - ``403 Forbidden``
      - javax.ws.rs.ForbiddenException
    * - ``404 Not Found``
      - javax.ws.rs.NotFoundException/co.cask.cdap.client.exception.NotFoundException
    * - ``405 Method Not Allowed``
      - javax.ws.rs.NotAcceptableException
    * - ``409 Conflict``
      - javax.ws.rs.NotAcceptableException
    * - ``500 Internal Server Error``
      - javax.ws.rs.ServerErrorException
    * - ``501 Not Implemented``
      - javax.ws.rs.NotSupportedException

In the case of a 200 OK response, no exception will be thrown.

File Tailer
-----------

File Tailer is a daemon process that performs tailing of sets of local files.
As soon as a new record has been appended to the end of a file that the daemon is monitoring,
it will send it to a Stream via the REST API.

Features
........

- Distributed as Debian and RPM packages
- Loads properties from a configuration file
- Supports rotation of log files
- Persists state and is able to resume from first unsent record
- Writes statistics info

Getting the File Tailer
.......................
Download: `File Tailer <https://github.com/caskdata/cdap-ingest/tree/release/1.0.0>`__

Installing File Tailer
......................

  on Debian/Ubuntu:
  ``apt-get install file-tailer.deb``

  on RHEL/Cent OS:
  ``rpm -ivh --force file-tailer.rpm``

Configuring File Tailer
.......................

After Installation, you can configure the daemon properties at /etc/file-tailer/conf/file-tailer.properties::

     # General pipe properties
     # Comma-separated list of pipes to be configured
     pipes=app1pipe,app2pipe

     # Pipe 1 source properties
     # Working directory (where to monitor files)
     pipes.app1pipe.source.work_dir=/var/log/app1
     # Name of log file
     pipes.app1pipe.source.file_name=app1.log

     # Pipe 1 sink properties
     # Name of the stream
     pipes.app1pipe.sink.stream_name=app1Stream
     # Host name that is used by stream client
     pipes.app1pipe.sink.host=cdap_host.example.com
     # Host port that is used by stream client
     pipes.app1pipe.sink.port=10000

**Note**: Please note that the target file must be accessible to the File Tailer user.


Starting and Stopping the Daemon
................................

To start a File Tailer daemon execute:
  ``service file-tailer start``

To stop a File Tailer daemon execute:
  ``service file-tailer start``

**Note**: File Tailer stores log files in the /var/log/file-tailer directory. PID, states and statistics are stored in the /var/run/file-tailer directory.

Configuring Authentication Client for File Tailer
.................................................

Authentication client parameters:
  - pipes.<pipe-name>.sink.auth_client - fully qualified class name of the authentication client.
  - pipes.<pipe-name>.sink.auth_client_properties - path to authentication client properties file , sample file is
    located at ``/etc/file-tailer/conf/auth-client.properties``

You can refer to :ref:`Authentication Client Usage for Java. <AuthClientJava>`


Description of Configuration Properties
.......................................

.. list-table::
    :widths: 30 60
    :header-rows: 1

    * - Property
      - Description
    * - pipes.<pipename>.name
      - ``name of the pipe``
    * - pipes.<pipename>.state_file
      - ``name of file, used to save state``
    * - pipes.<pipename>.statistics_file
      - ``name of file, used to save statistics``
    * - pipes.<pipename>.queue_size
      - ``size of queue (default 1000), of stored log records, before sending them to Stream``
    * - pipes.<pipename>.source.work_dir
      - ``path to directory being monitored for target log files``
    * - pipes.<pipename>.source.file_name
      - ``name of target log file``
    * - pipes.<pipename>.source.rotated_file_name_pattern
      - ``log file rollover pattern (default "(.*)" )``
    * - pipes.<pipename>.source.charset_name
      - ``name of charset used by Stream Client for sending logs (default "UT``
    * - pipes.<pipename>.source.record_separator
      - ``symbol that separates each log record (default "\n")``
    * - pipes.<pipename>.source.sleep_interval
      - ``interval to sleep after reading all log data (default 3000 ms)``
    * - pipes.<pipename>.source.failure_retry_limit
      - ``number of attempts to retry reading a log, if an error occurred while reading file data (default value is 0 for unlimited attempts)``
    * - pipes.<pipename>.source.failure_sleep_interval
      - ``interval to sleep if an error occurred while reading the file data (default 60000 ms)``
    * - pipes.<pipename>.sink.stream_name
      - ``name of target stream``
    * - pipes.<pipename>.sink.host
      - ``server host``
    * - pipes.<pipename>.sink.port
      - ``server port``
    * - pipes.<pipename>.sink.ssl
      - ``Secure Socket Layer mode [true|false] (default false)``
    * - pipes.<pipename>.sink.apiKey
      - ``SSL security key``
    * - pipes.<pipename>.sink.writerPoolSize
      - ``number of threads with which Stream Client sends events (default 10)``
    * - pipes.<pipename>.sink.version
      - ``CDAP server version (default "v2")``
    * - pipes.<pipename>.sink.packSize
      - ``number of logs sent at a time (default 1)``
    * - pipes.<pipename>.sink.failure_retry_limit
      - ``number of attempts to retry sending logs, if an error occurred while reading file data (default value is 0 for unlimited attempts)``
    * - pipes.<pipename>.sink.failure_sleep_interval
      - ``interval to sleep if an error occurred while sending the logs (default 60000 ms)``


Flume Sink
----------

The CDAP Sink is a `Apache Flume Sink <https://flume.apache.org>`__ implementation using the
RESTStreamWriter to write events received from a source. For example, you can configure the Flume Sink's
Agent to read data from a log file by tailing it and putting them into CDAP.

Building the Flume Sink Jar
...........................

Download: `Flume Sink <https://github.com/caskdata/cdap-ingest/tree/release/1.0.0>`__

.. list-table::
    :widths: 20 30 50
    :header-rows: 1

    * - Property
      - Value
      - Description
    * - a1.sinks.sink1.type
      - ``co.cask.cdap.flume.StreamSink``
      - Copy the CDAP sink jar to Flume lib directory and specify the fully qualified class name for this property.
    * - a1.sinks.sink1.host
      - ``host-name``
      - Host name used by the Stream client
    * - a1.sinks.sink1.streamName
      - ``Stream-name``
      - Target stream name
    * - a1.sinks.sink1.port
      - ``10000``
      - This parameter is optional; the default port is 10000
    * - a1.sinks.sink1.sslEnabled
      - ``false``
      - This parameter is used to specify if SSL is enabled, the auth client will be used if SSL is enabled, by default this value is false
    * - a1.sinks.sink1.writerPoolSize
      - ``10``
      - Number of threads to which the stream client can send events
    * - a1.sinks.sink1.version
      - ``v2``
      - CDAP Router server version

Authentication Client
.....................

To use authentication, add these authentication client configuration parameters to the sink configuration file:
  - a1.sinks.sink1.authClientClass - co.cask.cdap.security.authentication.client.basic.BasicAuthenticationClient,
    fully qualified class name of the client class
  - a1.sinks.sink1.authClientProperties - path to authentication client properties file , sample file is
    located at ``/usr/local/apache-flume/conf/auth_client.conf``

You can refer to :ref:`Authentication Client Usage for Java. <AuthClientJava>`

Flume Sink Example
..................

::

   a1.sources = r1
   a1.channels = c1
   a1.sources.r1.type = exec
   a1.sources.r1.command = tail -F /tmp/log
   a1.sources.r1.channels = c1
   a1.sinks = k1
   a1.sinks.k1.type = co.cask.cdap.flume.StreamSink
   a1.sinks.k1.channel = c1
   a1.sinks.k1.host  = 127.0.0.1
   a1.sinks.k1.port = 10000
   a1.sinks.k1.streamName = logEventStream
   a1.channels.c1.type = memory
   a1.channels.c1.capacity = 1000
   a1.channels.c1.transactionCapacity = 100



File DropZone
-------------

The File DropZone application allows you to easily perform the bulk ingestion of local files.
Files can either be directly uploaded, or they can be copied to a *work_dir*,
where they will automatically be ingested by a daemon process.

Features
........

- Distributed as Debian and RPM packages
- Loads properties from configuration file
- Supports multiple observers/topics
- Able to survive restart and resume, sending from the first unsent record of each of the existing files
- Cleanup of files that are completely sent

Getting File DropZone
.....................

Download: `File DropZone <https://github.com/caskdata/cdap-ingest/tree/release/1.0.0>`__

Installing File DropZone
........................
  on Debian/Ubuntu:
    ``apt-get install file-drop-zone.deb``

  on RHEL/Cent OS:
    ``rpm -ivh --force file-drop-zone.rpm``

Configuring File DropZone
.........................
After Installation, you can configure the daemon properties at /etc/file-drop-zone/conf/file-drop-zone.properties::

     # Polling directories interval in milliseconds
     polling_interval=5000

     # Comma-separated list of directories observers to be configured
     observers=obs1

     #Path to work directory
     work_dir=/var/file-drop-zone/

     # General observer configurations
     # Pipe is used for loading data from the file to the Stream
     observers.obs1.pipe=pipe1

     # Pipe sink properties
     # Name of the stream
     pipes.pipe1.sink.stream_name=logEventStream
     # Host name that is used by stream client
     pipes.pipe1.sink.host=localhost
     # Host port that is used by stream client
     pipes.pipe1.sink.port=10000


Starting and Stopping the Daemon
................................
To start a file DropZone daemon execute:
  ``service file-drop-zone start``

To stop a file DropZone daemon execute:
  ``service file-drop-zone stop``

Note:  File DropZone stores log files in the /var/log/file-drop-zone directory. PID, states and statistics are stored in the /var/run/file-drop-zone directory

Manual Upload of files
......................
If you would like to manually upload a file use::

  file-drop-zone load <file-path> <observer>

You can refer to :ref:`Authentication Client Usage for Java. <AuthClientJava>`

.. _AuthClientJava:

Authentication Client - Java
............................
The Authentication Client Java API fetches access tokens from the authentication service.

Supported Actions
+++++++++++++++++

- Fetch an access token from the authentication service with credentials supported by the active authentication mechanism
- Check that authentication is enabled in the CDAP instance

The current implementation supports three authentication mechanisms:
  - Basic Authentication
  - LDAP
  - JASPI

It is also possible to extend existing logic and implement a custom client for any other authentication
mechanisms. To create a new Authentication Client, implement the ``AuthenticationClient`` interface.

Download
++++++++

Download: `Authentication Client - Java <https://github.com/caskdata/cdap-clients/tree/release/1.0.0/cdap-authentication-clients/java>`__

Build
+++++

To build the Authentication Client Java API jar, use::

  mvn clean package

Usage
+++++

To use the Authentication Client Java API, include this Maven dependency in your project's ``pom.xml`` file::

 <dependency>
  <groupId>co.cask.cdap</groupId>
  <artifactId>cdap-authentication-client</artifactId>
  <version>1.0.0</version>
 </dependency>

Examples
........

This example creates a ``BasicAuthenticationClient`` to retrieve an access token::

  String authClientClassName = "co.cask.cdap.security.authentication.client.basic.BasicAuthenticationClient";
  AuthenticationClient authenticationClient = configuration.getClassByName(authClientClassName);

  // Set the CDAP instance connection info: hostname, port and flag to indicate if SSL is enabled or not
  authenticationClient.setConnectionInfo("localhost", 10000, false);

  // If you have additional properties, you can include them for configuration using the following
  authenticationClient.configure(properties);

  // Check if authentication is enabled
  boolean isEnabled = authenticationClient.isAuthEnabled();

  // Get the access token for the user from the authentication service
  // If access token is not available an IOException will be thrown
  String token = authenticationClient.getAccessToken();

This example illustrates an Authentication Client obtaining credentials from a user and then using them for configuration::

  authenticationClient.setConnectionInfo(hostname, port, ssl);
  Properties properties = new Properties();
  if (authenticationClient.isAuthEnabled()) {
    ConsoleReader reader = new ConsoleReader();
    for (Credential credential : authenticationClient.getRequiredCredentials()) {
      String credentialValue;
      output.printf("Please, specify "  credential.getDescription()  "> ");
      if (credential.isSecret()) {
          credentialValue = reader.readLine(prompt, '*');
      } else {
        credentialValue = reader.readLine(prompt);
      }
      properties.put(credential.getName(), credentialValue);
    }
    authenticationClient.configure(properties);
    cliConfig.getClientConfig().setAuthenticationClient(authenticationClient);
  }

To see the properties supported by the Authentication Client, look at :ref:`Configuring Authentication Client. <ConfiguringAuthClient>`

.. _AuthClientPython:

Authentication Client - Python
..............................

Download
++++++++

Download: `Authentication Client - Python <https://github.com/caskdata/cdap-clients/tree/release/1.0.0/cdap-authentication-clients/python>`__

Example Usage
+++++++++++++

1) Read the configuration from Json and retrieve an AccessToken::

    # Include these imports in your Python script
    from Config import Config
    from BasicAuthenticationClient import BasicAuthenticationClient

    # Create a BasicAuthenticationClient instance
    authentication_client = BasicAuthenticationClient()

    # Set the connection parameters: authentication service host, port and SSL mode
    authentication_client.set_connection_info('localhost', 10000, False)

    # Load configuration from JSON File
    config = Config().read_from_file('auth_config.json')

    # Configure the Authentication Client with the Config object
    authentication_client.configure(config)

    # Check if authentication is enabled in the CDAP instance
    is_enabled = authentication_client.is_auth_enabled()

    # Retrieve the access token from the authentication service:
    token = authentication_client.get_access_token()


   Sample config JSON file::

      {
        "security_auth_client_username": "admin",
        "security_auth_client_password": "secret",
        "security_ssl_cert_check": true
      }

2) Create a configuration object and configure it manually::

    # Include these imports in your Python script
    from Config import Config
    from BasicAuthenticationClient import BasicAuthenticationClient

    # Create a BasicAuthenticationClient instance
    authentication_client = BasicAuthenticationClient()

    # Set the connection parameters: authentication service host, port, SSL mode
    authentication_client.set_connection_info('localhost', 10000, False)

    # Load configuration from JSON File
    config = Config()
    config.security_auth_client_username = "admin"
    config.security_auth_client_password = "secret"
    config.security_ssl_cert_check = True

    # Configure the Authentication Client with the Config object
    authentication_client.configure(config)

    # Check if authentication is enabled in the CDAP instance
    is_enabled = authentication_client.is_auth_enabled()

    # Retrieve the access token from the authentication service:
    token = authentication_client.get_access_token()

To see the properties supported by the Authentication Client, look at ConfiguringAuthClient_

.. _ConfiguringAuthClient:

Authentication Client Configuration
...................................

.. list-table::
    :widths: 50 50
    :header-rows: 1

    * - Property
      - Description
    * - security.auth.client.username
      - authorized user name
    * - security.auth.client.password
      - password used for authenticating the user
    * - security.auth.client.verify.ssl.cert
      - When SSL is enabled , if you want to allow self-signed certificates set this to false - this will disable the certificate check.

.. |(TM)| unicode:: U+2122 .. trademark sign
