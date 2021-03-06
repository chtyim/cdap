.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2015 Cask Data, Inc.

.. _integrations:
 
============
Integrations
============


.. rubric:: Hub

.. |hub-overview| replace:: **Overview:**
.. _hub-overview: hub.html#overview

- |hub-overview|_ Summary of the **Hub,** a source for re-usable
  applications, data, and code for CDAP users

.. |hub-api| replace:: **API:**
.. _hub-api: hub.html#api

- |hub-api|_ Hub APIs used to create entities in a Hub

.. |hub-custom| replace:: **Custom Hosting:**
.. _hub-custom: hub.html#custom

- |hub-custom|_ Hosting your own custom Hub


.. rubric:: Cloudera

.. |cloudera-introduction| replace:: **Overview:**
.. _cloudera-introduction: partners/cloudera/index.html

- |cloudera-introduction|_ Utilizing CDAP on **Cloudera Enterprise Data Hub**


.. |cloudera-install| replace:: **Configuring and Installing:**
.. _cloudera-install: ../admin-manual/installation/cloudera.html

- |cloudera-install|_ Configuring and installing CDAP using **Cloudera Manager** *(Administration Manual)*


.. |cloudera-ingesting| replace:: **Ingestion and Exploration:**
.. _cloudera-ingesting: partners/cloudera/ingesting.html

- |cloudera-ingesting|_ Ingesting and exploring data using **Impala**


.. |cloudera-faq| replace:: **FAQ:**
.. _cloudera-faq: ../faqs/cloudera-manager..html

.. - |cloudera-faq|_ for Cloudera and Impala


.. rubric:: Ambari *(Administration Manual)*

.. |ambari| replace:: **Configuring and Installing:**
.. _ambari: ../admin-manual/installation/ambari.html

- |ambari|_ Configuring and installing CDAP using **Ambari** *(Administration Manual)*


.. rubric:: MapR *(Administration Manual)*

.. |mapr| replace:: **Configuring and Installing:**
.. _mapr: ../admin-manual/installation/mapr.html

- |mapr|_ Configuring and installing CDAP on **MapR** *(Administration Manual)*


.. rubric:: Apache Sentry

.. |apache-sentry| replace:: **Integrations:**
.. _apache-sentry: apache-sentry.html

- |apache-sentry|_ Configuring and integrating CDAP with **Apache Sentry**

.. rubric:: Apache Ranger

.. |apache-ranger| replace:: **Integrations:**
.. _apache-ranger: apache-ranger.html

- |apache-sentry|_ Configuring and integrating CDAP with **Apache Ranger**

.. rubric:: Apache Hadoop Key Management Server (KMS)

.. |hadoop-kms| replace:: **Integrations:**
.. _hadoop-kms: hadoop-kms.html

- |hadoop-kms|_ Configuring and integrating CDAP with **Apache Hadoop Key Management Service (KMS)**


.. rubric:: Accessing CDAP Datasets through JDBC and ODBC

Many Business Intelligence tools can integrate with relational databases using JDBC or ODBC
drivers. They often include drivers to connect to standard databases such as MySQL or
PostgreSQL. Most tools allow the addition of non-standard JDBC drivers.

Two business intelligence tools |---| :ref:`SquirrelSQL <squirrel-integration>` and 
:ref:`Pentaho Data Integration <pentaho-integration>` |---| are covered, explaining connecting
them to a running CDAP instance and interacting with CDAP datasets.

The example :ref:`cdap-bi-guide` includes the use of Pentaho.

.. |jdbc| replace:: **CDAP JDBC Driver:**
.. _jdbc: jdbc.html

- |jdbc|_ A JDBC driver provided with CDAP to make **integrations with external programs**
  and third-party BI (business intelligence) tools easier.


.. |odbc| replace:: **CDAP ODBC Driver:**
.. _odbc: odbc.html

- |odbc|_ An ODBC driver provided for CDAP to allow **integration with external Windows programs**.


.. |pentaho| replace:: **Pentaho Data Integration:**
.. _pentaho: pentaho.html

- |pentaho|_ An advanced, open source **business intelligence tool** that can execute
  transformations of data.


.. |squirrel| replace:: **SquirrelSQL:**
.. _squirrel: squirrel.html

- |squirrel|_ A simple JDBC client which **executes SQL queries** against many different relational databases.


