# Receive events from Azure Event Hubs using Java

This sample shows how to receive events from a particular Event Hub partition based on a sequence number. This sample here focuses on receive from a single partition of the many partitions in your event hub.
This is a lower-level API gesture that most applications will not use, but rather lean on the Event Processor Host to manage partition ownership and check-pointing. The [Event Processor Sample](../EventProcessorSample) shows this higher level functionality.
A receiver with the option of a sequence number provides the user with the added flexibility of providing a deterministic value for your start and end points. Internally this will map to an offset. This means, for high performance scenarios you would want to consider receiving by offset.

To run the sample, you need to edit the [sample code](src/main/java/com/microsoft/azure/eventhubs/samples/receivebydatetime/ReceiveUsingSequenceNumber.java) and provide the following information:

```java
    final String namespaceName = "----EventHubsNamespaceName-----";
    final String eventHubName = "----EventHubName-----";
    final String sasKeyName = "-----SharedAccessSignatureKeyName-----";
    final String sasKey = "---SharedAccessSignatureKey----";
```

## Prerequisites

Please refer to the [overview README](../../README.md) for prerequisites and setting up the sample environment, including creating an Event Hubs cloud namespace and an Event Hub.

## Build and run

The sample can be built independently with

```bash
mvn clean package
```

and then run with (or just from VS Code or another Java IDE)

```bash
java -jar ./target/receivebydatetime-1.0.0-jar-with-dependencies.jar
```