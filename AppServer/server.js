const express = require("express");
const http = require("http");
const WebSocket = require("ws");
const path = require("path");
const avsc = require("avsc");
const EventHubReader = require("./scripts/event-hub-reader.js");

const iotHubConnectionString =
  "HostName=cchack23-test-1.azure-devices.net;SharedAccessKeyName=iothubowner;SharedAccessKey=+KaXmSYiH/V/Jh+SQUf48vqQHFjg9eXhnAIoTGFEIQA=";
const eventHubConsumerGroup = "app_service";

// Redirect requests to the public subdirectory to the root
const app = express();
app.use(express.static(path.join(__dirname, "public")));
app.use((req, res /* , next */) => {
  res.redirect("/");
});

const server = http.createServer(app);
const wss = new WebSocket.Server({ server });

wss.on("connection", (ws) => {
  console.log("Client connected");
  ws.on("message", (message) => {
    console.log(`Received message: ${message}`);
  });
  ws.on("close", () => {
    console.log("Client disconnected");
  });
});
wss.broadcast = (data) => {
  wss.clients.forEach((client) => {
    if (client.readyState === WebSocket.OPEN) {
      try {
        console.log(`Broadcasting data ${data}`);
        client.send(data);
      } catch (e) {
        console.error(e);
      }
    }
  });
};

server.listen(process.env.PORT || "3000", () => {
  console.log("Listening on %d.", server.address().port);
});

const eventHubReader = new EventHubReader(
  iotHubConnectionString,
  eventHubConsumerGroup
);

// Avro schema
const avroSchema = {
  type: "record",
  name: "Message",
  fields: [{ name: "ts", type: "double" }],
};
const avroType = avsc.Type.forSchema(avroSchema);

(async () => {
  await eventHubReader.startReadMessage((message, date, deviceId) => {
    try {
      const payload = avroType.fromBuffer(message);

      wss.broadcast(payload.ts);
    } catch (err) {
      console.error("Error broadcasting: [%s] from [%s].", err, message);
    }
  });
})().catch();
