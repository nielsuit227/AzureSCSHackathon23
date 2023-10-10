/* eslint-disable max-classes-per-file */
/* eslint-disable no-restricted-globals */
/* eslint-disable no-undef */
function debounce(func, timeout = 300) {
  let timer;
  return (...args) => {
    clearTimeout(timer);
    timer = setTimeout(() => {
      func.apply(this, args);
    }, timeout);
  };
}

$(document).ready(() => {
  // if deployed to a site supporting SSL, use wss://
  const protocol = document.location.protocol.startsWith("https")
    ? "wss://"
    : "ws://";
  const webSocket = new WebSocket(protocol + location.host);
  const latencies = [];

  // When a web socket message arrives:
  webSocket.onopen = () => {
    console.log("WebSocket connected!");
    retryCount = 0; // reset the retry count on a successful connection
  };

  webSocket.onerror = (err) => {
    console.error("WebSocket Error:", err);
  };

  webSocket.onclose = (event) => {
    if (event.wasClean) {
      console.log(
        `Connection closed cleanly, code=${event.code}, reason=${event.reason}`
      );
    } else {
      console.error("Connection died");
      retryCount++;
      setTimeout(initializeWebSocket, 5000 * retryCount); // increasing delay
    }
  };
  const updateSessionLatency = debounce(() => {
    data[0].value = latencies.reduce((a, b) => a + b, 0) / latencies.length;
    Plotly.react("sessionLatency", data, layout);
  });
  webSocket.onmessage = function onMessage(message) {
    try {
      // Update live latency
      const latency = new Date() - Number(message.data) * 1000;
      data[0].value = latency;
      Plotly.react("liveLatency", data, layout);

      // Update session latency
      latencies.push(latency);
      updateSessionLatency();
    } catch (err) {
      console.error(err);
    }
  };

  // Gauge
  var data = [
    {
      domain: { x: [0, 1], y: [0, 1] },
      value: 50,
      title: { text: "Latency [ms]" },
      type: "indicator",
      mode: "gauge+number",
      gauge: {
        axis: { range: [0, 500] },
      },
    },
  ];
  const layout = { width: 600, height: 500, margin: { t: 0, b: 0 } };
  Plotly.newPlot("liveLatency", data, layout);
  Plotly.newPlot("sessionLatency", data, layout);
});
