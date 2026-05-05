(() => {
  Bangle.loadWidgets();
  Bangle.drawWidgets();

  const storage = require("Storage");

  // ---------------- SETTINGS ----------------

  let settings = {
    sensors: ["steps", "accel", "hr", "temp"], // default
    interval: 10, // seconds
    emaEnabled: false,
    emaInterval: 1, // 1 hour default
    rawMode: false,
    ramSize: 50, //default 50 rows
    timedTestMinutes: 6 // default 6 minutes for timed test
  };

  const config = {
    filename: "collectedData.bin",
    bytesPerStepCount: 1,
    appendPos: 0
  };

  // 4 bytes timestamp + 1 step + 1 accel + 1 HR + 1 conf + 1 battery + 1 temp + 1 padding = 11 bytes per row
  const RAW_FILE_LEN = 60000;
  const AGG_FILE_LEN = 20000;

  let rows = [];

  // ---------------- STREAMING STATE ----------------
  let hrmOn = false;
  let accelOn = false;
  let stepOn = false;
  let magOn = false;
  let pressureOn = false;
  let tempOn = false;
  let gpsOn = false;
  let streamStepTimer = null;

  // ---------------- STATE ----------------
  let startTime = 0;
  let isAggregated = false; // logging mode
  let isStreaming = false;
  let logTimer = null;
  let hrTimer = null;

  let lastStepStream = -1;
  let lastStepAgg = -1;
  let currentStepCount = 0;

  let accelSum = 0;
  let accelSamples = 0;

  let tempSum = 0;
  let tempSamples = 0;

  let hr = 0;
  let hrConfidence = 0;

  // HR buffer för "bästa 20s"
  let hrmBuffer = [];

  // barometer shared state (temp + pressure)
  let baroOn = false;
  
  // EMA variables
  let emaON = false;
  let emaTimer = null;

  // 6MWT Countdown 
  let sixMWTInterval;
  let sixMWTSeconds = 360; // 6 minutes

  let lastAcc = null;
  let lastTemp = null;
  let lastMag = null;
  let lastGPS = null;

//function to send messages
  function send(line) {
    Bluetooth.println(line);
  }

  //little endian num to bytes
  function numToBytes(num, len) {
    let arr = new Uint8Array(len);
    for (let i = 0; i < len; i++) {
      arr[i] = num & 255;
      num >>= 8;
    }
    return arr;
  }

  // ---------------- FILE LOGGING ----------------
function flushRows() {
  if (rows.length === 0) return;

  let rowSize = settings.rawMode ? 20 : 9;
  let block = new Uint8Array(rows.length * rowSize);

  let pos = 0;
  for (let r of rows) {
    block.set(r, pos);
    pos += rowSize;
  }

  if (config.appendPos + block.length > config.totalLen) {
    console.log("FILE FULL");
    return;
  }

  storage.write(config.filename, block, config.appendPos, config.totalLen);
  config.appendPos += block.length;
  rows = [];
}

function appendRawRow() {
  let row = new Uint8Array(20);
  let pos = 0;

  // timestamp
  let ts = Math.round(Date.now() / 1000);
  row.set(numToBytes(ts, 4), pos); pos += 4;

  // steps
  row[pos++] = currentStepCount & 255;

  // hr
  row[pos++] = hr & 255;

  // temp (baro temp)
  row[pos++] = lastTemp ? Math.round(lastTemp) : 0;

  // accel scaled
  let ax = lastAcc ? Math.round(lastAcc.x * 50) : 0;
  let ay = lastAcc ? Math.round(lastAcc.y * 50) : 0;
  let az = lastAcc ? Math.round(lastAcc.z * 50) : 0;
  row[pos++] = ax;
  row[pos++] = ay;
  row[pos++] = az;

  // mag scaled
  let mx = lastMag ? Math.round(lastMag.x) : 0;
  let my = lastMag ? Math.round(lastMag.y) : 0;
  let mz = lastMag ? Math.round(lastMag.z) : 0;
  row[pos++] = mx;
  row[pos++] = my;
  row[pos++] = mz;

  // gps lat/lon scaled
  let lat = lastGPS ? Math.round(lastGPS.lat * 10000) : 0;
  let lon = lastGPS ? Math.round(lastGPS.lon * 10000) : 0;
  row.set(numToBytes(lat, 2), pos); pos += 2;
  row.set(numToBytes(lon, 2), pos); pos += 2;

  // battery
  let batt = Math.round(E.getBattery() * 10);
  row.set(numToBytes(batt, 2), pos); pos += 2;

  // padding
  row[pos++] = 0;

  rows.push(row);
  if (rows.length >= settings.ramSize) flushRows();
}

function appendAggRow(ts) {
  let row = new Uint8Array(9);

  row.set(numToBytes(ts, 4), 0);

  row[4] = currentStepCount & 255;

  let accelAvg = accelSamples ? (accelSum / accelSamples) : 0;
  row[5] = Math.min(255, Math.round(accelAvg * 100));

  row[6] = hr & 255;
  row[7] = hrConfidence & 255;

  let tempAvg = tempSamples ? (tempSum / tempSamples) : 0;
  row[8] = Math.round(tempAvg);

  rows.push(row);
  if (rows.length >= settings.ramSize) flushRows();
}

function appendEventRow(code) {
  let rowSize = settings.rawMode ? 20 : 9;
  let row = new Uint8Array(rowSize);

  let ts = Math.round(Date.now() / 1000);
  let timestampBytes = numToBytes(ts, 4);
  row.set(timestampBytes, 0);

  row[4] = code; // event code

  rows.push(row);
}

  // ---------------- BAROMETER POWER ----------------

  function ensureBaroOn() {
    if (!baroOn) {
      baroOn = true;
      Bangle.setBarometerPower(true);
    }
  }

  function maybeBaroOff() {
    if (!tempOn && !pressureOn && baroOn) {
      baroOn = false;
      Bangle.setBarometerPower(false);
    }
  }

  // ---------------- SENSOR START/STOP ----------------

  function startHRM() {
    if (hrmOn) return;
    hrmOn = true;
    Bangle.on("HRM", onHRM);
    Bangle.setHRMPower(1);
    send("DEBUG: HRM STARTED");
  }

  function stopHRM() {
    if (!hrmOn) return;
    hrmOn = false;
    Bangle.removeListener("HRM", onHRM);
    Bangle.setHRMPower(0);
    send("DEBUG: HRM STOPPED");
  }

  function startAccel() {
    if (accelOn) return;
    accelOn = true;
    Bangle.on("accel", onACC);
    Bangle.getAccel(1);
    send("DEBUG: ACCEL STARTED");
  }

  function stopAccel() {
    if (!accelOn) return;
    accelOn = false;
    Bangle.removeListener("accel", onACC);
    Bangle.getAccel(0);
    send("DEBUG: ACCEL STOPPED");
  }

  function startSteps() {
    if (stepOn) return;
    stepOn = true;
    lastStepStream = -1;
    lastStepAgg = -1;
    currentStepCount = 0;
    Bangle.on("step", onSTEP);
    send("DEBUG: STEPS STARTED");
  }

  function stopSteps() {
    if (!stepOn) return;
    stepOn = false;
    Bangle.removeListener("step", onSTEP);
    lastStepStream = -1;
    lastStepAgg = -1;
    currentStepCount = 0;
    send("DEBUG: STEPS STOPPED");
  }

  function startMag() {
    if (magOn) return;
    magOn = true;
    Bangle.setCompassPower(true);
    send("DEBUG: MAG STARTED");
  }

  function stopMag() {
    if (!magOn) return;
    magOn = false;
    Bangle.setCompassPower(false);
    send("DEBUG: MAG STOPPED");
  }

  function startPressure() {
    if (pressureOn) return;
    pressureOn = true;
    ensureBaroOn();
    send("DEBUG: PRESSURE STARTED");
  }

  function stopPressure() {
    if (!pressureOn) return;
    pressureOn = false;
    maybeBaroOff();
    send("DEBUG: PRESSURE STOPPED");
  }

  function startTemp() {
    if (tempOn) return;
    tempOn = true;
    ensureBaroOn();
    send("DEBUG: TEMP STARTED");
  }

  function stopTemp() {
    if (!tempOn) return;
    tempOn = false;
    maybeBaroOff();
    send("DEBUG: TEMP STOPPED");
  }

  function startGps() {
    if (gpsOn) return;
    gpsOn = true;
    Bangle.setGPSPower(true);
    send("DEBUG: GPS STARTED");
  }

  function stopGps() {
    if (!gpsOn) return;
    gpsOn = false;
    Bangle.setGPSPower(false);
    send("DEBUG: GPS STOPPED");
  }

  // ---------------- SENSOR HANDLERS ----------------

  function onHRM(d) {
    // STREAMING
    if (isStreaming) {
      const ms = Date.now() - startTime;
      send(`DATA,HR,${ms},${d.bpm},${d.confidence || 0}`);
    }
    // AGGREGATED: buffra för "bästa 20s"
    if (isAggregated && hrmBuffer && d.confidence > 0 && d.bpm > 0) {
      hrmBuffer.push(d);
    }
    if (settings.rawMode) {
    hr = d.bpm;
    hrConfidence = d.confidence;
}
  }

  // HR-mätning: bästa HR under senaste 20 sek
  function measureHR() {
    if (hrmBuffer.length === 0) {
      hr = 0;
      hrConfidence = 0;
    } else {
      let best = hrmBuffer.reduce((a, b) =>
        (b.confidence || 0) > (a.confidence || 0) ? b : a
      );
      hr = best.bpm;
      hrConfidence = best.confidence || 0;
    }
    hrmBuffer = [];
  }

  function onSTEP(s) {
  // --- LOGGING (aggregated) ---
  if (isAggregated && stepOn) {
    if (lastStepAgg < 0) {
      lastStepAgg = s;
      return;
    }
    const diff = s - lastStepAgg;
    if (diff >= 0) currentStepCount += diff;
    lastStepAgg = s;
  }

  // --- STREAMING (räkna steg även om vi inte skickar direkt) ---
  if (isStreaming && stepOn) {
    if (lastStepStream < 0) {
      lastStepStream = s;
      return;
    }
    const diff = s - lastStepStream;
    if (diff >= 0) currentStepCount += diff;
    lastStepStream = s;
  }
  if (isStreaming && stepOn) {
    const ms = Date.now() - startTime;
    send(`DATA,STEPS,${ms},${currentStepCount}`);
  }

  }

  function onACC(a) {
    lastAcc = a;
    // LOGGING (aggregated rörelse)
    if (isAggregated && accelOn) {
      accelSum += Math.abs(a.mag - 1);
      accelSamples++;
    }

    // STREAMING
    if (isStreaming && accelOn) {
      const ms = Date.now() - startTime;
      send(`DATA,ACC,${ms},${a.x.toFixed(3)},${a.y.toFixed(3)},${a.z.toFixed(3)}`);
    }
  }

  //function for 6 minutes walking test
 function startSixMWT() {
  settings.rawMode = true; // force raw mode for 6MWT, since we only want raw data from this test 
  storage.writeJSON("awapp.settings.json", settings);

  sixMWTSeconds = 360;

 //Start logging raw data and it uses all raw sensors for logging 
  startCollection();

  // Event start
  let ts = Math.round(Date.now() / 1000);
  send(`EVENT,6MWT_START,${ts}`);
  appendEventRow(1);

  Bangle.buzz();

  sixMWTInterval = setInterval(() => {
    sixMWTSeconds--;

    if (sixMWTSeconds <= 0) {
      clearInterval(sixMWTInterval);
      sixMWTInterval = undefined;

      // Event end
      let ts2 = Math.round(Date.now() / 1000);
      send(`EVENT,6MWT_END,${ts2}`);
      appendEventRow(2);

      Bangle.buzz();

      // Stop logging
      stopCollection();
    }
  }, 1000);
  }

  //function for timed test (default 6 minutes, configurable in menu), for raw data 
function startTimedTest() {
  settings.rawMode = true;
  storage.writeJSON("awapp.settings.json", settings);

  let durationSec = settings.timedTestMinutes * 60;

  startCollection(); // startar raw logging

  let ts = Math.round(Date.now() / 1000);
  send(`EVENT,TIMED_START,${ts}`);
  appendEventRow(10);

  Bangle.buzz();

  let remaining = durationSec;
  let timer = setInterval(() => {
    remaining--;
    if (remaining <= 0) {
      clearInterval(timer);

      let ts2 = Math.round(Date.now() / 1000);
      send(`EVENT,TIMED_END,${ts2}`);
      appendEventRow(11);

      Bangle.buzz();
      stopCollection();
    }
  }, 1000);
}

// function for aggregated timed test
function startAggTimedTest() {
  settings.rawMode = false; // force aggregated mode
  storage.writeJSON("awapp.settings.json", settings);

  let durationSec = settings.timedTestMinutes * 60;

  startCollection(); // start aggregated logging

  let ts = Math.round(Date.now() / 1000);
  send(`EVENT,AGG_TIMED_START,${ts}`);
  appendEventRow(20); // new event code for aggregated timed test start

  Bangle.buzz();

  let remaining = durationSec;
  let timer = setInterval(() => {
    remaining--;
    if (remaining <= 0) {
      clearInterval(timer);

      let ts2 = Math.round(Date.now() / 1000);
      send(`EVENT,AGG_TIMED_END,${ts2}`);
      appendEventRow(21); // new event code for aggregated timed test end

      Bangle.buzz();
      stopCollection();
    }
  }, 1000);
}

  // ------------- Streaming extra sensorer -------------

  Bangle.on("mag", m => {
    if (isStreaming && magOn) {
      const ms = Date.now() - startTime;
      send(`DATA,MAG,${ms},${m.x.toFixed(3)},${m.y.toFixed(3)},${m.z.toFixed(3)}`);
    }
    lastMag = m;
  });

  Bangle.on("pressure", b => {
    lastTemp = b.temperature;
    // TEMP logging (aggregated)
    if (isAggregated && tempOn) {
      tempSum += b.temperature;
      tempSamples++;
    }

    // TEMP streaming
    if (isStreaming && tempOn) {
      const ms = Date.now() - startTime;
      send(`DATA,TEMP,${ms},${b.temperature.toFixed(2)}`);
    }

    // PRESSURE streaming
    if (isStreaming && pressureOn) {
      const ms = Date.now() - startTime;
      send(`DATA,pressure,${ms},${b.pressure.toFixed(2)},${b.altitude.toFixed(2)},${b.temperature.toFixed(2)}`);
    }
  });

  Bangle.on("GPS", g => {
    if (isStreaming && gpsOn) {
      const ms = Date.now() - startTime;
      send(`DATA,GPS,${ms},${g.lat.toFixed(6)},${g.lon.toFixed(6)},${g.alt}`);
    }
    lastGPS = g;
  });

  // ---------------- DATA COLLECTION (AGGREGATED) ----------------

  function startCollection() {
    isAggregated = !settings.rawMode;
    let isRaw = settings.rawMode;

    // välj fil baserat på mode
    if (isRaw) {
      config.filename = "collectedRawData.bin";
      config.totalLen = RAW_FILE_LEN;
    } else {
      config.filename = "collectedAggData.bin";
      config.totalLen = AGG_FILE_LEN;
    }

    Bangle.buzz(300);
    E.showAlert("Started collection").then(() => { //this function can be removed if the alert is not wanted.
      showMainMenu();
    });

    storage.erase(config.filename);
    storage.write(config.filename, new Uint8Array(config.totalLen), 0, config.totalLen);
    config.appendPos = 0;
    rows = [];

    // Start sensors
    // Start sensors for aggregated mode only
    if (!isRaw) {
      if (settings.sensors.includes("steps")) startSteps();
      if (settings.sensors.includes("accel")) startAccel();
      if (settings.sensors.includes("temp")) startTemp();
      if (settings.sensors.includes("hr")) {
        startHRM();
        hrmBuffer = [];
        hrTimer = setInterval(measureHR, 20000);
      }
    }

    if (settings.emaEnabled && !isRaw) startEMA();

      // ---------------- RAW LOGGING ----------------
      if (isRaw) {

      // Start ALL sensors needed for raw logging
      startSteps();
      startAccel();
      startHRM();
      startTemp();      // baro temp
      startPressure();  // barometer
      startMag();       // magnetometer
      startGps();       // GPS

      logTimer = setInterval(() => {
        appendRawRow();
        currentStepCount = 0;
      }, settings.interval * 1000);

      return;
    }

    logTimer = setInterval(() => {
      let ts = Math.round(Date.now() / 1000);
      let batt = E.getBattery();

      let accelAvg = accelSamples ? (accelSum / accelSamples) : 0;
      let accelByte = Math.min(255, Math.round(accelAvg * 100));

      let tempAvg = tempSamples ? (tempSum / tempSamples) : 0;
      let tempByte = Math.round(tempAvg); // °C som heltal

      appendAggRow(
        ts,
        settings.sensors.includes("steps") ? currentStepCount : 0,
        0, 0, 0, // inga råvärden i aggregated mode
        settings.sensors.includes("hr") ? hr : 0,
        settings.sensors.includes("hr") ? hrConfidence : 0,
        batt,
        settings.sensors.includes("temp") ? tempByte : 0
      );


      currentStepCount = 0;
      accelSum = 0;
      accelSamples = 0;
      tempSum = 0;
      tempSamples = 0;

    }, settings.interval * 1000);
  }

  function stopCollection() {
  if (rows.length > 0) flushRows();

  if (logTimer) clearInterval(logTimer);
  if (hrTimer) clearInterval(hrTimer);

  logTimer = null;
  hrTimer = null;

  stopSteps();
  stopAccel();
  stopHRM();
  stopTemp();
  stopPressure();
  stopMag();
  stopGps();

  stopEMA();

  isAggregated = false;

  Bangle.buzz(200);
  E.showAlert("Stopped collection").then(() => { //this function can be removed if the alert is not wanted.
      showMainMenu();
    });
}

  // ---------------- BLUETOOTH COMMANDS (STREAMING) ----------------

  Bluetooth.on("data", function(d) {
    d.split("\n").forEach(cmd => {
      cmd = cmd.trim();
      if (!cmd) return;

      send("DEBUG: GOT CMD " + cmd);

      if (cmd === "HR_ON") startHRM();
      if (cmd === "HR_OFF") stopHRM();

      if (cmd === "ACC_ON") startAccel();
      if (cmd === "ACC_OFF") stopAccel();

      if (cmd === "STEPS_ON") startSteps();
      if (cmd === "STEPS_OFF") stopSteps();

      if (cmd === "MAG_ON") startMag();
      if (cmd === "MAG_OFF") stopMag();

      if (cmd === "pressure_ON") startPressure();
      if (cmd === "pressure_OFF") stopPressure();

      if (cmd === "TEMP_ON") startTemp();
      if (cmd === "TEMP_OFF") stopTemp();

      if (cmd === "GPS_ON") startGps();
      if (cmd === "GPS_OFF") stopGps();

      if (cmd === "START") {
        isStreaming = true;
        isAggregated = false;
        startTime = Date.now();

        if (hrmOn) startHRM();
        if (accelOn) startAccel();
        if (stepOn) startSteps();
        if (magOn) startMag();
        if (pressureOn) startPressure();
        if (tempOn) startTemp();
        if (gpsOn) startGps();
        send("DEBUG: TEST STARTED");
      }

      if (cmd === "STOP") {
        isStreaming = false;
        if (streamStepTimer) {
          clearInterval(streamStepTimer);
          streamStepTimer = null;
        }
        stopHRM();
        stopAccel();
        stopSteps();
        stopMag();
        stopPressure();
        stopTemp();
        stopGps();
        send("STOPPED");
      }
    });
  });

  function sendEMA() {
  if (!emaON) return;

  // Delay ensures buzz works even when screen is off
  setTimeout(() => Bangle.buzz(300), 100);

  E.showPrompt("Are you stressed right now?", {
    title: "EMA",
    buttons: { "Yes": true, "No": false }
  }).then(answer => {

    // Log or send the EMA response
    let ts = Math.round(Date.now() / 1000);
    if (answer === true) {
      send(`EMA_RESPONSE,YES,${ts}`);
      appendEventRow(1); // optional event code for YES
      flushRows(); // ensure EMA response is saved immediately
    } else if (answer === false) {
      send(`EMA_RESPONSE,NO,${ts}`);
      appendEventRow(0); // optional event code for NO
      flushRows(); // ensure EMA response is saved immediately
    } 
    // Return to EMA menu
    showEMAMenu();
  });
}

function startEMA() {
  if (!isAggregated) return;
  if (emaON) return;

  emaON = true;

  // Initial buzz when EMA starts
  setTimeout(() => Bangle.buzz(300), 100);

  emaTimer = setInterval(() => {
    sendEMA();
  }, settings.emaInterval * 3600 * 1000);
}

function stopEMA() {
  if (!emaON) return;
  emaON = false;

  if (emaTimer) {
    clearInterval(emaTimer);
    emaTimer = null;
  }

  setTimeout(() => Bangle.buzz(200), 50);
}


  // ---------------- MENU ----------------

  function showMainMenu() {
    E.showMenu({
      "": { title: "AW app" },
      
      //for web app and mobile app streming 
      "Streaming": () => {
        Bluetooth.setConsole(false);   // gör BLE till datakanal
        Terminal.setConsole(true);     // flytta REPL till skärmen
        E.showAlert("Start streaming").then(() => {
            showMainMenu();
       });
      },
      "Stop streaming": () => {
        isStreaming = false;
        Bluetooth.setConsole(true);   // återställ BLE till REPL
        Terminal.setConsole(false);   // ta bort REPL från skärmen
        send("STOPPED");
        E.showAlert("Streaming stopped").then(() => showMainMenu());
      },
      "Raw logging": () => showRawMenu(),
      "Agg logging": () => showAggMenu(),
      "EMA settings": () => showEMAMenu()
    });
  }

  function intervalMenu() {
    const menu = {
      "" : { title : "Sampling freq (sec)" },
      "Value" : {
      value : settings.interval,
      min : 0,
      max : 3600, // max 60 min
      step : 30,
      format : v => v + " s",
      onchange : v => {
        settings.interval = v;
        storage.writeJSON("awapp.settings.json", settings);
      }
    },

      "< Back" : () => { showAggMenu(); }
    };
    E.showMenu(menu);
  }

  function showSensorList() {
    let menu = {
      "": { title: "Active sensors" },
      "< Back": () => showMainMenu()
    };

    let allSensors = ["steps", "accel", "hr", "temp"];

    allSensors.forEach(s => {
      menu[s] = {
        value: settings.sensors.includes(s),
        onchange: v => {
          if (v) {
            if (!settings.sensors.includes(s)) settings.sensors.push(s);
          } else {
            settings.sensors = settings.sensors.filter(x => x !== s);
          }
          storage.writeJSON("awapp.settings.json", settings); 
        }
      };
    });

    E.showMenu(menu);
  }

  function showRawMenu() {
    E.showMenu({
      "": { title: "Raw logging" },
      "Ramsize": {
        value: settings.ramSize,
        min: 0,
        max: 6144,
        step: 25, //this will allow the user to increse the RAM size in steps of 25 when configuring RAM size. 
        format: v => v + " rows",
        onchange: v => {
          settings.ramSize = v;
          storage.writeJSON("awapp.settings.json", settings);
        }
      },
      "Timed test": () => timedTest(),
      "Start": () => {
        settings.rawMode = true;
        storage.writeJSON("awapp.settings.json", settings);
        startCollection();
      },
      "Stop": () => stopCollection(),
      "< Back": () => showMainMenu()
    });
  }

  function showAggMenu() {
    E.showMenu({
      "": { title: "Agg logging" },
      "Ramsize": {
        value: settings.ramSize,
        min: 0,
        max: 6144,
        step: 25, //this will allow the user to increse the RAM size in steps of 25 when configuring RAM size. 
        format: v => v + " rows",
        onchange: v => {
          settings.ramSize = v;
          storage.writeJSON("awapp.settings.json", settings);
        }
      },
      "Choose sensors": () => showSensorList(),
      "Sampling freq": () => intervalMenu(),
      "Timed test": () => timedTestAgg(),
      "Start": () => {
        settings.rawMode = false;
        storage.writeJSON("awapp.settings.json", settings);
        startCollection();
      },
      "Stop": () => stopCollection(),
      "< Back": () => showMainMenu()
    });
  }
        
  function timedTest(){
    E.showMenu({
      "": { title: "Timed Test" },
    "Test duration": {
      value: settings.timedTestMinutes,
      min: 1,
      max: 60,
      step: 1,
      format: v => v + " min",
      onchange: v => {
        settings.timedTestMinutes = v;
        storage.writeJSON("awapp.settings.json", settings);
      }
    },
      "Start timed test": () => startTimedTest(),
      "Start 6MWT" : () => startSixMWT(),
      "< Back": () => showRawMenu()
    });
  }

  function timedTestAgg(){
    E.showMenu({
      "": { title: "Timed Test (Agg)" },
    "Test duration": {
      value: settings.timedTestMinutes,
      min: 1,
      max: 60,
      step: 1,
      format: v => v + " min",
      onchange: v => {
        settings.timedTestMinutes = v;
        storage.writeJSON("awapp.settings.json", settings);
      }
    },
      "Start timed test": () => startAggTimedTest(),
      "< Back": () => showAggMenu()
    });
  }

  function showEMAMenu() {
    E.showMenu({
      "": { title: "EMA Settings" },

      "Enabled": {
        value: settings.emaEnabled,
        onchange: v => {
          settings.emaEnabled = v;
          storage.writeJSON("awapp.settings.json", settings);

          if (!v) stopEMA(); // turn off if disabled
        }
      },

      "Interval (hours)": {
        value: settings.emaInterval,
        min: 1,
        max: 24, // max 24h
        step: 1,
        format: v => v + " h",
        onchange: v => {
          settings.emaInterval = v;

          // Restart timer
          if (emaON && isAggregated) {
            stopEMA();
            startEMA();
          }

          storage.writeJSON("awapp.settings.json", settings);
        }
      },

      "< Back": () => showMainMenu()
    });
  }

  showMainMenu();

  // Load settings from storage
  try {
    let savedSettings = storage.readJSON("awapp.settings.json");
    if (savedSettings) {
      settings = Object.assign(settings, savedSettings);
      if (settings.emaInterval > 24) {
        settings.emaInterval = Math.max(1, Math.round(settings.emaInterval / 3600));
      }
      if (settings.emaEnabled) {
        startEMA();
      }
    }
  } catch (e) {
    send("DEBUG: Could not load settings");
  }

})();