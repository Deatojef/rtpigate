(function () {
    "use strict";

    var MAX_PACKETS = 20;
    var MAX_HEARD = 30;
    var packetBody = document.getElementById("packet-body");

    // Station location (fetched from /api/location)
    var stationLat = null;
    var stationLon = null;

    // Status indicator elements
    var sseStatus = document.getElementById("sse-status");
    var rtpStatus = document.getElementById("rtp-status");
    var aprsisStatus = document.getElementById("aprsis-status");

    // Statistic value elements
    var statRfDirect = document.getElementById("stat-rf-direct");
    var statRfDigipeated = document.getElementById("stat-rf-digipeated");
    var statRfErrors = document.getElementById("stat-rf-errors");
    var statAprsisIgated = document.getElementById("stat-aprsis-igated");
    var statAprsisDropped = document.getElementById("stat-aprsis-dropped");
    var statAprsisReconnects = document.getElementById("stat-aprsis-reconnects");

    // Lifetime stat elements
    var ltRfDirect = document.getElementById("lt-rf-direct");
    var ltRfDigipeated = document.getElementById("lt-rf-digipeated");
    var ltRfErrors = document.getElementById("lt-rf-errors");
    var ltRfTotal = document.getElementById("lt-rf-total");
    var ltAprsisIgated = document.getElementById("lt-aprsis-igated");
    var ltAprsisDropped = document.getElementById("lt-aprsis-dropped");
    var ltAprsisReconnects = document.getElementById("lt-aprsis-reconnects");

    // Uptime
    var uptimeEl = document.getElementById("uptime");
    var startedAt = null;
    var uptimeTimer = null;

    // Station data from backend
    var stationData = null; // { stations: [...], frequencies: [...] }
    var activeTab = "top-talkers";
    var heardBody = document.getElementById("heard-body");
    var heardThead = document.getElementById("heard-thead");
    var freqChart = document.getElementById("freq-chart");
    var heardTable = document.getElementById("heard-table");

    // Satellite packet log (newest-first). Seeded from /api/satellite-packets on
    // load, then appended to live as rfpacket SSE events arrive with
    // is_satellite=true. Pruned to 24h on every render.
    var satPackets = [];
    var SAT_LOG_MS = 24 * 60 * 60 * 1000;


    // ---- Uptime display ----

    function updateUptime() {
        if (!startedAt) return;
        var diff = Math.floor((Date.now() - startedAt.getTime()) / 1000);
        var days = Math.floor(diff / 86400);
        var hours = Math.floor((diff % 86400) / 3600);
        var mins = Math.floor((diff % 3600) / 60);
        var secs = diff % 60;
        var parts = [];
        if (days > 0) parts.push(days + "d");
        parts.push(String(hours).padStart(2, "0") + "h");
        parts.push(String(mins).padStart(2, "0") + "m");
        parts.push(String(secs).padStart(2, "0") + "s");
        uptimeEl.textContent = "up " + parts.join(" ");
    }

    // ---- Station tabs ----

    function setupTabs() {
        var tabs = document.querySelectorAll(".tab-btn");
        for (var i = 0; i < tabs.length; i++) {
            (function (tab) {
                tab.addEventListener("click", function () {
                    for (var j = 0; j < tabs.length; j++) {
                        tabs[j].classList.remove("active");
                    }
                    tab.classList.add("active");
                    activeTab = tab.getAttribute("data-tab");
                    renderStations();
                });
            })(tabs[i]);
        }
    }

    function getSymbolImageFromEntry(entry) {
        if (!entry.symbol_table || !entry.symbol_code) return null;
        var tableChar = entry.symbol_table;
        var symbolChar = entry.symbol_code;
        var overlay = null;

        if (tableChar !== "/" && tableChar !== "\\") {
            overlay = tableChar;
            tableChar = "\\";
        }

        var key = tableChar + symbolChar;
        if (typeof symbols !== "undefined" && symbols[key]) {
            var identity = symbols[key].identity;
            var filename = overlay ? (overlay + "-" + identity + ".png") : (identity + ".png");
            return "/assets/aprssymbols/" + filename;
        }
        return null;
    }

    function renderStations() {
        if (activeTab === "frequencies") {
            if (!stationData) return;
            heardTable.style.display = "none";
            freqChart.style.display = "block";
            renderFrequencies();
            return;
        }

        if (activeTab === "satellites") {
            heardTable.style.display = "";
            freqChart.style.display = "none";
            renderSatellitePackets();
            return;
        }

        if (!stationData) return;
        heardTable.style.display = "";
        freqChart.style.display = "none";

        var stations = stationData.stations.slice();
        var showDistance = false;
        var showAltitude = false;
        var headers;

        if (activeTab === "top-talkers") {
            stations.sort(function (a, b) { return b.count - a.count; });
            headers = ["", "Callsign", "Last Heard", "Freq", "Position", "Direct Count", "Indirect Count"];
        } else if (activeTab === "most-distant") {
            stations = stations.filter(function (s) { return s.latitude != null && s.longitude != null; });
            if (stationLat !== null && stationLon !== null) {
                stations.forEach(function (s) { s._dist = haversineDistance(stationLat, stationLon, s.latitude, s.longitude); });
                stations.sort(function (a, b) { return b._dist - a._dist; });
            }
            showDistance = true;
            headers = ["", "Callsign", "Last Heard", "Freq", "Position", "Path", "Hops", "Distance"];
        } else if (activeTab === "nearest") {
            stations = stations.filter(function (s) { return s.latitude != null && s.longitude != null; });
            if (stationLat !== null && stationLon !== null) {
                stations.forEach(function (s) { s._dist = haversineDistance(stationLat, stationLon, s.latitude, s.longitude); });
                stations.sort(function (a, b) { return a._dist - b._dist; });
            }
            showDistance = true;
            headers = ["", "Callsign", "Last Heard", "Freq", "Position", "Path", "Hops", "Distance"];
        } else if (activeTab === "highest-alt") {
            stations = stations.filter(function (s) { return s.altitude_ft != null; });
            stations.sort(function (a, b) { return b.altitude_ft - a.altitude_ft; });
            showAltitude = true;
            headers = ["", "Callsign", "Last Heard", "Freq", "Altitude (ft)", "Position", "Path", "Hops"];
        }

        // Update table headers
        heardThead.innerHTML = "";
        var headerRow = document.createElement("tr");
        for (var h = 0; h < headers.length; h++) {
            var th = document.createElement("th");
            th.textContent = headers[h];
            headerRow.appendChild(th);
        }
        heardThead.appendChild(headerRow);

        // Render rows (limit to 30)
        heardBody.innerHTML = "";
        var displayCount = Math.min(stations.length, MAX_HEARD);

        if (displayCount === 0) {
            var tr = document.createElement("tr");
            var td = document.createElement("td");
            td.colSpan = headers.length;
            td.className = "empty-state";
            td.textContent = "No stations with qualifying data";
            tr.appendChild(td);
            heardBody.appendChild(tr);
            return;
        }

        for (var i = 0; i < displayCount; i++) {
            var s = stations[i];
            var tr = document.createElement("tr");

            // Symbol
            var tdSymbol = document.createElement("td");
            var imgSrc = getSymbolImageFromEntry(s);
            if (imgSrc) {
                var img = document.createElement("img");
                img.src = imgSrc;
                img.alt = "";
                img.onerror = function () { this.parentNode.removeChild(this); };
                tdSymbol.appendChild(img);
            }
            tr.appendChild(tdSymbol);

            // Callsign
            var tdCall = document.createElement("td");
            var callLink = document.createElement("a");
            callLink.href = aprsfiUrl(s.callsign);
            callLink.target = "_blank";
            callLink.rel = "noopener";
            callLink.textContent = s.callsign;
            tdCall.appendChild(callLink);
            if (s.transmitted_by) {
                var bySpan = document.createElement("span");
                bySpan.className = "transmitted-by";
                bySpan.textContent = " via " + s.transmitted_by;
                tdCall.appendChild(bySpan);
            }
            tr.appendChild(tdCall);

            // Last Heard
            var tdTime = document.createElement("td");
            tdTime.textContent = formatTime(s.last_heard);
            tr.appendChild(tdTime);

            // Frequency
            var tdFreq = document.createElement("td");
            tdFreq.textContent = s.frequency.toFixed(3);
            tr.appendChild(tdFreq);

            if (showAltitude) {
                // Altitude
                var tdAlt = document.createElement("td");
                tdAlt.textContent = s.altitude_ft != null ? Math.round(s.altitude_ft).toLocaleString() + " ft" : "--";
                tr.appendChild(tdAlt);

                // Position
                var tdPos = document.createElement("td");
                tdPos.className = "heard-coords";
                if (s.latitude != null && s.longitude != null) {
                    var link = document.createElement("a");
                    link.href = mapUrl(s.latitude, s.longitude, s.callsign);
                    link.target = "_blank";
                    link.rel = "noopener";
                    link.className = "coord-link";
                    link.textContent = s.latitude.toFixed(6) + ", " + s.longitude.toFixed(6);
                    tdPos.appendChild(link);
                } else {
                    tdPos.textContent = "--";
                }
                tr.appendChild(tdPos);

                // Path (from max altitude packet)
                var tdAltPath = document.createElement("td");
                tdAltPath.className = "heard-path";
                fillPathCell(tdAltPath, s.altitude_path);
                tr.appendChild(tdAltPath);

                // Hops (from max altitude packet)
                var tdAltHops = document.createElement("td");
                tdAltHops.className = "heard-hops";
                tdAltHops.textContent = s.altitude_hops != null ? s.altitude_hops : "0";
                tr.appendChild(tdAltHops);
            } else if (showDistance) {
                // Position
                var tdCoords = document.createElement("td");
                tdCoords.className = "heard-coords";
                if (s.latitude != null && s.longitude != null) {
                    var cLink = document.createElement("a");
                    cLink.href = mapUrl(s.latitude, s.longitude, s.callsign);
                    cLink.target = "_blank";
                    cLink.rel = "noopener";
                    cLink.className = "coord-link";
                    cLink.textContent = s.latitude.toFixed(6) + ", " + s.longitude.toFixed(6);
                    tdCoords.appendChild(cLink);
                } else {
                    tdCoords.textContent = "--";
                }
                tr.appendChild(tdCoords);

                // Path (from position-setting packet)
                var tdPosPath = document.createElement("td");
                tdPosPath.className = "heard-path";
                fillPathCell(tdPosPath, s.position_path);
                tr.appendChild(tdPosPath);

                // Hops (from position-setting packet)
                var tdPosHops = document.createElement("td");
                tdPosHops.className = "heard-hops";
                tdPosHops.textContent = s.position_hops != null ? s.position_hops : "0";
                tr.appendChild(tdPosHops);

                // Distance
                var tdDist = document.createElement("td");
                tdDist.textContent = s._dist != null ? Math.round(s._dist) + " mi" : "--";
                tr.appendChild(tdDist);
            } else {
                // Position with distance inline
                var tdCoords2 = document.createElement("td");
                tdCoords2.className = "heard-coords";
                if (s.latitude != null && s.longitude != null) {
                    var cLink2 = document.createElement("a");
                    cLink2.href = mapUrl(s.latitude, s.longitude, s.callsign);
                    cLink2.target = "_blank";
                    cLink2.rel = "noopener";
                    cLink2.className = "coord-link";
                    cLink2.textContent = s.latitude.toFixed(6) + ", " + s.longitude.toFixed(6);
                    tdCoords2.appendChild(cLink2);
                    if (stationLat !== null && stationLon !== null) {
                        var dist = haversineDistance(stationLat, stationLon, s.latitude, s.longitude);
                        var distSpan = document.createElement("span");
                        distSpan.className = "pkt-distance";
                        distSpan.textContent = " (" + Math.round(dist) + "mi)";
                        tdCoords2.appendChild(distSpan);
                    }
                } else {
                    tdCoords2.textContent = "--";
                }
                tr.appendChild(tdCoords2);

                // Direct Count / Indirect Count
                var tdDirect = document.createElement("td");
                tdDirect.textContent = s.count_direct.toLocaleString();
                tr.appendChild(tdDirect);
                var tdIndirect = document.createElement("td");
                tdIndirect.textContent = s.count_digipeated.toLocaleString();
                tr.appendChild(tdIndirect);
            }

            heardBody.appendChild(tr);
        }
    }

    function renderFrequencies() {
        freqChart.innerHTML = "";
        if (!stationData || !stationData.frequencies || stationData.frequencies.length === 0) {
            freqChart.innerHTML = '<div class="empty-state">No frequency data</div>';
            return;
        }

        var freqs = stationData.frequencies.slice();
        freqs.sort(function (a, b) { return b.count - a.count; });
        var maxCount = freqs[0].count;

        for (var i = 0; i < freqs.length; i++) {
            var row = document.createElement("div");
            row.className = "freq-row";

            var label = document.createElement("span");
            label.className = "freq-label";
            label.textContent = freqs[i].frequency + " MHz";
            row.appendChild(label);

            var barWrap = document.createElement("div");
            barWrap.className = "freq-bar-wrap";

            var bar = document.createElement("div");
            bar.className = "freq-bar";
            bar.style.width = Math.max(1, (freqs[i].count / maxCount) * 100) + "%";
            barWrap.appendChild(bar);

            var countLabel = document.createElement("span");
            countLabel.className = "freq-count";
            countLabel.textContent = freqs[i].count.toLocaleString();
            barWrap.appendChild(countLabel);

            row.appendChild(barWrap);
            freqChart.appendChild(row);
        }
    }

    function renderSatellitePackets() {
        pruneSatPackets();

        var headers = ["", "Callsign", "Time", "Igate", "Altitude (ft)",
                       "Position", "Path", "Hops", "Distance"];

        heardThead.innerHTML = "";
        var headerRow = document.createElement("tr");
        for (var h = 0; h < headers.length; h++) {
            var th = document.createElement("th");
            th.textContent = headers[h];
            headerRow.appendChild(th);
        }
        heardThead.appendChild(headerRow);

        heardBody.innerHTML = "";

        if (satPackets.length === 0) {
            var emptyTr = document.createElement("tr");
            var emptyTd = document.createElement("td");
            emptyTd.colSpan = headers.length;
            emptyTd.className = "empty-state";
            emptyTd.textContent = "No satellite packets in the last 24 hours";
            emptyTr.appendChild(emptyTd);
            heardBody.appendChild(emptyTr);
            return;
        }

        for (var i = 0; i < satPackets.length; i++) {
            var p = satPackets[i];
            var tr = document.createElement("tr");

            // Symbol
            var tdSymbol = document.createElement("td");
            var imgSrc = getSymbolImage(p.info, p.destination);
            if (imgSrc) {
                var img = document.createElement("img");
                img.src = imgSrc;
                img.alt = "";
                img.onerror = function () { this.parentNode.removeChild(this); };
                tdSymbol.appendChild(img);
            }
            tr.appendChild(tdSymbol);

            // Callsign (with "via" if object/item)
            var tdCall = document.createElement("td");
            var displaySource = p.object_name || p.source;
            var callLink = document.createElement("a");
            callLink.href = aprsfiUrl(displaySource);
            callLink.target = "_blank";
            callLink.rel = "noopener";
            callLink.textContent = displaySource;
            tdCall.appendChild(callLink);
            if (p.object_name) {
                var bySpan = document.createElement("span");
                bySpan.className = "transmitted-by";
                bySpan.textContent = " via " + p.source;
                tdCall.appendChild(bySpan);
            }
            tr.appendChild(tdCall);

            // Time
            var tdTime = document.createElement("td");
            tdTime.textContent = formatTime(p.receivetime);
            tr.appendChild(tdTime);

            // Igate (T green / F plain)
            var tdIgate = document.createElement("td");
            if (p.igated) {
                var markI = document.createElement("mark");
                markI.className = "highlight-true";
                markI.textContent = "T";
                tdIgate.appendChild(markI);
            } else {
                tdIgate.textContent = "F";
            }
            tr.appendChild(tdIgate);

            // Altitude
            var tdAlt = document.createElement("td");
            tdAlt.textContent = p.altitude_ft != null
                ? Math.round(p.altitude_ft).toLocaleString() + " ft"
                : "--";
            tr.appendChild(tdAlt);

            // Position
            var tdPos = document.createElement("td");
            tdPos.className = "heard-coords";
            if (p.latitude != null && p.longitude != null) {
                var posLink = document.createElement("a");
                posLink.href = mapUrl(p.latitude, p.longitude, displaySource);
                posLink.target = "_blank";
                posLink.rel = "noopener";
                posLink.className = "coord-link";
                posLink.textContent = p.latitude.toFixed(6) + ", " + p.longitude.toFixed(6);
                tdPos.appendChild(posLink);
            } else {
                tdPos.textContent = "--";
            }
            tr.appendChild(tdPos);

            // Path
            var tdPath = document.createElement("td");
            tdPath.className = "heard-path";
            fillPathCell(tdPath, p.digipeater_path);
            tr.appendChild(tdPath);

            // Hops
            var tdHops = document.createElement("td");
            tdHops.className = "heard-hops";
            tdHops.textContent = p.hops != null ? p.hops : "0";
            tr.appendChild(tdHops);

            // Distance
            var tdDist = document.createElement("td");
            if (p.latitude != null && p.longitude != null
                && stationLat !== null && stationLon !== null) {
                var d = haversineDistance(stationLat, stationLon, p.latitude, p.longitude);
                tdDist.textContent = Math.round(d) + " mi";
            } else {
                tdDist.textContent = "--";
            }
            tr.appendChild(tdDist);

            heardBody.appendChild(tr);
        }
    }

    // ---- Info tooltips (hover + tap support) ----

    function setupTooltips() {
        var tips = document.querySelectorAll(".info-tip");
        var activeTooltip = null;

        function positionTip(tipEl, textEl) {
            textEl.classList.add("visible");

            var rect = tipEl.getBoundingClientRect();
            var tipRect = textEl.getBoundingClientRect();

            var top = rect.top - tipRect.height - 6;
            if (top < 4) top = rect.bottom + 6;

            var left = rect.left + rect.width / 2 - tipRect.width / 2;
            if (left < 4) left = 4;
            if (left + tipRect.width > window.innerWidth - 4) {
                left = window.innerWidth - tipRect.width - 4;
            }

            textEl.style.top = top + "px";
            textEl.style.left = left + "px";
        }

        for (var i = 0; i < tips.length; i++) {
            (function (tip) {
                var textEl = tip.querySelector(".tip-text");

                // click/tap toggle
                tip.addEventListener("click", function (e) {
                    e.stopPropagation();
                    if (activeTooltip === textEl) {
                        textEl.classList.remove("visible");
                        activeTooltip = null;
                    } else {
                        if (activeTooltip) activeTooltip.classList.remove("visible");
                        positionTip(tip, textEl);
                        activeTooltip = textEl;
                    }
                });

                // hover (desktop): show via the same clamping logic so the
                // tooltip is positioned within the viewport instead of clipping.
                tip.addEventListener("mouseenter", function () {
                    if (activeTooltip === textEl) return;
                    positionTip(tip, textEl);
                });
                tip.addEventListener("mouseleave", function () {
                    if (activeTooltip !== textEl) textEl.classList.remove("visible");
                });
            })(tips[i]);
        }

        // dismiss on tap/click elsewhere
        document.addEventListener("click", function () {
            if (activeTooltip) {
                activeTooltip.classList.remove("visible");
                activeTooltip = null;
            }
            // also dismiss raw packet tooltip
            rawTip.classList.remove("visible");
            rawTip._source = null;
        });
    }

    // ---- Theme toggle ----

    function setupThemeToggle() {
        var btn = document.getElementById("theme-toggle");
        var saved = localStorage.getItem("theme");
        if (saved === "light") {
            document.body.classList.add("light");
            btn.textContent = "Dark";
        }
        btn.addEventListener("click", function () {
            document.body.classList.toggle("light");
            var isLight = document.body.classList.contains("light");
            btn.textContent = isLight ? "Dark" : "Light";
            localStorage.setItem("theme", isLight ? "light" : "dark");
        });
    }

    // ---- Apply config to UI ----

    function applyConfig(cfg) {
        // Set station location for distance calculations
        if (cfg.location && cfg.location.lat != null && cfg.location.lon != null) {
            stationLat = cfg.location.lat;
            stationLon = cfg.location.lon;
        }

        // Update title bar with callsign and station name
        var titleEl = document.getElementById("header-title");
        var callsign = cfg.station && cfg.station.callsign ? cfg.station.callsign : "";
        var name = cfg.station && cfg.station.name ? cfg.station.name : "";
        if (callsign && name) {
            titleEl.textContent = callsign + " - " + name;
            document.title = callsign + " - " + name;
        } else if (callsign) {
            titleEl.textContent = callsign + " iGate";
            document.title = callsign + " iGate";
        }

        // Start uptime timer
        if (cfg.started_at) {
            startedAt = new Date(cfg.started_at);
            if (!uptimeTimer) {
                uptimeTimer = setInterval(updateUptime, 1000);
                updateUptime();
            }
        }

        // Populate config panel
        var grid = document.getElementById("config-grid");
        grid.innerHTML = "";

        var items = [];
        if (cfg.location) {
            if (cfg.location.lat != null && cfg.location.lon != null) {
                var stationCall = (cfg.station && cfg.station.callsign) ? cfg.station.callsign : "";
                items.push(["Coordinates", { type: "coords", lat: cfg.location.lat, lon: cfg.location.lon, label: stationCall }]);
            }
            if (cfg.location.alt != null) items.push(["Altitude", cfg.location.alt + " ft"]);
        }
        if (cfg.aprsis) {
            if (cfg.aprsis.host) items.push(["APRS-IS Host", cfg.aprsis.host + ":" + (cfg.aprsis.port || 14580)]);
            if (cfg.aprsis.enabled != null) items.push(["APRS-IS", cfg.aprsis.enabled ? "Enabled" : "Disabled"]);
            if (cfg.aprsis.igating != null) items.push(["Igating", cfg.aprsis.igating ? "Enabled" : "Disabled"]);
            if (cfg.aprsis.beaconing != null) items.push(["Beaconing", cfg.aprsis.beaconing ? "Enabled" : "Disabled"]);
            if (cfg.aprsis.threshold) items.push(["Beacon Interval", (cfg.aprsis.threshold / 60) + " min"]);
        }
        if (cfg.rtp) {
            items.push(["RTP Multicast", cfg.rtp.host + ":" + cfg.rtp.port]);
        }

        for (var i = 0; i < items.length; i++) {
            var div = document.createElement("div");
            div.className = "config-item";

            var label = document.createElement("span");
            label.className = "config-label";
            label.textContent = items[i][0] + ":";
            div.appendChild(label);

            var value = document.createElement("span");
            value.className = "config-value";
            var itemVal = items[i][1];
            if (itemVal && itemVal.type === "coords") {
                var link = document.createElement("a");
                link.href = mapUrl(itemVal.lat, itemVal.lon, itemVal.label);
                link.target = "_blank";
                link.rel = "noopener";
                link.className = "coord-link";
                link.textContent = itemVal.lat.toFixed(6) + ", " + itemVal.lon.toFixed(6);
                value.appendChild(link);
            } else {
                if (itemVal === "Enabled") {
                    value.classList.add("config-enabled");
                }
                value.textContent = itemVal;
            }
            div.appendChild(value);

            grid.appendChild(div);
        }
    }

    // ---- Fetch station config ----

    function fetchConfig() {
        var xhr = new XMLHttpRequest();
        xhr.open("GET", "/api/config", true);
        xhr.onload = function () {
            if (xhr.status !== 200) return;
            applyConfig(JSON.parse(xhr.responseText));
        };
        xhr.send();
    }

    // ---- Satellite packet log helpers ----

    function pruneSatPackets() {
        var cutoff = Date.now() - SAT_LOG_MS;
        while (satPackets.length > 0) {
            var t = new Date(satPackets[satPackets.length - 1].receivetime).getTime();
            if (t < cutoff) {
                satPackets.pop();
            } else {
                break;
            }
        }
    }

    function fetchSatellitePackets() {
        var xhr = new XMLHttpRequest();
        xhr.open("GET", "/api/satellite-packets", true);
        xhr.onload = function () {
            if (xhr.status !== 200) return;
            try {
                satPackets = JSON.parse(xhr.responseText) || [];
            } catch (e) {
                satPackets = [];
            }
            pruneSatPackets();
            if (activeTab === "satellites") {
                renderStations();
            }
        };
        xhr.send();
    }

    // ---- Haversine distance (miles) ----

    function haversineDistance(lat1, lon1, lat2, lon2) {
        var R = 3958.8; // Earth radius in miles
        var dLat = (lat2 - lat1) * Math.PI / 180;
        var dLon = (lon2 - lon1) * Math.PI / 180;
        var a = Math.sin(dLat / 2) * Math.sin(dLat / 2) +
            Math.cos(lat1 * Math.PI / 180) * Math.cos(lat2 * Math.PI / 180) *
            Math.sin(dLon / 2) * Math.sin(dLon / 2);
        var c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
        return R * c;
    }

    // ---- APRS position parsing ----

    // Parse DDMM.MM format to decimal degrees
    function parseDDMM(ddmm, dir) {
        if (!ddmm || ddmm.length < 4) return null;
        // Latitude: DDMM.MM  Longitude: DDDMM.MM
        var dotIdx = ddmm.indexOf(".");
        if (dotIdx < 2) return null;
        var degLen = dotIdx - 2;
        var deg = parseFloat(ddmm.substring(0, degLen));
        var min = parseFloat(ddmm.substring(degLen));
        if (isNaN(deg) || isNaN(min)) return null;
        var result = deg + min / 60.0;
        if (dir === "S" || dir === "W") result = -result;
        return result;
    }

    // Parse APRS compressed position (base-91 encoding)
    function parseCompressedPos(s) {
        // s should be 12 chars: /YYYY XXXX CS T  (symbol table, 4 lat, 4 lon, cs, type)
        if (!s || s.length < 12) return null;
        var y1 = s.charCodeAt(1) - 33;
        var y2 = s.charCodeAt(2) - 33;
        var y3 = s.charCodeAt(3) - 33;
        var y4 = s.charCodeAt(4) - 33;
        var x1 = s.charCodeAt(5) - 33;
        var x2 = s.charCodeAt(6) - 33;
        var x3 = s.charCodeAt(7) - 33;
        var x4 = s.charCodeAt(8) - 33;
        if (y1 < 0 || y2 < 0 || y3 < 0 || y4 < 0) return null;
        if (x1 < 0 || x2 < 0 || x3 < 0 || x4 < 0) return null;
        var lat = 90.0 - (y1 * 753571 + y2 * 8281 + y3 * 91 + y4) / 380926.0;
        var lon = -180.0 + (x1 * 753571 + x2 * 8281 + x3 * 91 + x4) / 190463.0;
        return { lat: lat, lon: lon };
    }

    // Extract lat/lon from the APRS info field
    function parsePosition(info) {
        if (!info || info.length < 2) return null;
        var dataType = info.charAt(0);

        // Uncompressed position: !, =, /, @
        // !DDMM.MMN/DDDMM.MMW...  (/ or @ have 7-char timestamp prefix)
        if (dataType === "!" || dataType === "=") {
            var posStr = info.substring(1);
            var pc1 = posStr.charAt(0);
            if (pc1 >= "0" && pc1 <= "9") {
                // Uncompressed: DDMM.MMN/DDDMM.MMW
                if (posStr.length >= 18) {
                    var lat = parseDDMM(posStr.substring(0, 7), posStr.charAt(7));
                    var lon = parseDDMM(posStr.substring(9, 17), posStr.charAt(17));
                    if (lat !== null && lon !== null) return { lat: lat, lon: lon };
                }
            } else {
                // Compressed: sym YYYY XXXX sym_code
                return parseCompressedPos(posStr);
            }
        } else if (dataType === "/" || dataType === "@") {
            var posStr2 = info.substring(8); // skip /HHMMSSh or @HHMMSSh
            var pc8 = posStr2.charAt(0);
            if (pc8 >= "0" && pc8 <= "9") {
                // Uncompressed
                if (posStr2.length >= 18) {
                    var lat2 = parseDDMM(posStr2.substring(0, 7), posStr2.charAt(7));
                    var lon2 = parseDDMM(posStr2.substring(9, 17), posStr2.charAt(17));
                    if (lat2 !== null && lon2 !== null) return { lat: lat2, lon: lon2 };
                }
            } else {
                // Compressed
                return parseCompressedPos(posStr2);
            }
        }
        // Mic-E: ` or ' — position is encoded in the destination field, which we
        // don't have readily available here, so skip for now.
        // Objects: ;name(9)*timestamp(7)DDMM.MMN/DDDMM.MMW...
        else if (dataType === ";") {
            var starIdx = info.indexOf("*");
            if (starIdx === -1) starIdx = info.indexOf("_");
            if (starIdx >= 0) {
                var objPos = info.substring(starIdx + 8); // skip live/dead(1) + timestamp(7)
                if (objPos.length >= 18) {
                    var lat3 = parseDDMM(objPos.substring(0, 7), objPos.charAt(7));
                    var lon3 = parseDDMM(objPos.substring(9, 17), objPos.charAt(17));
                    if (lat3 !== null && lon3 !== null) return { lat: lat3, lon: lon3 };
                }
            }
        }

        return null;
    }

    // Format coordinates with distance
    function formatCoords(pos) {
        if (!pos) return "--";
        var text = pos.lat.toFixed(6) + ", " + pos.lon.toFixed(6);
        if (stationLat !== null && stationLon !== null) {
            var dist = haversineDistance(stationLat, stationLon, pos.lat, pos.lon);
            text += " (" + Math.round(dist) + "mi)";
        }
        return text;
    }

    // ---- Symbol lookup ----

    function getSymbolImage(info, destination) {
        if (!info || info.length < 2) return null;

        var tableChar = null;
        var symbolChar = null;
        var overlay = null;
        var dataType = info.charAt(0);

        if (dataType === "!" || dataType === "=") {
            if (info.length >= 2) {
                var c1 = info.charAt(1);
                if (c1 >= "0" && c1 <= "9") {
                    // uncompressed: !DDMM.MMN sym DDDMM.MMW sym_code
                    if (info.length >= 10) tableChar = info.charAt(9);
                    if (info.length >= 20) symbolChar = info.charAt(19);
                } else {
                    // compressed: !sym YYYY XXXX sym_code cs T
                    tableChar = c1;
                    if (info.length >= 11) symbolChar = info.charAt(10);
                }
            }
        } else if (dataType === "/" || dataType === "@") {
            if (info.length >= 9) {
                var c8 = info.charAt(8);
                if (c8 >= "0" && c8 <= "9") {
                    // uncompressed: @timestamp DDMM.MMN sym DDDMM.MMW sym_code
                    if (info.length >= 17) tableChar = info.charAt(16);
                    if (info.length >= 27) symbolChar = info.charAt(26);
                } else {
                    // compressed: @timestamp sym YYYY XXXX sym_code cs T
                    tableChar = c8;
                    if (info.length >= 18) symbolChar = info.charAt(17);
                }
            }
        } else if (dataType === "`" || dataType === "'") {
            if (info.length >= 9) {
                symbolChar = info.charAt(7);
                tableChar = info.charAt(8);
            }
        } else if (dataType === ";") {
            // Object: ;name(9)*timestamp(7)position...
            // starIdx points at '*' or '_' (live/dead marker at fixed offset 10)
            var starIdx = info.indexOf("*");
            if (starIdx === -1) starIdx = info.indexOf("_");
            if (starIdx >= 0) {
                var posStart = starIdx + 8; // skip live/dead(1) + timestamp(7)
                if (info.length > posStart) {
                    var posChar = info.charAt(posStart);
                    if (posChar >= "0" && posChar <= "9") {
                        // uncompressed: table at posStart+8, code at posStart+18
                        if (info.length >= posStart + 19) {
                            tableChar = info.charAt(posStart + 8);
                            symbolChar = info.charAt(posStart + 18);
                        }
                    } else {
                        // compressed: table at posStart, code at posStart+9
                        if (info.length >= posStart + 10) {
                            tableChar = posChar;
                            symbolChar = info.charAt(posStart + 9);
                        }
                    }
                }
            }
        }

        if (!tableChar || !symbolChar) return null;

        if (tableChar !== "/" && tableChar !== "\\") {
            overlay = tableChar;
            tableChar = "\\";
        }

        var key = tableChar + symbolChar;

        if (typeof symbols !== "undefined" && symbols[key]) {
            var identity = symbols[key].identity;
            var filename;
            if (overlay) {
                filename = overlay + "-" + identity + ".png";
            } else {
                filename = identity + ".png";
            }
            return "/assets/aprssymbols/" + filename;
        }

        return null;
    }

    // ---- aprs.fi link helper ----

    function aprsfiUrl(callsign) {
        return "https://aprs.fi/#!call=" + encodeURIComponent(callsign);
    }

    // ---- Path cell helper ----
    function fillPathCell(td, pathArray) {
        if (pathArray && pathArray.length > 0) {
            for (var i = 0; i < pathArray.length; i++) {
                if (i > 0) td.appendChild(document.createTextNode(", "));
                var a = document.createElement("a");
                a.href = aprsfiUrl(pathArray[i]);
                a.target = "_blank";
                a.rel = "noopener";
                a.textContent = pathArray[i];
                td.appendChild(a);
            }
        } else {
            td.textContent = "--";
        }
    }

    // ---- Map URL helper ----

    var isApplePlatform = /iPad|iPhone|iPod|Macintosh/.test(navigator.userAgent);

    function mapUrl(lat, lon, label) {
        if (isApplePlatform) {
            return "https://maps.apple.com/?q=" + encodeURIComponent(label) + "&ll=" + lat + "%2C" + lon;
        }
        return "https://www.google.com/maps/search/?api=1&query=" + lat + "%2C" + lon;
    }

    // ---- Time formatting ----

    function formatTime(isoString) {
        var d = new Date(isoString);
        var h = String(d.getHours()).padStart(2, "0");
        var m = String(d.getMinutes()).padStart(2, "0");
        var s = String(d.getSeconds()).padStart(2, "0");
        return h + ":" + m + ":" + s;
    }

    // ---- Latest data point value ----

    function latestValue(series) {
        if (!series || !series.data || series.data.length === 0) return "--";
        return series.data[series.data.length - 1].value;
    }


    // ---- Sparkline drawing ----

    function drawSparkline(canvasId, series, color) {
        var canvas = document.getElementById(canvasId);
        if (!canvas || !series || !series.data || series.data.length < 2) return;

        var ctx = canvas.getContext("2d");
        var w = canvas.width;
        var h = canvas.height;
        var data = series.data;
        var len = data.length;
        var padding = 2;

        // Find min/max for auto-scaling
        var min = Infinity;
        var max = -Infinity;
        for (var i = 0; i < len; i++) {
            var v = data[i].value;
            if (v < min) min = v;
            if (v > max) max = v;
        }

        // If all values are the same, center the line
        if (max === min) {
            max = min + 1;
        }

        var rangeY = max - min;
        var drawW = w - padding * 2;
        var drawH = h - padding * 2;

        ctx.clearRect(0, 0, w, h);

        // Draw filled area
        ctx.beginPath();
        ctx.moveTo(padding, h - padding);
        for (var j = 0; j < len; j++) {
            var x = padding + (j / (len - 1)) * drawW;
            var y = h - padding - ((data[j].value - min) / rangeY) * drawH;
            ctx.lineTo(x, y);
        }
        ctx.lineTo(padding + drawW, h - padding);
        ctx.closePath();
        ctx.fillStyle = color + "33"; // 20% opacity fill
        ctx.fill();

        // Draw line
        ctx.beginPath();
        for (var k = 0; k < len; k++) {
            var lx = padding + (k / (len - 1)) * drawW;
            var ly = h - padding - ((data[k].value - min) / rangeY) * drawH;
            if (k === 0) {
                ctx.moveTo(lx, ly);
            } else {
                ctx.lineTo(lx, ly);
            }
        }
        ctx.strokeStyle = color;
        ctx.lineWidth = 1.5;
        ctx.stroke();

        // Draw dot on latest value
        var lastX = padding + drawW;
        var lastY = h - padding - ((data[len - 1].value - min) / rangeY) * drawH;
        ctx.beginPath();
        ctx.arc(lastX, lastY, 2.5, 0, Math.PI * 2);
        ctx.fillStyle = color;
        ctx.fill();
    }

    // ---- Slicer waterfall heatmap ----

    // Monochrome-green color scale (low -> high). Interpolates between fixed
    // stops; t in [0, 1]. t === 0 (no packets) yields the darkest base color.
    var WATERFALL_STOPS = [
        [0x0a, 0x1a, 0x0a],   // base / empty
        [0x1f, 0x5a, 0x1f],
        [0x3f, 0xae, 0x3f],
        [0xa5, 0xd6, 0xa7],   // brightest
    ];

    function lerpGreen(t) {
        if (t <= 0) return "rgb(10,26,10)";
        if (t >= 1) t = 1;
        var span = WATERFALL_STOPS.length - 1;
        var pos = t * span;
        var i = Math.min(Math.floor(pos), span - 1);
        var f = pos - i;
        var a = WATERFALL_STOPS[i];
        var b = WATERFALL_STOPS[i + 1];
        var r = Math.round(a[0] + (b[0] - a[0]) * f);
        var g = Math.round(a[1] + (b[1] - a[1]) * f);
        var bl = Math.round(a[2] + (b[2] - a[2]) * f);
        return "rgb(" + r + "," + g + "," + bl + ")";
    }

    var WATERFALL_ROWS = 10;

    // Classify a slicer by its space-gain into a twist zone. gain < 1 attenuates
    // the space tone (compensating loud space = pre-emphasis); gain > 1 boosts it
    // (compensating loud mark = de-emphasis). Boundaries are heuristic.
    function slicerZone(g) {
        if (g < 0.8) return "preemph";
        if (g < 1.25) return "flat";
        return "deemph";
    }

    var ZONE_LABEL = { preemph: "pre-emph", flat: "flat", deemph: "de-emph" };
    var ZONE_DESC = {
        preemph: "favors pre-emphasized (loud-space) signals",
        flat: "favors balanced (flat-audio) signals",
        deemph: "favors de-emphasized (loud-mark) signals"
    };

    // Format a space-gain as a mark:space ratio. gain is the space multiplier, so
    // mark:space = (1/gain):1, e.g. gain 0.5 -> "2.0:1", gain 4.0 -> "1:4.0".
    function ratioLabel(g) {
        var r = 1 / g;
        return r >= 1 ? r.toFixed(1) + ":1" : "1:" + g.toFixed(1);
    }

    function drawWaterfall(telem) {
        var group = document.getElementById("waterfall-group");
        var grid = document.getElementById("waterfall");
        var colsRow = document.getElementById("waterfall-cols");
        if (!group || !grid || !telem) return;

        var cols = telem.slicer_count || 0;
        if (cols <= 0) return;
        var gains = telem.slicer_gains || [];

        // drive the CSS grid column count (inherited by the zone strip, header, grid)
        group.style.setProperty("--slicer-cols", String(cols));

        // (re)build the zone strip + header only when the column count changes.
        // Header per slicer: mark:space ratio over the slicer index; the zone strip
        // groups consecutive slicers sharing a twist zone into spanning segments.
        if (colsRow.childElementCount !== cols + 1) {
            var zonesRow = document.getElementById("waterfall-zones");

            // --- zone strip ---
            zonesRow.innerHTML = "";
            var zspacer = document.createElement("div");
            zspacer.className = "waterfall-colspacer";
            zonesRow.appendChild(zspacer);
            var seg = 0;
            while (seg < cols) {
                var zone = slicerZone(gains[seg] != null ? gains[seg] : 1);
                var span = 1;
                while (seg + span < cols && slicerZone(gains[seg + span] != null ? gains[seg + span] : 1) === zone) {
                    span++;
                }
                var zoneEl = document.createElement("div");
                zoneEl.className = "waterfall-zone zone-" + zone;
                zoneEl.textContent = ZONE_LABEL[zone];
                zoneEl.style.gridColumn = "span " + span;
                zonesRow.appendChild(zoneEl);
                seg += span;
            }

            // --- per-slicer header (ratio + index) ---
            colsRow.innerHTML = "";
            var spacer = document.createElement("div");
            spacer.className = "waterfall-colspacer";
            spacer.textContent = "time";
            colsRow.appendChild(spacer);
            for (var c = 0; c < cols; c++) {
                var g = gains[c] != null ? gains[c] : 1;
                var zoneC = slicerZone(g);
                var col = document.createElement("div");
                col.className = "waterfall-col";
                var ratio = document.createElement("span");
                ratio.className = "waterfall-col-ratio";
                ratio.textContent = ratioLabel(g);
                col.appendChild(ratio);
                col.title = "Slicer " + c + " · gain " + g.toFixed(2) +
                    " · mark:space " + ratioLabel(g) + " · " + ZONE_DESC[zoneC];
                colsRow.appendChild(col);
            }
        }

        // newest interval on top
        var intervals = (telem.intervals || []).slice().reverse();

        // global max across every cell for brightness scaling (>= 1)
        var globalMax = 1;
        for (var ii = 0; ii < intervals.length; ii++) {
            var counts = intervals[ii].counts || [];
            for (var jj = 0; jj < counts.length; jj++) {
                if (counts[jj] > globalMax) globalMax = counts[jj];
            }
        }

        // rebuild the heatmap: always WATERFALL_ROWS rows so the grid height is
        // stable; rows beyond the available intervals render as empty (base color).
        grid.innerHTML = "";
        for (var row = 0; row < WATERFALL_ROWS; row++) {
            var interval = intervals[row];
            var rowCounts = interval ? (interval.counts || []) : null;
            var rowTime = interval ? formatTime(interval.timestamp) : null;

            // leading per-row timestamp label
            var rowLabel = document.createElement("div");
            rowLabel.className = "waterfall-rowlabel";
            rowLabel.textContent = rowTime || "";
            grid.appendChild(rowLabel);

            for (var col = 0; col < cols; col++) {
                var cell = document.createElement("div");
                cell.className = "waterfall-cell";
                var count = rowCounts ? (rowCounts[col] || 0) : 0;
                var t = count / globalMax;
                cell.style.backgroundColor = lerpGreen(t);
                // show the count as a centered digit; pick text color for contrast
                // against the green scale (light on dark cells, dark on bright).
                if (count > 0) {
                    cell.textContent = count;
                    cell.style.color = t > 0.5 ? "#0a1a0a" : "#cfe8cf";
                }
                if (rowTime) {
                    cell.title = "Slicer " + col + " · " + rowTime + " · " + count + " packet" + (count === 1 ? "" : "s");
                }
                grid.appendChild(cell);
            }
        }
    }

    // ---- Raw packet tooltip ----

    var rawTip = document.createElement("span");
    rawTip.className = "raw-tooltip";
    document.body.appendChild(rawTip);

    function showRawTooltip(tdEl) {
        var raw = tdEl.getAttribute("data-raw");
        if (!raw) return;

        // toggle off if same cell clicked again
        if (rawTip.classList.contains("visible") && rawTip._source === tdEl) {
            rawTip.classList.remove("visible");
            rawTip._source = null;
            return;
        }

        rawTip.textContent = raw;
        rawTip.classList.add("visible");
        rawTip._source = tdEl;

        // position above the clicked cell
        var rect = tdEl.getBoundingClientRect();
        var tipRect = rawTip.getBoundingClientRect();

        var top = rect.top - tipRect.height - 6;
        if (top < 4) top = rect.bottom + 6;

        var left = rect.left;
        if (left + tipRect.width > window.innerWidth - 4) {
            left = window.innerWidth - tipRect.width - 4;
        }
        if (left < 4) left = 4;

        rawTip.style.top = top + "px";
        rawTip.style.left = left + "px";
    }

    // ---- Packet row creation ----

    function addPacketRow(type, data) {
        var tr = document.createElement("tr");
        tr.className = type === "rf" ? "rf-packet" : "inet-packet";

        // Time
        var tdTime = document.createElement("td");
        tdTime.className = "pkt-time";
        tdTime.textContent = formatTime(data.receivetime);
        tr.appendChild(tdTime);

        // Symbol
        var tdSymbol = document.createElement("td");
        tdSymbol.className = "pkt-symbol";
        if (type === "rf") {
            var imgSrc = getSymbolImage(data.info, data.destination);
            if (imgSrc) {
                var img = document.createElement("img");
                img.src = imgSrc;
                img.alt = "";
                img.onerror = function () { this.parentNode.removeChild(this); };
                tdSymbol.appendChild(img);
            }
        }
        tr.appendChild(tdSymbol);

        // Source
        var tdSource = document.createElement("td");
        tdSource.className = "pkt-source";
        var displaySource = data.object_name || data.source;
        var srcLink = document.createElement("a");
        srcLink.href = aprsfiUrl(displaySource);
        srcLink.target = "_blank";
        srcLink.rel = "noopener";
        srcLink.textContent = displaySource;
        tdSource.appendChild(srcLink);
        if (data.object_name) {
            var bySpan = document.createElement("span");
            bySpan.className = "transmitted-by";
            bySpan.textContent = " via " + data.source;
            tdSource.appendChild(bySpan);
        }
        tr.appendChild(tdSource);

        // Frequency
        var tdFreq = document.createElement("td");
        tdFreq.className = "pkt-freq";
        if (type === "rf") {
            var freqText = data.frequency.toFixed(3);
            if (data.frequency !== 144.390) {
                var markF = document.createElement("mark");
                markF.className = "highlight-true";
                markF.textContent = freqText;
                tdFreq.appendChild(markF);
            } else {
                tdFreq.textContent = freqText;
            }
        } else {
            tdFreq.textContent = "inet";
        }
        tr.appendChild(tdFreq);

        // Heard Direct
        var tdDirect = document.createElement("td");
        tdDirect.className = "pkt-direct";
        if (type === "rf") {
            if (data.heard_direct) {
                var markD = document.createElement("mark");
                markD.className = "highlight-true";
                markD.textContent = "T";
                tdDirect.appendChild(markD);
            } else {
                tdDirect.textContent = "F";
            }
        } else {
            tdDirect.textContent = "--";
        }
        tr.appendChild(tdDirect);

        // Satellite
        var tdSat = document.createElement("td");
        tdSat.className = "pkt-sat";
        if (type === "rf") {
            if (data.is_satellite) {
                var markS = document.createElement("mark");
                markS.className = "highlight-true";
                markS.textContent = "T";
                tdSat.appendChild(markS);
            } else {
                tdSat.textContent = "F";
            }
        } else {
            tdSat.textContent = "--";
        }
        tr.appendChild(tdSat);

        // Coordinates — prefer backend-parsed position, fall back to JS parser
        var tdCoords = document.createElement("td");
        tdCoords.className = "pkt-coords";
        var pos = null;
        if (data.latitude != null && data.longitude != null) {
            pos = { lat: data.latitude, lon: data.longitude };
        } else {
            pos = parsePosition(data.info);
        }
        if (pos) {
            var coordText = pos.lat.toFixed(6) + ", " + pos.lon.toFixed(6);
            var link = document.createElement("a");
            link.href = mapUrl(pos.lat, pos.lon, data.source || "");
            link.target = "_blank";
            link.rel = "noopener";
            link.className = "coord-link";
            link.textContent = coordText;
            tdCoords.appendChild(link);
            if (stationLat !== null && stationLon !== null) {
                var dist = haversineDistance(stationLat, stationLon, pos.lat, pos.lon);
                var distSpan = document.createElement("span");
                distSpan.className = "pkt-distance";
                distSpan.textContent = " (" + Math.round(dist) + "mi)";
                tdCoords.appendChild(distSpan);
            }
        } else {
            tdCoords.textContent = "--";
        }
        tr.appendChild(tdCoords);

        // Hops
        var tdHops = document.createElement("td");
        tdHops.className = "pkt-hops";
        if (type === "rf") {
            tdHops.textContent = data.hops != null ? data.hops : "0";
        } else {
            tdHops.textContent = "--";
        }
        tr.appendChild(tdHops);

        // Path
        var tdPath = document.createElement("td");
        tdPath.className = "pkt-path";
        fillPathCell(tdPath, type === "rf" ? data.digipeater_path : null);
        tr.appendChild(tdPath);

        // Packet text (info field) — click to show full raw packet
        var tdText = document.createElement("td");
        tdText.className = "pkt-text";
        tdText.textContent = data.info;
        tdText.setAttribute("data-raw", data.raw);
        tdText.addEventListener("click", function (e) {
            e.stopPropagation();
            showRawTooltip(this);
        });
        tr.appendChild(tdText);

        // Insert at top
        if (packetBody.firstChild) {
            packetBody.insertBefore(tr, packetBody.firstChild);
        } else {
            packetBody.appendChild(tr);
        }

        // Trim to MAX_PACKETS
        while (packetBody.children.length > MAX_PACKETS) {
            packetBody.removeChild(packetBody.lastChild);
        }
    }

    // ---- Status indicator helpers ----

    function setStatus(el, state, label) {
        el.className = "status-indicator " + state;
        if (label) el.textContent = label;
    }

    // ---- SSE Connection with managed lifecycle and capped exponential backoff ----

    var BACKOFF_INITIAL = 2000;
    var BACKOFF_MAX = 30000;
    var backoffMs = BACKOFF_INITIAL;
    var reconnectTimer = null;
    var currentES = null;

    function onMessage() {
        // Any successful message resets the backoff
        backoffMs = BACKOFF_INITIAL;
    }

    function destroyES() {
        if (currentES) {
            currentES.close();
            currentES = null;
        }
    }

    function scheduleReconnect() {
        if (reconnectTimer) return; // already scheduled
        var delaySec = (backoffMs / 1000).toFixed(0);
        setStatus(sseStatus, "disconnected", "SSE (" + delaySec + "s)");
        reconnectTimer = setTimeout(function () {
            reconnectTimer = null;
            connectSSE();
        }, backoffMs);
        backoffMs = Math.min(backoffMs * 2, BACKOFF_MAX);
    }

    function connectSSE() {
        destroyES();

        var es = new EventSource("/api/sse");
        currentES = es;

        es.onopen = function () {
            backoffMs = BACKOFF_INITIAL;
            setStatus(sseStatus, "connected", "SSE");
            fetchConfig();
        };

        es.onerror = function () {
            // EventSource may be in CONNECTING (auto-retry) or CLOSED state
            // We take over reconnection in both cases for consistent backoff
            destroyES();
            scheduleReconnect();
        };

        es.addEventListener("config", function (e) {
            onMessage();
            var cfg = JSON.parse(e.data);
            applyConfig(cfg);
        });

        es.addEventListener("rfpacket", function (e) {
            onMessage();
            var data = JSON.parse(e.data);
            addPacketRow("rf", data);
            if (data.is_satellite) {
                satPackets.unshift(data);
                pruneSatPackets();
                if (activeTab === "satellites") {
                    renderStations();
                }
            }
            setStatus(rtpStatus, "connected");
        });

        es.addEventListener("packet_statistics", function (e) {
            onMessage();
            var data = JSON.parse(e.data);
            statRfDirect.textContent = latestValue(data.heard_direct);
            statRfDigipeated.textContent = latestValue(data.digipeated);
            statRfErrors.textContent = latestValue(data.decode_errors);
            drawSparkline("spark-rf-direct", data.heard_direct, "#a5d6a7");
            drawSparkline("spark-rf-digipeated", data.digipeated, "#fff176");
            drawSparkline("spark-rf-errors", data.decode_errors, "#ef9a9a");
            // Lifetime counters
            ltRfDirect.textContent = (data.lifetime_heard_direct || 0).toLocaleString();
            ltRfDigipeated.textContent = (data.lifetime_digipeated || 0).toLocaleString();
            ltRfErrors.textContent = (data.lifetime_decode_errors || 0).toLocaleString();
            ltRfTotal.textContent = (data.lifetime_total_packets || 0).toLocaleString();
            setStatus(rtpStatus, "connected");
        });

        es.addEventListener("aprsis_statistics", function (e) {
            onMessage();
            var data = JSON.parse(e.data);
            statAprsisIgated.textContent = latestValue(data.packets_igated);
            statAprsisDropped.textContent = latestValue(data.packets_dropped);
            statAprsisReconnects.textContent = latestValue(data.reconnects);
            drawSparkline("spark-aprsis-igated", data.packets_igated, "#fff176");
            drawSparkline("spark-aprsis-dropped", data.packets_dropped, "#ef9a9a");
            drawSparkline("spark-aprsis-reconnects", data.reconnects, "#ce93d8");
            // Lifetime counters
            ltAprsisIgated.textContent = (data.lifetime_packets_igated || 0).toLocaleString();
            ltAprsisDropped.textContent = (data.lifetime_packets_dropped || 0).toLocaleString();
            ltAprsisReconnects.textContent = (data.lifetime_reconnects || 0).toLocaleString();
            setStatus(aprsisStatus, "connected");
        });

        es.addEventListener("slicer_statistics", function (e) {
            onMessage();
            drawWaterfall(JSON.parse(e.data));
        });

        es.addEventListener("station_statistics", function (e) {
            onMessage();
            stationData = JSON.parse(e.data);
            renderStations();
        });
    }

    // Start
    setupTooltips();
    setupThemeToggle();
    setupTabs();
    fetchConfig();
    fetchSatellitePackets();
    connectSSE();
})();
