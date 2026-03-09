const host = window.location.hostname;

const ENDPOINTS = {
    WS: `ws://${host}:8000/ws`,
    API: `http://${host}:8000/api/commands`,
    SIMULATOR: `http://${host}:8000/api/sensors`,
    RULES: `http://${host}:8000/api/rules`
};

const SENSORS_REGISTRY = [
    { id: 'greenhouse_temperature_value', simId: 'greenhouse_temperature', label: 'Greenhouse Temp', shortLabel: 'TEMP.', unit: '°C', min: 0, max: 40 },
    { id: 'entrance_humidity_value', simId: 'entrance_humidity', label: 'Entrance Humidity', shortLabel: 'HUM.', unit: '%', min: 0, max: 100 },
    { id: 'co2_hall_value', simId: 'co2_hall', label: 'CO2 Hall Level', shortLabel: 'CO2', unit: 'ppm', min: 400, max: 1000 },
    { id: 'corridor_pressure_value', simId: 'corridor_pressure', label: 'Corridor Pressure', shortLabel: 'PRE.', unit: 'kPA', min: 90, max: 115 },
    { id: 'water_tank_level_level_pct', simId: 'water_tank_level', label: 'Water Tank Level', shortLabel: 'WATER %', unit: '%', min: 0, max: 100 },
    { id: 'water_tank_level_level_liters', simId: 'water_tank_level', label: 'Water Tank Vol', shortLabel: 'WATER L', unit: 'L', min: 0, max: 3000 },
    { id: 'hydroponic_ph_ph', simId: 'hydroponic_ph', label: 'Hydroponic pH', shortLabel: 'HYDRO', unit: 'pH', min: 4.0, max: 9.0 },
    { id: 'air_quality_pm25_pm25_ug_m3', simId: 'air_quality_pm25', label: 'PM 2.5 Level', shortLabel: 'PM 2.5', unit: 'µg', min: 0, max: 50 },
    { id: 'air_quality_pm25_pm1_ug_m3', simId: 'air_quality_pm25', label: 'PM 1.0 Level', shortLabel: 'PM 1.0', unit: 'µg', min: 0, max: 30 },
    { id: 'air_quality_pm25_pm10_ug_m3', simId: 'air_quality_pm25', label: 'PM 10 Level', shortLabel: 'PM 10', unit: 'µg', min: 0, max: 60 },
    { id: 'air_quality_voc_voc_ppb', simId: 'air_quality_voc', label: 'Volatile Org. Comp', shortLabel: 'VOC', unit: 'ppb', min: 0, max: 600 },
    { id: 'air_quality_voc_co2e_ppm', simId: 'air_quality_voc', label: 'CO2 Equivalent', shortLabel: 'CO2e', unit: 'ppm', min: 400, max: 1500 }
];

const ACTUATOR_IDS = ['cooling_fan', 'habitat_heater', 'hall_ventilation', 'entrance_humidifier'];

let systemState = { booted: false, sensorsReceived: new Set(), actuators: {}, criticalSensors: new Set() };
let auditLogs = []; 
let ingestionTimestamps = {}; 
let ingestionCounts = {}; 
let lastOperationTime = "N/A";
let lastManualCommandTime = 0;
let currentRulesList = [];
let editingRuleId = null;

let sensorHistory = {}; 
let activeChart = null;
let activeChartId = null;

function initMissionControl() {
    document.body.classList.add('no-scroll');
    renderGrid();
    renderEngineerViews();
    renderSafetyView();
    
    const savedRole = localStorage.getItem('aresguard_active_role') || 'specialist';
    setRole(savedRole);
    
    connect();
    setInterval(updateIngestionTimers, 1000);
}

function renderGrid() {
    const grid = document.getElementById('sensor-grid');
    if (!grid) return;
    grid.innerHTML = SENSORS_REGISTRY.map(s => `
        <div class="sensor-card" id="card-${s.id}" onclick="toggleCardView('${s.id}')">
            <div class="view-primary">
                <span class="status-label status-ok" id="status-${s.id}">● NORMAL</span>
                <div class="sensor-value-group"><h2 id="val-${s.id}">--.-</h2><span class="unit">${s.unit}</span></div>
                <p class="sensor-label">${s.label}</p>
            </div>
            <div class="view-secondary">
                <button class="graph-btn" onclick="openGraph('${s.id}', event)">~ CHART</button>
                <span class="status-label" style="color:#aaa;">MONITORING RANGE</span>
                <div class="sensor-value-group"><h2 id="val-sec-${s.id}">--.-</h2><span class="unit">${s.unit}</span></div>
                <div class="mini-progress-container"><div class="mini-progress-bar" id="bar-${s.id}" style="width: 0%;"></div></div>
                <div style="display:flex; justify-content:space-between; width:100%;">
                    <p class="sensor-label">MIN: ${s.min}</p><p class="sensor-label">MAX: ${s.max}</p>
                </div>
                <div class="refresh-section">
                    <span class="auto-refresh-label">● AUTO-REFRESH (5s)</span>
                    <button class="refresh-btn" onclick="forceRefresh('${s.id}', event)">↻ FETCH</button>
                </div>
            </div>
        </div>
    `).join('');
}

function renderEngineerViews() {
    const miniGrid = document.getElementById('mini-sensor-grid');
    if (miniGrid) {
        miniGrid.innerHTML = SENSORS_REGISTRY.map(s => `
            <div class="mini-card">
                <span class="mini-card-label">${s.shortLabel}</span>
                <span class="mini-card-val" id="mini-val-${s.id}">--.- <span style="font-size:9px;color:#555">${s.unit}</span></span>
            </div>
        `).join('');
    }

    const sensorSelect = document.getElementById('rule-sensor');
    if (sensorSelect) {
        sensorSelect.innerHTML = SENSORS_REGISTRY.map(s => `<option value="${s.id}">${s.label}</option>`).join('');
        sensorSelect.onchange = () => {
            const sel = SENSORS_REGISTRY.find(x => x.id === sensorSelect.value);
            if (sel) {
                const unitEl = document.getElementById('rule-unit');
                const valInput = document.getElementById('rule-value');
                if (unitEl) unitEl.innerText = sel.unit;
                if (valInput) {
                    valInput.min = sel.min;
                    valInput.max = sel.max;
                    valInput.placeholder = `${sel.min} - ${sel.max}`;
                }
            }
        };
        sensorSelect.onchange(); 
    }

    const actuatorSelect = document.getElementById('rule-actuator');
    if (actuatorSelect) {
        actuatorSelect.innerHTML = ACTUATOR_IDS.map(a => `<option value="${a}">${a.toUpperCase().replace('_', ' ')}</option>`).join('');
    }
}

async function fetchRules() {
    try {
        const res = await fetch(ENDPOINTS.RULES);
        currentRulesList = await res.json();
        renderRules(currentRulesList);
    } catch (e) {
        const tbody = document.getElementById('rules-tbody');
        if (tbody) tbody.innerHTML = `<tr><td colspan="5" style="text-align:center; color:#ef4444; padding:20px;">Database Offline.</td></tr>`;
    }
}

function renderRules(rules) {
    const tbody = document.getElementById('rules-tbody');
    if (!tbody) return;
    if (rules.length === 0) {
        tbody.innerHTML = `<tr><td colspan="5" style="text-align:center; color:#888; padding:20px;">No automation rules active.</td></tr>`;
        return;
    }
    
    tbody.innerHTML = rules.sort((a, b) => a.rule_id - b.rule_id).map(r => {
        const s = SENSORS_REGISTRY.find(x => x.id === r.sensor_id);
        const displayName = s ? s.label : r.sensor_id;
        
        return `
        <tr>
            <td style="color:#f59e0b;">${r.rule_id}</td>
            <td>IF <strong style="text-transform:uppercase;">${displayName}</strong> ${r.operator} ${r.threshold}</td>
            <td>SET <strong>${r.actuator_id.toUpperCase().replace('_', ' ')}</strong> TO ${r.action}</td>
            <td>
                <div style="display: flex; gap: 8px; justify-content: flex-start; align-items: center; height: 100%;">
                    <button class="btn-edit" onclick="editRule('${r.rule_id}')">! EDIT</button>
                    <button class="btn-del" onclick="askDeleteRule('${r.rule_id}')">\\ DELETE</button>
                </div>
            </td>
        </tr>
    `}).join('');
}

async function saveRule() {
    const sId = document.getElementById('rule-sensor').value;
    const op = document.getElementById('rule-operator').value;
    const val = parseFloat(document.getElementById('rule-value').value);
    const act = document.getElementById('rule-actuator').value;
    const action = document.getElementById('rule-action').value;

    if (isNaN(val)) { showToast("INVALID VALUE", "error"); return; }
    
    const url = editingRuleId ? `${ENDPOINTS.RULES}/${editingRuleId}` : ENDPOINTS.RULES;
    const method = editingRuleId ? 'PUT' : 'POST';

    try {
        const res = await fetch(url, {
            method: method,
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({ sensor_id: sId, operator: op, threshold: val, actuator_id: act, action: action })
        });

        if(res.ok) {
            const data = await res.json();
            fetchRules(); 
            
            if (editingRuleId) {
                showToast("RULE UPDATED", "success");
            } else if (data.action === "updated") {
                showToast(`RULE OVERWRITTEN TO NEW THRESHOLD: ${val}`, "info");
            } else {
                showToast("NEW RULE CREATED", "success");
            }
            
            addLog(`Automation DB: ${data.action || 'updated'} rule for ${sId}`, "#22c55e");
            resetRuleForm();
            lastOperationTime = new Date().toLocaleString();
            updateSystemHealth();
        }
    } catch (e) { showToast("SAVE FAILED", "error"); }
}

function editRule(id) {
    const rule = currentRulesList.find(r => String(r.rule_id) === String(id));
    if (!rule) return;

    editingRuleId = id;
    document.getElementById('rule-sensor').value = rule.sensor_id;
    document.getElementById('rule-operator').value = rule.operator;
    document.getElementById('rule-value').value = rule.threshold;
    document.getElementById('rule-actuator').value = rule.actuator_id;
    document.getElementById('rule-action').value = rule.action;

    const saveBtn = document.querySelector('.btn-save');
    saveBtn.innerHTML = "↻ UPDATE RULE";
    saveBtn.classList.add('btn-update'); 

    document.getElementById('rule-sensor').onchange();
    showToast(`EDITING RULE ${id}`, "info");
}

function resetRuleForm() {
    editingRuleId = null;
    document.getElementById('rule-value').value = "";
    
    const saveBtn = document.querySelector('.btn-save');
    saveBtn.innerHTML = "+ SAVE TO DB";
    saveBtn.classList.remove('btn-update'); 
}

function renderSafetyView() {
    renderIngestionTable();
    renderAuditTable();
    updateSystemHealth();
}

function renderIngestionTable() {
    const tbody = document.getElementById('ingestion-tbody');
    if (!tbody) return;
    const uniqueSimIds = [...new Set(SENSORS_REGISTRY.map(s => s.simId))];
    tbody.innerHTML = uniqueSimIds.map(simId => {
        const packetCount = ingestionCounts[simId] || 0;
        const packetInfo = packetCount > 0 ? `<span style="color:#555; font-size:9px">[${packetCount} pkts]</span>` : "";
        return `
            <tr>
                <td style="color:#fff; font-weight:bold;">${simId}</td>
                <td style="font-family:'Courier New'; font-size:10px; color:#aaa;">rest_polling</td>
                <td style="font-family:'Courier New'; font-size:10px;">JSON/Telemetry ${packetInfo}</td>
                <td id="ingest-cell-${simId}"><span style="color:#666">● WAITING...</span></td>
            </tr>
        `;
    }).join('');
}

function updateIngestionTimers() {
    const uniqueSimIds = [...new Set(SENSORS_REGISTRY.map(s => s.simId))];
    uniqueSimIds.forEach(simId => {
        const cell = document.getElementById(`ingest-cell-${simId}`);
        if (!cell) return;
        
        const lastTime = ingestionTimestamps[simId];
        if (lastTime) {
            const latency = Math.floor((Date.now() - lastTime) / 1000);
            if (latency < 10) cell.innerHTML = `<span class="status-green">● LIVE</span>`; 
            else if (latency < 30) cell.innerHTML = `<span class="status-warn">● STALE</span>`;
            else cell.innerHTML = `<span class="status-red">● NO SIGNAL</span>`;
        }
    });
}

function addToAuditLog(type, actuator, action, reason) {
    const cleanActuator = actuator.toUpperCase();
    
    if (auditLogs.length > 0) {
        const lastLog = auditLogs[0];
        if (lastLog.actuator === cleanActuator && lastLog.action === action && lastLog.type === type) {
            return; 
        }
    }

    const newLog = { 
        timestamp: new Date().toLocaleTimeString(), 
        type: type, 
        actuator: cleanActuator, 
        action: action, 
        reason: reason 
    };
    
    lastOperationTime = newLog.timestamp;
    auditLogs.unshift(newLog); 
    if (auditLogs.length > 5) auditLogs.pop(); 
    
    updateSystemHealth();

    const tbody = document.getElementById('audit-tbody');
    if (!tbody) return;

    if (tbody.firstElementChild && tbody.firstElementChild.innerText.includes('No recent events')) {
        tbody.innerHTML = '';
    }

    let badgeColor = type === 'MANUAL' ? '#3b82f6' : (type === 'AUTO' ? '#f59e0b' : '#666');
    let textColor = type === 'SYSTEM' ? '#fff' : '#000';
    
    let formattedAction = action
        .replace('ON', '<span style="color:#22c55e;">ON</span>')
        .replace('OFF', '<span>OFF</span>');

    const tr = document.createElement('tr');
    tr.style.animation = 'slideInRow 0.4s ease-out forwards'; 
    tr.innerHTML = `
        <td style="color:#888; font-size:10px;">${newLog.timestamp}</td>
        <td><span class="badge" style="background:${badgeColor}; color:${textColor};">${newLog.type}</span></td>
        <td style="font-weight:bold; color:#fff;">${newLog.actuator}</td>
        <td style="color:#888; font-weight:bold;">${formattedAction}</td>
        <td style="font-size:10px; color:#aaa;">${newLog.reason}</td>
    `;
    
    tbody.prepend(tr); 
    
    while (tbody.children.length > 5) {
        tbody.lastElementChild.remove();
    }
}

function renderAuditTable() {
    const tbody = document.getElementById('audit-tbody');
    if (!tbody) return;
    
    if (auditLogs.length === 0) {
        tbody.innerHTML = `<tr><td colspan="5" style="text-align:center; color:#555; padding:15px;">No recent events.</td></tr>`;
        return;
    }

    tbody.innerHTML = auditLogs.map(log => {
        let badgeColor = log.type === 'MANUAL' ? '#3b82f6' : (log.type === 'AUTO' ? '#f59e0b' : '#666');
        let textColor = log.type === 'SYSTEM' ? '#fff' : '#000';
        
        let formattedAction = log.action
            .replace('ON', '<span style="color:#22c55e;">ON</span>')
            .replace('OFF', '<span>OFF</span>');
        
        return `
        <tr>
            <td style="color:#888; font-size:10px;">${log.timestamp}</td>
            <td><span class="badge" style="background:${badgeColor}; color:${textColor};">${log.type}</span></td>
            <td style="font-weight:bold; color:#fff;">${log.actuator}</td>
            <td style="color:#888; font-weight:bold;">${formattedAction}</td>
            <td style="font-size:10px; color:#aaa;">${log.reason}</td>
        </tr>
    `}).join('');
}

function updateSystemHealth() {
    const alertEl = document.getElementById('active-alerts-count');
    const opEl = document.getElementById('last-backup-time');
    if (alertEl) {
        alertEl.innerText = systemState.criticalSensors.size;
        alertEl.style.color = systemState.criticalSensors.size > 0 ? '#ef4444' : '#22c55e';
    }
    if (opEl) opEl.innerText = lastOperationTime;
}

function connect() {
    const socket = new WebSocket(ENDPOINTS.WS);
    socket.onopen = () => { 
        const status = document.getElementById('conn-status');
        if(status) { status.innerText = "ONLINE"; status.style.color = "#22c55e"; }
        addLog("Data Uplink Established.", "#22c55e");
    };
    socket.onmessage = (event) => {
        try {
            const msg = JSON.parse(event.data);
            if (msg.type === "FULL_STATE") { 
                Object.values(msg.data).forEach(entry => processEventData(entry)); 
                checkBootSequence(); 
            }
            else if (msg.type === "LIVE_UPDATE") { 
                processEventData(msg.data); 
                checkBootSequence(); 
            }
        } catch(e) {}
    };
    socket.onclose = () => {
        const status = document.getElementById('conn-status');
        if(status) { status.innerText = "OFFLINE"; status.style.color = "#ef4444"; }
        setTimeout(connect, 3000);
    };
}

function matchSensorId(target, received) {
    const normalize = (s) => (s || '').toString().toLowerCase().replace(/[^a-z0-9]/g, '');
    return normalize(target).includes(normalize(received)) || normalize(received).includes(normalize(target));
}

function processEventData(entry) {
    if (!entry || !entry.source) return;
    const id = entry.source.identifier;
    appendRawLog(entry);
    
    const sensorConfig = SENSORS_REGISTRY.find(s => matchSensorId(s.id, id) || s.simId === id);
    const targetSimId = sensorConfig ? sensorConfig.simId : id;
    ingestionTimestamps[targetSimId] = Date.now();
    ingestionCounts[targetSimId] = (ingestionCounts[targetSimId] || 0) + 1;

    if (ACTUATOR_IDS.includes(id)) {
        const newState = (String(entry.payload.value).toUpperCase() === "ON" || entry.payload.value === true) ? "ON" : "OFF";
        
        const isRecentManual = (Date.now() - lastManualCommandTime) < 7000;
        
        if (!systemState.actuators.hasOwnProperty(id)) {
            addToAuditLog("SYSTEM", id, `STATE: ${newState}`, "Last Recent Setting");
        } else if (!isRecentManual && systemState.actuators[id] !== newState) {
            addToAuditLog("AUTO", id, `SET TO ${newState}`, "Automation Rule Trigger");
        }
        
        syncActuator(id, entry.payload.value);
    } else {
        if (entry.payload && entry.payload.measurements) {
            entry.payload.measurements.forEach(m => {
                const metricName = m.name || m.metric;
                const config = SENSORS_REGISTRY.find(s => matchSensorId(s.id, metricName));
                if (config) updateSensor(config.id, m.value);
            });
        } else {
            const val = entry.payload ? entry.payload.value : entry.value;
            const regEntry = SENSORS_REGISTRY.find(s => s.simId === id || matchSensorId(s.id, id));
            if (val !== undefined) updateSensor(regEntry ? regEntry.id : id, val);
        }
    }
}

function updateSensor(id, val) {
    const config = SENSORS_REGISTRY.find(s => s.id === id || s.simId === id);
    if (!config) return;

    if (!sensorHistory[config.id]) sensorHistory[config.id] = [];
    sensorHistory[config.id].push({ x: new Date().toLocaleTimeString(), y: val });
    if (sensorHistory[config.id].length > 50) sensorHistory[config.id].shift();

    if (activeChart && activeChartId === config.id) {
        activeChart.data.labels = sensorHistory[config.id].map(d => d.x);
        activeChart.data.datasets[0].data = sensorHistory[config.id].map(d => d.y);
        activeChart.update('none'); 
    }
    
    systemState.sensorsReceived.add(config.id);
    const displayVal = typeof val === 'number' ? val.toFixed(1) : val;

    const elPrim = document.getElementById(`val-${config.id}`);
    const elSec = document.getElementById(`val-sec-${config.id}`); 
    const elMini = document.getElementById(`mini-val-${config.id}`); 
    
    if (elPrim) elPrim.innerText = displayVal;
    if (elSec) elSec.innerText = displayVal; 
    if (elMini) elMini.innerHTML = `${displayVal} <span style="font-size:9px;color:#555">${config.unit}</span>`;
    
    const bar = document.getElementById(`bar-${config.id}`);
    if (bar && typeof val === 'number') {
        let pct = ((val - config.min) / (config.max - config.min)) * 100;
        bar.style.width = `${Math.max(0, Math.min(100, pct))}%`;
        bar.style.background = (val < config.min || val > config.max) ? '#ef4444' : '#22c55e';
    }

    const isCrit = (val > config.max || val < config.min);
    const card = document.getElementById(`card-${config.id}`);
    
    const statusEl = document.getElementById(`status-${config.id}`);
    
    if (isCrit) {
        if (card) card.classList.add('card-alert');
        
        if (statusEl) {
            statusEl.innerText = "● CRITICAL";
            statusEl.className = "status-label status-crit";
        }

        if (!systemState.criticalSensors.has(config.id)) {
            addLog(`Critical Status: ${config.label} (${displayVal} ${config.unit}) out of safe range!`, "#ef4444");
            systemState.criticalSensors.add(config.id);
        }
    } else {
        if (card) card.classList.remove('card-alert');
        
        if (statusEl) {
            statusEl.innerText = "● NORMAL";
            statusEl.className = "status-label status-ok";
        }

        if (systemState.criticalSensors.has(config.id)) {
            addLog(`Recovery Status: ${config.label} returned to nominal levels.`, "#22c55e");
            systemState.criticalSensors.delete(config.id);
        }
    }

    const banner = document.getElementById('critical-banner');
    if (banner) {
        if (config.id === 'corridor_pressure_value' && val < 90) {
            banner.style.display = 'block';
            banner.style.background = '#f97316';
            banner.innerText = "⚠️ CRITICAL ALERT: HULL BREACH DETECTED - PRESSURE UNDER SAFE THRESHOLD";
        } else if (systemState.criticalSensors.size > 0) {
            banner.style.display = 'block';
            banner.style.background = '#ef4444';
            banner.innerText = "CRITICAL OVERRIDE: ENVIRONMENTAL PARAMETERS BEYOND SAFETY LIMITS";
        } else {
            banner.style.display = 'none';
        }
    }
    updateSystemHealth();
}

function openGraph(id, event) {
    event.stopPropagation();
    const config = SENSORS_REGISTRY.find(s => s.id === id);
    if (!config) return;

    activeChartId = id;
    document.getElementById('graph-title').innerText = `ANALYTICS: ${config.label.toUpperCase()}`;
    
    document.getElementById('graph-unit-badge').innerText = `UNIT [ ${config.unit} ]`;
    
    document.getElementById('graph-modal-overlay').style.display = 'flex';

    const ctx = document.getElementById('liveChart').getContext('2d');
    
    if (activeChart) activeChart.destroy();

    activeChart = new Chart(ctx, {
        type: 'line',
        data: {
            labels: (sensorHistory[id] || []).map(d => d.x),
            datasets: [{
                label: `${config.label}`,
                data: (sensorHistory[id] || []).map(d => d.y),
                borderColor: '#3b82f6',
                backgroundColor: 'rgba(59, 130, 246, 0.1)',
                borderWidth: 2,
                pointRadius: 2,
                fill: true,
                tension: 0.3
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            scales: {
                y: { 
                    grid: { color: '#222' }, 
                    ticks: { color: '#888' }
                },
                x: { 
                    grid: { display: false }, 
                    ticks: { color: '#888', maxRotation: 45, minRotation: 45, font: { size: 9 } } 
                }
            },
            plugins: { legend: { display: false } }
        }
    });
}

function closeGraph() {
    document.getElementById('graph-modal-overlay').style.display = 'none';
    if (activeChart) activeChart.destroy();
    activeChart = null;
    activeChartId = null;
}

async function manualToggle(id, isChecked) {
    const toggle = document.getElementById(`toggle-${id}`);
    
    const currentState = systemState.actuators[id] === "ON";
    
    toggle.checked = currentState; 
    toggle.disabled = true; 
    
    if (toggle.nextElementSibling) toggle.nextElementSibling.style.opacity = "0.5";
    document.body.style.cursor = 'wait';

    const newState = isChecked ? "ON" : "OFF";
    lastManualCommandTime = Date.now();
    
    addLog(`Manual CMD: Transmitting ${newState} to ${id}...`, "#3b82f6");
    addToAuditLog("MANUAL", id, `SET TO ${newState}`, "Mission Specialist Override");

    try {
        await fetch(`${ENDPOINTS.API}/${id}`, { 
            method: 'POST', 
            headers: {'Content-Type': 'application/json'}, 
            body: JSON.stringify({ state: newState }) 
        });
    } catch (e) { 
        addLog(`Link Failure: ${id} command lost.`, "#ef4444");
        toggle.disabled = false;
        if (toggle.nextElementSibling) toggle.nextElementSibling.style.opacity = "1";
        document.body.style.cursor = 'default';
    }
}

async function forceRefresh(id, event) {
    if(event) event.stopPropagation();
    const config = SENSORS_REGISTRY.find(s => s.id === id);
    if(!config) return;

    try {
        const res = await fetch(`${ENDPOINTS.SIMULATOR}/${config.simId}`);
        if(!res.ok) throw new Error();
        
        const data = await res.json();
        let val = 0;
        
        if (data.measurements) {
            const m = data.measurements.find(m => id.includes(m.name) || id.includes(m.metric));
            val = m ? m.value : data.measurements[0].value;
        } else {
            const subKey = id.replace(config.simId + '_', '');
            val = (data[subKey] !== undefined) ? data[subKey] : (data.value || data.level || 0);
        }
        
        updateSensor(id, val);
        addLog(`Manual Fetch: ${config.label} updated to ${val.toFixed(1)} ${config.unit}`, "#f59e0b");
    } catch (e) { 
        addLog(`Error: Manual fetch for ${config.label} failed.`, "#ef4444");
    }
}

function askDeleteRule(id) {
    openModal(`Delete Rule ${id}?`, async () => {
        await fetch(`${ENDPOINTS.RULES}/${id}`, { method: 'DELETE' });
        fetchRules(); 
        showToast("RULE DELETED", "info");
        addLog(`Audit: Automation Rule ${id} removed.`, "#ef4444");
        if (editingRuleId === id) resetRuleForm();
        lastOperationTime = new Date().toLocaleString();
        updateSystemHealth();
    });
}

function checkBootSequence() {
    if (systemState.booted) return;
    
    const current = systemState.sensorsReceived.size;
    const total = SENSORS_REGISTRY.length; 
    const pct = (current / total) * 100;
    
    const bar = document.getElementById('boot-progress');
    const log = document.getElementById('boot-log');
    
    if (bar) bar.style.width = `${Math.min(pct, 100)}%`;
    if (log) log.innerHTML = `Loading Modules<span class="bouncing-dots"><span>.</span><span>.</span><span>.</span></span> (${current}/${total})`;

    if (current >= total) {
        systemState.booted = true;
        setTimeout(() => {
            const overlay = document.getElementById('boot-overlay');
            if (overlay) overlay.style.display = 'none';
            document.body.classList.remove('no-scroll');
            
            addLog("AresGuard Online. Mission Active.", "#22c55e");
        }, 800);
    }
}

function showToast(msg, type) {
    const container = document.getElementById('toast-container');
    if (!container) return;

    while (container.children.length >= 2) {
        container.firstElementChild.remove();
    }

    const t = document.createElement('div');
    t.className = `toast toast-${type}`;
    
    let color = "#22c55e"; 
    let title = "SYSTEM NOTIFICATION";
    if (type === "error") { color = "#ef4444"; title = "SYSTEM ALERT"; }
    else if (type === "info") { color = "#3b82f6"; title = "SYSTEM INFO"; }

    t.style.borderLeftColor = color;
    
    t.innerHTML = `
        <div style="color: ${color}; font-size: 9px; margin-bottom: 5px; font-weight: bold; letter-spacing: 1px;">${title}</div>
        <div style="color: #fff; font-size: 11px;">> ${msg}</div>
    `;
    
    container.appendChild(t);
    
    setTimeout(() => {
        t.classList.add('toast-fadeout');
        setTimeout(() => {
            if (t.parentNode === container) t.remove();
        }, 400); 
    }, 3000);
}

function openModal(msg, onConfirm) {
    const overlay = document.getElementById('custom-modal-overlay');
    if (!overlay) return;
    document.getElementById('modal-message').innerText = msg;
    overlay.style.display = 'flex';
    const confirmBtn = document.getElementById('btn-confirm-action');
    const newConfirmBtn = confirmBtn.cloneNode(true);
    confirmBtn.parentNode.replaceChild(newConfirmBtn, confirmBtn);
    newConfirmBtn.addEventListener('click', () => { onConfirm(); closeModal(); });
}

function closeModal() { 
    const overlay = document.getElementById('custom-modal-overlay');
    if (overlay) overlay.style.display = 'none'; 
}

function appendRawLog(entry) {
    const log = document.getElementById('raw-log-console');
    if (!log) return;

    const time = new Date().toLocaleTimeString();
    const blockId = `log-${time}`; 

    const firstChild = log.firstElementChild;
    if (firstChild && firstChild.dataset.blockId === blockId) {
        const pre = document.createElement('pre');
        pre.textContent = JSON.stringify(entry, null, 2);
        firstChild.appendChild(pre);
        return;
    }

    const row = document.createElement('div');
    row.className = "raw-log-entry";
    row.dataset.blockId = blockId; 
    
    row.innerHTML = `<span style="color:#22c55e; font-weight:bold;">> Last Normalized Event:</span> [${time}] <pre>${JSON.stringify(entry, null, 2)}</pre>`;
    
    log.prepend(row);
    
    if (log.children.length > 50) log.removeChild(log.lastChild);
}

function addLog(msg, color) {
    const log = document.getElementById('log-console'); 
    if(log) log.innerHTML = `<div><span style="color:#555">[${new Date().toLocaleTimeString()}]</span> <span style="color:${color}">${msg}</span></div>` + log.innerHTML; 
}

function syncActuator(id, rawState) {
    let newState = (String(rawState).toUpperCase() === "ON" || rawState === true) ? "ON" : "OFF";
    const toggle = document.getElementById(`toggle-${id}`);
    
    if (toggle) { 
        toggle.checked = (newState === "ON"); 
        toggle.disabled = false; 
        if (toggle.nextElementSibling) toggle.nextElementSibling.style.opacity = "1";
    }
    
    const txt = document.getElementById(`status-text-${id}`);
    if (txt) { txt.innerHTML = `STATUS: <span style="color: ${newState==="ON"?"#22c55e":"#555"}">${newState}</span>`; }
    
    if (systemState.actuators[id] !== newState && systemState.booted) {
        addLog(`System Confirmed: ${id} is now ${newState}`, "#f59e0b");
    }
    
    systemState.actuators[id] = newState;
    document.body.style.cursor = 'default'; 
}

function toggleCardView(id) { 
    const card = document.getElementById(`card-${id}`);
    if (card) card.classList.toggle('active-view'); 
}

function setRole(role) {
    const roles = ['specialist', 'engineer', 'safety'];
    roles.forEach(r => {
        const btn = document.getElementById(`btn-role-${r}`);
        const view = document.getElementById(`${r}-view`);
        if (btn) btn.classList.toggle('active', r === role);
        if (view) view.style.display = r === role ? 'block' : 'none';
    });
    
    const title = document.getElementById('main-title');
    if (role === 'specialist') {
        title.innerText = "ARESGUARD: MISSION CONTROL";
    } else if (role === 'engineer') { 
        title.innerText = "ARESGUARD: AUTOMATION ENGINE"; 
        fetchRules(); 
        resetRuleForm(); 
    } else if (role === 'safety') { 
        title.innerText = "ARESGUARD: SAFETY AUDIT"; 
        renderSafetyView(); 
    }
    
    localStorage.setItem('aresguard_active_role', role);
}

window.onload = initMissionControl;