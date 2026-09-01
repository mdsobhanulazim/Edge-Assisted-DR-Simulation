function friendlyPhaseName(phase) {
    const names = {
        normal_baseline: "workload",
        cloud_outage: "outage",
        post_recovery_verification: "recovery"
    };

    return names[phase] || phase;
}

let currentOrderSource = 'cloud';
let activeTab = 'orders';
let toastTimer;

const scenarioHelp = {
  TC01: {
    text: 'Cloud and edge are available, but orders are processed normally by the cloud.',
    cloud: 'Online', edge: 'Online', outcome: 'Orders use cloud'
  },
  TC02: {
    text: 'The cloud is unavailable and the edge remains available, a failover run orders in edge-degraded mode.',
    cloud: 'Offline', edge: 'Online', outcome: 'Orders continue on edge'
  },
  TC03: {
    text: 'The cloud is unavailable and failover is disabled, this is the cloud-only outage comparator.',
    cloud: 'Offline', edge: 'Available but not used', outcome: 'Orders fail'
  },
  TC04: {
    text: 'Both cloud and edge services are unavailable, there is no functioning service path.',
    cloud: 'Offline', edge: 'Offline', outcome: 'Orders fail'
  }
};

function escapeHtml(value) {
  return String(value ?? '').replace(/[&<>'"]/g, char => ({'&':'&amp;','<':'&lt;','>':'&gt;',"'":'&#39;','"':'&quot;'}[char]));
}

async function api(url, options = {}) {
  const response = await fetch(url, {headers: {'Content-Type': 'application/json', ...(options.headers || {})}, ...options});
  let data = {};
  try { data = await response.json(); } catch (_) { data = {message: response.statusText}; }
  if (!response.ok) throw new Error(data.message || `Request failed (${response.status})`);
  return data;
}

function notify(message) {
  const toast = document.getElementById('toast');
  toast.textContent = message;
  toast.classList.add('show');
  clearTimeout(toastTimer);
  toastTimer = setTimeout(() => toast.classList.remove('show'), 3500);
}

function formatNumber(value, suffix = '') {
  if (value === undefined || value === null || value === '' || value === 'N/A') return '—';
  return `${value}${suffix}`;
}

function updateScenarioHelp() {
  const select = document.getElementById('scenarioSelect');
  if (!select) return;
  const tc = select.value;
  const help = scenarioHelp[tc];
  const target = document.getElementById('scenarioHelp');
  if (!help || !target) return;
  target.innerHTML = `
    <strong>${escapeHtml(help.title)}</strong>
    <p>${escapeHtml(help.text)}</p>
    <div class="scenario-state-row">
      <span><b>Cloud:</b> ${escapeHtml(help.cloud)}</span>
      <span><b>Edge:</b> ${escapeHtml(help.edge)}</span>
    </div>`;
}

function updateRate(inputId = 'requestInterval', labelId = 'rateLabel') {
  const input = document.getElementById(inputId);
  const label = document.getElementById(labelId);
  if (!input || !label) return;
  const interval = Number(input.value);
  label.textContent = interval > 0 ? `${(1 / interval).toFixed(2)} orders/second` : '—';
}

async function serviceAction(name, action) {
  try {
    const data = await api(`/api/services/${name}/${action}`, {method: 'POST', body: '{}'});
    notify(data.message);
    await refreshStatus();
  } catch (error) { notify(error.message); }
}

async function runScenario() {
  const payload = {
    test_case_id: document.getElementById('scenarioSelect').value,
    repetitions: Number(document.getElementById('repetitions').value),
    total_requests: Number(document.getElementById('requestCount').value),
    request_interval: Number(document.getElementById('requestInterval').value),
    auto_manage: document.getElementById('autoManage').checked
  };
  try {
    const data = await api('/api/scenarios/run', {method: 'POST', body: JSON.stringify(payload)});
    notify(data.message);
    await refreshStatus();
  } catch (error) { notify(error.message); }
}

async function runLifecycle() {
  const payload = {
    test_case_id: 'TC07',
    repetitions: Number(document.getElementById('lifecycleRepetitions').value),
    total_requests: Number(document.getElementById('lifecycleRequestCount').value),
    request_interval: Number(document.getElementById('lifecycleInterval').value),
    auto_manage: true
  };
  try {
    const data = await api('/api/scenarios/run', {method: 'POST', body: JSON.stringify(payload)});
    notify(data.message);
    await refreshStatus();
  } catch (error) { notify(error.message); }
}

async function runAction(action) {
  try {
    const data = await api(`/api/actions/${action}`, {method: 'POST', body: '{}'});
    notify(data.message);
    await refreshStatus();
  } catch (error) { notify(error.message); }
}

async function resetExperiment() {
  const confirmed = confirm('This permanently deletes cloud.db, edge.db and the complete v4_1_results folder. Archive evidence first. Continue?');
  if (!confirmed) return;
  const payload = {restart_services: document.getElementById('restartAfterReset').checked};
  try {
    const data = await api('/api/actions/reset', {method: 'POST', body: JSON.stringify(payload)});
    notify(data.message);
    await refreshStatus();
  } catch (error) { notify(error.message); }
}

function updateServiceDisplay(id, service) {
  const element = document.getElementById(id);
  element.textContent = service.online ? 'ONLINE' : 'OFFLINE';
  element.className = service.online ? 'online' : 'offline';
}

function renderActivity(events) {
  const target = document.getElementById('activityLog');
  if (!events.length) { target.innerHTML = '<p class="empty">No dashboard activity yet.</p>'; return; }
  target.innerHTML = events.map(event => `
    <div class="log-line"><span>${escapeHtml(event.timestamp)}</span><span class="level ${escapeHtml(event.level)}">${escapeHtml(event.level)}</span><span>${escapeHtml(event.message)}</span></div>`).join('');
}

async function refreshStatus() {
  try {
    const data = await api('/api/status');
    updateServiceDisplay('cloudStatus', data.services.cloud);
    updateServiceDisplay('edgeStatus', data.services.edge);
    document.getElementById('pendingMetric').textContent = formatNumber(data.metrics.pending_edge_sync_count);
    document.getElementById('cloudCountMetric').textContent = formatNumber(data.metrics.cloud_record_count);
    document.getElementById('edgeCountMetric').textContent = formatNumber(data.metrics.edge_record_count);

    const latest = data.latest_result || {};
    document.getElementById('recoveryMetric').textContent = formatNumber(latest.recovery_completeness_percent, latest.recovery_completeness_percent && latest.recovery_completeness_percent !== 'N/A' ? '%' : '');
    document.getElementById('availabilityMetric').textContent = formatNumber(latest.availability_percent, latest.availability_percent && latest.availability_percent !== 'N/A' ? '%' : '');
    document.getElementById('failoverMetric').textContent = formatNumber(latest.observed_failover_recovery_seconds, latest.observed_failover_recovery_seconds && latest.observed_failover_recovery_seconds !== 'N/A' ? ' s' : '');
    document.getElementById('rpoMetric').textContent = formatNumber(latest.rpo_related_exposure_records, latest.rpo_related_exposure_records && latest.rpo_related_exposure_records !== 'N/A' ? ' rec' : '');
    document.getElementById('convergenceMetric').textContent = formatNumber(latest.replica_convergence_percent, latest.replica_convergence_percent && latest.replica_convergence_percent !== 'N/A' ? '%' : '');
    const configured = formatNumber(latest.configured_requests_per_second);
    const achieved = formatNumber(latest.achieved_requests_per_second);
    document.getElementById('throughputMetric').textContent = configured === '—' && achieved === '—' ? '—' : `${configured} / ${achieved}`;
    document.getElementById('edgeRssMetric').textContent = formatNumber(latest.edge_process_rss_mb, latest.edge_process_rss_mb && latest.edge_process_rss_mb !== 'N/A' ? ' MB' : '');

    const banner = document.getElementById('jobBanner');
    const job = data.job;
    banner.className = `job-banner ${job.active ? 'running' : job.success === true ? 'success' : job.success === false ? 'failed' : 'idle'}`;
    document.getElementById('jobTitle').textContent = job.active ? job.name : (job.success === false ? 'Operation failed' : job.success === true ? 'Operation completed' : 'Ready');
    document.getElementById('jobMessage').textContent = job.active
      ? job.message
      : job.success === false
        ? (job.error || 'The operation did not complete. Check the system log for details.')
        : job.success === true
          ? `${job.name} finished successfully.`
          : 'Choose a test from the control panel.';
    document.getElementById('lastUpdated').textContent = `Updated ${data.timestamp}`;
    renderActivity(data.events || []);

    document.querySelectorAll('.control-column button').forEach(button => { button.disabled = Boolean(job.active); });
    document.querySelectorAll('.card-controls button').forEach(button => { button.disabled = false; });
    if (!job.active && activeTab === 'orders') loadOrders(currentOrderSource, false);
  } catch (error) {
    document.getElementById('jobMessage').textContent = `Dashboard status error: ${error.message}`;
  }
}

function pill(value, positiveLabel = 'Yes', negativeLabel = 'No') {
  const positive = String(value) === '1' || value === true || String(value).toLowerCase() === 'true';
  return `<span class="pill ${positive ? 'success' : 'danger'}">${positive ? positiveLabel : negativeLabel}</span>`;
}

async function loadOrders(source, announce = true) {
  currentOrderSource = source;
  document.getElementById('cloudOrdersButton').classList.toggle('active', source === 'cloud');
  document.getElementById('edgeOrdersButton').classList.toggle('active', source === 'edge');
  try {
    const data = await api(`/api/orders?source=${source}&limit=100`);
    const head = document.getElementById('ordersHead');
    const body = document.getElementById('ordersBody');
    if (source === 'cloud') {
      head.innerHTML = '<tr><th>ID</th><th>Test / phase</th><th>Customer</th><th>Order details</th><th>Source</th><th>Restored</th><th>Created</th><th>Client order ID</th><th>Batch</th></tr>';
      body.innerHTML = data.orders.length ? data.orders.map(order => `<tr>
        <td>${escapeHtml(order.id)}</td><td>${escapeHtml(order.test_case_id || '—')} / ${escapeHtml(order.phase || '—')}</td>
        <td>${escapeHtml(order.customer_name)}</td><td>${escapeHtml(order.order_details)}</td><td><span class="pill info">${escapeHtml(order.source)}</span></td>
        <td>${pill(order.restored_from_edge)}</td><td>${escapeHtml(order.created_at)}</td><td class="mono">${escapeHtml(order.client_order_id)}</td><td class="mono">${escapeHtml(order.batch_id || '—')}</td>
      </tr>`).join('') : '<tr><td colspan="9" class="empty">No cloud orders found.</td></tr>';
    } else {
      head.innerHTML = '<tr><th>ID</th><th>Test / phase</th><th>Customer</th><th>Order details</th><th>Source</th><th>Pending</th><th>Created</th><th>Client order ID</th><th>Batch</th></tr>';
      body.innerHTML = data.orders.length ? data.orders.map(order => `<tr>
        <td>${escapeHtml(order.id)}</td><td>${escapeHtml(order.test_case_id || '—')} / ${escapeHtml(order.phase || '—')}</td>
        <td>${escapeHtml(order.customer_name)}</td><td>${escapeHtml(order.order_details)}</td><td><span class="pill info">${escapeHtml(order.source)}</span></td>
        <td>${pill(order.pending_cloud_sync, 'Pending', 'Synced')}</td><td>${escapeHtml(order.created_at)}</td><td class="mono">${escapeHtml(order.client_order_id)}</td><td class="mono">${escapeHtml(order.batch_id || '—')}</td>
      </tr>`).join('') : '<tr><td colspan="9" class="empty">No edge orders found.</td></tr>';
    }
    if (announce) notify(`Loaded ${data.orders.length} ${source} orders.`);
  } catch (error) { if (announce) notify(error.message); }
}

async function loadResults() {
  try {
    const data = await api('/api/results?limit=50');
    const body = document.getElementById('resultsBody');
    body.innerHTML = data.results.length ? data.results.map(row => `<tr>
      <td>${escapeHtml(row.timestamp)}</td><td>${escapeHtml(row.test_case_id)}</td><td>${escapeHtml(row.run_id)}</td>
      <td>${escapeHtml(row.successful_requests)}/${escapeHtml(row.total_requests)}</td><td>${escapeHtml(row.availability_percent)}</td>
      <td>${escapeHtml(row.observed_failover_recovery_seconds)}</td><td>${escapeHtml(row.configured_requests_per_second)} / ${escapeHtml(row.achieved_requests_per_second)}</td>
      <td>${escapeHtml(row.rpo_related_exposure_records)}</td><td>${escapeHtml(row.recovery_completeness_percent)}</td>
      <td>${escapeHtml(row.replica_convergence_percent)}</td><td>${escapeHtml(row.pending_edge_sync_count)}</td><td>${escapeHtml(row.edge_process_rss_mb)}</td>
    </tr>`).join('') : '<tr><td colspan="12" class="empty">No V4.1 experiment results found.</td></tr>';
  } catch (error) { notify(error.message); }
}

async function loadRequestEvents() {
  try {
    const data = await api('/api/request-events?limit=100');
    const body = document.getElementById('requestsBody');
    body.innerHTML = data.events.length ? data.events.map(row => `<tr>
      <td>${escapeHtml(row.event_timestamp)}</td><td>${escapeHtml(row.test_case_id)} / ${escapeHtml(row.run_id)}</td><td>${escapeHtml(friendlyPhaseName(row.phase))}</td><td>${escapeHtml(row.request_number)}</td>
      <td>${escapeHtml(row.operating_state_before)} → ${escapeHtml(row.operating_state_after)}</td><td><span class="pill info">${escapeHtml(row.final_target)}</span></td>
      <td>${pill(row.final_success, 'Success', 'Failed')}</td><td>${escapeHtml(row.cloud_failure_detection_seconds)}</td><td>${escapeHtml(row.edge_latency_seconds)}</td>
      <td>${escapeHtml(row.observed_failover_recovery_seconds)}</td><td>${escapeHtml(row.total_request_latency_seconds)}</td>
    </tr>`).join('') : '<tr><td colspan="11" class="empty">No request-level events found.</td></tr>';
  } catch (error) { notify(error.message); }
}

async function loadCharts() {
  try {
    const data = await api('/api/charts');
    const grid = document.getElementById('chartsGrid');
    grid.innerHTML = data.charts.length ? data.charts.map(name => `<figure class="chart-card"><img loading="lazy" src="/charts/${encodeURIComponent(name)}?v=${Date.now()}" alt="${escapeHtml(name)}"><figcaption>${escapeHtml(name)}</figcaption></figure>`).join('') : '<p class="empty">No charts found. Use “Generate results and charts” under Tools & Evidence.</p>';
  } catch (error) { notify(error.message); }
}

function showTab(name, button) {
  activeTab = name;
  document.querySelectorAll('.tab').forEach(tab => tab.classList.remove('active'));
  document.querySelectorAll('.tab-content').forEach(tab => tab.classList.remove('active'));
  button.classList.add('active');
  document.getElementById(`${name}Tab`).classList.add('active');
  if (name === 'orders') loadOrders(currentOrderSource, false);
  if (name === 'results') loadResults();
  if (name === 'requests') loadRequestEvents();
  if (name === 'charts') loadCharts();
  if (name === 'logs') refreshStatus();
}

updateScenarioHelp();
updateRate();
updateRate('lifecycleInterval', 'lifecycleRateLabel');
refreshStatus();
loadOrders('cloud', false);
setInterval(refreshStatus, 2000);
