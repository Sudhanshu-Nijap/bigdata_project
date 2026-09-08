let pieChart = null;
let barChart = null;
let eventSource = null;
let allTweetsData = [];
let activeFilter = 'all';
let tweetCounter = 0;
let liveCounts = { 'Positive': 0, 'Negative': 0, 'Neutral': 0, 'Irrelevant': 0 };

// Clean High-Contrast Palette
const SENTIMENT_COLORS = {
    'Positive': '#16a34a',
    'Negative': '#dc2626',
    'Neutral': '#2563eb',
    'Irrelevant': '#9333ea'
};

document.addEventListener('DOMContentLoaded', () => {
    initCharts();
    setupFilterTabs();
    setupSearchFilter();
    refreshVerifiedCount();

    if (window.INITIAL_DATA) {
        tweetCounter = window.INITIAL_DATA.total || 0;
        liveCounts = window.INITIAL_DATA.counts || liveCounts;
        allTweetsData = window.INITIAL_DATA.tweets || [];
        updateMetricsUI();
        if (allTweetsData.length > 0) {
            renderFilteredTable();
        }
    }
});

function setSearchQuery(text) {
    const q = text.startsWith('#') ? text.substring(1) : text;
    document.getElementById('dashboardQuery').value = q;
}

function initCharts() {
    const labels = ['Positive', 'Negative', 'Neutral', 'Irrelevant'];
    const colors = labels.map(l => SENTIMENT_COLORS[l]);

    const pieCtx = document.getElementById('sentimentPieChart');
    if (pieCtx) {
        pieChart = new Chart(pieCtx, {
            type: 'doughnut',
            data: {
                labels: labels,
                datasets: [{
                    data: [0, 0, 0, 0],
                    backgroundColor: colors,
                    borderWidth: 1,
                    borderColor: '#ffffff'
                }]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                plugins: { 
                    legend: { 
                        position: 'bottom',
                        labels: {
                            font: { family: '-apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif', size: 12 },
                            color: '#475569',
                            boxWidth: 12,
                            padding: 14
                        }
                    } 
                },
                cutout: '60%'
            }
        });
    }

    const barCtx = document.getElementById('sentimentBarChart');
    if (barCtx) {
        barChart = new Chart(barCtx, {
            type: 'bar',
            data: {
                labels: labels,
                datasets: [{
                    data: [0, 0, 0, 0],
                    backgroundColor: colors,
                    borderRadius: 4
                }]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                plugins: { legend: { display: false } },
                scales: { 
                    y: { 
                        beginAtZero: true,
                        ticks: { font: { family: '-apple-system, sans-serif' }, color: '#64748b' },
                        grid: { color: '#f1f5f9' }
                    },
                    x: {
                        ticks: { font: { family: '-apple-system, sans-serif', weight: '600' }, color: '#475569' },
                        grid: { display: false }
                    }
                }
            }
        });
    }
}

function updateCharts() {
    const labels = ['Positive', 'Negative', 'Neutral', 'Irrelevant'];
    const dataVals = labels.map(l => liveCounts[l] || 0);

    if (pieChart) {
        pieChart.data.datasets[0].data = dataVals;
        pieChart.update();
    }
    if (barChart) {
        barChart.data.datasets[0].data = dataVals;
        barChart.update();
    }
}

function updateMetricsUI() {
    setText('stat-total', tweetCounter.toLocaleString());

    const total = tweetCounter > 0 ? tweetCounter : 1;
    const posRate = Math.round((liveCounts.Positive / total) * 100);
    const negRate = Math.round((liveCounts.Negative / total) * 100);
    const neuRate = Math.round((liveCounts.Neutral / total) * 100);
    const irrRate = Math.round((liveCounts.Irrelevant / total) * 100);

    setText('stat-pos-rate', `${posRate}%`);
    setText('stat-pos-count', `${liveCounts.Positive} tweets`);

    setText('stat-neg-rate', `${negRate}%`);
    setText('stat-neg-count', `${liveCounts.Negative} tweets`);

    setText('stat-neu-rate', `${neuRate}%`);
    setText('stat-neu-count', `${liveCounts.Neutral} tweets`);

    setText('stat-irr-rate', `${irrRate}%`);
    setText('stat-irr-count', `${liveCounts.Irrelevant} tweets`);

    updateCharts();
}

function handleStartStream(e) {
    if (e) e.preventDefault();
    const query = document.getElementById('dashboardQuery').value.trim();
    if (!query) return;

    handleStopStream();

    tweetCounter = 0;
    allTweetsData = [];
    liveCounts = { 'Positive': 0, 'Negative': 0, 'Neutral': 0, 'Irrelevant': 0 };

    setText('stat-query-label', `Topic: #${query.replace(/^#/, '')}`);
    document.getElementById('tweetsTableBody').innerHTML = '';
    updateMetricsUI();

    const startBtn = document.getElementById('startBtn');
    const stopBtn = document.getElementById('stopBtn');
    const spinner = document.getElementById('streamSpinner');
    const badge = document.getElementById('streamStatusBadge');
    const subtitle = document.getElementById('feedSubtitle');

    startBtn.classList.add('d-none');
    stopBtn.classList.remove('d-none');
    spinner.classList.remove('d-none');
    badge.innerHTML = `<span class="badge bg-danger">Streaming #${escapeHtml(query.replace(/^#/, ''))}</span>`;
    subtitle.textContent = `Streaming live tweets for #${query}...`;

    eventSource = new EventSource(`/api/stream-tweets?query=${encodeURIComponent(query)}`);

    eventSource.onmessage = function(event) {
        const data = JSON.parse(event.data);
        tweetCounter++;

        const pred = data.prediction || 'Neutral';
        if (liveCounts[pred] !== undefined) {
            liveCounts[pred]++;
        } else {
            liveCounts['Neutral']++;
        }

        allTweetsData.unshift(data);
        updateMetricsUI();
        subtitle.textContent = `Live Stream Active: ${tweetCounter} tweets processed`;
        renderFilteredTable();
    };

    eventSource.onerror = function() {
        badge.innerHTML = `<span class="badge bg-warning text-dark">Connecting...</span>`;
    };
}

function handleStopStream() {
    if (eventSource) {
        eventSource.close();
        eventSource = null;
    }
    const startBtn = document.getElementById('startBtn');
    const stopBtn = document.getElementById('stopBtn');
    const spinner = document.getElementById('streamSpinner');
    const badge = document.getElementById('streamStatusBadge');
    const subtitle = document.getElementById('feedSubtitle');

    if (startBtn) startBtn.classList.remove('d-none');
    if (stopBtn) stopBtn.classList.add('d-none');
    if (spinner) spinner.classList.add('d-none');
    if (badge) badge.innerHTML = `<span class="badge bg-secondary">Stream Paused</span>`;
    if (subtitle && tweetCounter > 0) subtitle.textContent = `Stream paused. Total ${tweetCounter} tweets analyzed.`;
}

async function handleSingleBatch() {
    const query = document.getElementById('dashboardQuery').value.trim();
    if (!query) return;

    handleStopStream();
    const badge = document.getElementById('streamStatusBadge');
    badge.innerHTML = `<span class="badge bg-info text-dark">Fetching batch...</span>`;

    try {
        const response = await fetch('/api/scrape-analyze', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ query: query, count: 15, save_to_db: true })
        });
        const data = await response.json();
        if (response.ok) {
            tweetCounter = data.total || 0;
            liveCounts = data.sentiment_counts || liveCounts;
            allTweetsData = data.results || [];
            setText('stat-query-label', `Topic: #${data.query.replace(/^#/, '')}`);
            updateMetricsUI();
            renderFilteredTable();
            badge.innerHTML = `<span class="badge bg-success">Batch Complete</span>`;
            document.getElementById('feedSubtitle').textContent = `Batch complete: ${tweetCounter} tweets analyzed for #${query}`;
        }
    } catch (err) {
        console.error('Single batch error:', err);
    }
}

async function handleClearDB() {
    handleStopStream();
    tweetCounter = 0;
    allTweetsData = [];
    liveCounts = { 'Positive': 0, 'Negative': 0, 'Neutral': 0, 'Irrelevant': 0 };
    setText('stat-query-label', 'Topic: None');
    updateMetricsUI();

    document.getElementById('tweetsTableBody').innerHTML = `
        <tr id="emptyRow">
            <td colspan="5" class="text-center py-4 text-muted">
                Data cleared. Enter a topic above and click <strong>Start Live Stream</strong>.
            </td>
        </tr>
    `;
    document.getElementById('streamStatusBadge').innerHTML = `<span class="badge bg-secondary">Ready</span>`;
    document.getElementById('feedSubtitle').textContent = 'Enter a topic above to begin real-time analysis';

    try {
        await fetch('/api/clear-db', { method: 'POST' });
        refreshVerifiedCount();
    } catch (e) {}
}

async function handleVerifySentiment(idx, newSentiment) {
    if (idx < 0 || idx >= allTweetsData.length) return;
    const item = allTweetsData[idx];
    const oldSentiment = item.prediction;

    // Update locally
    if (liveCounts[oldSentiment] > 0) liveCounts[oldSentiment]--;
    liveCounts[newSentiment] = (liveCounts[newSentiment] || 0) + 1;

    item.prediction = newSentiment;
    item.is_verified = true;
    item.verified_sentiment = newSentiment;

    updateMetricsUI();
    renderFilteredTable();

    try {
        const res = await fetch('/api/verify-sentiment', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                tweet: item.tweet,
                original_prediction: oldSentiment,
                verified_sentiment: newSentiment
            })
        });
        const data = await res.json();
        if (data.total_verified_records !== undefined) {
            setText('verifiedCountBadge', data.total_verified_records);
        }
    } catch (err) {
        console.error('Error verifying sentiment:', err);
    }
}

async function refreshVerifiedCount() {
    try {
        const res = await fetch('/api/verified-stats');
        const data = await res.json();
        if (data.verified_samples_count !== undefined) {
            setText('verifiedCountBadge', data.verified_samples_count);
        }
    } catch (e) {}
}

async function handleTriggerRetraining() {
    const btn = document.getElementById('retrainBtn');
    const spinner = document.getElementById('retrainSpinner');
    const alertBox = document.getElementById('retrainAlert');

    btn.disabled = true;
    spinner.classList.remove('d-none');
    alertBox.className = "mt-2 small p-2 border rounded alert alert-info";
    alertBox.textContent = "Running PySpark MLlib Retraining Pipeline on baseline CSV + verified feedback...";
    alertBox.classList.remove('d-none');

    try {
        const res = await fetch('/api/retrain-model', { method: 'POST' });
        const result = await res.json();

        if (result.status === 'success') {
            alertBox.className = "mt-2 small p-2 border rounded alert alert-success";
            alertBox.innerHTML = `
                <strong>Retraining Successful:</strong> Validation Accuracy: <strong>${result.accuracy}%</strong> | 
                F1-Score: <strong>${result.f1_score}</strong> | Feedback Samples: <strong>${result.verified_samples_used}</strong>
            `;
        } else {
            alertBox.className = "mt-2 small p-2 border rounded alert alert-warning";
            alertBox.textContent = `Retraining Notice: ${result.message || 'Complete'}`;
        }
    } catch (err) {
        alertBox.className = "mt-2 small p-2 border rounded alert alert-danger";
        alertBox.textContent = `Error executing PySpark retraining: ${err}`;
    } finally {
        btn.disabled = false;
        spinner.classList.add('d-none');
        refreshVerifiedCount();
    }
}

function getBadgeClass(sentiment) {
    const s = (sentiment || '').toLowerCase();
    if (s.startsWith('pos')) return 'badge-pos';
    if (s.startsWith('neg')) return 'badge-neg';
    if (s.startsWith('neu')) return 'badge-neu';
    return 'badge-irr';
}

function renderFilteredTable() {
    const tbody = document.getElementById('tweetsTableBody');
    if (!tbody) return;

    const searchTerm = (document.getElementById('tweetSearchInput')?.value || '').toLowerCase().trim();

    const filtered = allTweetsData.filter(item => {
        const matchesFilter = (activeFilter === 'all') || (item.prediction.toLowerCase() === activeFilter);
        const matchesSearch = !searchTerm || (item.tweet.toLowerCase().includes(searchTerm));
        return matchesFilter && matchesSearch;
    });

    if (filtered.length === 0) {
        tbody.innerHTML = `
            <tr id="emptyRow">
                <td colspan="5" class="text-center py-4 text-muted">No matching tweets found.</td>
            </tr>
        `;
        return;
    }

    tbody.innerHTML = filtered.map((item, idx) => {
        const isVerified = item.is_verified;
        const pred = item.prediction;
        const badgeClass = getBadgeClass(pred);
        const originalIndex = allTweetsData.indexOf(item);

        const cleanUser = (item.user || 'twitter_user').replace(/^@+/, '');
        return `
            <tr>
                <td class="text-muted fw-bold">${idx + 1}</td>
                <td>
                    <div class="fw-semibold">@${escapeHtml(cleanUser)}</div>
                    <small class="text-muted" style="font-size: 0.75rem;">${escapeHtml(item.date || 'Live')}</small>
                </td>
                <td>
                    ${escapeHtml(item.tweet)}
                    ${isVerified ? '<br><span class="badge bg-success-subtle text-success border border-success-subtle" style="font-size: 0.68rem;">Verified Label</span>' : ''}
                </td>
                <td style="text-align: center;">
                    <span class="${badgeClass}">
                        ${escapeHtml(pred)}
                    </span>
                </td>
                <td style="text-align: center;">
                    <div class="btn-group btn-group-sm" role="group">
                        <button type="button" class="btn btn-outline-success py-0 px-1" style="font-size: 0.72rem;" title="Mark Positive" onclick="handleVerifySentiment(${originalIndex}, 'Positive')">+Pos</button>
                        <button type="button" class="btn btn-outline-danger py-0 px-1" style="font-size: 0.72rem;" title="Mark Negative" onclick="handleVerifySentiment(${originalIndex}, 'Negative')">-Neg</button>
                        <button type="button" class="btn btn-outline-primary py-0 px-1" style="font-size: 0.72rem;" title="Mark Neutral" onclick="handleVerifySentiment(${originalIndex}, 'Neutral')">Neu</button>
                        <button type="button" class="btn btn-outline-secondary py-0 px-1" style="font-size: 0.72rem;" title="Mark Irrelevant" onclick="handleVerifySentiment(${originalIndex}, 'Irrelevant')">Irr</button>
                    </div>
                </td>
            </tr>
        `;
    }).join('');
}

function setupFilterTabs() {
    document.querySelectorAll('.filter-btn').forEach(btn => {
        btn.addEventListener('click', () => {
            document.querySelectorAll('.filter-btn').forEach(b => b.classList.remove('active'));
            btn.classList.add('active');
            activeFilter = btn.getAttribute('data-filter') || 'all';
            renderFilteredTable();
        });
    });
}

function setupSearchFilter() {
    const searchInput = document.getElementById('tweetSearchInput');
    if (searchInput) {
        searchInput.addEventListener('input', () => {
            renderFilteredTable();
        });
    }
}

function setText(id, text) {
    const el = document.getElementById(id);
    if (el) el.textContent = text;
}

function escapeHtml(str) {
    if (!str) return '';
    return String(str)
        .replace(/&/g, "&amp;")
        .replace(/</g, "&lt;")
        .replace(/>/g, "&gt;")
        .replace(/"/g, "&quot;")
        .replace(/'/g, "&#039;");
}
