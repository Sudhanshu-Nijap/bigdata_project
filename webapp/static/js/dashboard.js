let pieChart = null;
let barChart = null;
let eventSource = null;
let allTweetsData = [];
let activeFilter = 'all';
let tweetCounter = 0;
let liveCounts = { 'Positive': 0, 'Negative': 0, 'Neutral': 0, 'Irrelevant': 0 };

const SENTIMENT_COLORS = {
    'Positive': '#16a34a',
    'Negative': '#dc2626',
    'Neutral': '#2563eb',
    'Irrelevant': '#7c3aed'
};

document.addEventListener('DOMContentLoaded', () => {
    initCharts();
    setupFilterTabs();
    setupSearchFilter();
});

function setSearchQuery(text) {
    document.getElementById('dashboardQuery').value = text;
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
                    borderWidth: 1
                }]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                plugins: { legend: { position: 'bottom' } },
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
                    backgroundColor: colors
                }]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                plugins: { legend: { display: false } },
                scales: { y: { beginAtZero: true } }
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

    setText('stat-query-label', `Active Query: ${query}`);
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
    badge.innerHTML = `<span class="badge bg-danger">🔴 Streaming ${escapeHtml(query)}</span>`;
    subtitle.textContent = `Streaming live tweets for ${query} & classifying in real-time...`;

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
        subtitle.textContent = `Live Stream Active: ${tweetCounter} tweets classified`;
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
    if (subtitle && tweetCounter > 0) subtitle.textContent = `Stream paused. Total ${tweetCounter} tweets processed.`;
}

async function handleSingleBatch() {
    const query = document.getElementById('dashboardQuery').value.trim();
    if (!query) return;

    handleStopStream();
    const badge = document.getElementById('streamStatusBadge');
    badge.innerHTML = `<span class="badge bg-info text-dark">Scraping batch...</span>`;

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
            setText('stat-query-label', `Active Query: ${data.query}`);
            updateMetricsUI();
            renderFilteredTable();
            badge.innerHTML = `<span class="badge bg-success">Batch Completed</span>`;
            document.getElementById('feedSubtitle').textContent = `Batch complete: ${tweetCounter} tweets analyzed for ${query}`;
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
    setText('stat-query-label', 'Active Query: None');
    updateMetricsUI();

    document.getElementById('tweetsTableBody').innerHTML = `
        <tr id="emptyRow">
            <td colspan="4" class="text-center text-muted py-4">
                Data cleared. Search a keyword or hashtag above and click <strong>Start Live Stream</strong>.
            </td>
        </tr>
    `;
    document.getElementById('streamStatusBadge').innerHTML = `<span class="badge bg-secondary">Waiting for Search</span>`;
    document.getElementById('feedSubtitle').textContent = 'Search a topic above to begin real-time analysis';

    try {
        await fetch('/api/clear-db', { method: 'POST' });
    } catch (e) {}
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
                <td colspan="4" class="text-center text-muted py-4">No matching tweets found in stream.</td>
            </tr>
        `;
        return;
    }

    tbody.innerHTML = filtered.map((item, idx) => `
        <tr data-sentiment="${escapeHtml(item.prediction.toLowerCase())}">
            <td>${idx + 1}</td>
            <td><strong>@${escapeHtml(item.user || 'twitter_user')}</strong><br><small class="text-muted">${escapeHtml(item.date || 'Live')}</small></td>
            <td>${escapeHtml(item.tweet)}</td>
            <td style="text-align: center;">
                <span class="badge badge-${escapeHtml(item.prediction.toLowerCase())} px-2 py-1">
                    ${escapeHtml(item.prediction)}
                </span>
            </td>
        </tr>
    `).join('');
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
