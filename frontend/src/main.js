import './style.css'

var isRunning = false;


async function updateResults() {
  try {
    const resultsElement = document.getElementById('results');
    if (!resultsElement) {
      console.error('Results element not found');
      return;
    }

    const response = await fetch('http://localhost:8080/getResults', {
      method: 'GET',
      headers: {
        'Content-Type': 'application/json',
      },
    });

    if (!response.ok) {
      throw new Error(`HTTP error! status: ${response.status}`);
    }

    const data = await response.text();
    const resultsText = data.toString();
    resultsElement.innerHTML = `<pre style="margin: 0; color: #ca85eaff;">${resultsText}</pre>`;
  } catch (error) {
    console.error('Error in updateResults:', error);
  }
}

async function submitMapReduceJob() {
  if (isRunning) {
    return;
  }
  isRunning = true;

  const response = await fetch('http://localhost:8080/process'
    + '?numberOfMappers=2&numberOfReducers=2&partitionStrategy=NAIVE', {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
    },
    body: JSON.stringify(
      {
        "#Basketball": 200,
        "#Soccer": 8000,
        "#Football": 100,
        "#Racquetball": 200,
        "#Pickleball": 300,
        "#Boxing": 100,
      }),
  });

  const data = await response.json();
  console.log(data);
}
// Initialize the app
document.querySelector('#app').innerHTML = `
  <div class="background-animation"></div>
  
  <div class="header">
    <div class="logo-section">
      <span class="logo"><img src="/logo.png" alt="Elephlow Logo" /></span>
      <h1>Elephlow</h1>
    </div>
    <p class="tagline">MapReduce with Smart Partitioning to address Hot/Cold HashTags</p>
  </div>
  <div class="container">
    <div class="stats-grid">
      <div class="stat-card">
        <div class="stat-label">Active Jobs</div>
        <div class="stat-value" id="active-jobs">0</div>
      </div>
      <div class="stat-card">
        <div class="stat-label">Partitions</div>
        <div class="stat-value" id="partitions">0</div>
      </div>
      <div class="stat-card">
        <div class="stat-label">Throughput</div>
        <div class="stat-value" id="throughput">0 MB/s</div>
      </div>
      <div class="stat-card">
        <div class="stat-label">Total Processed</div>
        <div class="stat-value" id="total-processed">0</div>
      </div>
    </div>

    <div class="action-panel">
      <h2>Submit MapReduce Job</h2>
      
      <div class="input-group">
        <label for="input-data">Input Data</label>
        <textarea id="input-data" rows="6" placeholder='Enter your data here or paste JSON...
Example:
{"user": "alice", "action": "login"}
{"user": "bob", "action": "purchase"}
{"user": "alice", "action": "logout"}'></textarea>
      </div>

      <div class="button-group">
        <button class="btn-primary" id="submit-job">
          <span id="submit-text">Run MapReduce</span>
        </button>
        <button id="clear-btn">Clear</button>
      </div>
    </div>

    <div class="results-section">
      <h3>Results</h3>
      <div class="results-content" id="results">
        <span style="color: #666;">Job results will appear here...</span>
      </div>
    </div>
  </div>
`

// createParticles();

// Attach event listeners
document.getElementById('submit-job').addEventListener('click', submitMapReduceJob);

// Simulate stats update
let activeJobs = 0;
let partitions = 0;
let throughput = 0;
let totalProcessed = 0;

function updateStats() {
  document.getElementById('active-jobs').textContent = activeJobs;
  document.getElementById('partitions').textContent = partitions;
  document.getElementById('throughput').textContent = throughput.toFixed(1) + ' MB/s';
  document.getElementById('total-processed').textContent = totalProcessed.toLocaleString();
}

// Handle clear button
document.getElementById('clear-btn').addEventListener('click', () => {
  document.getElementById('input-data').value = '';
});

// Initialize stats
updateStats();

// Simulate background activity
setInterval(() => {
  if (activeJobs === 0) {
    throughput = Math.max(0, throughput - Math.random() * 2);
  }
  updateStats();
}, 2000);

setInterval(async () => {
  try {
    await updateResults();
  } catch (error) {
    console.error('Error updating results:', error);
  }
}, 500);
