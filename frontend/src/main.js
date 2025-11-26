import './style.css'

// Create animated background particles
function createParticles() {
  const container = document.querySelector('.background-animation');
  for (let i = 0; i < 30; i++) {
    const particle = document.createElement('div');
    particle.className = 'particle';
    particle.style.left = Math.random() * 100 + '%';
    particle.style.top = Math.random() * 100 + '%';
    particle.style.animationDelay = Math.random() * 15 + 's';
    particle.style.animationDuration = (10 + Math.random() * 10) + 's';
    container.appendChild(particle);
  }
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
        <button id="clear-btn" onclick="document.getElementById('input-data').value = '';">Clear</button>
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

createParticles();

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

// Handle job submission
document.getElementById('submit-job').addEventListener('click', async () => {
  const submitBtn = document.getElementById('submit-job');
  const submitText = document.getElementById('submit-text');
  const resultsDiv = document.getElementById('results');

  const jobName = document.getElementById('job-name').value;
  const inputData = document.getElementById('input-data').value;
  const mapFunc = document.getElementById('map-function').value;
  const reduceFunc = document.getElementById('reduce-function').value;

  if (!inputData) {
    resultsDiv.innerHTML = '<span style="color: #ff3333;">❌ Please fill in all fields</span>';
    return;
  }

  submitBtn.disabled = true;
  submitText.innerHTML = '<span class="loading"></span> Processing...';

  // Simulate job execution
  activeJobs++;
  partitions = Math.floor(Math.random() * 8) + 4;
  throughput = Math.random() * 50 + 10;
  updateStats();

  resultsDiv.innerHTML = `<span style="color: #39ff14;">⚡ Job "${jobName}" submitted...</span>
<span style="color: #a0a0a0;">Analyzing data distribution...</span>
<span style="color: #a0a0a0;">Creating ${partitions} intelligent partitions...</span>
<span style="color: #a0a0a0;">Running map phase...</span>`;

  // Simulate processing delay
  await new Promise(resolve => setTimeout(resolve, 2000));

  resultsDiv.innerHTML += `
<span style="color: #a0a0a0;">Running reduce phase...</span>
<span style="color: #39ff14;">✓ Job completed successfully!</span>

<span style="color: #7fff00;">━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━</span>
<span style="color: #e0e0e0;">Job: ${jobName}</span>
<span style="color: #e0e0e0;">Partitions Used: ${partitions}</span>
<span style="color: #e0e0e0;">Processing Time: ${(Math.random() * 5 + 1).toFixed(2)}s</span>
<span style="color: #e0e0e0;">Data Processed: ${(Math.random() * 100 + 50).toFixed(1)} MB</span>
<span style="color: #7fff00;">━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━</span>

<span style="color: #39ff14;">Sample Results:</span>
<span style="color: #a0a0a0;">{</span>
<span style="color: #a0a0a0;">  "key_1": 42,</span>
<span style="color: #a0a0a0;">  "key_2": 37,</span>
<span style="color: #a0a0a0;">  "key_3": 58</span>
<span style="color: #a0a0a0;">}</span>`;

  activeJobs--;
  totalProcessed += Math.floor(Math.random() * 1000 + 500);
  updateStats();

  submitBtn.disabled = false;
  submitText.textContent = 'Submit Job';
});

// Handle clear button
document.getElementById('clear-btn').addEventListener('click', () => {
  document.getElementById('job-name').value = '';
  document.getElementById('input-data').value = '';
  document.getElementById('map-function').value = '';
  document.getElementById('reduce-function').value = '';
  document.getElementById('results').innerHTML = '<span style="color: #666;">Job results will appear here...</span>';
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
