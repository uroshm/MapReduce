import './style.css'

var isRunning = false;

document.querySelector('#app').innerHTML = `
  <div class="background-animation"></div>
  
  <div class="header">
    <div class="logo-section">
      <span class="logo"><img src="/logo.png" alt="Elephlow Logo" /></span>
      <h2>Elephlow</h2>
    </div>
    <p class="tagline">MapReduce with Smart Partitioning to address Hot/Cold HashTags</p>
  </div>
  <div class="container">
    <div class="action-panel">
      <h2>Submit MapReduce Job</h2>
      
      <div class="input-group">
        <label for="input-data">Input Data</label>
        <textarea id="input-data" rows="6"></textarea>
        <label for="input-data">Number of Mappers</label>
        <textarea id="input-mappers" rows="1" placeholder='Number of Mappers'></textarea>        

        <label for="input-data">Number of Reducers</label>
        <textarea id="input-reducers" rows="1" placeholder='Number of Reducers'></textarea>

        <label for="partition-strategy">Partition Strategy</label>
        <select id="partition-strategy">
          <option value="NAIVE">NAIVE</option>
          <option value="SMART">SMART</option>
        </select>
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
document.getElementById('submit-job').addEventListener('click', submitMapReduceJob);
document.getElementById('clear-btn').addEventListener('click', () => {
  isRunning = false;
  document.getElementById('input-data').value = '';
  document.getElementById('results').innerHTML = '<span style="color: #666;">Job results</span>';
  const response = fetch('http://localhost:8080/reset', {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
    },
  });
});

setInterval(async () => {
  if (!isRunning) {
    return;
  }
  try {
    await updateResults();
  } catch (error) {
    console.error('Error updating results:', error);
  }
}, 500);

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
      throw new Error(`Error: ${response.status}`);
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

  const partitionStrategy = document.getElementById('partition-strategy').value;
  const numberOfMappers = document.getElementById('input-mappers').value || 2;
  const numberOfReducers = document.getElementById('input-reducers').value || 2;

  const response = await fetch('http://localhost:8080/process'
    + `?numberOfMappers=${numberOfMappers}&numberOfReducers=${numberOfReducers}&partitionStrategy=${partitionStrategy}`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
    },
    body: JSON.stringify(JSON.parse(document.getElementById('input-data').value)),
  });

  const data = await response.json();
}