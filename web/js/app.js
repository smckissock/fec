// app.js - Huey Integration with PostMessage API

const PARQUET_FILE_PATH = 'data/committee_contributions.parquet';

// Get the full URL to the parquet file
function getParquetUrl() {
    const baseUrl = window.location.origin + window.location.pathname.replace('index.html', '');
    return baseUrl + PARQUET_FILE_PATH;
}

// Wait for iframe to load and send load command
window.addEventListener('DOMContentLoaded', () => {
    const hueyFrame = document.getElementById('hueyFrame');
    const parquetUrl = getParquetUrl();
    
    // Display the URL in the instructions
    const urlDisplay = document.getElementById('parquetUrl');
    if (urlDisplay) {
        urlDisplay.textContent = parquetUrl;
    }
    
    console.log('Parquet URL:', parquetUrl);
    
    // Set iframe source
    hueyFrame.src = 'https://rpbouman.github.io/huey/src/index.html';
    
    hueyFrame.addEventListener('load', () => {
        console.log('Huey iframe loaded');
        
        // Wait a bit for Huey to fully initialize
        setTimeout(() => {
            // Send command to load the parquet file via postMessage
            const message = {
                action: 'loadDataFromUrl',
                url: parquetUrl
            };
            
            console.log('Sending message to Huey:', message);
            hueyFrame.contentWindow.postMessage(message, 'https://rpbouman.github.io');
        }, 2000); // Wait 2 seconds for Huey to initialize
    });
});

// Listen for messages from Huey
window.addEventListener('message', (event) => {
    if (event.origin === 'https://rpbouman.github.io') {
        console.log('Message from Huey:', event.data);
    }
});