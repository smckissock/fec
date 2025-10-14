// app.js - Testing Huey URL (open in new tab)

const HUEY_PATH = 'huey/index.html';

// Get the local Huey URL
function getHueyBaseUrl() {
    const baseUrl = window.location.origin + window.location.pathname.replace('index.html', '').replace(/\/$/, '');
    return baseUrl + '/' + HUEY_PATH;
}

// Initialize on page load
window.addEventListener('DOMContentLoaded', () => {
    const hueyBaseUrl = getHueyBaseUrl();
    
    // Use the exact hash from the LA Airport example
    const laAirportHash = 'JTdCJTIycXVlcnlNb2RlbCUyMiUzQSU3QiUyMmRhdGFzb3VyY2VJZCUyMiUzQSUyMmZpbGUlM0ElNUMlMjJodHRwcyUzQSUyRiUyRmRhdGEubGFjaXR5Lm9yZyUyRmFwaSUyRnZpZXdzJTJGYWppdi11YzYzJTJGcm93cy5jc3YlM0ZhY2Nlc3NUeXBlJTNERE9XTkxPQUQlNUMlMjIlMjIlMkMlMjJjZWxsc0hlYWRlcnMlMjIlM0ElMjJjb2x1bW5zJTIyJTJDJTIyYXhlcyUyMiUzQSU3QiUyMmNlbGxzJTIyJTNBJTVCJTdCJTIyY29sdW1uTmFtZSUyMiUzQSUyMkZsaWdodE9wc0NvdW50JTIyJTJDJTIyY29sdW1uVHlwZSUyMiUzQSUyMkJJR0lOVCUyMiUyQyUyMmFnZ3JlZ2F0b3IlMjIlM0ElMjJzdW0lMjIlN0QlNUQlMkMlMjJjb2x1bW5zJTIyJTNBJTVCJTdCJTIyY29sdW1uTmFtZSUyMiUzQSUyMkZsaWdodFR5cGUlMjIlMkMlMjJjb2x1bW5UeXBlJTIyJTNBJTIyVkFSQ0hBUiUyMiUyQyUyMmluY2x1ZGVUb3RhbHMlMjIlM0F0cnVlJTdEJTJDJTdCJTIyY29sdW1uTmFtZSUyMiUzQSUyMkFycml2YWxfRGVwYXJ0dXJlJTIyJTJDJTIyY29sdW1uVHlwZSUyMiUzQSUyMlZBUkNIQVIlMjIlN0QlNUQ';
    
    const hueyUrl = `${hueyBaseUrl}#${laAirportHash}`;
    
    console.log('=== Testing Huey URL ===');
    console.log('Full URL:', hueyUrl);
    console.log('\nClick the "Open Huey" button to test in a new tab');
    console.log('If it works there, it\'s an iframe issue');
    console.log('If it doesn\'t work there either, it\'s a URL hash loading issue');
    
    // Create a clickable link to open in new tab
    const openButton = document.getElementById('openHueyBtn');
    if (openButton) {
        openButton.onclick = () => {
            window.open(hueyUrl, '_blank');
        };
    }
    
    // Also try loading in iframe
    const hueyFrame = document.getElementById('hueyFrame');
    if (hueyFrame) {
        hueyFrame.src = hueyUrl;
        
        hueyFrame.addEventListener('load', () => {
            console.log('✓ Iframe loaded');
        });
    }
});