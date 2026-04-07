// utils.js
// Utility functions for the scheduler

let _fetch;
async function getFetch() {
  if (!_fetch) {
    _fetch = (await import('node-fetch')).default;
  }
  return _fetch;
}

// Function to trigger workflow via webhook
async function triggerWorkflow(workflowId, scheduleId, contentData = null) {
  const fetch = await getFetch();
  try {
    console.log(`Triggering workflow ${workflowId} for schedule ${scheduleId}`);
    const WEBHOOK_URL = process.env.WEBHOOK_URL || 'https://ef0ps4gk.rcsrv.net/webhook/';
const webhookUrl = `${WEBHOOK_URL.replace(/\/+$/, '')}/${workflowId}`;
console.log(`Webhook URL: ${webhookUrl}`);
    console.log(`Content data received:`, JSON.stringify(contentData, null, 2));

    // Prepare payload with content data if available
    const payload = {
      triggeredBy: 'scheduled-trigger',
      scheduleId: scheduleId,
      timestamp: new Date().toISOString()
    };

    // Add content data if provided
    if (contentData) {
      payload.content = {
        id: contentData.id,
        title: contentData.title,
        description: contentData.description,
        resourceUrls: contentData.resource_urls || []
      };
      console.log(`Payload with content:`, JSON.stringify(payload, null, 2));
    } else {
      console.log(`No content data provided, sending basic payload`);
    }

    const response = await fetch(webhookUrl, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify(payload)
    });

    console.log(`Webhook response status: ${response.status}`);
    console.log(`Webhook response ok: ${response.ok}`);

    const result = await response.text();
    console.log(`Workflow ${workflowId} triggered. Response:`, result);

    if (response.ok) {
      console.log(`Workflow ${workflowId} triggered successfully.`);
      return { success: true, response: result };
    } else {
      console.error(`Failed to trigger workflow ${workflowId}. Status: ${response.status}`);
      return { success: false, error: `HTTP ${response.status}: ${response.statusText}`, response: result };
    }
  } catch (error) {
    console.error(`Failed to trigger workflow ${workflowId}:`, error);
    return { success: false, error: error.message };
  }
}

module.exports = { triggerWorkflow };
