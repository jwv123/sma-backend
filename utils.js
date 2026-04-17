// utils.js
// Utility functions for the scheduler

let _fetch;
async function getFetch() {
  if (!_fetch) {
    _fetch = (await import('node-fetch')).default;
  }
  return _fetch;
}

// Validate workflow ID to prevent SSRF
function validateWorkflowId(workflowId) {
  if (!workflowId || typeof workflowId !== 'string') {
    throw new Error('Workflow ID is required');
  }
  // Only allow alphanumeric characters, hyphens, and underscores
  if (!/^[a-zA-Z0-9_-]+$/.test(workflowId)) {
    throw new Error('Invalid workflow ID: must be alphanumeric with hyphens/underscores only');
  }
  if (workflowId.length > 100) {
    throw new Error('Invalid workflow ID: too long');
  }
}

// Function to trigger workflow via webhook
async function triggerWorkflow(workflowId, scheduleId, contentData = null) {
  const fetch = await getFetch();
  try {
    // Validate workflow ID to prevent SSRF
    validateWorkflowId(workflowId);

    console.log(`Triggering workflow ${workflowId} for schedule ${scheduleId}`);
    const WEBHOOK_URL = process.env.WEBHOOK_URL || 'https://ef0ps4gk.rcsrv.net/webhook/';
    const webhookUrl = `${WEBHOOK_URL.replace(/\/+$/, '')}/${workflowId}`;

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
    }

    const response = await fetch(webhookUrl, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify(payload)
    });

    console.log(`Webhook response status: ${response.status}`);

    const result = await response.text();

    if (response.ok) {
      console.log(`Workflow ${workflowId} triggered successfully.`);
      return { success: true, response: result };
    } else {
      console.error(`Failed to trigger workflow ${workflowId}. Status: ${response.status}`);
      return { success: false, error: `HTTP ${response.status}: ${response.statusText}`, response: result };
    }
  } catch (error) {
    console.error(`Failed to trigger workflow ${workflowId}:`, error.message);
    return { success: false, error: error.message };
  }
}

module.exports = { triggerWorkflow };