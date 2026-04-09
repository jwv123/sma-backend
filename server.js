require('dotenv').config();
const express = require('express');
const cors = require('cors');
const { setupAllSchedules, setupSchedule, removeSchedule, updateSchedule, calculateNextExecutionTime } = require('./scheduler');
const {
  getSchedules,
  createSchedule,
  updateSchedule: updateScheduleDB,
  deleteSchedule,
  createContentItem,
  getContentItems,
  updateContentItem,
  deleteContentItem,
  supabase
} = require('./database');
const { triggerWorkflow } = require('./utils');

const app = express();
const PORT = process.env.PORT || 3001;

// Middleware
app.use(cors());
app.use(express.json());

// Initialize schedules on startup
(async () => {
  try {
    await setupAllSchedules();
    console.log('Scheduler initialized successfully');
  } catch (error) {
    console.error('Error initializing scheduler:', error);
  }
})();

// API Routes

// Health check endpoint
app.get('/health', (req, res) => {
  res.json({ status: 'OK', timestamp: new Date().toISOString() });
});

// Get all schedules with next execution time
app.get('/api/schedules', async (req, res) => {
  try {
    const schedules = await getSchedules();
    const timezone = process.env.SCHEDULE_TIMEZONE || 'America/New_York';

    // Calculate next execution time for each schedule
    const schedulesWithNext = schedules.map(schedule => {
      const { nextExecution, formatted } = calculateNextExecutionTime(schedule.cron_expression, timezone);
      return {
        ...schedule,
        next_execution: formatted,
        next_execution_time: nextExecution ? nextExecution.toISOString() : null
      };
    });

    res.json({ success: true, schedules: schedulesWithNext });
  } catch (error) {
    res.status(500).json({ success: false, error: error.message });
  }
});

// Create a new schedule
app.post('/api/schedules', async (req, res) => {
  try {
    const scheduleData = req.body;

    // Validate required fields
    if (!scheduleData.workflow_id || !scheduleData.cron_expression || !scheduleData.user_id) {
      return res.status(400).json({
        success: false,
        error: 'Missing required fields: workflow_id, cron_expression, user_id'
      });
    }

    // Create in database
    const newSchedule = await createSchedule(scheduleData);

    // Setup in scheduler
    await setupSchedule(newSchedule);

    res.json({ success: true, schedule: newSchedule });
  } catch (error) {
    res.status(500).json({ success: false, error: error.message });
  }
});

// Update a schedule
app.put('/api/schedules/:id', async (req, res) => {
  try {
    const { id } = req.params;
    const updates = req.body;

    // Update in database
    const updatedSchedule = await updateScheduleDB(id, updates);

    // Update in scheduler
    await updateSchedule(updatedSchedule);

    res.json({ success: true, schedule: updatedSchedule });
  } catch (error) {
    res.status(500).json({ success: false, error: error.message });
  }
});

// Delete a schedule
app.delete('/api/schedules/:id', async (req, res) => {
  try {
    const { id } = req.params;

    // Remove from scheduler
    removeSchedule(id);

    // Delete from database
    await deleteSchedule(id);

    res.json({ success: true, message: 'Schedule deleted successfully' });
  } catch (error) {
    res.status(500).json({ success: false, error: error.message });
  }
});

// Manual trigger endpoint
app.post('/api/trigger/:workflowId', async (req, res) => {
  try {
    const { workflowId } = req.params;
    const result = await triggerWorkflow(workflowId, 'manual-trigger');
    res.json(result);
  } catch (error) {
    res.status(500).json({ success: false, error: error.message });
  }
});

// Reload all schedules (useful for manual refresh)
app.post('/api/reload', async (req, res) => {
  try {
    await setupAllSchedules();
    res.json({ success: true, message: 'Schedules reloaded successfully' });
  } catch (error) {
    res.status(500).json({ success: false, error: error.message });
  }
});

// Content Management API Routes

// Create a new content item
app.post('/api/content', async (req, res) => {
  try {
    const contentData = req.body;

    // Validate required fields
    if (!contentData.title) {
      return res.status(400).json({
        success: false,
        error: 'Missing required field: title'
      });
    }

    // Create in database (user_id should come from authentication middleware)
    const newContent = await createContentItem(contentData);
    res.json({ success: true, content: newContent });
  } catch (error) {
    res.status(500).json({ success: false, error: error.message });
  }
});

// Get all content items for a user
app.get('/api/content/:userId', async (req, res) => {
  try {
    const { userId } = req.params;
    const contentItems = await getContentItems(userId);
    res.json({ success: true, content: contentItems });
  } catch (error) {
    res.status(500).json({ success: false, error: error.message });
  }
});

// Update a content item
app.put('/api/content/:id', async (req, res) => {
  try {
    const { id } = req.params;
    const updates = req.body;

    // Update in database
    const updatedContent = await updateContentItem(id, updates);
    res.json({ success: true, content: updatedContent });
  } catch (error) {
    res.status(500).json({ success: false, error: error.message });
  }
});

// Delete a content item
app.delete('/api/content/:id', async (req, res) => {
  try {
    const { id } = req.params;

    // Delete from database
    await deleteContentItem(id);
    res.json({ success: true, message: 'Content item deleted successfully' });
  } catch (error) {
    res.status(500).json({ success: false, error: error.message });
  }
});

// Start the server
const server = app.listen(PORT, () => {
  console.log(`Workflow scheduler server running on port ${PORT}`);
  console.log(`Health check: http://localhost:${PORT}/health`);
});

// Graceful shutdown
process.on('SIGINT', () => {
  console.log('Shutting down scheduler...');
  server.close(() => {
    console.log('Server closed');
    process.exit(0);
  });
});

module.exports = app;
