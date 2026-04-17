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

// --- CORS Configuration ---
const allowedOrigins = [
  'https://sma-backend-lujo.onrender.com',
  'http://localhost:3000',
  'http://localhost:5500',
  'http://127.0.0.1:5500'
];

app.use(cors({
  origin: function (origin, callback) {
    // Allow requests with no origin (e.g., server-to-server, curl)
    if (!origin || allowedOrigins.includes(origin)) {
      callback(null, true);
    } else {
      callback(new Error('Not allowed by CORS'));
    }
  }
}));

app.use(express.json());

// --- Authentication Middleware ---
async function requireAuth(req, res, next) {
  const authHeader = req.headers.authorization;
  if (!authHeader || !authHeader.startsWith('Bearer ')) {
    return res.status(401).json({ success: false, error: 'Unauthorized: missing or invalid token' });
  }

  const token = authHeader.split(' ')[1];
  try {
    const { data: { user }, error } = await supabase.auth.getUser(token);
    if (error || !user) {
      return res.status(401).json({ success: false, error: 'Unauthorized: invalid or expired token' });
    }
    req.user = user;
    next();
  } catch (err) {
    return res.status(401).json({ success: false, error: 'Unauthorized: token verification failed' });
  }
}

// --- Input Validation Helpers ---
function validateScheduleInput(data) {
  const errors = [];
  if (!data.workflow_id || typeof data.workflow_id !== 'string') {
    errors.push('workflow_id is required');
  } else if (!/^[a-zA-Z0-9_-]+$/.test(data.workflow_id) || data.workflow_id.length > 100) {
    errors.push('workflow_id must be alphanumeric (hyphens/underscores allowed), max 100 chars');
  }
  if (!data.cron_expression || typeof data.cron_expression !== 'string') {
    errors.push('cron_expression is required');
  } else if (data.cron_expression.length > 100) {
    errors.push('cron_expression must be max 100 chars');
  }
  if (data.name && typeof data.name !== 'string') {
    errors.push('name must be a string');
  } else if (data.name && data.name.length > 200) {
    errors.push('name must be max 200 chars');
  }
  if (data.description && data.description.length > 2000) {
    errors.push('description must be max 2000 chars');
  }
  if (data.content_item_id && !Number.isInteger(Number(data.content_item_id))) {
    errors.push('content_item_id must be an integer');
  }
  if (data.is_one_time && typeof data.is_one_time !== 'boolean') {
    errors.push('is_one_time must be a boolean');
  }
  return errors;
}

function validateContentInput(data) {
  const errors = [];
  if (!data.title || typeof data.title !== 'string' || data.title.trim().length === 0) {
    errors.push('title is required');
  } else if (data.title.length > 200) {
    errors.push('title must be max 200 chars');
  }
  if (data.description && typeof data.description !== 'string') {
    errors.push('description must be a string');
  } else if (data.description && data.description.length > 2000) {
    errors.push('description must be max 2000 chars');
  }
  if (data.resource_urls) {
    if (!Array.isArray(data.resource_urls)) {
      errors.push('resource_urls must be an array');
    } else if (data.resource_urls.length > 10) {
      errors.push('resource_urls must have max 10 items');
    } else {
      for (const url of data.resource_urls) {
        if (typeof url !== 'string' || url.length > 2000) {
          errors.push('each resource_url must be a string, max 2000 chars');
          break;
        }
      }
    }
  }
  return errors;
}

// --- Public Routes (no auth required) ---

// Health check endpoint
app.get('/health', (req, res) => {
  res.json({ status: 'OK', timestamp: new Date().toISOString() });
});

// Get server timezone
app.get('/api/timezone', (req, res) => {
  const timezone = process.env.SCHEDULE_TIMEZONE || 'Africa/Johannesburg';
  res.json({ success: true, timezone });
});

// --- Authenticated Routes ---

// Initialize schedules on startup
(async () => {
  try {
    await setupAllSchedules();
    console.log('Scheduler initialized successfully');
  } catch (error) {
    console.error('Error initializing scheduler:', error);
  }
})();

// Get all schedules for the authenticated user
app.get('/api/schedules', requireAuth, async (req, res) => {
  try {
    const schedules = await getSchedules(req.user.id);
    const timezone = process.env.SCHEDULE_TIMEZONE || 'Africa/Johannesburg';

    // Calculate next execution time for each schedule
    const schedulesWithNext = schedules.map(schedule => {
      if (schedule.is_one_time) {
        const oneTimeDate = schedule.one_time_date ? new Date(schedule.one_time_date) : null;
        const formatted = oneTimeDate && !isNaN(oneTimeDate.getTime())
          ? new Intl.DateTimeFormat('en-US', {
              year: 'numeric', month: 'long', day: 'numeric',
              hour: '2-digit', minute: '2-digit',
              hourCycle: 'h23', timeZone: timezone
            }).format(oneTimeDate)
          : 'No date set';
        return {
          ...schedule,
          next_execution: formatted,
          next_execution_time: oneTimeDate && !isNaN(oneTimeDate.getTime()) ? oneTimeDate.toISOString() : null
        };
      }

      const { nextExecution, formatted } = calculateNextExecutionTime(schedule.cron_expression, timezone);
      return {
        ...schedule,
        next_execution: formatted,
        next_execution_time: nextExecution ? nextExecution.toISOString() : null
      };
    });

    res.json({ success: true, schedules: schedulesWithNext, timezone });
  } catch (error) {
    res.status(500).json({ success: false, error: error.message });
  }
});

// Create a new schedule
app.post('/api/schedules', requireAuth, async (req, res) => {
  try {
    const scheduleData = req.body;

    // Validate required fields
    const validationErrors = validateScheduleInput(scheduleData);
    if (validationErrors.length > 0) {
      return res.status(400).json({ success: false, error: validationErrors.join('; ') });
    }

    if (!scheduleData.cron_expression) {
      return res.status(400).json({ success: false, error: 'Missing required fields: workflow_id, cron_expression' });
    }

    // Use authenticated user's ID (ignore any user_id in the request body)
    scheduleData.user_id = req.user.id;

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
app.put('/api/schedules/:id', requireAuth, async (req, res) => {
  try {
    const { id } = req.params;
    const updates = req.body;

    // Validate input
    const validationErrors = validateScheduleInput(updates);
    if (validationErrors.length > 0) {
      return res.status(400).json({ success: false, error: validationErrors.join('; ') });
    }

    // Update in database (with user scope check)
    const updatedSchedule = await updateScheduleDB(id, req.user.id, updates);

    if (!updatedSchedule) {
      return res.status(404).json({ success: false, error: 'Schedule not found or not owned by user' });
    }

    // Update in scheduler
    await updateSchedule(updatedSchedule);

    res.json({ success: true, schedule: updatedSchedule });
  } catch (error) {
    res.status(500).json({ success: false, error: error.message });
  }
});

// Delete a schedule
app.delete('/api/schedules/:id', requireAuth, async (req, res) => {
  try {
    const { id } = req.params;

    // Remove from scheduler
    removeSchedule(id);

    // Delete from database (with user scope check)
    const deleted = await deleteSchedule(id, req.user.id);

    if (!deleted) {
      return res.status(404).json({ success: false, error: 'Schedule not found or not owned by user' });
    }

    res.json({ success: true, message: 'Schedule deleted successfully' });
  } catch (error) {
    res.status(500).json({ success: false, error: error.message });
  }
});

// Manual trigger endpoint
app.post('/api/trigger/:workflowId', requireAuth, async (req, res) => {
  try {
    const { workflowId } = req.params;
    const result = await triggerWorkflow(workflowId, 'manual-trigger');
    res.json(result);
  } catch (error) {
    res.status(500).json({ success: false, error: error.message });
  }
});

// Reload all schedules (useful for manual refresh)
app.post('/api/reload', requireAuth, async (req, res) => {
  try {
    await setupAllSchedules();
    res.json({ success: true, message: 'Schedules reloaded successfully' });
  } catch (error) {
    res.status(500).json({ success: false, error: error.message });
  }
});

// Create a new content item
app.post('/api/content', requireAuth, async (req, res) => {
  try {
    const contentData = req.body;

    // Validate input
    const validationErrors = validateContentInput(contentData);
    if (validationErrors.length > 0) {
      return res.status(400).json({ success: false, error: validationErrors.join('; ') });
    }

    // Use authenticated user's ID
    contentData.user_id = req.user.id;

    const newContent = await createContentItem(contentData);
    res.json({ success: true, content: newContent });
  } catch (error) {
    res.status(500).json({ success: false, error: error.message });
  }
});

// Get all content items for the authenticated user
app.get('/api/content/:userId', requireAuth, async (req, res) => {
  try {
    // Only allow users to access their own content
    if (req.params.userId !== req.user.id) {
      return res.status(403).json({ success: false, error: 'Forbidden: cannot access another user\'s content' });
    }

    const contentItems = await getContentItems(req.user.id);
    res.json({ success: true, content: contentItems });
  } catch (error) {
    res.status(500).json({ success: false, error: error.message });
  }
});

// Update a content item
app.put('/api/content/:id', requireAuth, async (req, res) => {
  try {
    const { id } = req.params;
    const updates = req.body;

    // Validate input
    const validationErrors = validateContentInput(updates);
    if (validationErrors.length > 0) {
      return res.status(400).json({ success: false, error: validationErrors.join('; ') });
    }

    const updatedContent = await updateContentItem(id, req.user.id, updates);

    if (!updatedContent) {
      return res.status(404).json({ success: false, error: 'Content item not found or not owned by user' });
    }

    res.json({ success: true, content: updatedContent });
  } catch (error) {
    res.status(500).json({ success: false, error: error.message });
  }
});

// Delete a content item
app.delete('/api/content/:id', requireAuth, async (req, res) => {
  try {
    const { id } = req.params;

    const deleted = await deleteContentItem(id, req.user.id);

    if (!deleted) {
      return res.status(404).json({ success: false, error: 'Content item not found or not owned by user' });
    }

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