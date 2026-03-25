// scheduler.js
const cron = require('node-cron');
const { getSchedules, logExecution, supabase } = require('./database');
const { triggerWorkflow } = require('./utils');

// Function to fetch content item by ID as fallback
async function fetchContentItemById(contentItemId) {
  try {
    console.log(`Fetching content item ${contentItemId} from database`);
    const { data, error } = await supabase
      .from('content_items')
      .select('*')
      .eq('id', contentItemId)
      .single();

    if (error) {
      console.error(`Error fetching content item ${contentItemId}:`, error);
      return null;
    }

    console.log(`Successfully fetched content item:`, JSON.stringify(data, null, 2));
    return data;
  } catch (error) {
    console.error(`Exception fetching content item ${contentItemId}:`, error);
    return null;
  }
}

// Store active jobs (both cron and timeouts)
const activeJobs = new Map();

// Function to setup all schedules from database
async function setupAllSchedules() {
  try {
    console.log('Loading schedules from database...');
    const schedules = await getSchedules();

    console.log(`Retrieved ${schedules ? schedules.length : 0} schedules from database`);
    if (schedules && schedules.length > 0) {
      console.log('First schedule from database:', JSON.stringify(schedules[0], null, 2));
    }

    // Clear existing jobs
    activeJobs.forEach((job, id) => {
      if (job.type === 'cron' && job.instance.destroy) {
        job.instance.destroy();
      } else if (job.type === 'timeout') {
        clearTimeout(job.instance);
      }
    });
    activeJobs.clear();

    // Setup each schedule
    for (const schedule of schedules) {
      await setupSchedule(schedule);
    }

    console.log(`Loaded and setup ${schedules.length} schedules`);
  } catch (error) {
    console.error('Error setting up schedules:', error);
  }
}

// Determine if a schedule is one-time based on database field
function isOneTimeSchedule(schedule) {
  // Check if the schedule is marked as one-time in the database
  return schedule.is_one_time === true;
}

// Calculate timeout for one-time execution from schedule data
function calculateTimeoutForOneTime(schedule) {
  try {
    if (!schedule.one_time_date) {
      console.log(`No one_time_date for schedule ${schedule.id}`);
      return { timeout: 0, scheduledTime: new Date() };
    }

    console.log(`Parsing one_time_date for schedule ${schedule.id}: ${schedule.one_time_date}`);

    // Parse the one-time date
    // The date comes in ISO format which represents the intended time
    // We treat this as the actual scheduled time, not needing timezone conversion
    const scheduledTime = new Date(schedule.one_time_date);

    // If the date is invalid, log error and return
    if (isNaN(scheduledTime.getTime())) {
      console.error(`Invalid date for schedule ${schedule.id}: ${schedule.one_time_date}`);
      return { timeout: 0, scheduledTime: new Date() };
    }

    const now = new Date();

    console.log(`Scheduled time: ${scheduledTime}`);
    console.log(`Current time: ${now}`);

    // Calculate timeout in milliseconds
    const timeout = scheduledTime.getTime() - now.getTime();

    console.log(`Calculated timeout: ${timeout}ms`);

    // Only return positive timeouts (future dates)
    if (timeout > 0) {
      return { timeout, scheduledTime };
    }

    console.log(`Schedule ${schedule.id} is in the past, skipping`);
    return { timeout: 0, scheduledTime: new Date() };
  } catch (error) {
    console.error(`Error calculating timeout for schedule ${schedule.id}:`, error);
    return { timeout: 0, scheduledTime: new Date() };
  }
}

// Setup a single schedule
async function setupSchedule(schedule) {
  try {
    // Check if it's a one-time schedule
    if (isOneTimeSchedule(schedule)) {
      // Handle one-time schedule with setTimeout
      const { timeout, scheduledTime } = calculateTimeoutForOneTime(schedule);

      if (timeout > 0) {
        console.log(`Setting up one-time schedule ${schedule.id} to run at ${scheduledTime}`);
        console.log(`Schedule data:`, JSON.stringify(schedule, null, 2));

        // Create a placeholder job entry first to prevent race conditions
        const jobId = schedule.id;
        activeJobs.set(jobId, {
          type: 'timeout',
          instance: null,
          scheduledTime: scheduledTime
        });

        const timeoutId = setTimeout(async () => {
          console.log(`Executing one-time scheduled workflow ${schedule.workflow_id} for schedule ${schedule.id}`);
          console.log(`Current time: ${new Date()}`);
          console.log(`Schedule data:`, JSON.stringify(schedule, null, 2));

          try {
            // Pass content data if available
            let contentData = null;
            if (schedule.content_items) {
              contentData = schedule.content_items;
              console.log(`Using content_items from schedule:`, JSON.stringify(contentData, null, 2));
            } else if (schedule.content_item_id) {
              console.log(`Schedule has content_item_id ${schedule.content_item_id} but no content_items data`);
              // Try to fetch content item data manually as fallback
              contentData = await fetchContentItemById(schedule.content_item_id);
              console.log(`Fetched content data manually:`, JSON.stringify(contentData, null, 2));
            } else {
              console.log(`No content data available for schedule`);
            }

            const result = await triggerWorkflow(schedule.workflow_id, schedule.id, contentData);
            await logExecution(schedule.id, result.success, result.response || result.error);

            // Remove the job after execution
            activeJobs.delete(schedule.id);
            console.log(`One-time schedule ${schedule.id} executed and removed`);
          } catch (error) {
            console.error(`Error executing one-time schedule ${schedule.id}:`, error);
            await logExecution(schedule.id, false, error.message);

            // Remove the job after execution even if it failed
            activeJobs.delete(schedule.id);
          }
        }, timeout);

        // Update the job with the actual timeout ID
        activeJobs.set(schedule.id, {
          type: 'timeout',
          instance: timeoutId,
          scheduledTime: scheduledTime
        });

        console.log(`One-time schedule ${schedule.id} set up successfully`);
      } else {
        console.log(`One-time schedule ${schedule.id} is in the past, skipping`);
        console.log(`Schedule data:`, JSON.stringify(schedule, null, 2));
      }
    } else {
      // Validate cron expression for recurring schedules
      if (!cron.validate(schedule.cron_expression)) {
        console.error(`Invalid cron expression for schedule ${schedule.id}: ${schedule.cron_expression}`);
        return;
      }

      // Handle recurring schedule with cron
      const job = cron.schedule(schedule.cron_expression, async () => {
        console.log(`Executing scheduled workflow ${schedule.workflow_id} for schedule ${schedule.id}`);
        console.log(`Schedule data:`, JSON.stringify(schedule, null, 2));

        try {
          // Pass content data if available
          let contentData = null;
          if (schedule.content_items) {
            contentData = schedule.content_items;
            console.log(`Using content_items from schedule:`, JSON.stringify(contentData, null, 2));
          } else if (schedule.content_item_id) {
            console.log(`Schedule has content_item_id ${schedule.content_item_id} but no content_items data`);
            // Try to fetch content item data manually as fallback
            contentData = await fetchContentItemById(schedule.content_item_id);
            console.log(`Fetched content data manually:`, JSON.stringify(contentData, null, 2));
          } else {
            console.log(`No content data available for schedule`);
          }

          const result = await triggerWorkflow(schedule.workflow_id, schedule.id, contentData);
          await logExecution(schedule.id, result.success, result.response || result.error);
        } catch (error) {
          console.error(`Error executing schedule ${schedule.id}:`, error);
          await logExecution(schedule.id, false, error.message);
        }
      });

      // Store the cron job
      activeJobs.set(schedule.id, {
        type: 'cron',
        instance: job
      });

      console.log(`Recurring schedule ${schedule.id} set up successfully`);
    }
  } catch (error) {
    console.error(`Error setting up schedule ${schedule.id}:`, error);
  }
}

// Remove a schedule
function removeSchedule(scheduleId) {
  if (activeJobs.has(scheduleId)) {
    const job = activeJobs.get(scheduleId);
    if (job.type === 'cron' && job.instance.destroy) {
      job.instance.destroy();
    } else if (job.type === 'timeout') {
      clearTimeout(job.instance);
    }
    activeJobs.delete(scheduleId);
    console.log(`Schedule ${scheduleId} removed`);
  }
}

// Update a schedule (remove old, setup new)
async function updateSchedule(schedule) {
  removeSchedule(schedule.id);
  await setupSchedule(schedule);
}

module.exports = {
  setupAllSchedules,
  setupSchedule,
  removeSchedule,
  updateSchedule
};