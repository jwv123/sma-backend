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
      if (job.type === 'cron' && typeof job.instance.stop === 'function') {
        job.instance.stop();
        console.log(`Stopped cron job ${id}`);
      } else if (job.type === 'timeout') {
        clearTimeout(job.instance);
        console.log(`Cleared timeout job ${id}`);
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

// Helper function to get timezone from environment or use default
function getTimezone() {
  return process.env.SCHEDULE_TIMEZONE || 'America/New_York';
}

// Helper function to calculate next execution time for a cron expression
function calculateNextExecutionTime(cronExpression, timezone) {
  try {
    // Parse cron expression manually to calculate next execution
    // Format: minute hour day-of-month month day-of-week
    const parts = cronExpression.split(' ');
    if (parts.length < 5) {
      return { nextExecution: null, formatted: 'Invalid cron expression' };
    }

    const [minuteExpr, hourExpr, dayOfMonthExpr, monthExpr, dayOfWeekExpr] = parts;

    // Helper to parse a cron field into an array of valid values
    function parseCronField(field, min, max) {
      const values = [];
      const fieldParts = field.split(',');

      for (const part of fieldParts) {
        if (part === '*') {
          for (let i = min; i <= max; i++) {
            values.push(i);
          }
        } else if (part.includes('/')) {
          const [range, step] = part.split('/');
          const stepVal = parseInt(step);

          let start = min;
          let end = max;

          if (range !== '*') {
            const rangeParts = range.split('-');
            if (rangeParts.length === 2) {
              start = parseInt(rangeParts[0]);
              end = parseInt(rangeParts[1]);
            }
          }

          for (let i = start; i <= end; i += stepVal) {
            values.push(i);
          }
        } else if (part.includes('-')) {
          const [start, end] = part.split('-').map(Number);
          for (let i = start; i <= end; i++) {
            values.push(i);
          }
        } else {
          values.push(parseInt(part));
        }
      }

      return [...new Set(values)].sort((a, b) => a - b);
    }

    // Parse cron fields
    const minutes = parseCronField(minuteExpr, 0, 59);
    const hours = parseCronField(hourExpr, 0, 23);
    const daysOfMonth = parseCronField(dayOfMonthExpr, 1, 31);
    const months = parseCronField(monthExpr, 1, 12);
    const daysOfWeek = parseCronField(dayOfWeekExpr, 0, 6);

    // Use Intl.DateTimeFormat to get current time in the target timezone
    const dtf = new Intl.DateTimeFormat('en-US', {
      year: 'numeric', month: '2-digit', day: '2-digit',
      hour: '2-digit', minute: '2-digit', second: '2-digit',
      hourCycle: 'h23', timeZone: timezone
    });

    const now = new Date();
    const nowInTz = new Date(dtf.format(now));

    // Find next execution time (search next 366 days to handle leap years)
    for (let daysToAdd = 0; daysToAdd < 366; daysToAdd++) {
      const testDate = new Date(nowInTz);
      testDate.setDate(nowInTz.getDate() + daysToAdd);

      const testDayOfMonth = testDate.getDate();
      const testMonth = testDate.getMonth() + 1;
      const testDayOfWeek = testDate.getDay();

      // Check if this date matches the cron expression
      const matchesDayOfMonth = dayOfMonthExpr === '*' || daysOfMonth.includes(testDayOfMonth);
      const matchesMonth = months.includes(testMonth);
      const matchesDayOfWeek = dayOfWeekExpr === '*' || daysOfWeek.includes(testDayOfWeek);

      // Handle day-of-month/day-of-week conflict:
      // If both are restricted (not *), match if either matches (OR logic)
      // If one is *, match only the other (AND logic)
      let matchesDate = false;
      if (dayOfMonthExpr !== '*' && dayOfWeekExpr !== '*') {
        // Both restricted: OR logic
        matchesDate = (matchesDayOfMonth || matchesDayOfWeek) && matchesMonth;
      } else {
        // One or both are *: AND logic
        matchesDate = matchesDayOfMonth && matchesDayOfWeek && matchesMonth;
      }

      if (matchesDate) {
        // Check hours and minutes
        for (const hour of hours) {
          for (const minute of minutes) {
            const testTime = new Date(testDate);
            testTime.setHours(hour, minute, 0, 0);

            // Skip past times (compare in the target timezone)
            if (testTime.getTime() > nowInTz.getTime()) {
              // Format the result in the target timezone for display
              const resultDate = new Date(testTime.getTime() - nowInTz.getTime() + now.getTime());
              const formattedDt = new Intl.DateTimeFormat('en-US', {
                year: 'numeric', month: 'long', day: 'numeric',
                hour: '2-digit', minute: '2-digit',
                hourCycle: 'h23', timeZone: timezone
              }).format(resultDate);

              return {
                nextExecution: resultDate,
                formatted: formattedDt
              };
            }
          }
        }
      }
    }

    return { nextExecution: null, formatted: 'Unknown' };
  } catch (error) {
    console.error(`Error calculating next execution time for cron '${cronExpression}':`, error);
    return { nextExecution: null, formatted: 'Error' };
  }
}

// Setup a single schedule
async function setupSchedule(schedule) {
  try {
    const timezone = getTimezone();

    // Check if it's a one-time schedule
    if (isOneTimeSchedule(schedule)) {
      // Handle one-time schedule with setTimeout
      const { timeout, scheduledTime } = calculateTimeoutForOneTime(schedule);

      if (timeout > 0) {
        console.log(`Setting up one-time schedule ${schedule.id} to run at ${scheduledTime.toISOString()} (timezone: ${timezone})`);
        console.log(`Schedule data:`, JSON.stringify(schedule, null, 2));

        // Store the timeout ID directly to avoid race conditions
        const timeoutId = setTimeout(async () => {
          // Check if this schedule still exists in activeJobs before executing
          const storedJob = activeJobs.get(schedule.id);
          if (!storedJob) {
            console.log(`Schedule ${schedule.id} no longer exists, skipping execution`);
            return;
          }

          console.log(`[ONE-TIME EXECUTION] Executing scheduled workflow ${schedule.workflow_id} for schedule ${schedule.id}`);
          console.log(`[ONE-TIME EXECUTION] Current time: ${new Date().toISOString()}`);
          console.log(`[ONE-TIME EXECUTION] Schedule data:`, JSON.stringify(schedule, null, 2));

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
            console.error(`[ONE-TIME ERROR] Error executing one-time schedule ${schedule.id}:`, error);
            await logExecution(schedule.id, false, error.message);

            // Remove the job after execution even if it failed
            activeJobs.delete(schedule.id);
          }
        }, timeout);

        // Update the job with the actual timeout ID immediately
        activeJobs.set(schedule.id, {
          type: 'timeout',
          instance: timeoutId,
          scheduledTime: scheduledTime
        });

        console.log(`One-time schedule ${schedule.id} set up successfully (will run in ${Math.round(timeout / 1000)} seconds)`);
      } else {
        console.log(`One-time schedule ${schedule.id} is in the past or invalid, skipping`);
        console.log(`Schedule data:`, JSON.stringify(schedule, null, 2));
      }
    } else {
      // Validate cron expression for recurring schedules
      if (!cron.validate(schedule.cron_expression)) {
        console.error(`Invalid cron expression for schedule ${schedule.id}: ${schedule.cron_expression}`);
        return;
      }

      // Calculate next execution time for logging
      const { nextExecution, formatted } = calculateNextExecutionTime(schedule.cron_expression, timezone);
      console.log(`[SCHEDULE SETUP] Next execution will be at: ${formatted}`);

      // Handle recurring schedule with cron
      // Use options to ensure job runs on schedule with correct timezone
      const job = cron.schedule(schedule.cron_expression, async () => {
        console.log(`[CRON EXECUTION] Executing scheduled workflow ${schedule.workflow_id} for schedule ${schedule.id}`);
        console.log(`[CRON EXECUTION] Current time: ${new Date().toISOString()}`);
        console.log(`[CRON EXECUTION] Timezone: ${timezone}`);
        console.log(`[CRON EXECUTION] Schedule data:`, JSON.stringify(schedule, null, 2));

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
          console.log(`[CRON SUCCESS] Execution logged for schedule ${schedule.id}`);
        } catch (error) {
          console.error(`[CRON ERROR] Error executing schedule ${schedule.id}:`, error);
          await logExecution(schedule.id, false, error.message);
        }
      }, {
        timezone: timezone,
        recoverMissedExecutions: true
      });

      // Store the cron job
      activeJobs.set(schedule.id, {
        type: 'cron',
        instance: job
      });

      // Verify the job is scheduled
      const isScheduled = job.scheduled !== false;
      console.log(`Recurring schedule ${schedule.id} set up successfully (scheduled: ${isScheduled})`);
      console.log(`Cron expression: ${schedule.cron_expression}`);
      console.log(`Next execution will be at the next cron interval matching: ${formatted}`);
    }
  } catch (error) {
    console.error(`Error setting up schedule ${schedule.id}:`, error);
  }
}

// Remove a schedule
function removeSchedule(scheduleId) {
  if (activeJobs.has(scheduleId)) {
    const job = activeJobs.get(scheduleId);
    if (job.type === 'cron' && typeof job.instance.stop === 'function') {
      job.instance.stop();
      console.log(`Stopped cron job ${scheduleId}`);
    } else if (job.type === 'timeout') {
      // Clear the timeout if we have a valid timeout ID
      if (job.instance !== null && typeof job.instance !== 'undefined') {
        clearTimeout(job.instance);
        console.log(`Cleared timeout job ${scheduleId}`);
      } else {
        console.log(`Timeout job ${scheduleId} has no valid timeout ID, job may have already executed`);
      }
    }
    activeJobs.delete(scheduleId);
    console.log(`Schedule ${scheduleId} removed`);
  } else {
    console.log(`Schedule ${scheduleId} not found in active jobs`);
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
  updateSchedule,
  calculateNextExecutionTime
};
