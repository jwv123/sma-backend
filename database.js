// database.js
const { createClient } = require('@supabase/supabase-js');

// Supabase configuration (same as your frontend)
const SUPABASE_URL = 'https://jvysmxdkiynzqlnzidze.supabase.co';
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;

// Create Supabase client with service role key for backend access
const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY);

// Get all active schedules from database with associated content
async function getSchedules() {
  try {
    console.log('Fetching schedules from database with JOIN query...');

    // First, get all active schedules with content item data using JOIN
    const { data: schedules, error: schedulesError } = await supabase
      .from('workflow_schedules')
      .select(`
        *,
        content_items (
          id,
          title,
          description,
          resource_urls
        )
      `)
      .eq('active', true)
      .order('created_at', { ascending: true });

    if (schedulesError) {
      console.error('Error fetching schedules with JOIN:', schedulesError);
      throw schedulesError;
    }

    console.log(`Fetched ${schedules ? schedules.length : 0} schedules`);
    if (schedules && schedules.length > 0) {
      console.log('First schedule sample:', JSON.stringify(schedules[0], null, 2));

      // Check if content_items are present in the data
      schedules.forEach((schedule, index) => {
        if (schedule.content_item_id && !schedule.content_items) {
          console.warn(`Schedule ${schedule.id} has content_item_id ${schedule.content_item_id} but no content_items data`);
        }
      });
    }

    return schedules || [];
  } catch (error) {
    console.error('Error fetching schedules:', error);
    // Return empty array but log the specific error
    return [];
  }
}

// Create a new schedule
async function createSchedule(scheduleData) {
  try {
    // Prepare data for insertion, ensuring we only include valid fields
    const dataToInsert = {
      user_id: scheduleData.user_id,
      workflow_id: scheduleData.workflow_id,
      cron_expression: scheduleData.cron_expression,
      name: scheduleData.name,
      description: scheduleData.description,
      active: scheduleData.active !== undefined ? scheduleData.active : true,
      is_one_time: scheduleData.is_one_time || false,
      one_time_date: scheduleData.one_time_date || null,
      content_item_id: scheduleData.content_item_id || null
    };

    const { data, error } = await supabase
      .from('workflow_schedules')
      .insert([dataToInsert])
      .select();

    if (error) throw error;
    return data[0];
  } catch (error) {
    console.error('Error creating schedule:', error);
    throw error;
  }
}

// Update a schedule
async function updateSchedule(id, updates) {
  try {
    // Prepare updates, ensuring we only include valid fields
    const validUpdates = {
      workflow_id: updates.workflow_id,
      cron_expression: updates.cron_expression,
      name: updates.name,
      description: updates.description,
      active: updates.active,
      is_one_time: updates.is_one_time,
      one_time_date: updates.one_time_date,
      content_item_id: updates.content_item_id
    };

    // Remove undefined fields
    Object.keys(validUpdates).forEach(key => {
      if (validUpdates[key] === undefined) {
        delete validUpdates[key];
      }
    });

    const { data, error } = await supabase
      .from('workflow_schedules')
      .update(validUpdates)
      .eq('id', id)
      .select();

    if (error) throw error;
    return data[0];
  } catch (error) {
    console.error('Error updating schedule:', error);
    throw error;
  }
}

// Delete a schedule
async function deleteSchedule(id) {
  try {
    const { error } = await supabase
      .from('workflow_schedules')
      .delete()
      .eq('id', id);

    if (error) throw error;
    return true;
  } catch (error) {
    console.error('Error deleting schedule:', error);
    throw error;
  }
}

// Log schedule execution
async function logExecution(scheduleId, success = true, response = null) {
  try {
    const { error } = await supabase
      .from('schedule_executions')
      .insert([{
        schedule_id: scheduleId,
        executed_at: new Date().toISOString(),
        success: success,
        response: response
      }]);

    if (error) throw error;
    return true;
  } catch (error) {
    console.error('Error logging execution:', error);
    return false;
  }
}

// Create a new content item
async function createContentItem(contentData) {
  try {
    // Ensure user_id is provided and valid
    if (!contentData.user_id) {
      throw new Error('user_id is required to create a content item');
    }

    const { data, error } = await supabase
      .from('content_items')
      .insert([contentData])
      .select();

    if (error) throw error;
    return data[0];
  } catch (error) {
    console.error('Error creating content item:', error);
    throw error;
  }
}

// Get all content items for a user
async function getContentItems(userId) {
  try {
    const { data, error } = await supabase
      .from('content_items')
      .select('*')
      .eq('user_id', userId)
      .order('created_at', { ascending: false });

    if (error) throw error;
    return data;
  } catch (error) {
    console.error('Error fetching content items:', error);
    return [];
  }
}

// Update a content item
async function updateContentItem(id, updates) {
  try {
    const { data, error } = await supabase
      .from('content_items')
      .update(updates)
      .eq('id', id)
      .select();

    if (error) throw error;
    return data[0];
  } catch (error) {
    console.error('Error updating content item:', error);
    throw error;
  }
}

// Delete a content item
async function deleteContentItem(id) {
  try {
    const { error } = await supabase
      .from('content_items')
      .delete()
      .eq('id', id);

    if (error) throw error;
    return true;
  } catch (error) {
    console.error('Error deleting content item:', error);
    throw error;
  }
}

module.exports = {
  getSchedules,
  createSchedule,
  updateSchedule,
  deleteSchedule,
  logExecution,
  createContentItem,
  getContentItems,
  updateContentItem,
  deleteContentItem,
  supabase
};