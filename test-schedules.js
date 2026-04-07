require('dotenv').config();
const { getSchedules } = require('./database');

async function test() {
  try {
    console.log('Fetching schedules...');
    const schedules = await getSchedules();
    console.log('Schedules fetched successfully:');
    console.log(JSON.stringify(schedules, null, 2));
  } catch (error) {
    console.error('Error fetching schedules:', error);
    console.error('Error stack:', error.stack);
  }
}

test();
