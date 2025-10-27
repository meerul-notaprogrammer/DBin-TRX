const express = require('express');
const { createClient } = require('@supabase/supabase-js');
require('dotenv').config();

const app = express();
app.use(express.json());

// Initialize Supabase client
const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_SERVICE_KEY
);

// Middleware to log all incoming headers
app.use((req, res, next) => {
  console.log('Incoming Headers:', req.headers);
  console.log('Request Body:', req.body);
  next();
});

// Health check endpoint
app.get('/', (req, res) => {
  res.json({ status: 'ok', message: 'Sensor Data API Server Running' });
});

// Main endpoint to handle sensor data
app.post('/MagnetAPI', async (req, res) => {
  try {
    const data = req.body;
    const headers = req.headers;

    // Validate required fields
    if (!data.cmd || !data.device) {
      return res.json({
        status: '00',
        message: 'Missing required fields: cmd or device'
      });
    }

    let result;

    // Handle Manhole sensor data (cmd = "06" or "01")
    if (data.cmd === '06' || data.cmd === '01') {
      result = await handleManholeData(data, headers);
    }
    // Handle Dustbin sensor data (cmd = "RP")
    else if (data.cmd === 'RP') {
      result = await handleDustbinData(data, headers);
    }
    else {
      return res.json({
        status: '00',
        message: `Unknown command type: ${data.cmd}`
      });
    }

    if (result.error) {
      console.error('Database Error:', result.error);
      return res.json({
        status: '00',
        message: result.error.message || 'Database operation failed'
      });
    }

    // Success response
    res.json({
      status: '01',
      message: ''
    });

  } catch (error) {
    console.error('Server Error:', error);
    res.json({
      status: '00',
      message: error.message || 'Internal server error'
    });
  }
});

// Handle Manhole sensor data
async function handleManholeData(data, headers) {
  const manholeData = {
    cmd: data.cmd,
    device: data.device,
    battery: parseFloat(data.battery) || null,
    battery_low: data.battery_low || null,
    time: data.time,
    d_index: data.dIndex,
    dt_state: data.dt_state || null,
    dt_water_lv: data.dt_waterLV ? parseInt(data.dt_waterLV) : null,
    dt_x: data.dt_x ? parseInt(data.dt_x) : null,
    dt_y: data.dt_y ? parseInt(data.dt_y) : null,
    dt_z: data.dt_z ? parseInt(data.dt_z) : null,
    headers: JSON.stringify(headers),
    received_at: new Date().toISOString()
  };

  return await supabase
    .from('manhole_data')
    .insert([manholeData]);
}

// Handle Dustbin sensor data
async function handleDustbinData(data, headers) {
  const dustbinData = {
    cmd: data.cmd,
    device: data.device,
    battery: parseFloat(data.battery) || null,
    time: data.time,
    d_index: data.dIndex,
    overflow_percentage: data.data ? parseInt(data.data) : null,
    headers: JSON.stringify(headers),
    received_at: new Date().toISOString()
  };

  return await supabase
    .from('dustbin_data')
    .insert([dustbinData]);
}

// Error handling middleware
app.use((err, req, res, next) => {
  console.error('Unhandled Error:', err);
  res.json({
    status: '00',
    message: 'Internal server error'
  });
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`Server running on port ${PORT}`);
  console.log(`Endpoint: http://localhost:${PORT}/MagnetAPI`);
});
