// server.js - Render + Supabase Version
const express = require('express');
const { createClient } = require('@supabase/supabase-js');
const moment = require('moment');
require('dotenv').config();

const app = express();
const PORT = process.env.PORT || 3000;

// Middleware
app.use(express.json());

// Initialize Supabase Client
const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_SERVICE_KEY
);

// Test database connection on startup
async function testConnection() {
  try {
    const { data, error } = await supabase
      .from('sensor_data')
      .select('count')
      .limit(1);
    
    if (error) throw error;
    console.log('✓ Connected to Supabase successfully');
  } catch (error) {
    console.error('✗ Supabase connection error:', error.message);
  }
}

testConnection();

// Authentication middleware
function authenticateRequest(req, res, next) {
  const M = req.headers['m'];  
  const K = req.headers['k']; 
  
  if (M !== 'nhrdata' || K !== '9fg3gk56y9!#z#8g%yswqzd7p3357%x') {
    return res.status(401).json({
      status: '00',
      message: 'Authentication failed: Invalid credentials'
    });
  }
  
  next();
}

// Main API endpoint to receive sensor data from NHR
app.post('/MagnetAPI', authenticateRequest, async (req, res) => {
  try {
    const { cmd, device, battery, time, dIndex, data } = req.body;
    
    // Validation
    if (!cmd || !device || !battery || !time || !dIndex || !data) {
      return res.status(400).json({
        status: '00',
        message: 'Missing required fields'
      });
    }
    
    if (cmd !== 'RP') {
      return res.status(400).json({
        status: '00',
        message: 'Invalid command type'
      });
    }
    
    if (dIndex !== '0410') {
      return res.status(400).json({
        status: '00',
        message: 'Invalid data index'
      });
    }
    
    // Parse and validate data
    const batteryValue = parseFloat(battery);
    const overflowPercentage = parseInt(data);
    
    if (isNaN(batteryValue) || isNaN(overflowPercentage)) {
      return res.status(400).json({
        status: '00',
        message: 'Invalid battery or data value'
      });
    }
    
    // Convert UTC+0 time to local time
    const utcTime = moment.utc(time, 'YYYY-MM-DD HH:mm:ss');
    const localTime = utcTime.local().format('YYYY-MM-DD HH:mm:ss');
    
    // Insert data into Supabase
    const { data: insertedData, error } = await supabase
      .from('sensor_data')
      .insert([
        {
          device_id: device.trim(),
          battery: batteryValue,
          received_time: localTime,
          received_time_utc: time,
          data_index: dIndex,
          overflow_percentage: overflowPercentage
        }
      ])
      .select();
    
    if (error) {
      console.error('Database error:', error.message);
      return res.status(500).json({
        status: '00',
        message: 'Database error: ' + error.message
      });
    }
    
    console.log(`✓ Data inserted - Device: ${device}, Overflow: ${data}%`);
    
    // Success response (as per NHR specification)
    res.json({
      status: '01',
      message: ''
    });
    
  } catch (error) {
    console.error('Server error:', error.message);
    res.status(500).json({
      status: '00',
      message: 'Internal server error: ' + error.message
    });
  }
});

// Query endpoint - Get all sensor data with filters
app.get('/api/sensors', async (req, res) => {
  try {
    const { device, limit = 100, offset = 0 } = req.query;
    
    let query = supabase
      .from('sensor_data')
      .select('*')
      .order('created_at', { ascending: false })
      .range(parseInt(offset), parseInt(offset) + parseInt(limit) - 1);
    
    if (device) {
      query = query.eq('device_id', device);
    }
    
    const { data, error, count } = await query;
    
    if (error) throw error;
    
    res.json({ 
      data: data || [], 
      count: data?.length || 0 
    });
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

// Get latest data for a specific device
app.get('/api/sensors/:deviceId/latest', async (req, res) => {
  try {
    const { deviceId } = req.params;
    
    const { data, error } = await supabase
      .from('sensor_data')
      .select('*')
      .eq('device_id', deviceId)
      .order('created_at', { ascending: false })
      .limit(1)
      .single();
    
    if (error && error.code !== 'PGRST116') throw error; // PGRST116 = no rows
    
    res.json({ data: data || null });
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

// Get statistics for a device
app.get('/api/sensors/:deviceId/stats', async (req, res) => {
  try {
    const { deviceId } = req.params;
    
    const { data, error } = await supabase
      .from('sensor_data')
      .select('overflow_percentage, battery')
      .eq('device_id', deviceId);
    
    if (error) throw error;
    
    if (!data || data.length === 0) {
      return res.json({ stats: null });
    }
    
    // Calculate statistics
    const stats = {
      total_readings: data.length,
      avg_overflow: (data.reduce((sum, row) => sum + row.overflow_percentage, 0) / data.length).toFixed(2),
      max_overflow: Math.max(...data.map(row => row.overflow_percentage)),
      min_overflow: Math.min(...data.map(row => row.overflow_percentage)),
      avg_battery: (data.reduce((sum, row) => sum + row.battery, 0) / data.length).toFixed(2),
      min_battery: Math.min(...data.map(row => row.battery)).toFixed(2)
    };
    
    res.json({ stats });
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

// Get all unique devices
app.get('/api/devices', async (req, res) => {
  try {
    const { data, error } = await supabase
      .from('sensor_data')
      .select('device_id')
      .order('device_id');
    
    if (error) throw error;
    
    // Get unique device IDs
    const uniqueDevices = [...new Set(data.map(row => row.device_id))];
    
    res.json({ devices: uniqueDevices, count: uniqueDevices.length });
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

// Health check endpoint
app.get('/health', (req, res) => {
  res.json({ 
    status: 'ok', 
    timestamp: new Date().toISOString(),
    service: 'Wastebin Sensor API'
  });
});

// Root endpoint
app.get('/', (req, res) => {
  res.json({ 
    message: 'Wastebin Sensor API',
    endpoints: {
      post: '/MagnetAPI - Receive sensor data from NHR',
      get: '/api/sensors - Query all sensor data',
      get: '/api/sensors/:deviceId/latest - Get latest reading',
      get: '/api/sensors/:deviceId/stats - Get device statistics',
      get: '/api/devices - Get all devices',
      get: '/health - Health check'
    }
  });
});

// Start server
app.listen(PORT, () => {
  console.log(`🚀 Server running on port ${PORT}`);
  console.log(`📍 POST endpoint: /MagnetAPI`);
  console.log(`📊 Query endpoint: /api/sensors`);
});

// Graceful shutdown
process.on('SIGTERM', () => {
  console.log('SIGTERM signal received: closing server');
  process.exit(0);
});
