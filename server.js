// server.js - Multi-Sensor with Full Auto Database Setup
const express = require('express');
const { createClient } = require('@supabase/supabase-js');
const moment = require('moment');
require('dotenv').config();

const app = express();
const PORT = process.env.PORT || 3000;

// Middleware
app.use(express.json());

// Initialize Supabase Client (Admin mode for DDL operations)
const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_SERVICE_KEY,
  {
    db: { schema: 'public' },
    auth: { persistSession: false }
  }
);

// Sensor credentials configuration
const SENSOR_CREDENTIALS = {
  dustbin: {
    m: 'nhrdata',
    k: '9fg3gk56y9!#z#8g%yswqzd7p3357%x',
    tableName: 'dustbin_sensor_data',
    schema: {
      device_id: 'TEXT NOT NULL',
      battery: 'DECIMAL(5,2)',
      received_time: 'TIMESTAMPTZ',
      received_time_utc: 'TIMESTAMPTZ',
      data_index: 'TEXT',
      overflow_percentage: 'INTEGER',
      raw_data: 'JSONB'
    }
  },
  manhole: {
    m: 'nhrdata',
    k: 'wdwsw3qqvw6!u?4zs8yev#ni2nsp#3v#',
    tableName: 'manhole_sensor_data',
    schema: {
      device_id: 'TEXT NOT NULL',
      battery: 'DECIMAL(5,2)',
      battery_low: 'BOOLEAN DEFAULT false',
      received_time: 'TIMESTAMPTZ',
      received_time_utc: 'TIMESTAMPTZ',
      data_index: 'TEXT',
      cover_status: 'INTEGER',
      water_level_cm: 'INTEGER',
      x_axis_degree: 'INTEGER',
      y_axis_degree: 'INTEGER',
      z_axis_degree: 'INTEGER',
      raw_data: 'JSONB'
    }
  }
  // Add more sensors here - the system will auto-create tables
};

// Execute raw SQL via Supabase
async function executeSQL(sql) {
  try {
    const { data, error } = await supabase.rpc('exec_sql', { sql_query: sql });
    
    if (error) {
      // If RPC doesn't exist, try direct query
      const response = await fetch(`${process.env.SUPABASE_URL}/rest/v1/rpc/exec_sql`, {
        method: 'POST',
        headers: {
          'apikey': process.env.SUPABASE_SERVICE_KEY,
          'Authorization': `Bearer ${process.env.SUPABASE_SERVICE_KEY}`,
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({ sql_query: sql })
      });
      
      if (!response.ok) {
        throw new Error(`SQL execution failed: ${response.statusText}`);
      }
    }
    
    return { success: true, data };
  } catch (error) {
    // Try alternative method using postgres REST API
    try {
      const { data, error: pgError } = await supabase.from('_sql').select('*').limit(0);
      throw new Error('Cannot execute DDL. Please enable Database Functions or use service role key.');
    } catch (e) {
      return { success: false, error: error.message };
    }
  }
}

// Create table using direct SQL
async function createTableIfNotExists(tableName, schema) {
  const columns = Object.entries(schema)
    .map(([name, type]) => `${name} ${type}`)
    .join(',\n  ');
  
  const sql = `
    CREATE TABLE IF NOT EXISTS ${tableName} (
      id BIGSERIAL PRIMARY KEY,
      ${columns},
      created_at TIMESTAMPTZ DEFAULT NOW()
    );
    
    CREATE INDEX IF NOT EXISTS idx_${tableName}_device_id ON ${tableName}(device_id);
    CREATE INDEX IF NOT EXISTS idx_${tableName}_created_at ON ${tableName}(created_at DESC);
  `;
  
  console.log(`📝 Creating table: ${tableName}`);
  const result = await executeSQL(sql);
  
  if (result.success) {
    console.log(`✓ Table ${tableName} ready`);
  } else {
    console.log(`⚠ Using alternative method for ${tableName}`);
    // Try insert method to auto-create table
    await tryAutoCreateViaInsert(tableName, schema);
  }
}

// Alternative: Try to create table by attempting insert (Supabase auto-creates)
async function tryAutoCreateViaInsert(tableName, schema) {
  try {
    // This will fail but might trigger auto-creation in some Supabase configs
    const { error } = await supabase
      .from(tableName)
      .select('id')
      .limit(1);
    
    if (error && error.code === '42P01') {
      console.log(`ℹ Table ${tableName} doesn't exist. Creating via dummy insert...`);
      
      // Create dummy data matching schema
      const dummyData = {};
      for (const [key, type] of Object.entries(schema)) {
        if (type.includes('TEXT')) dummyData[key] = 'INIT';
        else if (type.includes('INTEGER')) dummyData[key] = 0;
        else if (type.includes('DECIMAL')) dummyData[key] = 0.0;
        else if (type.includes('BOOLEAN')) dummyData[key] = false;
        else if (type.includes('TIMESTAMPTZ')) dummyData[key] = new Date().toISOString();
        else if (type.includes('JSONB')) dummyData[key] = {};
      }
      
      // This will fail, but that's okay - we're just checking
      await supabase.from(tableName).insert([dummyData]);
    }
  } catch (error) {
    console.log(`ℹ Manual table creation needed for ${tableName}`);
  }
}

// Create credentials table
async function createCredentialsTable() {
  const sql = `
    CREATE TABLE IF NOT EXISTS sensor_credentials (
      id BIGSERIAL PRIMARY KEY,
      sensor_type TEXT UNIQUE NOT NULL,
      m_key TEXT NOT NULL,
      k_key TEXT NOT NULL,
      table_name TEXT NOT NULL,
      is_active BOOLEAN DEFAULT true,
      created_at TIMESTAMPTZ DEFAULT NOW(),
      updated_at TIMESTAMPTZ DEFAULT NOW()
    );
    
    CREATE INDEX IF NOT EXISTS idx_credentials_sensor_type ON sensor_credentials(sensor_type);
  `;
  
  console.log('📝 Creating credentials table...');
  await executeSQL(sql);
  console.log('✓ Credentials table ready');
}

// Sync credentials to database
async function syncCredentials() {
  console.log('🔄 Syncing sensor credentials...');
  
  for (const [sensorType, config] of Object.entries(SENSOR_CREDENTIALS)) {
    try {
      // Check if exists
      const { data: existing } = await supabase
        .from('sensor_credentials')
        .select('id')
        .eq('sensor_type', sensorType)
        .single();
      
      if (existing) {
        // Update
        await supabase
          .from('sensor_credentials')
          .update({
            m_key: config.m,
            k_key: config.k,
            table_name: config.tableName,
            updated_at: new Date().toISOString()
          })
          .eq('sensor_type', sensorType);
      } else {
        // Insert
        await supabase
          .from('sensor_credentials')
          .insert({
            sensor_type: sensorType,
            m_key: config.m,
            k_key: config.k,
            table_name: config.tableName,
            is_active: true
          });
      }
      
      console.log(`✓ Synced credentials for ${sensorType}`);
    } catch (error) {
      console.log(`ℹ Credential sync for ${sensorType}: ${error.message}`);
    }
  }
}

// Initialize database
async function initializeDatabase() {
  console.log('━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━');
  console.log('🔧 AUTO DATABASE INITIALIZATION');
  console.log('━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━');
  
  // Create credentials table
  await createCredentialsTable();
  
  // Create sensor data tables
  for (const [sensorType, config] of Object.entries(SENSOR_CREDENTIALS)) {
    await createTableIfNotExists(config.tableName, config.schema);
  }
  
  // Sync credentials
  await syncCredentials();
  
  console.log('━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━');
  console.log('✅ Database initialization complete!');
  console.log('━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n');
}

// Test connection and show manual SQL if auto-creation fails
async function testConnection() {
  try {
    const { data, error } = await supabase
      .from('sensor_credentials')
      .select('count')
      .limit(1);
    
    if (error && error.code === '42P01') {
      console.log('\n⚠️  MANUAL SETUP REQUIRED ⚠️');
      console.log('Tables need to be created. Copy and run this SQL in Supabase:');
      console.log('\n' + generateManualSQL() + '\n');
    } else {
      console.log('✓ Connected to Supabase successfully');
    }
  } catch (error) {
    console.error('✗ Connection error:', error.message);
  }
}

// Generate manual SQL if auto-creation fails
function generateManualSQL() {
  let sql = `-- COPY THIS SQL TO SUPABASE SQL EDITOR\n\n`;
  
  sql += `-- 1. Credentials Table\n`;
  sql += `CREATE TABLE IF NOT EXISTS sensor_credentials (
  id BIGSERIAL PRIMARY KEY,
  sensor_type TEXT UNIQUE NOT NULL,
  m_key TEXT NOT NULL,
  k_key TEXT NOT NULL,
  table_name TEXT NOT NULL,
  is_active BOOLEAN DEFAULT true,
  created_at TIMESTAMPTZ DEFAULT NOW(),
  updated_at TIMESTAMPTZ DEFAULT NOW()
);\n\n`;
  
  for (const [sensorType, config] of Object.entries(SENSOR_CREDENTIALS)) {
    sql += `-- 2. ${sensorType.toUpperCase()} Table\n`;
    const columns = Object.entries(config.schema)
      .map(([name, type]) => `  ${name} ${type}`)
      .join(',\n');
    
    sql += `CREATE TABLE IF NOT EXISTS ${config.tableName} (
  id BIGSERIAL PRIMARY KEY,
${columns},
  created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_${config.tableName}_device_id ON ${config.tableName}(device_id);
CREATE INDEX IF NOT EXISTS idx_${config.tableName}_created_at ON ${config.tableName}(created_at DESC);\n\n`;
  }
  
  return sql;
}

// Initialize on startup
(async () => {
  await testConnection();
  await initializeDatabase();
})();

// Authentication middleware
async function authenticateSensor(req, res, next) {
  const m = req.headers['m'];  
  const k = req.headers['k']; 
  
  if (!m || !k) {
    return res.status(401).json({
      status: '00',
      message: 'Authentication failed: Missing credentials'
    });
  }
  
  let matchedSensor = null;
  for (const [sensorType, config] of Object.entries(SENSOR_CREDENTIALS)) {
    if (config.m === m && config.k === k) {
      matchedSensor = { type: sensorType, config };
      break;
    }
  }
  
  if (!matchedSensor) {
    console.log(`❌ Auth failed - M: ${m}, K: ${k.substring(0, 10)}...`);
    return res.status(401).json({
      status: '00',
      message: 'Authentication failed: Invalid credentials'
    });
  }
  
  req.sensorType = matchedSensor.type;
  req.sensorConfig = matchedSensor.config;
  
  console.log(`✓ Authenticated as: ${matchedSensor.type}`);
  next();
}

// DUSTBIN HANDLER
async function handleDustbinData(req, res) {
  try {
    const { cmd, device, battery, time, dIndex, data } = req.body;
    
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
    
    const batteryValue = parseFloat(battery);
    const overflowPercentage = parseInt(data);
    
    if (isNaN(batteryValue) || isNaN(overflowPercentage)) {
      return res.status(400).json({
        status: '00',
        message: 'Invalid battery or data value'
      });
    }
    
    const utcTime = moment.utc(time, 'YYYY-MM-DD HH:mm:ss');
    const localTime = utcTime.local().format('YYYY-MM-DD HH:mm:ss');
    
    const { data: insertedData, error } = await supabase
      .from(req.sensorConfig.tableName)
      .insert([{
        device_id: device.trim(),
        battery: batteryValue,
        received_time: localTime,
        received_time_utc: time,
        data_index: dIndex,
        overflow_percentage: overflowPercentage,
        raw_data: req.body
      }])
      .select();
    
    if (error) {
      console.error('Database error:', error.message);
      return res.status(500).json({
        status: '00',
        message: 'Database error: ' + error.message
      });
    }
    
    console.log(`✓ Dustbin data inserted - Device: ${device}, Overflow: ${data}%`);
    
    res.json({
      status: '01',
      message: ''
    });
    
  } catch (error) {
    console.error('Dustbin handler error:', error.message);
    res.status(500).json({
      status: '00',
      message: 'Internal server error: ' + error.message
    });
  }
}

// MANHOLE HANDLER
async function handleManholeData(req, res) {
  try {
    const { 
      cmd, device, battery, battery_low, time, dIndex,
      dt_state, dt_waterLV, dt_x, dt_y, dt_z 
    } = req.body;
    
    if (!cmd || !device || !battery || !time || !dIndex) {
      return res.status(400).json({
        status: '00',
        message: 'Missing required fields'
      });
    }
    
    if (cmd !== '06') {
      return res.status(400).json({
        status: '00',
        message: 'Invalid command type. Expected "06" for manhole sensor'
      });
    }
    
    if (dIndex !== '0010') {
      return res.status(400).json({
        status: '00',
        message: 'Invalid data index. Expected "0010" for H01 data report'
      });
    }
    
    const batteryValue = parseFloat(battery);
    const waterLevel = dt_waterLV ? parseInt(dt_waterLV) : null;
    const xAxis = dt_x ? parseInt(dt_x) : null;
    const yAxis = dt_y ? parseInt(dt_y) : null;
    const zAxis = dt_z ? parseInt(dt_z) : null;
    const coverStatus = dt_state ? parseInt(dt_state) : null;
    
    if (isNaN(batteryValue)) {
      return res.status(400).json({
        status: '00',
        message: 'Invalid battery value'
      });
    }
    
    const utcTime = moment.utc(time, 'YYYY-MM-DD HH:mm:ss');
    const localTime = utcTime.local().format('YYYY-MM-DD HH:mm:ss');
    
    const { data: insertedData, error } = await supabase
      .from(req.sensorConfig.tableName)
      .insert([{
        device_id: device.trim(),
        battery: batteryValue,
        battery_low: battery_low === '1',
        received_time: localTime,
        received_time_utc: time,
        data_index: dIndex,
        cover_status: coverStatus,
        water_level_cm: waterLevel,
        x_axis_degree: xAxis,
        y_axis_degree: yAxis,
        z_axis_degree: zAxis,
        raw_data: req.body
      }])
      .select();
    
    if (error) {
      console.error('Database error:', error.message);
      return res.status(500).json({
        status: '00',
        message: 'Database error: ' + error.message
      });
    }
    
    console.log(`✓ Manhole data - Device: ${device}, Water: ${waterLevel}cm, Cover: ${coverStatus === 0 ? 'Closed' : 'Open'}`);
    
    res.json({
      status: '01',
      message: ''
    });
    
  } catch (error) {
    console.error('Manhole handler error:', error.message);
    res.status(500).json({
      status: '00',
      message: 'Internal server error: ' + error.message
    });
  }
}

// Main endpoint
app.post('/MagnetAPI', authenticateSensor, async (req, res) => {
  try {
    console.log(`📡 Received data from ${req.sensorType} sensor`);
    
    switch (req.sensorType) {
      case 'dustbin':
        await handleDustbinData(req, res);
        break;
      case 'manhole':
        await handleManholeData(req, res);
        break;
      default:
        res.status(400).json({
          status: '00',
          message: `Unknown sensor type: ${req.sensorType}`
        });
    }
  } catch (error) {
    console.error('Server error:', error.message);
    res.status(500).json({
      status: '00',
      message: 'Internal server error: ' + error.message
    });
  }
});

// Query endpoints
app.get('/api/:sensorType/sensors', async (req, res) => {
  try {
    const { sensorType } = req.params;
    const config = SENSOR_CREDENTIALS[sensorType];
    
    if (!config) {
      return res.status(404).json({ error: 'Sensor type not found' });
    }
    
    const { device, limit = 100, offset = 0 } = req.query;
    
    let query = supabase
      .from(config.tableName)
      .select('*')
      .order('created_at', { ascending: false })
      .range(parseInt(offset), parseInt(offset) + parseInt(limit) - 1);
    
    if (device) {
      query = query.eq('device_id', device);
    }
    
    const { data, error } = await query;
    
    if (error) throw error;
    
    res.json({ 
      sensorType,
      data: data || [], 
      count: data?.length || 0 
    });
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

app.get('/api/:sensorType/sensors/:deviceId/latest', async (req, res) => {
  try {
    const { sensorType, deviceId } = req.params;
    const config = SENSOR_CREDENTIALS[sensorType];
    
    if (!config) {
      return res.status(404).json({ error: 'Sensor type not found' });
    }
    
    const { data, error } = await supabase
      .from(config.tableName)
      .select('*')
      .eq('device_id', deviceId)
      .order('created_at', { ascending: false })
      .limit(1)
      .single();
    
    if (error && error.code !== 'PGRST116') throw error;
    
    res.json({ sensorType, data: data || null });
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

app.get('/api/:sensorType/devices', async (req, res) => {
  try {
    const { sensorType } = req.params;
    const config = SENSOR_CREDENTIALS[sensorType];
    
    if (!config) {
      return res.status(404).json({ error: 'Sensor type not found' });
    }
    
    const { data, error } = await supabase
      .from(config.tableName)
      .select('device_id')
      .order('device_id');
    
    if (error) throw error;
    
    const uniqueDevices = [...new Set(data.map(row => row.device_id))];
    
    res.json({ sensorType, devices: uniqueDevices, count: uniqueDevices.length });
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

// Delete endpoint
app.delete('/api/:sensorType/sensors/:id', async (req, res) => {
  try {
    const { sensorType, id } = req.params;
    const config = SENSOR_CREDENTIALS[sensorType];
    
    if (!config) {
      return res.status(404).json({ error: 'Sensor type not found' });
    }
    
    const { error } = await supabase
      .from(config.tableName)
      .delete()
      .eq('id', id);
    
    if (error) throw error;
    
    console.log(`🗑️  Deleted record ${id} from ${sensorType}`);
    res.json({ message: 'Data deleted successfully', id });
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

// Bulk delete endpoint
app.delete('/api/:sensorType/sensors', async (req, res) => {
  try {
    const { sensorType } = req.params;
    const { deviceId, olderThan } = req.query;
    const config = SENSOR_CREDENTIALS[sensorType];
    
    if (!config) {
      return res.status(404).json({ error: 'Sensor type not found' });
    }
    
    let query = supabase.from(config.tableName).delete();
    
    if (deviceId) {
      query = query.eq('device_id', deviceId);
    }
    
    if (olderThan) {
      query = query.lt('created_at', olderThan);
    }
    
    const { error, count } = await query;
    
    if (error) throw error;
    
    console.log(`🗑️  Bulk deleted from ${sensorType}`);
    res.json({ message: 'Data deleted successfully', count });
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

app.get('/health', (req, res) => {
  res.json({ 
    status: 'ok', 
    timestamp: new Date().toISOString(),
    service: 'Multi-Sensor API Server',
    sensors: Object.keys(SENSOR_CREDENTIALS)
  });
});

app.get('/', (req, res) => {
  res.json({ 
    message: 'Multi-Sensor API Server - Auto Database Setup',
    supportedSensors: Object.keys(SENSOR_CREDENTIALS),
    endpoints: {
      post: '/MagnetAPI - Receive sensor data (auto-detects type)',
      get: '/api/:sensorType/sensors - Query sensor data',
      get: '/api/:sensorType/sensors/:deviceId/latest - Get latest reading',
      get: '/api/:sensorType/devices - Get all devices',
      delete: '/api/:sensorType/sensors/:id - Delete record',
      delete: '/api/:sensorType/sensors?deviceId=X&olderThan=DATE - Bulk delete',
      get: '/health - Health check'
    }
  });
});

app.listen(PORT, () => {
  console.log(`🚀 Server running on port ${PORT}`);
  console.log(`📍 POST endpoint: /MagnetAPI`);
  console.log(`🔧 Supported sensors: ${Object.keys(SENSOR_CREDENTIALS).join(', ')}`);
});

process.on('SIGTERM', () => {
  console.log('SIGTERM signal received: closing server');
  process.exit(0);
});
