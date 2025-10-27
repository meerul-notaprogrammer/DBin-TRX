// server.js - Multi-Sensor with ENV Configuration
const express = require('express');
const { createClient } = require('@supabase/supabase-js');
const moment = require('moment');
require('dotenv').config();

const app = express();
const PORT = process.env.PORT || 3000;

// Middleware
app.use(express.json());
app.use(express.urlencoded({ extended: true }));

// Debug middleware - log all requests
app.use((req, res, next) => {
  console.log('\n📨 INCOMING REQUEST:');
  console.log('Method:', req.method);
  console.log('Path:', req.path);
  console.log('Headers:', JSON.stringify(req.headers, null, 2));
  console.log('Body:', JSON.stringify(req.body, null, 2));
  console.log('Raw Body Type:', typeof req.body);
  next();
});

// Initialize Supabase Client
const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_SERVICE_KEY,
  {
    db: { schema: 'public' },
    auth: { persistSession: false }
  }
);

// Load sensor configurations from ENV
function loadSensorConfigs() {
  const configs = {};
  
  // Parse sensor list from ENV (comma-separated)
  const sensorTypes = process.env.SENSOR_TYPES ? 
    process.env.SENSOR_TYPES.split(',').map(s => s.trim()) : 
    ['dustbin', 'manhole'];
  
  console.log('📡 Loading sensor configurations...');
  
  for (const sensorType of sensorTypes) {
    const prefix = sensorType.toUpperCase();
    
    // Get header keys from ENV
    const headerM = process.env[`${prefix}_HEADER_M`];
    const headerK = process.env[`${prefix}_HEADER_K`];
    const tableName = process.env[`${prefix}_TABLE_NAME`] || `${sensorType}_sensor_data`;
    
    // Get schema from ENV (JSON string)
    const schemaJson = process.env[`${prefix}_SCHEMA`];
    
    if (!headerM || !headerK) {
      console.log(`⚠️  Skipping ${sensorType}: Missing headers in ENV`);
      continue;
    }
    
    // Default schemas if not provided
    let schema;
    if (schemaJson) {
      try {
        schema = JSON.parse(schemaJson);
      } catch (e) {
        console.log(`⚠️  Invalid schema JSON for ${sensorType}, using defaults`);
        schema = getDefaultSchema(sensorType);
      }
    } else {
      schema = getDefaultSchema(sensorType);
    }
    
    configs[sensorType] = {
      m: headerM,
      k: headerK,
      tableName: tableName,
      schema: schema,
      // Additional header names (customizable)
      headerNames: {
        m: process.env[`${prefix}_HEADER_M_NAME`] || 'm',
        k: process.env[`${prefix}_HEADER_K_NAME`] || 'k'
      },
      // Command validation
      expectedCmd: process.env[`${prefix}_EXPECTED_CMD`],
      expectedIndex: process.env[`${prefix}_EXPECTED_INDEX`]
    };
    
    console.log(`✓ Loaded config for ${sensorType}`);
  }
  
  return configs;
}

// Default schemas for known sensor types
function getDefaultSchema(sensorType) {
  const schemas = {
    dustbin: {
      device_id: 'TEXT NOT NULL',
      battery: 'DECIMAL(5,2)',
      received_time: 'TIMESTAMPTZ',
      received_time_utc: 'TIMESTAMPTZ',
      data_index: 'TEXT',
      overflow_percentage: 'INTEGER',
      raw_data: 'JSONB'
    },
    manhole: {
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
    },
    default: {
      device_id: 'TEXT NOT NULL',
      battery: 'DECIMAL(5,2)',
      received_time: 'TIMESTAMPTZ',
      received_time_utc: 'TIMESTAMPTZ',
      data_index: 'TEXT',
      raw_data: 'JSONB'
    }
  };
  
  return schemas[sensorType] || schemas.default;
}

// Load configurations from ENV
const SENSOR_CREDENTIALS = loadSensorConfigs();

// Execute raw SQL
async function executeSQL(sql) {
  try {
    const { data, error } = await supabase.rpc('exec_sql', { sql_query: sql });
    
    if (error) {
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
    return { success: false, error: error.message };
  }
}

// Create table
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
    console.log(`⚠ Manual setup may be needed for ${tableName}`);
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
      const { data: existing } = await supabase
        .from('sensor_credentials')
        .select('id')
        .eq('sensor_type', sensorType)
        .single();
      
      if (existing) {
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
  
  await createCredentialsTable();
  
  for (const [sensorType, config] of Object.entries(SENSOR_CREDENTIALS)) {
    await createTableIfNotExists(config.tableName, config.schema);
  }
  
  await syncCredentials();
  
  console.log('━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━');
  console.log('✅ Database initialization complete!');
  console.log(`📊 Active sensors: ${Object.keys(SENSOR_CREDENTIALS).join(', ')}`);
  console.log('━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n');
}

// Test connection
async function testConnection() {
  try {
    const { data, error } = await supabase
      .from('sensor_credentials')
      .select('count')
      .limit(1);
    
    if (error && error.code === '42P01') {
      console.log('\n⚠️  MANUAL SETUP MAY BE REQUIRED');
      console.log('If auto-creation fails, check Supabase SQL Editor\n');
    } else {
      console.log('✓ Connected to Supabase successfully');
    }
  } catch (error) {
    console.error('✗ Connection error:', error.message);
  }
}

// Initialize on startup
(async () => {
  console.log('\n🚀 STARTING MULTI-SENSOR SERVER');
  console.log('═══════════════════════════════════════════\n');
  await testConnection();
  await initializeDatabase();
})();

// Authentication middleware
async function authenticateSensor(req, res, next) {
  // Try to find matching credentials
  let matchedSensor = null;
  
  for (const [sensorType, config] of Object.entries(SENSOR_CREDENTIALS)) {
    // Get header values using custom header names
    const m = req.headers[config.headerNames.m.toLowerCase()];
    const k = req.headers[config.headerNames.k.toLowerCase()];
    
    if (m && k && config.m === m && config.k === k) {
      matchedSensor = { type: sensorType, config };
      break;
    }
  }
  
  if (!matchedSensor) {
    console.log(`❌ Authentication failed`);
    console.log(`   Headers received:`, Object.keys(req.headers).filter(h => !h.startsWith('host')));
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

// Generic data handler
async function handleSensorData(req, res) {
  try {
    const { cmd, device, battery, time, dIndex } = req.body;
    const config = req.sensorConfig;
    
    // Basic validation
    if (!device || !battery || !time) {
      return res.status(400).json({
        status: '00',
        message: 'Missing required fields: device, battery, time'
      });
    }
    
    // Optional: Command validation from ENV
    if (config.expectedCmd && cmd !== config.expectedCmd) {
      return res.status(400).json({
        status: '00',
        message: `Invalid command type. Expected "${config.expectedCmd}", got "${cmd}"`
      });
    }
    
    // Optional: Index validation from ENV
    if (config.expectedIndex && dIndex !== config.expectedIndex) {
      return res.status(400).json({
        status: '00',
        message: `Invalid data index. Expected "${config.expectedIndex}", got "${dIndex}"`
      });
    }
    
    const batteryValue = parseFloat(battery);
    if (isNaN(batteryValue)) {
      return res.status(400).json({
        status: '00',
        message: 'Invalid battery value'
      });
    }
    
    // Convert time
    const utcTime = moment.utc(time, 'YYYY-MM-DD HH:mm:ss');
    const localTime = utcTime.local().format('YYYY-MM-DD HH:mm:ss');
    
    // Build insert data dynamically based on request body
    const insertData = {
      device_id: device.trim(),
      battery: batteryValue,
      received_time: localTime,
      received_time_utc: time,
      data_index: dIndex,
      raw_data: req.body
    };
    
    // Map additional fields dynamically
    for (const [key, value] of Object.entries(req.body)) {
      if (!['cmd', 'device', 'battery', 'time', 'dIndex'].includes(key)) {
        // Handle different data types
        if (typeof value === 'string') {
          const numValue = parseFloat(value);
          if (!isNaN(numValue)) {
            insertData[key] = numValue;
          } else if (value === '0' || value === '1') {
            insertData[key] = value === '1';
          } else {
            insertData[key] = value;
          }
        } else {
          insertData[key] = value;
        }
      }
    }
    
    // Insert into database
    const { data: insertedData, error } = await supabase
      .from(config.tableName)
      .insert([insertData])
      .select();
    
    if (error) {
      console.error('Database error:', error.message);
      return res.status(500).json({
        status: '00',
        message: 'Database error: ' + error.message
      });
    }
    
    console.log(`✓ ${req.sensorType} data inserted - Device: ${device}`);
    
    res.json({
      status: '01',
      message: ''
    });
    
  } catch (error) {
    console.error('Handler error:', error.message);
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
    await handleSensorData(req, res);
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

app.get('/health', (req, res) => {
  res.json({ 
    status: 'ok', 
    timestamp: new Date().toISOString(),
    service: 'Multi-Sensor API Server',
    sensors: Object.keys(SENSOR_CREDENTIALS),
    config: Object.entries(SENSOR_CREDENTIALS).map(([type, cfg]) => ({
      type,
      table: cfg.tableName,
      headers: cfg.headerNames
    }))
  });
});

app.get('/', (req, res) => {
  res.json({ 
    message: 'Multi-Sensor API Server - ENV Configured',
    supportedSensors: Object.keys(SENSOR_CREDENTIALS),
    endpoints: {
      post: '/MagnetAPI - Receive sensor data (auto-detects type)',
      get: '/api/:sensorType/sensors - Query sensor data',
      get: '/api/:sensorType/sensors/:deviceId/latest - Get latest reading',
      get: '/api/:sensorType/devices - Get all devices',
      delete: '/api/:sensorType/sensors/:id - Delete record',
      get: '/health - Health check'
    }
  });
});

app.listen(PORT, () => {
  console.log(`\n🚀 Server running on port ${PORT}`);
  console.log(`📍 POST endpoint: /MagnetAPI`);
  console.log(`🔧 Configured sensors: ${Object.keys(SENSOR_CREDENTIALS).join(', ')}`);
  console.log('\n═══════════════════════════════════════════\n');
});

process.on('SIGTERM', () => {
  console.log('SIGTERM signal received: closing server');
  process.exit(0);
});
