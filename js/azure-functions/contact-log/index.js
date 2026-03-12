const sql = require('mssql');

// Configuration
const ALLOWED_ORIGINS = [
  'https://clugodmd.github.io',
  'http://localhost:3000',
  'http://localhost:8000'
];

const CONTACT_TYPES = ['sms', 'call', 'email', 'weave', 'zcode', 'skip'];
const CONTACT_STATUSES = ['completed', 'failed', 'pending'];

// Connection pool (reuse across invocations for efficiency)
let pool = null;

// Initialize database connection pool
async function getPool() {
  if (pool) return pool;

  const connStr = process.env.AZURE_SQL_CONN_STR;
  if (!connStr) {
    throw new Error('AZURE_SQL_CONN_STR environment variable not set');
  }

  pool = new sql.ConnectionPool(connStr);
  await pool.connect();
  return pool;
}

// CORS helper
function setCorsHeaders(res, origin) {
  if (ALLOWED_ORIGINS.includes(origin)) {
    res.headers['Access-Control-Allow-Origin'] = origin;
  }
  res.headers['Access-Control-Allow-Methods'] = 'GET, POST, PATCH, OPTIONS';
  res.headers['Access-Control-Allow-Headers'] = 'Content-Type, Authorization';
}

// Validation helper
function validateContactEntry(data) {
  const errors = [];

  if (!data.row_id || typeof data.row_id !== 'string') {
    errors.push('row_id is required (string)');
  }
  if (!data.contact_type || !CONTACT_TYPES.includes(data.contact_type)) {
    errors.push(`contact_type must be one of: ${CONTACT_TYPES.join(', ')}`);
  }
  if (!data.staff_name || typeof data.staff_name !== 'string') {
    errors.push('staff_name is required (string)');
  }
  if (!data.workstation || typeof data.workstation !== 'string') {
    errors.push('workstation is required (string)');
  }

  return errors;
}

// GET: Fetch all active contacts or poll for updates
async function handleGet(req) {
  try {
    const pool = await getPool();
    const since = req.query.since;

    let query = `
      SELECT id, row_id, household_id, phone, contact_name, contact_type,
             contact_status, message, staff_name, workstation, office, created_at
      FROM rpt.vw_active_contacts
    `;
    const request = pool.request();

    if (since) {
      // Parse the ISO timestamp
      const sinceDate = new Date(since);
      if (isNaN(sinceDate.getTime())) {
        return {
          status: 400,
          body: JSON.stringify({ error: 'Invalid since timestamp format' })
        };
      }
      query += ' WHERE created_at > @since';
      request.input('since', sql.DateTime2, sinceDate);
    }

    query += ' ORDER BY created_at DESC';

    const result = await request.query(query);

    return {
      status: 200,
      body: JSON.stringify({
        success: true,
        data: result.recordset,
        count: result.recordset.length,
        timestamp: new Date().toISOString()
      })
    };
  } catch (error) {
    return {
      status: 500,
      body: JSON.stringify({
        error: 'Database query failed',
        message: error.message
      })
    };
  }
}

// POST: Create a new contact log entry
async function handlePost(req) {
  try {
    const body = req.body;

    // Validate input
    const errors = validateContactEntry(body);
    if (errors.length > 0) {
      return {
        status: 400,
        body: JSON.stringify({
          error: 'Validation failed',
          details: errors
        })
      };
    }

    const pool = await getPool();
    const request = pool.request();

    // Prepare parameters
    request.input('row_id', sql.NVarChar(100), body.row_id);
    request.input('household_id', sql.NVarChar(50), body.household_id || null);
    request.input('phone', sql.NVarChar(20), body.phone || null);
    request.input('contact_name', sql.NVarChar(200), body.contact_name || null);
    request.input('contact_type', sql.NVarChar(20), body.contact_type);
    request.input('contact_status', sql.NVarChar(20), body.contact_status || 'completed');
    request.input('message', sql.NVarChar(500),
      (body.message || '').substring(0, 500) || null);
    request.input('staff_name', sql.NVarChar(100), body.staff_name);
    request.input('workstation', sql.NVarChar(100), body.workstation);
    request.input('office', sql.NVarChar(50), body.office || null);

    // Insert and return the new record
    const result = await request.query(`
      INSERT INTO rpt.contact_log
        (row_id, household_id, phone, contact_name, contact_type,
         contact_status, message, staff_name, workstation, office)
      VALUES
        (@row_id, @household_id, @phone, @contact_name, @contact_type,
         @contact_status, @message, @staff_name, @workstation, @office);

      SELECT @@IDENTITY as id;
    `);

    const insertedId = result.recordset[0].id;

    // Fetch and return the newly created record
    const selectResult = await pool.request()
      .input('id', sql.Int, insertedId)
      .query(`
        SELECT id, row_id, household_id, phone, contact_name, contact_type,
               contact_status, message, staff_name, workstation, office, created_at
        FROM rpt.contact_log
        WHERE id = @id
      `);

    return {
      status: 201,
      body: JSON.stringify({
        success: true,
        data: selectResult.recordset[0]
      })
    };
  } catch (error) {
    return {
      status: 500,
      body: JSON.stringify({
        error: 'Failed to create contact log entry',
        message: error.message
      })
    };
  }
}

// PATCH: Undo a contact log entry
async function handlePatch(req, route) {
  try {
    // Extract ID from route parameter
    const routeParts = route.split('/');
    let contactId = null;
    let undoIndex = routeParts.indexOf('undo');

    if (undoIndex > 0) {
      contactId = parseInt(routeParts[undoIndex - 1], 10);
    }

    if (!contactId || isNaN(contactId)) {
      return {
        status: 400,
        body: JSON.stringify({ error: 'Invalid contact ID in route' })
      };
    }

    const pool = await getPool();
    const request = pool.request();

    // Update the record to mark as undone
    const result = await request
      .input('id', sql.Int, contactId)
      .query(`
        UPDATE rpt.contact_log
        SET is_undone = 1
        WHERE id = @id;

        SELECT id, row_id, contact_name, contact_type, staff_name,
               workstation, created_at, is_undone
        FROM rpt.contact_log
        WHERE id = @id;
      `);

    if (result.recordset.length === 0) {
      return {
        status: 404,
        body: JSON.stringify({ error: 'Contact log entry not found' })
      };
    }

    return {
      status: 200,
      body: JSON.stringify({
        success: true,
        data: result.recordset[0],
        message: 'Contact entry marked as undone'
      })
    };
  } catch (error) {
    return {
      status: 500,
      body: JSON.stringify({
        error: 'Failed to undo contact entry',
        message: error.message
      })
    };
  }
}

// Main Azure Function handler
module.exports = async function (context, req) {
  const method = req.method;
  const origin = req.headers.origin || '';
  const route = (req.params.route || '').toLowerCase().trim('/');

  // Set CORS headers
  const res = {
    status: 200,
    headers: {
      'Content-Type': 'application/json'
    }
  };
  setCorsHeaders(res, origin);

  // Handle CORS preflight
  if (method === 'OPTIONS') {
    return {
      status: 200,
      headers: res.headers,
      body: ''
    };
  }

  let response;

  try {
    switch (method) {
      case 'GET':
        response = await handleGet(req);
        break;
      case 'POST':
        response = await handlePost(req);
        break;
      case 'PATCH':
        response = await handlePatch(req, route);
        break;
      default:
        response = {
          status: 405,
          body: JSON.stringify({ error: 'Method not allowed' })
        };
    }
  } catch (error) {
    response = {
      status: 500,
      body: JSON.stringify({
        error: 'Unexpected server error',
        message: error.message
      })
    };
  }

  // Merge response headers with CORS headers
  if (response.headers) {
    Object.assign(res.headers, response.headers);
  } else {
    response.headers = res.headers;
  }

  return response;
};
