# Portfolio WebSocket API

A FastAPI application that demonstrates real-time portfolio analysis progress using WebSockets. Perfect for showing users live progress updates while processing large datasets.

## 🚀 Features

- **Real-time Progress Updates**: WebSocket-based progress tracking
- **RESTful API**: Standard HTTP endpoints for integration
- **PostgreSQL Integration**: Connects to your existing database (with dummy data fallback)
- **Multiple Portfolio Types**: Stocks, Bonds, Cryptocurrency, Real Estate
- **Interactive Demo**: Built-in web interface for testing
- **Postman Ready**: Included collection for API testing

## 📁 Project Structure

```
websocket/
├── main.py                 # FastAPI application
├── client.js              # Node.js WebSocket client
├── test_requests.js       # Automated test suite
├── package.json           # Node.js dependencies
├── requirements.txt       # Python dependencies
├── .env                   # Environment configuration
├── Postman_Collection.json # Postman test collection
└── README.md              # This file
```

## 🛠️ Installation

### 1. Python Environment Setup

```bash
# Navigate to websocket folder
cd /home/pranjal/Downloads/RACE/Helpers/websocket

# Install Python dependencies (if not using pipenv from parent)
pip install -r requirements.txt

# Or use pipenv from parent directory
cd ..
pipenv shell
cd websocket
```

### 2. Node.js Setup (for testing)

```bash
# Install Node.js dependencies
npm install
```

### 3. Environment Configuration

Edit `.env` file with your database credentials:

```env
PG_HOST=your_host
PG_DB=your_database
PG_USER=your_username
PG_PW=your_password
PG_PORT=5432
```

## 🏃‍♂️ Running the Application

### Start the FastAPI Server

```bash
# Method 1: Direct Python
python main.py

# Method 2: Using uvicorn
uvicorn main:app --reload --host 0.0.0.0 --port 8000

# Method 3: With pipenv
pipenv run python main.py
```

The server will start at: `http://localhost:8000`

### Access the Demo Page

Open your browser and go to: `http://localhost:8000`

This provides a complete web interface to test the WebSocket functionality.

## 🧪 Testing Methods

### 1. Browser Demo (Easiest)

1. Start the FastAPI server
2. Open `http://localhost:8000` in your browser
3. Fill in user ID and select portfolio types
4. Click "Start Analysis"
5. Watch real-time progress updates

### 2. Node.js Client

```bash
# Run single test
node client.js

# Run comprehensive test suite
node test_requests.js
```

### 3. Postman Testing

#### Import Collection:
1. Open Postman
2. File → Import
3. Select `Postman_Collection.json`

#### Test Steps:
1. **Start Analysis**: Use "Start Portfolio Analysis" request
2. **Copy Task ID**: From the response, copy the `task_id`
3. **Check Status**: Use "Check Analysis Status" (replace `{{task_id}}`)
4. **Get Results**: Use "Get Final Results" when status is "completed"

#### Sample Requests:

**Start Analysis:**
```bash
POST http://localhost:8000/api/portfolio/analyze
Content-Type: application/json

{
    "user_id": "test_user_123",
    "portfolio_types": ["stocks", "bonds", "crypto"],
    "calculation_type": "full_analysis"
}
```

**Check Status:**
```bash
GET http://localhost:8000/api/portfolio/status/{task_id}
```

**Get Results:**
```bash
GET http://localhost:8000/api/portfolio/result/{task_id}
```

### 4. WebSocket Testing

#### Using wscat (install with `npm install -g wscat`):

```bash
# Connect to WebSocket
wscat -c ws://localhost:8000/ws/test_user

# Send ping
{"type": "ping"}

# Check task status
{"type": "get_status", "task_id": "your-task-id"}
```

#### Using JavaScript in Browser Console:

```javascript
// Connect to WebSocket
const ws = new WebSocket('ws://localhost:8000/ws/test_user');

// Listen for messages
ws.onmessage = function(event) {
    console.log('Received:', JSON.parse(event.data));
};

// Send ping
ws.send(JSON.stringify({type: 'ping'}));
```

## 📊 API Endpoints

### REST API

| Method | Endpoint | Description |
|--------|----------|-------------|
| POST | `/api/portfolio/analyze` | Start portfolio analysis |
| GET | `/api/portfolio/status/{task_id}` | Get current status |
| GET | `/api/portfolio/result/{task_id}` | Get final results |
| GET | `/` | Demo web page |

### WebSocket

| URL | Description |
|-----|-------------|
| `ws://localhost:8000/ws/{user_id}` | Real-time progress updates |

## 🔄 Progress Flow

1. **User starts analysis** → POST to `/api/portfolio/analyze`
2. **Server returns task_id** → Client can track progress
3. **WebSocket sends updates** → Real-time progress: 1/10, 2/10, etc.
4. **Processing completes** → Final results available
5. **Client gets results** → GET from `/api/portfolio/result/{task_id}`

## 📈 Sample Progress Messages

```json
{
    "task_id": "123e4567-e89b-12d3-a456-426614174000",
    "user_id": "test_user",
    "total_items": 10,
    "processed_items": 3,
    "current_step": "Processing stocks",
    "progress_percentage": 30.0,
    "status": "processing"
}
```

## 🔧 Customization

### Adding Real Database Data

Replace the `generate_dummy_portfolio_data()` function in `main.py` with your actual database queries:

```python
def get_real_portfolio_data(user_id: str) -> Dict:
    conn = get_db_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    
    # Your actual queries here
    cursor.execute("SELECT * FROM user_stocks WHERE user_id = %s", (user_id,))
    stocks = cursor.fetchall()
    
    # Return structured data
    return {"stocks": stocks, "bonds": [], ...}
```

### Adjusting Processing Speed

Modify the sleep time in `process_portfolio_analysis()`:

```python
# Faster processing
await asyncio.sleep(random.uniform(0.1, 0.3))

# Slower processing (for demo)
await asyncio.sleep(random.uniform(1.0, 2.0))
```

## 🐛 Troubleshooting

### Common Issues:

1. **Port already in use**: Change port in `main.py` or kill existing process
2. **Database connection fails**: Check `.env` file, app will use dummy data
3. **WebSocket connection refused**: Ensure FastAPI server is running
4. **Node.js tests fail**: Install dependencies with `npm install`

### Debug Mode:

Set `DEBUG=True` in `.env` for verbose logging.

## 🏗️ Architecture

```
Frontend (Browser/Postman)
    ↓ HTTP Request
FastAPI Server
    ↓ WebSocket
Real-time Updates
    ↓ Background Task
Portfolio Processing
    ↓ Database Query
PostgreSQL (or dummy data)
```

## 🎯 Use Cases

- **Financial Portfolio Management**: Real-time calculation of portfolio values
- **Data Processing Jobs**: Show progress of large data operations
- **Report Generation**: Live updates during report creation
- **Batch Operations**: Progress tracking for bulk operations
- **Import/Export**: Real-time feedback during file processing

## 📝 Testing Checklist

- [ ] FastAPI server starts successfully
- [ ] Demo page loads at `http://localhost:8000`
- [ ] WebSocket connects and receives ping/pong
- [ ] POST `/api/portfolio/analyze` returns task_id
- [ ] WebSocket receives progress updates
- [ ] GET `/api/portfolio/status/{task_id}` returns current status
- [ ] Analysis completes with status "completed"
- [ ] GET `/api/portfolio/result/{task_id}` returns final data
- [ ] Node.js client runs successfully
- [ ] Postman collection imports and works

## 🚀 Production Considerations

- Add authentication and authorization
- Implement rate limiting
- Add database connection pooling
- Use Redis for task storage in distributed setup
- Add error handling and retry logic
- Implement WebSocket connection cleanup
- Add monitoring and logging
🚀 What's Running:
✅ FastAPI WebSocket Server on http://localhost:8000
✅ Real-time Progress Tracking via WebSocket
✅ Beautiful Dashboard at http://localhost:8000
✅ RESTful API endpoints for portfolio processing
✅ Node.js testing clients ready to use
🔧 How to Test in Postman:
WebSocket Connection Test:

Import the Postman_Collection.json file
Test WebSocket at: ws://localhost:8000/ws/user123
API Endpoints:

Frontend Testing:

Open: http://localhost:8000
Click "Connect WebSocket"
Click any processing button to see real-time progress!
📊 Features Working:
✅ Real-time progress updates (1/10, 2/10, etc.)
✅ Processing simulation for portfolio, assets, investments
✅ WebSocket bidirectional communication
✅ Beautiful progress bars and statistics
✅ Message logging with timestamps
✅ Connection status indicators
✅ Multiple processing types
🎯 Test Commands:
# Test with Node.js
node test_client.js portfolio
node test_client.js assets  
node test_client.js investments

# Test multiple scenarios
node test_requests.js
Your WebSocket portfolio processing system is 100% functional and ready for real-time asset calculation with beautiful progress tracking! 🎊