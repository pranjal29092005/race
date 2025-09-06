from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse
from pydantic import BaseModel
import asyncio
import json
import random
import time
from datetime import datetime
from typing import List, Dict, Optional
import uuid
import psycopg2
from psycopg2.extras import RealDictCursor
import os
from dotenv import load_dotenv

load_dotenv()

app = FastAPI(title="Portfolio Processing WebSocket API")

# Mount static files
app.mount("/static", StaticFiles(directory="static"), name="static")

# Connection manager for WebSocket connections
class ConnectionManager:
    def __init__(self):
        self.active_connections: Dict[str, WebSocket] = {}

    async def connect(self, websocket: WebSocket, client_id: str):
        await websocket.accept()
        self.active_connections[client_id] = websocket

    def disconnect(self, client_id: str):
        if client_id in self.active_connections:
            del self.active_connections[client_id]

    async def send_personal_message(self, message: dict, client_id: str):
        if client_id in self.active_connections:
            await self.active_connections[client_id].send_text(json.dumps(message))

    async def broadcast(self, message: dict):
        for connection in self.active_connections.values():
            await connection.send_text(json.dumps(message))

manager = ConnectionManager()

# Pydantic models
class PortfolioRequest(BaseModel):
    user_id: str
    portfolio_types: List[str] = ["stocks", "bonds", "crypto", "real_estate"]
    calculation_type: str = "full_analysis"  # full_analysis, quick_summary
    
class ProcessingStatus(BaseModel):
    task_id: str
    user_id: str
    total_items: int
    processed_items: int
    current_step: str
    progress_percentage: float
    status: str  # "processing", "completed", "failed"
    data: Optional[Dict] = None

# In-memory storage for processing tasks
processing_tasks: Dict[str, ProcessingStatus] = {}

# Database connection (with fallback to dummy data)
def get_db_connection():
    try:
        conn = psycopg2.connect(
            host=os.getenv("PG_HOST", "localhost"),
            database=os.getenv("PG_DB", "portfolio_db"),
            user=os.getenv("PG_USER", "postgres"),
            password=os.getenv("PG_PW", "password"),
            port=os.getenv("PG_PORT", 5432)
        )
        return conn
    except Exception as e:
        print(f"Database connection failed: {e}")
        return None

# Dummy data for testing
def generate_dummy_portfolio_data(user_id: str) -> Dict:
    return {
        "stocks": [
            {"symbol": "AAPL", "shares": 100, "current_price": 150.25},
            {"symbol": "GOOGL", "shares": 50, "current_price": 2500.75},
            {"symbol": "MSFT", "shares": 75, "current_price": 300.50},
            {"symbol": "TSLA", "shares": 25, "current_price": 800.00},
            {"symbol": "AMZN", "shares": 30, "current_price": 3200.25}
        ],
        "bonds": [
            {"type": "US Treasury", "amount": 10000, "yield": 4.5},
            {"type": "Corporate Bond", "amount": 15000, "yield": 5.2}
        ],
        "crypto": [
            {"symbol": "BTC", "amount": 0.5, "current_price": 45000},
            {"symbol": "ETH", "amount": 2.0, "current_price": 3000}
        ],
        "real_estate": [
            {"property": "Residential Property 1", "value": 250000},
            {"property": "Commercial Property 1", "value": 500000}
        ]
    }

# Portfolio processing simulation
async def process_portfolio_analysis(task_id: str, user_id: str, portfolio_types: List[str]):
    try:
        # Get portfolio data
        conn = get_db_connection()
        if conn:
            # Try to get real data from database
            cursor = conn.cursor(cursor_factory=RealDictCursor)
            # Add your actual database queries here
            portfolio_data = generate_dummy_portfolio_data(user_id)  # Fallback to dummy
        else:
            portfolio_data = generate_dummy_portfolio_data(user_id)
        
        # Calculate total items to process
        total_items = sum(len(portfolio_data.get(ptype, [])) for ptype in portfolio_types)
        
        processing_tasks[task_id].total_items = total_items
        processing_tasks[task_id].status = "processing"
        
        processed_items = 0
        calculated_values = {}
        
        # Process each portfolio type
        for portfolio_type in portfolio_types:
            items = portfolio_data.get(portfolio_type, [])
            calculated_values[portfolio_type] = []
            
            processing_tasks[task_id].current_step = f"Processing {portfolio_type}"
            await manager.send_personal_message(processing_tasks[task_id].dict(), user_id)
            
            for item in items:
                # Simulate processing time
                await asyncio.sleep(random.uniform(0.5, 1.5))
                
                # Calculate values based on type
                if portfolio_type == "stocks":
                    calculated_value = item["shares"] * item["current_price"]
                    calculated_values[portfolio_type].append({
                        **item,
                        "total_value": calculated_value,
                        "processed_at": datetime.now().isoformat()
                    })
                elif portfolio_type == "bonds":
                    calculated_value = item["amount"] * (1 + item["yield"] / 100)
                    calculated_values[portfolio_type].append({
                        **item,
                        "projected_value": calculated_value,
                        "processed_at": datetime.now().isoformat()
                    })
                elif portfolio_type == "crypto":
                    calculated_value = item["amount"] * item["current_price"]
                    calculated_values[portfolio_type].append({
                        **item,
                        "total_value": calculated_value,
                        "processed_at": datetime.now().isoformat()
                    })
                elif portfolio_type == "real_estate":
                    calculated_values[portfolio_type].append({
                        **item,
                        "current_value": item["value"],
                        "processed_at": datetime.now().isoformat()
                    })
                
                processed_items += 1
                progress_percentage = (processed_items / total_items) * 100
                
                # Update processing status
                processing_tasks[task_id].processed_items = processed_items
                processing_tasks[task_id].progress_percentage = progress_percentage
                
                # Send progress update
                await manager.send_personal_message(processing_tasks[task_id].dict(), user_id)
        
        # Calculate final summary
        total_portfolio_value = 0
        for ptype, items in calculated_values.items():
            if ptype == "stocks":
                total_portfolio_value += sum(item.get("total_value", 0) for item in items)
            elif ptype == "bonds":
                total_portfolio_value += sum(item.get("projected_value", 0) for item in items)
            elif ptype == "crypto":
                total_portfolio_value += sum(item.get("total_value", 0) for item in items)
            elif ptype == "real_estate":
                total_portfolio_value += sum(item.get("current_value", 0) for item in items)
        
        # Complete processing
        processing_tasks[task_id].status = "completed"
        processing_tasks[task_id].current_step = "Analysis completed"
        processing_tasks[task_id].data = {
            "portfolio_breakdown": calculated_values,
            "total_portfolio_value": total_portfolio_value,
            "processing_summary": {
                "total_items_processed": processed_items,
                "processing_time": f"{processed_items * 1.0:.1f} seconds",
                "completed_at": datetime.now().isoformat()
            }
        }
        
        await manager.send_personal_message(processing_tasks[task_id].dict(), user_id)
        
    except Exception as e:
        processing_tasks[task_id].status = "failed"
        processing_tasks[task_id].current_step = f"Error: {str(e)}"
        await manager.send_personal_message(processing_tasks[task_id].dict(), user_id)

# WebSocket endpoint
@app.websocket("/ws/{client_id}")
async def websocket_endpoint(websocket: WebSocket, client_id: str):
    await manager.connect(websocket, client_id)
    try:
        while True:
            data = await websocket.receive_text()
            message = json.loads(data)
            
            if message.get("type") == "ping":
                await manager.send_personal_message({"type": "pong", "timestamp": datetime.now().isoformat()}, client_id)
            elif message.get("type") == "get_status":
                task_id = message.get("task_id")
                if task_id in processing_tasks:
                    await manager.send_personal_message(processing_tasks[task_id].dict(), client_id)
                    
    except WebSocketDisconnect:
        manager.disconnect(client_id)

# REST API Endpoints
@app.post("/api/portfolio/analyze")
async def start_portfolio_analysis(request: PortfolioRequest):
    """Start portfolio analysis and return task ID for tracking progress"""
    task_id = str(uuid.uuid4())
    
    # Initialize processing task
    processing_tasks[task_id] = ProcessingStatus(
        task_id=task_id,
        user_id=request.user_id,
        total_items=0,
        processed_items=0,
        current_step="Initializing analysis",
        progress_percentage=0.0,
        status="initializing"
    )
    
    # Start background processing
    asyncio.create_task(process_portfolio_analysis(task_id, request.user_id, request.portfolio_types))
    
    return {
        "task_id": task_id,
        "status": "started",
        "message": "Portfolio analysis started. Connect to WebSocket for real-time updates.",
        "websocket_url": f"/ws/{request.user_id}"
    }

@app.get("/api/portfolio/status/{task_id}")
async def get_processing_status(task_id: str):
    """Get current processing status via REST API"""
    if task_id not in processing_tasks:
        raise HTTPException(status_code=404, detail="Task not found")
    
    return processing_tasks[task_id]

@app.get("/api/portfolio/result/{task_id}")
async def get_portfolio_result(task_id: str):
    """Get final portfolio analysis result"""
    if task_id not in processing_tasks:
        raise HTTPException(status_code=404, detail="Task not found")
    
    task = processing_tasks[task_id]
    if task.status != "completed":
        raise HTTPException(status_code=400, detail="Analysis not completed yet")
    
    return task.data

@app.get("/")
async def get_demo_page():
    """Serve the demo HTML page"""
    return HTMLResponse(content="""
    <!DOCTYPE html>
    <html>
    <head>
        <title>Portfolio Processing Demo</title>
        <style>
            body { font-family: Arial, sans-serif; max-width: 800px; margin: 0 auto; padding: 20px; }
            .container { border: 1px solid #ddd; padding: 20px; margin: 10px 0; border-radius: 5px; }
            .progress-bar { width: 100%; height: 20px; background-color: #f0f0f0; border-radius: 10px; overflow: hidden; }
            .progress-fill { height: 100%; background-color: #4CAF50; transition: width 0.3s ease; }
            .status { margin: 10px 0; padding: 10px; border-radius: 5px; }
            .processing { background-color: #fff3cd; border: 1px solid #ffeaa7; }
            .completed { background-color: #d4edda; border: 1px solid #c3e6cb; }
            .failed { background-color: #f8d7da; border: 1px solid #f5c6cb; }
            button { background-color: #007bff; color: white; padding: 10px 20px; border: none; border-radius: 5px; cursor: pointer; }
            button:hover { background-color: #0056b3; }
            button:disabled { background-color: #6c757d; cursor: not-allowed; }
            .result { background-color: #f8f9fa; padding: 15px; border-radius: 5px; margin-top: 20px; }
        </style>
    </head>
    <body>
        <h1>Portfolio Processing Demo</h1>
        
        <div class="container">
            <h3>Start Portfolio Analysis</h3>
            <label>User ID: <input type="text" id="userId" value="user123" /></label><br><br>
            <label>Portfolio Types:</label><br>
            <input type="checkbox" id="stocks" checked> Stocks<br>
            <input type="checkbox" id="bonds" checked> Bonds<br>
            <input type="checkbox" id="crypto" checked> Cryptocurrency<br>
            <input type="checkbox" id="real_estate" checked> Real Estate<br><br>
            <button onclick="startAnalysis()" id="startBtn">Start Analysis</button>
        </div>
        
        <div class="container" id="progressContainer" style="display:none;">
            <h3>Processing Progress</h3>
            <div class="progress-bar">
                <div class="progress-fill" id="progressFill" style="width: 0%;"></div>
            </div>
            <div id="progressText">0%</div>
            <div id="statusText" class="status">Ready to start...</div>
            <div id="currentStep"></div>
        </div>
        
        <div class="container" id="resultContainer" style="display:none;">
            <h3>Analysis Results</h3>
            <div id="resultContent" class="result"></div>
        </div>

        <script>
            let ws = null;
            let currentTaskId = null;
            
            function startAnalysis() {
                const userId = document.getElementById('userId').value;
                const portfolioTypes = [];
                
                if (document.getElementById('stocks').checked) portfolioTypes.push('stocks');
                if (document.getElementById('bonds').checked) portfolioTypes.push('bonds');
                if (document.getElementById('crypto').checked) portfolioTypes.push('crypto');
                if (document.getElementById('real_estate').checked) portfolioTypes.push('real_estate');
                
                if (portfolioTypes.length === 0) {
                    alert('Please select at least one portfolio type');
                    return;
                }
                
                document.getElementById('startBtn').disabled = true;
                document.getElementById('progressContainer').style.display = 'block';
                document.getElementById('resultContainer').style.display = 'none';
                
                // Connect WebSocket
                connectWebSocket(userId);
                
                // Start analysis
                fetch('/api/portfolio/analyze', {
                    method: 'POST',
                    headers: {'Content-Type': 'application/json'},
                    body: JSON.stringify({
                        user_id: userId,
                        portfolio_types: portfolioTypes,
                        calculation_type: 'full_analysis'
                    })
                })
                .then(response => response.json())
                .then(data => {
                    currentTaskId = data.task_id;
                    console.log('Analysis started:', data);
                });
            }
            
            function connectWebSocket(userId) {
                const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
                ws = new WebSocket(`${protocol}//${window.location.host}/ws/${userId}`);
                
                ws.onmessage = function(event) {
                    const data = JSON.parse(event.data);
                    updateProgress(data);
                };
                
                ws.onclose = function() {
                    console.log('WebSocket disconnected');
                };
                
                ws.onerror = function(error) {
                    console.error('WebSocket error:', error);
                };
            }
            
            function updateProgress(data) {
                const progressFill = document.getElementById('progressFill');
                const progressText = document.getElementById('progressText');
                const statusText = document.getElementById('statusText');
                const currentStep = document.getElementById('currentStep');
                
                progressFill.style.width = data.progress_percentage + '%';
                progressText.textContent = Math.round(data.progress_percentage) + '%';
                currentStep.textContent = `${data.current_step} (${data.processed_items}/${data.total_items})`;
                
                statusText.className = 'status ' + data.status;
                statusText.textContent = `Status: ${data.status}`;
                
                if (data.status === 'completed') {
                    document.getElementById('startBtn').disabled = false;
                    showResults(data.data);
                } else if (data.status === 'failed') {
                    document.getElementById('startBtn').disabled = false;
                    statusText.textContent = `Error: ${data.current_step}`;
                }
            }
            
            function showResults(data) {
                const resultContainer = document.getElementById('resultContainer');
                const resultContent = document.getElementById('resultContent');
                
                resultContent.innerHTML = `
                    <h4>Portfolio Analysis Complete</h4>
                    <p><strong>Total Portfolio Value:</strong> $${data.total_portfolio_value.toLocaleString()}</p>
                    <p><strong>Processing Summary:</strong></p>
                    <ul>
                        <li>Items Processed: ${data.processing_summary.total_items_processed}</li>
                        <li>Processing Time: ${data.processing_summary.processing_time}</li>
                        <li>Completed At: ${new Date(data.processing_summary.completed_at).toLocaleString()}</li>
                    </ul>
                    <details>
                        <summary>Detailed Breakdown</summary>
                        <pre>${JSON.stringify(data.portfolio_breakdown, null, 2)}</pre>
                    </details>
                `;
                
                resultContainer.style.display = 'block';
            }
        </script>
    </body>
    </html>
    """)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
