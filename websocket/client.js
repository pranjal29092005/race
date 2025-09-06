const WebSocket = require('ws');
const axios = require('axios');
const { v4: uuidv4 } = require('uuid');

class PortfolioWebSocketClient {
    constructor(baseUrl = 'http://localhost:8000') {
        this.baseUrl = baseUrl;
        this.ws = null;
        this.userId = `user_${uuidv4().substring(0, 8)}`;
    }

    // Connect to WebSocket
    connectWebSocket() {
        const wsUrl = this.baseUrl.replace('http', 'ws') + `/ws/${this.userId}`;
        console.log(`🔌 Connecting to WebSocket: ${wsUrl}`);
        
        this.ws = new WebSocket(wsUrl);
        
        this.ws.on('open', () => {
            console.log('✅ WebSocket connected successfully');
            
            // Send ping to test connection
            this.ws.send(JSON.stringify({ type: 'ping' }));
        });
        
        this.ws.on('message', (data) => {
            const message = JSON.parse(data.toString());
            this.handleWebSocketMessage(message);
        });
        
        this.ws.on('close', () => {
            console.log('❌ WebSocket connection closed');
        });
        
        this.ws.on('error', (error) => {
            console.error('🚨 WebSocket error:', error.message);
        });
    }

    // Handle incoming WebSocket messages
    handleWebSocketMessage(message) {
        if (message.type === 'pong') {
            console.log('🏓 Received pong from server');
            return;
        }

        // Handle progress updates
        if (message.task_id) {
            console.log(`📊 Progress Update:`);
            console.log(`   Task ID: ${message.task_id}`);
            console.log(`   Status: ${message.status}`);
            console.log(`   Progress: ${message.processed_items}/${message.total_items} (${message.progress_percentage.toFixed(1)}%)`);
            console.log(`   Current Step: ${message.current_step}`);
            
            if (message.status === 'completed' && message.data) {
                console.log(`🎉 Analysis Complete!`);
                console.log(`   Total Portfolio Value: $${message.data.total_portfolio_value.toLocaleString()}`);
                console.log(`   Processing Time: ${message.data.processing_summary.processing_time}`);
            } else if (message.status === 'failed') {
                console.log(`❌ Analysis Failed: ${message.current_step}`);
            }
            console.log('─'.repeat(50));
        }
    }

    // Start portfolio analysis
    async startPortfolioAnalysis(portfolioTypes = ['stocks', 'bonds', 'crypto', 'real_estate']) {
        try {
            console.log(`🚀 Starting portfolio analysis for user: ${this.userId}`);
            console.log(`📋 Portfolio types: ${portfolioTypes.join(', ')}`);
            
            const response = await axios.post(`${this.baseUrl}/api/portfolio/analyze`, {
                user_id: this.userId,
                portfolio_types: portfolioTypes,
                calculation_type: 'full_analysis'
            });
            
            console.log(`✅ Analysis started successfully`);
            console.log(`   Task ID: ${response.data.task_id}`);
            console.log(`   WebSocket URL: ${response.data.websocket_url}`);
            
            return response.data.task_id;
        } catch (error) {
            console.error('🚨 Failed to start analysis:', error.response?.data || error.message);
            throw error;
        }
    }

    // Get status via REST API
    async getTaskStatus(taskId) {
        try {
            const response = await axios.get(`${this.baseUrl}/api/portfolio/status/${taskId}`);
            return response.data;
        } catch (error) {
            console.error('🚨 Failed to get status:', error.response?.data || error.message);
            throw error;
        }
    }

    // Get final result via REST API
    async getTaskResult(taskId) {
        try {
            const response = await axios.get(`${this.baseUrl}/api/portfolio/result/${taskId}`);
            return response.data;
        } catch (error) {
            console.error('🚨 Failed to get result:', error.response?.data || error.message);
            throw error;
        }
    }

    // Disconnect WebSocket
    disconnect() {
        if (this.ws) {
            this.ws.close();
        }
    }
}

// Demo function
async function runDemo() {
    console.log('🎯 Portfolio WebSocket Client Demo');
    console.log('═'.repeat(50));
    
    const client = new PortfolioWebSocketClient();
    
    try {
        // Connect WebSocket first
        client.connectWebSocket();
        
        // Wait a bit for connection
        await new Promise(resolve => setTimeout(resolve, 1000));
        
        // Start analysis
        const taskId = await client.startPortfolioAnalysis(['stocks', 'bonds', 'crypto']);
        
        // Let the WebSocket handle real-time updates
        // You can also poll via REST API if needed
        setTimeout(async () => {
            try {
                const status = await client.getTaskStatus(taskId);
                console.log(`\n📊 REST API Status Check:`);
                console.log(`   Status: ${status.status}`);
                console.log(`   Progress: ${status.progress_percentage.toFixed(1)}%`);
            } catch (error) {
                // Task might not exist yet
            }
        }, 3000);
        
        // Disconnect after 30 seconds
        setTimeout(() => {
            console.log('\n🔌 Disconnecting client...');
            client.disconnect();
            process.exit(0);
        }, 30000);
        
    } catch (error) {
        console.error('🚨 Demo failed:', error.message);
        client.disconnect();
        process.exit(1);
    }
}

// Export for use in other files
module.exports = PortfolioWebSocketClient;

// Run demo if this file is executed directly
if (require.main === module) {
    runDemo();
}
